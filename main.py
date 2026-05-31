import os
import json
import requests
from fastapi import FastAPI, Request, BackgroundTasks

app = FastAPI()

FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# ================= 新增：记忆大脑 =================
# 用于存储不同聊天框的上下文历史
# 格式: { "chat_id": [ {"role": "user", "parts": [...]}, {"role": "model", "parts": [...]} ] }
chat_history = {}
MAX_HISTORY_LENGTH = 20  # 最多记住最近的20个对话轮次，防止长文本撑爆内存
# =================================================

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    data = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    try:
        response = requests.post(url, headers=headers, json=data)
        return response.json().get("tenant_access_token")
    except:
        return None

def reply_feishu_message(message_id, text_content):
    token = get_tenant_access_token()
    if not token: 
        return
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    payload = {"msg_type": "text", "content": json.dumps({"text": text_content})}
    requests.post(url, headers=headers, json=payload)

def process_message(message_id, user_text, chat_id):
    """处理消息，包含上下文记忆"""
    global chat_history
    
    # 1. 获取该聊天的历史记录，如果没有则新建空列表
    history = chat_history.get(chat_id, [])
    
    # 2. 将用户的新问题加入历史记录
    history.append({
        "role": "user",
        "parts": [{"text": user_text}]
    })
    
    # 3. 截断历史记录，防止携带的文字太多导致 API 报错或变慢
    if len(history) > MAX_HISTORY_LENGTH:
        history = history[-MAX_HISTORY_LENGTH:]

    # 4. 请求 Gemini (建议在这里使用 2.5-flash，若需深度推理可换 2.5-pro)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    
    # 💡 核心改变：以前只发一条 text，现在把整个 history 数组发给 Google
    payload = {"contents": history}
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            result = response.json()
            gemini_reply = result['candidates'][0]['content']['parts'][0]['text']
            
            # 5. 如果请求成功，把机器人的回复也存进记忆里
            history.append({
                "role": "model",
                "parts": [{"text": gemini_reply}]
            })
            # 更新大脑缓存
            chat_history[chat_id] = history
            
        else:
            # 报错时，为了防止死循环，把刚刚加进去的用户问题弹出来
            history.pop() 
            gemini_reply = f"API报错: {response.status_code} - {response.text}"
            
    except Exception as e:
        if history: history.pop()
        gemini_reply = f"网络错误: {str(e)}"
        
    reply_feishu_message(message_id, gemini_reply)

@app.get("/")
async def root():
    return {"message": "飞书 Gemini 机器人已开启记忆功能！"}

@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    
    if "challenge" in data:
        return {"challenge": data["challenge"]}
    
    if "header" in data and "event" in data:
        if data["header"].get("event_type") == "im.message.receive_v1":
            message = data["event"]["message"]
            
            # 只处理普通文本消息
            if message["message_type"] == "text":
                user_text = json.loads(message["content"]).get("text", "")
                
                # 💡 获取这段对话的唯一 ID (单聊或群聊)
                chat_id = message.get("chat_id")
                
                # 将 chat_id 传给后台任务
                background_tasks.add_task(process_message, message["message_id"], user_text, chat_id)
                
    return {"status": "success"}
