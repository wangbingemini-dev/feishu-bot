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

# ================= 新增：暴力递归文字提取器 =================
def extract_all_text(parsed_data):
    """不管飞书嵌套多少层，把所有隐藏的纯文字全部榨取出来"""
    text_list = []
    
    def traverse(node):
        if isinstance(node, dict):
            # 只要节点里有 'text' 这个键，并且是字符串，就全部抓取
            if "text" in node and isinstance(node["text"], str):
                text_list.append(node["text"])
            # 继续往下层翻找
            for key, value in node.items():
                if key != "text":  # 避免重复
                    traverse(value)
        elif isinstance(node, list):
            for item in node:
                traverse(item)
            # 一个列表通常代表一个段落结束，加个换行符保证排版不乱
            text_list.append("\n")

    traverse(parsed_data)
    return "".join(text_list).strip()
# =========================================================

@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    
    # 飞书网址验证
    if "challenge" in data:
        return {"challenge": data["challenge"]}
    
    if "header" in data and "event" in data:
        if data["header"].get("event_type") == "im.message.receive_v1":
            message = data["event"]["message"]
            
            try:
                # 把飞书发来的加密内容解开
                content_dict = json.loads(message["content"])
                # 核心改变：直接丢进暴力提取器，无视所有排版格式！
                user_text = extract_all_text(content_dict)
            except Exception as e:
                user_text = ""
                
            # 只要成功提取到了哪怕一个字，就派发给大脑处理
            if user_text.strip():
                chat_id = message.get("chat_id")
                background_tasks.add_task(process_message, message["message_id"], user_text, chat_id)
                
    return {"status": "success"}
