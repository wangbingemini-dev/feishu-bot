import os
import json
import requests
from fastapi import FastAPI, Request

app = FastAPI()

FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# 在内存中记录已经处理完成的事件 ID，防止重复回复
processed_events = set()

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    data = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    try:
        response = requests.post(url, headers=headers, json=data)
        return response.json().get("tenant_access_token")
    except Exception:
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

@app.get("/")
async def root():
    return {"message": "Hello! 飞书 Gemini 机器人已成功上线运行中！"}

@app.post("/webhook")
async def feishu_webhook(request: Request):
    data = await request.json()
    
    # 1. 响应飞书的网址验证
    if "challenge" in data:
        return {"challenge": data["challenge"]}
    
    if "header" in data and "event" in data:
        header = data["header"]
        event = data["event"]
        event_id = header.get("event_id")
        
        # 2. 检查这个事件是否已经处理过（防重复）
        if event_id in processed_events:
            print(f"⚠️ 检测到飞书重试请求，事件 {event_id} 已在处理中或已结束，直接忽略。")
            return {"status": "ignored"}
        
        if header.get("event_type") == "im.message.receive_v1":
            message = event["message"]
            if message["message_type"] == "text":
                content_dict = json.loads(message["content"])
                user_text = content_dict.get("text", "")
                message_id = message["message_id"]
                
                # 3. 在本次请求中同步等待 Gemini（让 Vercel 进程保持清醒）
                url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
                headers = {'Content-Type': 'application/json'}
                payload = {"contents": [{"parts": [{"text": user_text}]}]}
                
                try:
                    # 如果这里耗时 4 秒，飞书的第一轮 3 秒会提示超时，并立刻发起第二轮重试
                    # 此时第二轮重试进入 Webhook，会被上面的 processed_events 拦截，不干扰这个正在运行的请求
                    response = requests.post(url, headers=headers, json=payload, timeout=10)
                    if response.status_code == 200:
                        result = response.json()
                        gemini_reply = result['candidates'][0]['content']['parts'][0]['text']
                    else:
                        gemini_reply = f"Gemini 接口返回错误码: {response.status_code}"
                except Exception as e:
                    gemini_reply = f"请求 Gemini 超时或失败: {str(e)}"
                
                # 4. 成功拿到结果，发送给飞书
                reply_feishu_message(message_id, gemini_reply)
                
                # 5. 将该事件记录到已处理集合中
                processed_events.add(event_id)
                
                # 保持集合大小，防止内存溢出（只保留最近 100 个）
                if len(processed_events) > 100:
                    processed_events.pop()
                    
    return {"status": "success"}
