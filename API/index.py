import os
import json
import requests
from fastapi import FastAPI, Request, BackgroundTasks

app = FastAPI()

# 从 Vercel 环境变量中读取密钥
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

def get_tenant_access_token():
    """获取飞书 Token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    data = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    response = requests.post(url, headers=headers, json=data)
    return response.json().get("tenant_access_token")

def reply_feishu_message(message_id, text_content):
    """发送回复给飞书"""
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

def process_message(message_id, user_text):
    """请求 Gemini 1.5 Flash 并回复"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    data = {"contents": [{"parts": [{"text": user_text}]}]}
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            result = response.json()
            gemini_reply = result['candidates'][0]['content']['parts'][0]['text']
        else:
            gemini_reply = f"Gemini 接口报错了: 状态码 {response.status_code}"
    except Exception as e:
        gemini_reply = f"网络遇到了问题: {str(e)}"
        
    reply_feishu_message(message_id, gemini_reply)

@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    """接收飞书消息的路由"""
    data = await request.json()
    
    # 飞书网址验证
    if "challenge" in data:
        return {"challenge": data["challenge"]}
    
    # 处理用户消息
    if "header" in data and "event" in data:
        event = data["event"]
        if data["header"]["event_type"] == "im.message.receive_v1":
            message = event["message"]
            if message["message_type"] == "text":
                content_dict = json.loads(message["content"])
                user_text = content_dict.get("text", "")
                
                # 在后台处理请求，防止飞书超时重试
                background_tasks.add_task(process_message, message["message_id"], user_text)
                
    return {"status": "success"}
