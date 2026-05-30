import os
import json
import requests
from fastapi import FastAPI, Request, BackgroundTasks

app = FastAPI()

FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

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

def process_message(message_id, user_text):
    # 这里在后台慢慢请求 Gemini，完全不怕飞书超时
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    payload = {"contents": [{"parts": [{"text": user_text}]}]}
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            gemini_reply = response.json()['candidates'][0]['content']['parts'][0]['text']
        else:
            gemini_reply = f"API报错: {response.status_code}"
    except Exception as e:
        gemini_reply = f"网络错误: {str(e)}"
        
    reply_feishu_message(message_id, gemini_reply)

@app.get("/")
async def root():
    return {"message": "Hello! 飞书 Gemini 机器人已在 Render 完美上线！"}

@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    
    # 飞书网址验证
    if "challenge" in data:
        return {"challenge": data["challenge"]}
    
    if "header" in data and "event" in data:
        if data["header"].get("event_type") == "im.message.receive_v1":
            message = data["event"]["message"]
            if message["message_type"] == "text":
                user_text = json.loads(message["content"]).get("text", "")
                
                # ✨ 核心魔法：把耗时的任务扔给后台，0.1秒内立刻执行下方的 return
                background_tasks.add_task(process_message, message["message_id"], user_text)
                
    # 瞬间返回成功，飞书绝对不会再报超时！
    return {"status": "success"}
