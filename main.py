import time
import os
import json
import requests
import pymysql
from fastapi import FastAPI, Request, BackgroundTasks

app = FastAPI()

# ================= 1. 环境变量配置 =================
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME", "test")

# ================= 2. 数据库操作工具 =================
def get_db_connection():
    """建立与 TiDB Serverless 的连接"""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

def load_history_from_db(chat_id, limit=40):
    """从数据库读取最近的记忆并正序排列"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 倒序取最新的 limit 条
            sql = "SELECT role, content FROM chat_records WHERE chat_id = %s ORDER BY id DESC LIMIT %s"
            cursor.execute(sql, (chat_id, limit))
            records = cursor.fetchall()
            
            history = []
            # 翻转回正序，供大模型阅读
            for row in reversed(records):
                history.append({
                    "role": row["role"],
                    "parts": [{"text": row["content"]}]
                })
            return history
    except Exception as e:
        print(f"读取数据库失败: {e}")
        return []
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

def save_message_to_db(chat_id, role, content):
    """保存单条消息到数据库"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = "INSERT INTO chat_records (chat_id, role, content) VALUES (%s, %s, %s)"
            cursor.execute(sql, (chat_id, role, content))
        conn.commit()
    except Exception as e:
        print(f"保存数据库失败: {e}")
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

# ================= 3. 飞书通信工具 =================
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

# ================= 4. 暴力文字提取器 =================
def extract_all_text(parsed_data):
    """不管飞书嵌套多少层，把所有隐藏的纯文字全部榨取出来"""
    text_list = []
    
    def traverse(node):
        if isinstance(node, dict):
            if "text" in node and isinstance(node["text"], str):
                text_list.append(node["text"])
            for key, value in node.items():
                if key != "text":
                    traverse(value)
        elif isinstance(node, list):
            for item in node:
                traverse(item)
            text_list.append("\n")

    traverse(parsed_data)
    return "".join(text_list).strip()

# ================= 5. 核心：大模型处理大脑 =================
def process_message(message_id, user_text, chat_id):
    """处理消息，包含数据库记忆及自动重试机制"""
    
    # 1. 从数据库捞出最近 20 条对话记忆
    history = load_history_from_db(chat_id, limit=20)
    
    # 2. 将当前用户的新问题，放进发给大模型的包裹里
    history.append({
        "role": "user",
        "parts": [{"text": user_text}]
    })

    # 3. 组装请求参数
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    payload = {
        "system_instruction": {
            "parts": [
                {
                    "text": "你的名字叫Xavier，你是一个资深的电商运营专家。在回答时，请务必遵循以下规则：1. 态度热情专业；2. 尽量使用清晰的序号、列表和加粗来排版；3. 对于复杂的运营策略，给出具有实操性的建议。"
                }
            ]
        },
        "contents": history
    }
    
    # 4. 请求 Gemini (带 3 次重试保护)
    max_retries = 3
    gemini_reply = ""
    
    for attempt in range(max_retries):
        try:
            # timeout 放宽到 40 秒，防止思考长文时断连
            response = requests.post(url, headers=headers, json=payload, timeout=40)
            
            if response.status_code == 200:
                result = response.json()
                gemini_reply = result['candidates'][0]['content']['parts'][0]['text']
                
                # 💡【核心逻辑优化】：只有大模型成功回复了，才把"用户的问题"和"大模型的回答"成对存入数据库！
                # 这样可以绝对避免 API 失败时，数据库里存了两个连续的 user 发言导致后续崩溃。
                save_message_to_db(chat_id, "user", user_text)
                save_message_to_db(chat_id, "model", gemini_reply)
                
                break  # 成功，跳出循环
                
            elif response.status_code in [503, 429]:
                print(f"⚠️ API 繁忙 (状态码: {response.status_code})，正在进行第 {attempt + 1} 次重试...")
                time.sleep(2)
                if attempt == max_retries - 1:
                    gemini_reply = "🤖 Google 服务器当前太拥挤啦，Xavier 试了3次都没挤进去通道，请稍等半分钟后再把刚才的话发给我一次哦！"
            else:
                gemini_reply = f"API报错: {response.status_code} - {response.text}"
                break
                
        except Exception as e:
            gemini_reply = f"网络连接异常: {str(e)}"
            break
            
    # 5. 回复飞书用户
    reply_feishu_message(message_id, gemini_reply)

# ================= 6. Webhook 路由 =================
@app.get("/")
async def root():
    return {"message": "Xavier 电商专家（数据库记忆版）已成功上线运行中！"}

@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    
    if "challenge" in data:
        return {"challenge": data["challenge"]}
    
    if "header" in data and "event" in data:
        if data["header"].get("event_type") == "im.message.receive_v1":
            message = data["event"]["message"]
            
            try:
                content_dict = json.loads(message["content"])
                # 开启吸尘器模式，提取长文中的所有文字
                user_text = extract_all_text(content_dict)
            except Exception:
                user_text = ""
                
            if user_text.strip():
                chat_id = message.get("chat_id")
                background_tasks.add_task(process_message, message["message_id"], user_text, chat_id)
                
    return {"status": "success"}
# ================= 7. 接收飞书多维表格数据的专属通道 (多表智能路由版) =================
@app.post("/bitable-sync")
async def receive_bitable_data(request: Request):
    try:
        # 1. 接收飞书推过来的所有数据
        data = await request.json()
        
        # 2. 提取“数据来源标签” (如果在飞书里没配这个字段，默认是 unknown)
        source_table = data.get("source_table", "unknown")
        
        # 3. 开启数据库连接
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # ------------------ 分拣逻辑开始 ------------------
        
        # 路由 A：如果是【昆仑系列销售数据】发来的
        if source_table == "kunlun_sales":
            # 提取专属字段
            date = data.get("date")
            model = data.get("model")
            sales = data.get("sales")
            # 存入 TiDB 里对应的 kunlun_sales_table (需提前在数据库建好这张表)
            sql = "INSERT INTO kunlun_sales_table (date, model, sales) VALUES (%s, %s, %s)"
            cursor.execute(sql, (date, model, sales))
            print(f"✅ 成功同步一条 [昆仑系列] 数据: {model} - {sales}")

        # 路由 B：如果是【品类GSV同比数据】发来的
        elif source_table == "category_gsv":
            # 提取专属字段（名字必须和飞书 JSON 里左边的名字一模一样）
            record_time = data.get("record_time")
            category = data.get("category")
            # 兼容处理：飞书传过来的金额可能是带逗号的字符串，比如 "1,000.00"
            # 去除逗号，防止数据库报错
            gsv = str(data.get("gsv", "0")).replace(",", "")
            last_year_gsv = str(data.get("last_year_gsv", "0")).replace(",", "")
            yoy_ratio = data.get("yoy_ratio")
            
            # 存入 TiDB 
            sql = "INSERT INTO category_gsv_table (record_time, category, gsv, last_year_gsv, yoy_ratio) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (record_time, category, gsv, last_year_gsv, yoy_ratio))
            
            print(f"✅ 成功同步一条 [GSV同比] 数据: {record_time} - {category}")
            
        # 路由 C：可以无限往下加...
        
        else:
            print(f"⚠️ 收到未知来源的数据，未执行入库: {data}")
            
        # ------------------ 分拣逻辑结束 ------------------

        # 提交并关闭数据库
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"status": "success", "message": f"Data routed for {source_table}"}
        
    except Exception as e:
        print(f"❌ 数据同步失败: {e}")
        return {"status": "error", "message": str(e)}
