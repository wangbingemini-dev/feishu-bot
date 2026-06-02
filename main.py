import time
import os
import json
import requests
import pymysql
import re
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
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

def load_history_from_db(chat_id, limit=20):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = "SELECT role, content FROM chat_records WHERE chat_id = %s ORDER BY id DESC LIMIT %s"
            cursor.execute(sql, (chat_id, limit))
            records = cursor.fetchall()
            return [{"role": row["role"], "parts": [{"text": row["content"]}]} for row in reversed(records)]
    except:
        return []
    finally:
        if 'conn' in locals() and conn.open: conn.close()

def save_message_to_db(chat_id, role, content):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = "INSERT INTO chat_records (chat_id, role, content) VALUES (%s, %s, %s)"
            cursor.execute(sql, (chat_id, role, content))
        conn.commit()
    except Exception as e:
        print(f"保存记录失败: {e}")
    finally:
        if 'conn' in locals() and conn.open: conn.close()

# ================= 3. 🌟 新增：数据库超能力 =================
def get_database_schema():
    """动态获取当前数据库中所有的表名和字段，告诉大模型全库结构"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [list(row.values())[0] for row in cursor.fetchall()]
            if not tables:
                return "当前数据库没有任何表。"
            
            schema_info = "【你的数据库表结构说明】\n"
            for table in tables:
                # 排除用来存聊天记录的表，防止它拿聊天记录胡说八道
                if table == 'chat_records': 
                    continue
                cursor.execute(f"DESCRIBE {table}")
                columns = [f"{row['Field']}({row['Type']})" for row in cursor.fetchall()]
                schema_info += f"表名: {table} | 包含字段: {', '.join(columns)}\n"
            return schema_info
    except Exception as e:
        return f"获取结构失败: {e}"
    finally:
        if 'conn' in locals() and conn.open: conn.close()

def execute_ai_sql(sql):
    """执行大模型自己写的 SQL 语句，并配备安全锁"""
    sql = sql.strip()
    # 🚨 终极安全锁：绝对禁止 AI 执行删除(DELETE)或修改(UPDATE)等危险操作！
    if not sql.upper().startswith("SELECT"):
        return "操作被拒绝：出于数据安全考虑，你只能执行 SELECT 查询语句。"
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            records = cursor.fetchall()
            # 截断结果防止过大撑爆 API (最多给 AI 看最新的 30 条)
            if len(records) > 30:
                return f"{records[:30]}\n...(共查询到 {len(records)} 条数据，其余已隐藏)"
            return str(records)
    except Exception as e:
        return f"SQL执行报错: {e}，请检查你的 SQL 语法后重试。"
    finally:
        if 'conn' in locals() and conn.open: conn.close()

# ================= 4. 飞书通信与文字提取工具 =================
def get_tenant_access_token():
    try:
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        data = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
        return requests.post(url, headers={"Content-Type": "application/json"}, json=data).json().get("tenant_access_token")
    except: return None

def reply_feishu_message(message_id, text_content):
    token = get_tenant_access_token()
    if token:
        url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
        payload = {"msg_type": "text", "content": json.dumps({"text": text_content})}
        requests.post(url, headers=headers, json=payload)

def extract_all_text(parsed_data):
    text_list = []
    def traverse(node):
        if isinstance(node, dict):
            if "text" in node and isinstance(node["text"], str): text_list.append(node["text"])
            for k, v in node.items():
                if k != "text": traverse(v)
        elif isinstance(node, list):
            for item in node: traverse(item)
            text_list.append("\n")
    traverse(parsed_data)
    return "".join(text_list).strip()

# ================= 5. 核心：大模型智能体引擎 =================
def call_gemini_api(payload):
    """专门负责向 Gemini 发送请求并处理重试的网络组件"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    for _ in range(3):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=40)
            if response.status_code == 200:
                return response.json()['candidates'][0]['content']['parts'][0]['text']
            elif response.status_code in [503, 429]:
                time.sleep(2)
        except Exception as e:
            print(f"API 请求波动: {e}")
    return "🤖 Google 服务器当前太拥挤啦，Xavier 试了 3 次都没进去，请稍等半分钟再试哦！"

def process_message(message_id, user_text, chat_id):
    history = load_history_from_db(chat_id, limit=20)
    history.append({"role": "user", "parts": [{"text": user_text}]})
    
    # 动态获取全库表结构，注入灵魂
    db_schema = get_database_schema()
    sys_instruction = f"""你的名字叫Xavier，你是一个资深的电商运营专家。
你的大脑已直连公司的 TiDB 云数据库。以下是你目前可以随时查询的所有表结构：
{db_schema}

【核心指令：如何调取真实数据】
1. 当你需要结合真实数据来回答问题时，请务必自己编写 MySQL SQL 语句去查询。
2. 如果你要查数据，请严格按照以下格式输出你的请求（必须使用 [SQL] 和 [/SQL] 包裹，且不要输出任何多余废话）：
[SQL]
SELECT * FROM table_name ORDER BY id DESC LIMIT 10;
[/SQL]
3. 系统会拦截这段 SQL 并去数据库执行，然后把真实数字返回给你。拿到数字后，你再给用户专业的最终解答。
4. 如果用户只是闲聊，不需要查数据，请直接正常回答。
"""
    
    payload = {
        "system_instruction": {"parts": [{"text": sys_instruction}]},
        "contents": history
    }
    
    # 第 1 步：尝试获取大模型初步思考结果
    gemini_reply = call_gemini_api(payload)
    
    # 第 2 步：拦截器判断 - 大模型是不是想查数据库？
    if "[SQL]" in gemini_reply and "[/SQL]" in gemini_reply:
        print(f"🤖 AI 决定查询数据库，原始输出: {gemini_reply}")
        
        # 提取 SQL 语句
        sql_match = re.search(r'\[SQL\](.*?)\[/SQL\]', gemini_reply, re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()
            
            # 去 TiDB 执行查询
            db_data = execute_ai_sql(sql_query)
            print(f"📊 数据库执行结果已返回给 AI: {db_data[:100]}...")
            
            # 把“AI的查询动作”和“数据库的结果”放入临时上下文中
            history.append({"role": "model", "parts": [{"text": gemini_reply}]})
            history.append({"role": "user", "parts": [{"text": f"系统已成功执行你的SQL。查询结果如下:\n{db_data}\n\n请严格基于以上真实数据，回答我最初的问题。"}]})
            
            # 拿着带有结果的数据，发起第 2 次请求，获取最终答案
            payload["contents"] = history
            gemini_reply = call_gemini_api(payload)

    # 第 3 步：把最终对话结果存入永久记忆库，并回复飞书
    # (注意：中间查 SQL 的思考过程不会存进数据库污染记录，只存一问一答)
    if "Google 服务器当前太拥挤啦" not in gemini_reply:
        save_message_to_db(chat_id, "user", user_text)
        save_message_to_db(chat_id, "model", gemini_reply)
        
    reply_feishu_message(message_id, gemini_reply)

# ================= 6. Webhook 路由 =================
@app.get("/")
async def root():
    return {"message": "Xavier AI Agent (动态全库检索版) 已上线！"}

@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    if "challenge" in data: return {"challenge": data["challenge"]}
    if "header" in data and data["header"].get("event_type") == "im.message.receive_v1":
        message = data["event"]["message"]
        try:
            content_dict = json.loads(message["content"])
            user_text = extract_all_text(content_dict)
        except Exception: user_text = ""
        if user_text.strip():
            background_tasks.add_task(process_message, message["message_id"], user_text, message.get("chat_id"))
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
