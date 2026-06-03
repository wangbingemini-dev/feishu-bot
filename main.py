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

# ================= 2. 数据库底层基建 =================
def get_db_connection():
    # 增加 charset='utf8mb4' 彻底杜绝中文乱码问题
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

def load_history_from_db(chat_id, limit=15):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 建表前提：确保你有 chat_records 这张表用来存聊天记忆
            sql = "SELECT role, content FROM chat_records WHERE chat_id = %s ORDER BY id DESC LIMIT %s"
            cursor.execute(sql, (chat_id, limit))
            records = cursor.fetchall()
            return [{"role": row["role"], "parts": [{"text": row["content"]}]} for row in reversed(records)]
    except: return []

def save_message_to_db(chat_id, role, content):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = "INSERT INTO chat_records (chat_id, role, content) VALUES (%s, %s, %s)"
            cursor.execute(sql, (chat_id, role, content))
        conn.commit()
    except Exception as e: print(f"保存聊天记录失败: {e}")

# ================= 3. AI 数据导览与安全执行器 =================
def get_database_schema():
    """让大模型看一眼家里有多少矿（获取表名和列名）"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [list(row.values())[0] for row in cursor.fetchall()]
            
            schema_info = "【当前数据库结构】\n"
            for table in tables:
                if table == 'chat_records': continue  # 不给它看聊天记录表
                cursor.execute(f"DESCRIBE {table}")
                columns = [f"{row['Field']}({row['Type']})" for row in cursor.fetchall()]
                schema_info += f"- 表 {table}: 包含 {', '.join(columns)}\n"
            return schema_info
    except Exception as e: return f"获取数据库结构失败: {e}"

def execute_ai_sql(sql):
    """替 AI 跑腿查数据，并带上防删库手铐"""
    sql = sql.strip()
    if not sql.upper().startswith("SELECT"):
        return "⚠️ 操作拒绝：安全限制生效，你只能执行 SELECT 语句读取数据，禁止修改或删除。"
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            records = cursor.fetchall()
            # 如果查询结果超过40条，截断一下防止把 Gemini 内存撑爆
            if len(records) > 40: 
                return f"{records[:40]}\n...(提示AI: 数据行数超过40条，仅展示部分，请使用 SUM/AVG/LIMIT 等函数优化你的 SQL)"
            return str(records) if records else "查询执行成功，但结果为空（没有找到符合条件的数据）。"
    except Exception as e: 
        return f"❌ SQL执行报错: {e}。请检查你的 SQL 语法（例如字段名是否拼错）并重新思考。"

# ================= 4. 飞书通信基站 =================
def reply_feishu_message(message_id, text_content):
    try:
        url_token = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        token = requests.post(url_token, headers={"Content-Type": "application/json"}, json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}).json().get("tenant_access_token")
        if token:
            url_reply = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
            requests.post(url_reply, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"msg_type": "text", "content": json.dumps({"text": text_content})})
    except Exception as e: print(f"飞书消息发送失败: {e}")

def extract_all_text(parsed_data):
    text_list = []
    def traverse(node):
        if isinstance(node, dict):
            if "text" in node and isinstance(node["text"], str): text_list.append(node["text"])
            for k, v in node.items():
                if k != "text": traverse(v)
        elif isinstance(node, list):
            for item in node: traverse(item)
    traverse(parsed_data)
    return "".join(text_list).strip()

# ================= 5. AI 大脑核心中枢 (Text-to-SQL 引擎) =================
def call_gemini_api(payload):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    for _ in range(3):
        try:
            response = requests.post(url, headers={'Content-Type': 'application/json'}, json=payload, timeout=45)
            if response.status_code == 200:
                return response.json()['candidates'][0]['content']['parts'][0]['text']
            else:
                # 🌟 新增的高音喇叭：如果 Google 拒绝，把真实的错误原因打印在日志里！
                print(f"⚠️ Google 拒绝了请求！状态码: {response.status_code} | 报错详情: {response.text}")
                time.sleep(2)
        except Exception as e: 
            print(f"API 网络层波动: {e}")
            
    return "Xavier 的大脑正在高速运转，API 通道稍微有点拥堵，请半分钟后再问我一次！"

def process_message(message_id, user_text, chat_id):
    history = load_history_from_db(chat_id, limit=15)
    history.append({"role": "user", "parts": [{"text": user_text}]})
    
    db_schema = get_database_schema()
    
    # 🌟 针对 6 张表的终极 System Prompt 优化
    sys_instruction = f"""你的名字叫Xavier，是全渠道净水器/厨房电器的顶级电商数据参谋。
你的大脑已直连公司 TiDB 数据库，掌握着所有核心战报。以下是目前数据库的表结构：
{db_schema}

【查数指令规范 —— 极其重要！】
1. 回答任何涉及数据的业务问题，务必自己写 MySQL 语句去查询真实数据。禁止瞎编。
2. 📅 时间查询规则：数据库中的时间是极其标准的 DATE 格式（例如 '2026-04-01' 或 '2026-06-01'），请严格使用这种带横杠的 `YYYY-MM-DD` 格式进行 WHERE 筛选。
3. 💰 数值计算规则：表中的 GSV 等金额字段已经是纯数字格式（DOUBLE），无需去除逗号，直接使用 SUM() 等函数计算即可。
4. 🔍 全局视野：如果用户询问某一天的大盘或各品类对比，务必使用 GROUP BY 品类，把该日期下所有存在的品类（如冰箱、厨电、净水器等）全部汇总出来，不要遗漏。
5. 输出 SQL 时，必须且只能用以下格式包裹：
[SQL]
SELECT * FROM table_name LIMIT 10;
[/SQL]
6. 系统执行后会把真实结果喂给你。拿到结果后，请用极具商业洞察的视角、分点、加粗的 Markdown 格式输出你的最终分析。
"""
    
    payload = {"system_instruction": {"parts": [{"text": sys_instruction}]}, "contents": history}
    
    # 第 1 步：尝试让大模型思考，看它是否发出 SQL 查询暗号
    gemini_reply = call_gemini_api(payload)
    
    # 第 2 步：智能拦截器
    if "[SQL]" in gemini_reply and "[/SQL]" in gemini_reply:
        sql_match = re.search(r'\[SQL\](.*?)\[/SQL\]', gemini_reply, re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()
            print(f"🤖 AI 生成了查询指令: {sql_query}")
            
            # 去数据库拿货
            db_data = execute_ai_sql(sql_query)
            
            # 将数据库返回的冷冰冰的数据，作为新的上下文补充给 AI
            history.append({"role": "model", "parts": [{"text": gemini_reply}]})
            history.append({"role": "user", "parts": [{"text": f"系统已执行你的SQL，数据库返回的真实数据如下:\n{db_data}\n\n请严格基于这些数据，用专业的排版直接回答我最初的业务问题。"}]})
            
            payload["contents"] = history
            # 发起第 2 次 API 调用，获取人类能看懂的漂亮分析
            gemini_reply = call_gemini_api(payload)

    # 第 3 步：沉淀记忆与回复飞书
    if "通道稍微有点拥堵" not in gemini_reply:
        save_message_to_db(chat_id, "user", user_text)
        save_message_to_db(chat_id, "model", gemini_reply)
        
    reply_feishu_message(message_id, gemini_reply)

# ================= 6. 飞书消息入口 =================
@app.get("/")
async def root(): return {"message": "Xavier AI (百万级BI引擎版) 已全功率运行"}

@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    if "challenge" in data: return {"challenge": data["challenge"]}
    if "header" in data and data["header"].get("event_type") == "im.message.receive_v1":
        msg = data["event"]["message"]
        try: user_text = extract_all_text(json.loads(msg["content"]))
        except: user_text = ""
        if user_text.strip():
            background_tasks.add_task(process_message, msg["message_id"], user_text, msg.get("chat_id"))
    return {"status": "success"}

# ================= 7. 飞书多维表格【增量更新】通道 =================
@app.post("/bitable-sync")
async def receive_bitable_data(request: Request):
    """
    不管飞书发来的是日爆表还是月度表，只要JSON里有 table_name，这里就能智能匹配入库。
    """
    try:
        data = await request.json()
        table_name = data.pop("table_name", None)
        if not table_name: return {"status": "error", "message": "缺失 table_name 字段"}
            
        cleaned_values = []
        for v in data.values():
            if v is None or str(v).strip() == "":
                cleaned_values.append(None)
            else:
                cleaned_values.append(str(v).replace(",", "")) # 去除逗号
                
        keys = list(data.keys())
        columns_str = ", ".join(keys)
        placeholders_str = ", ".join(["%s"] * len(keys))
        
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str})"
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql, tuple(cleaned_values))
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
