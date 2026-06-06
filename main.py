import time
import os
import json
import requests
import pymysql
import re
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Request, BackgroundTasks

app = FastAPI()

# ================= 1. 核心钥匙与环境变量 =================
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
SILICONFLOW_API_KEY = os.environ.get("SILICONFLOW_API_KEY")

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME", "test")

WIKI_TOKEN = "Re61wxHP9iO5NFk6IshckxNYnKc"

history_memory = {}
processed_message_ids = set()

# ================= 2. 飞书数据地图 =================
TABLE_CONFIGS = [
    {"table_id": "tblWWoVwjP9l1xIG", "db_table": "daily_category_gsv", "mapping": {"时间": "时间", "店铺": "店铺", "品类": "品类", "GSV": "GSV", "同期GSV": "同期GSV", "同比": "同比", "目标": "目标", "目标达成率": "目标达成率"}},
    {"table_id": "tbl6yvd1FSN5atno", "db_table": "category_gsv_data", "mapping": {"时间": "时间", "品类": "品类", "GSV": "GSV", "同期GSV": "同期GSV", "同比": "同比"}},
    {"table_id": "tbllTcE3CS2FdN5b", "db_table": "month_gsv_data", "mapping": {"月份": "月份", "店铺": "店铺", "品类": "品类", "目标": "目标", "合计": "合计", "1日": "1日", "2日": "2日", "3日": "3日", "4日": "4日", "5日": "5日", "6日": "6日", "7日": "7日", "8日": "8日", "9日": "9日", "10日": "10日", "11日": "11日", "12日": "12日", "13日": "13日", "14日": "14日", "15日": "15日", "16日": "16日", "17日": "17日", "18日": "18日", "19日": "19日", "20日": "20日", "21日": "21日", "22日": "22日", "23日": "23日", "24日": "24日", "25日": "25日", "26日": "26日", "27日": "27日", "28日": "28日", "29日": "29日", "30日": "30日", "31日": "31日"}},
    {"table_id": "tbl2MaSswe4Osoou", "db_table": "kunlun_sales", "mapping": {"月份": "月份", "店铺": "店铺", "商品id": "商品id", "产品名称": "产品名称", "目标": "目标", "达成率": "达成率", "合计": "合计", "1日": "1日", "2日": "2日", "3日": "3日", "4日": "4日", "5日": "5日", "6日": "6日", "7日": "7日", "8日": "8日", "9日": "9日", "10日": "10日", "11日": "11日", "12日": "12日", "13日": "13日", "14日": "14日", "15日": "15日", "16日": "16日", "17日": "17日", "18日": "18日", "19日": "19日", "20日": "20日", "21日": "21日", "22日": "22日", "23日": "23日", "24日": "24日", "25日": "25日", "26日": "26日", "27日": "27日", "28日": "28日", "29日": "29日", "30日": "30日", "31日": "31日"}},
    {"table_id": "tbl4ehsa3xc0z5nq", "db_table": "dragons1_sales", "mapping": {"月份": "月份", "店铺": "店铺", "商品id": "商品id", "产品名称": "产品名称", "目标": "目标", "达成率": "达成率", "合计": "合计", "1日": "1日", "2日": "2日", "3日": "3日", "4日": "4日", "5日": "5日", "6日": "6日", "7日": "7日", "8日": "8日", "9日": "9日", "10日": "10日", "11日": "11日", "12日": "12日", "13日": "13日", "14日": "14日", "15日": "15日", "16日": "16日", "17日": "17日", "18日": "18日", "19日": "19日", "20日": "20日", "21日": "21日", "22日": "22日", "23日": "23日", "24日": "24日", "25日": "25日", "26日": "26日", "27日": "27日", "28日": "28日", "29日": "29日", "30日": "30日", "31日": "31日"}},
]

# ================= 3. 底层基础组件 =================
def get_db_connection():
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    res = requests.post(url, headers={"Content-Type": "application/json"}, json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}).json()
    return res.get("tenant_access_token")

# ================= 4. 记忆系统与知识解析 =================
def init_chat_memory_db():
    """初始化群聊记忆数据库"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS chat_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chat_id VARCHAR(100),
                sender_id VARCHAR(100),
                message_text TEXT,
                create_time DATETIME
            )
            """)
        conn.commit()
        print("✅ 群聊记忆数据库 (chat_records) 已准备就绪！")
    except Exception as e:
        print(f"创建记忆表失败: {e}")
    finally:
        conn.close()

@app.on_event("startup")
def on_startup():
    init_chat_memory_db()

def save_chat_history(chat_id, sender_id, text):
    """保存群聊记录"""
    clean_text = re.sub(r'@_user_\w+', '', text).strip()
    if not clean_text: return
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO chat_records (chat_id, sender_id, message_text, create_time) VALUES (%s, %s, %s, %s)"
            now_str = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(sql, (chat_id, sender_id, clean_text, now_str))
        conn.commit()
    except Exception as e:
        print(f"记忆保存失败: {e}")
    finally:
        conn.close()

def read_feishu_docx(doc_id):
    """专门读取飞书新版 Docx 文档的提取器"""
    tenant_token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{doc_id}/blocks"
    headers = {"Authorization": f"Bearer {tenant_token}", "Content-Type": "application/json"}
    full_text = ""
    page_token = ""
    has_more = True
    try:
        while has_more:
            req_url = url + f"?page_token={page_token}" if page_token else url
            response = requests.get(req_url, headers=headers).json()
            if response.get("code") != 0:
                print(f"❌ 读取文档失败: {response}")
                return "抱歉，我无法读取这篇文档，请确认文档是否已向我开放阅读权限。"
            items = response.get("data", {}).get("items", [])
            for block in items:
                block_type = block.get("block_type")
                if block_type in [1, 2, 3, 4, 5, 6, 7, 8, 9]: 
                    for element in block.get("text", {}).get("elements", []):
                        full_text += element.get("text_run", {}).get("content", "")
                    full_text += "\n"
            has_more = response.get("data", {}).get("has_more", False)
            page_token = response.get("data", {}).get("page_token", "")
        return full_text.strip()
    except Exception as e:
        return f"读取文档时发生物理异常: {e}"

# ================= 5. 数据自动同步模块 =================
def get_real_app_token(wiki_token, token):
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token={wiki_token}"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"}).json()
    if res.get("code") == 0: return res["data"]["node"]["obj_token"]
    return wiki_token

def clean_feishu_value(v):
    if v is None or v == "": return None
    if isinstance(v, list) and len(v) > 0:
        if isinstance(v[0], dict): v = v[0].get("value", v[0].get("text", v[0]))
        else: v = v[0]
    if isinstance(v, dict): v = v.get("value", v.get("text", v))
    if isinstance(v, (int, float)):
        if v > 1000000000000:
            dt = datetime.fromtimestamp(v / 1000.0, timezone.utc) + timedelta(hours=8)
            return dt.strftime('%Y-%m-%d')
        return float(v)
    if isinstance(v, str): 
        cleaned = v.replace(',', '').replace('¥', '').replace('￥', '').replace('\xa0', '').strip()
        if not cleaned: return None
        try: return float(cleaned)
        except ValueError: return cleaned 
    return str(v)

def run_full_sync():
    print("🚀 [后台任务] 开始执行全量数据主动拉取...")
    tenant_token = get_tenant_access_token()
    if not tenant_token: return
    app_token = get_real_app_token(WIKI_TOKEN, tenant_token)
    conn = get_db_connection()
    
    for config in TABLE_CONFIGS:
        table_id = config["table_id"]
        if "REPLACE" in table_id: continue
        db_table = config["db_table"]
        mapping = config["mapping"]
        records = []
        has_more = True
        page_token = ""
        
        while has_more:
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records?page_size=500"
            if page_token: url += f"&page_token={page_token}"
            res = requests.get(url, headers={"Authorization": f"Bearer {tenant_token}"}).json()
            if res.get("code") != 0: break
            for item in res.get("data", {}).get("items", []): records.append(item.get("fields", {}))
            has_more = res.get("data", {}).get("has_more", False)
            page_token = res.get("data", {}).get("page_token", "")
            
        if not records: continue
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {db_table}")
                cols = [f"`{col}`" for col in mapping.values()]
                cols_str = ", ".join(cols)
                placeholders = ", ".join(["%s"] * len(cols))
                sql = f"INSERT INTO {db_table} ({cols_str}) VALUES ({placeholders})"
                
                insert_data = []
                for rec in records:
                    row = [clean_feishu_value(rec.get(feishu_col)) for feishu_col in mapping.keys()]
                    insert_data.append(tuple(row))
                cursor.executemany(sql, insert_data)
            conn.commit()
            print(f"✅ [{db_table}] 同步成功！")
        except Exception as e: 
            print(f"❌ [{db_table}] 写入失败: {e}")
    conn.close()

# 🌟 加入 HEAD 方法，完美兼容 UptimeRobot 免费版
@app.api_route("/force-sync", methods=["GET", "POST", "HEAD"])
async def manual_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_full_sync)
    return {"status": "success", "message": "全量拉取同步已在后台启动！"}

# ================= 6. Xavier AI 大脑中枢 =================
def get_database_schema():
    conn = get_db_connection()
    schema_info = ""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [list(row.values())[0] for row in cursor.fetchall()]
            for table in tables:
                cursor.execute(f"SHOW COLUMNS FROM {table}")
                cols = [row['Field'] for row in cursor.fetchall()]
                schema_info += f"表名: {table}, 字段: {', '.join(cols)}\n"
    except Exception as e:
        print(f"获取表结构失败: {e}")
    finally:
        conn.close()
    return schema_info

def execute_ai_sql(sql_query):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            return str(result)
    except Exception as e:
        return f"SQL执行报错: {e}"
    finally:
        conn.close()

def reply_feishu_message(message_id, text):
    tenant_token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
    payload = {"msg_type": "text", "content": json.dumps({"text": text})}
    requests.post(url, headers={"Authorization": f"Bearer {tenant_token}", "Content-Type": "application/json"}, json=payload)

def call_ai_api(sys_instruction, history):
    url = "https://api.siliconflow.cn/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SILICONFLOW_API_KEY}"
    }
    messages = [{"role": "system", "content": sys_instruction}]
    for msg in history:
        role = "assistant" if msg["role"] == "model" else "user"
        content = msg["parts"][0]["text"]
        messages.append({"role": role, "content": content})

    payload = {
        # 推荐使用 V3 以保证最高稳定性。若需最强逻辑且不怕拥堵，可改为 deepseek-ai/DeepSeek-V4-Pro
        "model": "deepseek-ai/DeepSeek-V3", 
        "messages": messages,
        "temperature": 0.1 
    }

    for _ in range(3):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            if response.status_code == 200:
                return response.json()['choices'][0]['message']['content']
            else:
                time.sleep(2)
        except Exception as e: 
            pass
    return "Xavier 的大脑正在高速运转，API 通道稍微拥堵，请尝试拆分您的提问！"

def process_message(message_id, user_text, chat_id):
    if chat_id not in history_memory:
        history_memory[chat_id] = []
    
    # 🌟 嗅探飞书文档链接并自动阅读
    docx_match = re.search(r'https://[a-zA-Z0-9-]+\.feishu\.cn/docx/([a-zA-Z0-9]+)', user_text)
    if docx_match:
        doc_id = docx_match.group(1)
        print(f"📖 嗅探到飞书文档，正在自动抓取内容，文档ID: {doc_id}")
        doc_content = read_feishu_docx(doc_id)
        if "抱歉，我无法读取" not in doc_content:
            user_text += f"\n\n【系统附加知识】用户发送了飞书文档，我已替你提取内容如下，请作为重要背景知识：\n---\n{doc_content}\n---"
        else:
            user_text += f"\n\n【系统附加知识】读取文档失败，请提醒用户去文档右上角赋予你阅读权限。"

    history = history_memory[chat_id][-10:]
    history.append({"role": "user", "parts": [{"text": user_text}]})
    
    db_schema = get_database_schema()
    today_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    
    # 🌟 终极版反幻觉与宽表提示词
    sys_instruction = f"""你的名字叫Xavier，是水净化行业电商数据参谋。
直连 TiDB 数据库。当前北京时间：{today_date}。表中的金额（如GSV）已是纯数字(DOUBLE)，直接计算。

当前表结构：
{db_schema}

【🔥 业务字典与防错铁律】
1. 【大盘】：问总额、各品类销售，优先查 `daily_category_gsv` 或 `category_gsv_data`。
2. 【特定系列（⚠️宽表防空值跨月法则）】：
   - 昆仑或小京龙必须查 `kunlun_sales` 或 `dragons1_sales`。
   - 它们是宽表，`月份` 严格为 `YYYY年MM月`（如 '2026年06月'）。每天对应 `1日`...`31日` 列（必须用反引号保护，如 `1日`）。
   - 单日查询示例：SELECT IFNULL(SUM(`5日`), 0) FROM kunlun_sales WHERE 月份 = '2026年06月'
   - 跨月查询示例：SELECT SUM(CASE WHEN 月份='2026年05月' THEN IFNULL(`31日`,0) ELSE 0 END) + SUM(CASE WHEN 月份='2026年06月' THEN IFNULL(`1日`,0) ELSE 0 END) FROM kunlun_sales
3. 【记忆】：问“群里聊了什么”，去查 `chat_records` 表。

【🤖 工作流纪律】
阶段一：仅思考并输出一段被 ```sql ``` 包裹的代码！绝对禁止输出假数据示例或文字。
阶段二：收到真实数据后汇报。若结果为空/NULL/None，如实回答数据未录入，严禁瞎编。

【⚙️ 物理限制】每次仅支持单条 SQL！多维度合并请用 UNION ALL。遇到极其复杂跨表需求，请提示用户拆分提问。
"""
    
    ai_reply = call_ai_api(sys_instruction, history)
    
    sql_query = None
    if "[SQL]" in ai_reply and "[/SQL]" in ai_reply:
        match = re.search(r'\[SQL\](.*?)\[/SQL\]', ai_reply, re.DOTALL)
        if match: sql_query = match.group(1).strip()
    elif "```sql" in ai_reply.lower() and "```" in ai_reply:
        match = re.search(r'```sql(.*?)```', ai_reply, re.DOTALL | re.IGNORECASE)
        if match: sql_query = match.group(1).strip()

    if sql_query:
        print(f"🤖 AI 执行 SQL: {sql_query}")
        db_data = execute_ai_sql(sql_query)
        
        history.append({"role": "model", "parts": [{"text": ai_reply}]})
        second_prompt = f"系统已执行你的SQL，数据库返回的真实数据如下:\n{db_data}\n\n请严格基于上述数据向用户汇报。数据为空或为0就直说。绝不能再输出 SQL 代码！"
        history.append({"role": "user", "parts": [{"text": second_prompt}]})
        
        ai_reply = call_ai_api(sys_instruction, history)

    if "拥堵" not in ai_reply:
        history_memory[chat_id].append({"role": "user", "parts": [{"text": user_text}]})
        history_memory[chat_id].append({"role": "model", "parts": [{"text": ai_reply}]})
        
    reply_feishu_message(message_id, ai_reply)

# ================= 7. 飞书 Webhook 接收入口 =================
@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    if "challenge" in body: return {"challenge": body["challenge"]}
        
    event = body.get("event", {})
    msg = event.get("message", {})
    
    message_id = msg.get("message_id")
    chat_id = msg.get("chat_id")
    chat_type = msg.get("chat_type", "p2p")
    sender_id = event.get("sender", {}).get("sender_id", {}).get("open_id", "unknown")
    
    if msg.get("message_type") == "text":
        if message_id in processed_message_ids: return {"status": "ok"}
        processed_message_ids.add(message_id)
        
        content = json.loads(msg.get("content", "{}"))
        user_text = content.get("text", "")
        
        background_tasks.add_task(save_chat_history, chat_id, sender_id, user_text)
        
        should_reply = False
        if chat_type == "p2p": should_reply = True
        elif chat_type == "group" and len(msg.get("mentions", [])) > 0: should_reply = True
                
        if should_reply:
            clean_text = re.sub(r'@_user_\w+', '', user_text).strip()
            background_tasks.add_task(process_message, message_id, clean_text, chat_id)
            
    return {"status": "ok"}
