import time
import os
import json
import requests
import pymysql
import re
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Request, BackgroundTasks
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()

# ================= 1. 核心钥匙与环境变量 =================
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME", "test")

# 你的飞书知识库外壳 Token
WIKI_TOKEN = "Re61wxHP9iO5NFk6IshckxNYnKc"

# 全局记忆体（用于防重复轰炸和对话上下文）
history_memory = {}
processed_message_ids = set()

# ================= 2. 飞书数据地图 (全中文原味对齐版) =================
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

# ================= 4. 飞书主动拉取与清洗核心逻辑 =================
def get_real_app_token(wiki_token, token):
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token={wiki_token}"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"}).json()
    if res.get("code") == 0: return res["data"]["node"]["obj_token"]
    return wiki_token

def clean_feishu_value(v):
    """暴力清洗飞书嵌套格式，强行炼化数字，并修正北京时区"""
    if v is None or v == "": return None
    
    # 破除飞书俄罗斯套娃格式
    if isinstance(v, list) and len(v) > 0:
        if isinstance(v[0], dict):
            v = v[0].get("value", v[0].get("text", v[0]))
        else:
            v = v[0]
    if isinstance(v, dict):
        v = v.get("value", v.get("text", v))

    # 处理标准数字与修正北京时区
    if isinstance(v, (int, float)):
        if v > 1000000000000:
            dt = datetime.fromtimestamp(v / 1000.0, timezone.utc) + timedelta(hours=8)
            return dt.strftime('%Y-%m-%d')
        return float(v)

    # 字符串剔除杂质与强行数字炼化
    if isinstance(v, str): 
        cleaned = v.replace(',', '').replace('¥', '').replace('￥', '').replace('\xa0', '').strip()
        if not cleaned: return None
        try:
            return float(cleaned)
        except ValueError:
            return cleaned 
            
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
            
            items = res.get("data", {}).get("items", [])
            for item in items: records.append(item.get("fields", {}))
            
            has_more = res.get("data", {}).get("has_more", False)
            page_token = res.get("data", {}).get("page_token", "")
            
        if not records: continue
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {db_table}") # 清空旧表
                
                # 因为列名可能以数字开头，必须用反引号包裹保护
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
            print(f"✅ [{db_table}] 成功拉取并覆盖 {len(records)} 条新数据！")
        except Exception as e: 
            print(f"❌ [{db_table}] 写入失败: {e}")
        
    conn.close()
    print("🎉 全量同步执行完毕！")

@app.on_event("startup")
def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_full_sync, 'interval', hours=2)
    scheduler.start()

@app.api_route("/force-sync", methods=["GET", "POST"])
async def manual_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_full_sync)
    return {"status": "success", "message": "全量拉取同步已在后台启动！"}

# ================= 5. Xavier AI 大脑查询与生成中枢 =================
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

def call_gemini_api(payload):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    for _ in range(3):
        try:
            response = requests.post(url, headers={'Content-Type': 'application/json'}, json=payload, timeout=45)
            if response.status_code == 200:
                return response.json()['candidates'][0]['content']['parts'][0]['text']
            else:
                print(f"⚠️ Google 拒绝了请求！状态码: {response.status_code} | 报错详情: {response.text}")
                time.sleep(2)
        except Exception as e: 
            print(f"API 网络层波动: {e}")
            
    return "Xavier 的大脑正在高速运转，API 通道稍微有点拥堵，请半分钟后再问我一次！"

def reply_feishu_message(message_id, text):
    tenant_token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
    payload = {"msg_type": "text", "content": json.dumps({"text": text})}
    requests.post(url, headers={"Authorization": f"Bearer {tenant_token}", "Content-Type": "application/json"}, json=payload)

def process_message(message_id, user_text, chat_id):
    if chat_id not in history_memory:
        history_memory[chat_id] = []
    
    history = history_memory[chat_id][-10:]
    history.append({"role": "user", "parts": [{"text": user_text}]})
    
    db_schema = get_database_schema()
    
    sys_instruction = f"""你的名字叫Xavier，是全渠道净水器/厨房电器的顶级电商数据参谋。
你的大脑已直连公司 TiDB 数据库，掌握着所有核心战报。以下是目前数据库的表结构：
{db_schema}

【查数指令规范 —— 极其重要！】
1. 回答任何涉及数据的业务问题，务必自己写 MySQL 语句去查询真实数据。禁止瞎编。
2. 📅 时间查询规则：数据库中的时间是极其标准的 DATE 格式（例如 '2026-04-01'），请严格使用这种带横杠的 `YYYY-MM-DD` 格式进行 WHERE 筛选。
3. 💰 数值计算规则：表中的 GSV 等金额字段已经是纯数字格式（DOUBLE），无需去除逗号，直接使用 SUM() 等函数计算即可。
4. 🔍 全局视野：如果用户询问某一天的大盘或各品类对比，务必使用 GROUP BY 品类，把该日期下所有存在的品类（如冰箱、厨电、净水器等）全部汇总出来。
5. 输出 SQL 时，请直接输出 SQL 语句，可以用 [SQL] 包裹，也可以用普通 Markdown 的 ```sql 格式。
6. 系统执行后会把真实结果喂给你。拿到结果后，请用极具商业洞察的视角、分点、加粗的 Markdown 格式输出你的最终分析。
"""
    
    payload = {"system_instruction": {"parts": [{"text": sys_instruction}]}, "contents": history}
    
    gemini_reply = call_gemini_api(payload)
    
    sql_query = None
    if "[SQL]" in gemini_reply and "[/SQL]" in gemini_reply:
        match = re.search(r'\[SQL\](.*?)\[/SQL\]', gemini_reply, re.DOTALL)
        if match: sql_query = match.group(1).strip()
    elif "```sql" in gemini_reply.lower() and "```" in gemini_reply:
        match = re.search(r'```sql(.*?)```', gemini_reply, re.DOTALL | re.IGNORECASE)
        if match: sql_query = match.group(1).strip()

    if sql_query:
        print(f"🤖 AI 生成了查询指令: {sql_query}")
        db_data = execute_ai_sql(sql_query)
        
        history.append({"role": "model", "parts": [{"text": gemini_reply}]})
        history.append({"role": "user", "parts": [{"text": f"系统已执行你的SQL，数据库返回的真实数据如下:\n{db_data}\n\n请严格基于这些真实数据，直接回答我最初的业务问题，绝对禁止瞎编数字。"}]})
        
        payload["contents"] = history
        gemini_reply = call_gemini_api(payload)

    if "通道稍微有点拥堵" not in gemini_reply:
        history_memory[chat_id].append({"role": "user", "parts": [{"text": user_text}]})
        history_memory[chat_id].append({"role": "model", "parts": [{"text": gemini_reply}]})
        
    reply_feishu_message(message_id, gemini_reply)

# ================= 6. 飞书 Webhook 接收入口 (防连环轰炸版) =================
@app.post("/webhook")
async def feishu_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    
    if "challenge" in body:
        return {"challenge": body["challenge"]}
        
    event = body.get("event", {})
    msg = event.get("message", {})
    
    message_id = msg.get("message_id")
    chat_id = msg.get("chat_id")
    
    if msg.get("message_type") == "text":
        
        # 🛡️ 拦截器：如果飞书因为超时重发，直接挡在门外
        if message_id in processed_message_ids:
            print(f"🛡️ 拦截到飞书重复发送的消息，已忽略: {message_id}")
            return {"status": "ok"}
            
        processed_message_ids.add(message_id)
        
        content = json.loads(msg.get("content", "{}"))
        user_text = content.get("text", "")
        background_tasks.add_task(process_message, message_id, user_text, chat_id)
        
    return {"status": "ok"}
