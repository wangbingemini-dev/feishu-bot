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
# 🌟 改为读取硅基流动的 API KEY
SILICONFLOW_API_KEY = os.environ.get("SILICONFLOW_API_KEY")

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
            
            items = res.get("data", {}).get("items", [])
            for item in items: records.append(item.get("fields", {}))
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
            print(f"✅ [{db_table}] 成功拉取并覆盖 {len(records)} 条新数据！")
        except Exception as e: 
            print(f"❌ [{db_table}] 写入失败: {e}")
    conn.close()
    print("🎉 全量同步执行完毕！")

# 🌟 双保险：兼容 GET 和 POST，配合 UptimeRobot 无敌
@app.api_route("/force-sync", methods=["GET", "POST"])
async def manual_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_full_sync)
    return {"status": "success", "message": "全量拉取同步已在后台启动！"}

# ================= 5. Xavier AI 大脑 (硅基流动 DeepSeek 驱动版) =================
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
    """调用硅基流动平台提供的 DeepSeek 大模型"""
    # 🌟 硅基流动的 API 地址
    url = "https://api.siliconflow.cn/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SILICONFLOW_API_KEY}"
    }

    # 自动转换格式为 OpenAI / DeepSeek 标准格式
    messages = [{"role": "system", "content": sys_instruction}]
    for msg in history:
        role = "assistant" if msg["role"] == "model" else "user"
        content = msg["parts"][0]["text"]
        messages.append({"role": role, "content": content})

    payload = {
        # 🌟 硅基流动平台上的 DeepSeek V3 官方调用名称
        "model": "deepseek-ai/DeepSeek-V4-Pro", 
        "messages": messages,
        "temperature": 0.1 # 保持冷静客观，严谨写代码
    }

    for _ in range(3):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            if response.status_code == 200:
                return response.json()['choices'][0]['message']['content']
            else:
                print(f"⚠️ API 拒绝了请求！状态码: {response.status_code} | 详情: {response.text}")
                time.sleep(2)
        except Exception as e: 
            print(f"API 网络层波动: {e}")
            
    return "Xavier 的大脑正在高速运转，API 通道稍微有点拥堵，请半分钟后再问我一次！"

def process_message(message_id, user_text, chat_id):
    if chat_id not in history_memory:
        history_memory[chat_id] = []
    
    history = history_memory[chat_id][-10:]
    history.append({"role": "user", "parts": [{"text": user_text}]})
    
    db_schema = get_database_schema()
    
    # 🌟 修复 1：获取当前的真实北京时间，强行注入给 AI
    today_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    
    # 🌟 修复 2：在系统指令中加入时间锚点
    sys_instruction = f"""你的名字叫Xavier，是全渠道净水器/大家电的资深电商运营专家。
你的大脑已直连公司 TiDB 数据库。
【当前时间基准】⏰ 今天是北京时间：{today_date}。你在推断用户说的“今天”、“昨天”时，必须严格以此日期为基准！

以下是目前数据库的表结构：
{db_schema}

【🔥 核心业务表关系字典 —— 跨表查询必读！】
为了给出最准确的答案，你必须深刻理解以下几张表的业务关系：
1. 【看大盘/全店业绩】：只要用户问“今天总销售额”、“各品类销售”、“达成率”，必须优先且唯一使用 `daily_category_gsv` 或 `category_gsv_data` 进行汇总。
2. 【看特定系列】：如果用户明确问到“昆仑系列”或“小京龙系列”，必须去查 `kunlun_sales` 或 `dragons1_sales`。
3. 【跨表组合分析 (JOIN/UNION)】：
   - 如果用户问“昆仑系列和小京龙系列的总对比”，你必须使用 UNION ALL 将这两张表的数据合并后，再做 SUM 汇总。
   - 如果用户问“某个品类的整体表现，且要求带上核心单品的 ROI”，你需要使用 JOIN 语句，通过时间或店铺字段，将 `daily_category_gsv` 与 `core_item_data` 联合查询。

【查数指令规范 —— 极其重要！】
1. 回答任何涉及数据的业务问题，务必自己写 MySQL 语句去查询真实数据。禁止瞎编。遇到复杂的跨业务问题，大胆使用 JOIN 或 UNION。
2. 📅 时间查询规则：严格使用带横杠的 `YYYY-MM-DD` 格式进行 WHERE 筛选。
3. 💰 数值计算规则：表中的金额已经是纯数字格式（DOUBLE），直接使用 SUM() 等函数计算即可。
4. 输出 SQL 时，请直接输出 SQL 语句，可以用 [SQL] 包裹，也可以用普通 Markdown 的 ```sql 格式。
5. 🔍 全局视野：如果用户询问某一天的大盘或各品类对比，务必使用 GROUP BY 品类，把该日期下所有存在的品类全部汇总出来。
6. ⚠️ 致命物理限制：底层数据库每次查询【仅支持单条 SQL 语句】！绝对禁止使用分号 (;) 拼接多条 SELECT 语句一并输出。如果你既需要大盘汇总，又需要分品类明细，请必须使用 `GROUP BY 品类 WITH ROLLUP` 语法，或者用 `UNION ALL` 把它们合并成一条单一的 SQL 语句！
    
    ai_reply = call_ai_api(sys_instruction, history)
    
    sql_query = None
    if "[SQL]" in ai_reply and "[/SQL]" in ai_reply:
        match = re.search(r'\[SQL\](.*?)\[/SQL\]', ai_reply, re.DOTALL)
        if match: sql_query = match.group(1).strip()
    elif "```sql" in ai_reply.lower() and "```" in ai_reply:
        match = re.search(r'```sql(.*?)```', ai_reply, re.DOTALL | re.IGNORECASE)
        if match: sql_query = match.group(1).strip()

    if sql_query:
        print(f"🤖 AI 生成了查询指令: {sql_query}")
        db_data = execute_ai_sql(sql_query)
        
        history.append({"role": "model", "parts": [{"text": ai_reply}]})
        
        # 🌟 修复 3：明确的第二回合“禁言令”，彻底杜绝二次输出 SQL
        second_prompt = f"系统已执行你的SQL，数据库返回的真实数据如下:\n{db_data}\n\n请严格基于上述数据，用专业的商业口吻直接向用户汇报分析结果。如果数据为空，请直接告诉用户“系统内暂无该日期的数据”。【警告】：绝对禁止在这次回复中再次输出任何 SQL 语句代码！"
        history.append({"role": "user", "parts": [{"text": second_prompt}]})
        
        ai_reply = call_ai_api(sys_instruction, history)

    if "通道稍微有点拥堵" not in ai_reply:
        history_memory[chat_id].append({"role": "user", "parts": [{"text": user_text}]})
        history_memory[chat_id].append({"role": "model", "parts": [{"text": ai_reply}]})
        
    reply_feishu_message(message_id, ai_reply)

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
