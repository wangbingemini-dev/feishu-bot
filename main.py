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

# ================= 4. 记忆系统与向量长文档解析 (RAG架构) =================
def init_databases():
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
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS document_vectors (
                id INT AUTO_INCREMENT PRIMARY KEY,
                doc_id VARCHAR(100) COMMENT '飞书文档ID',
                chunk_text TEXT COMMENT '文档段落文本',
                embedding VECTOR(1024) COMMENT '语义向量',
                create_time DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """)
        conn.commit()
        print("✅ 记忆系统与向量知识库已就绪！")
    except Exception as e:
        print(f"创建记忆/向量表失败: {e}")
    finally:
        conn.close()

scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

@app.on_event("startup")
def on_startup():
    init_databases()
    # 设定每天 22:30 准时执行日报生成任务
    scheduler.add_job(generate_and_send_daily_report, 'cron', hour=22, minute=30)
    scheduler.start()
    print("✅ 22:30 日报定时引擎已启动！")

def save_chat_history(chat_id, sender_id, text):
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

def get_text_embedding(text):
    url = "https://api.siliconflow.cn/v1/embeddings"
    headers = {"Authorization": f"Bearer {SILICONFLOW_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "BAAI/bge-m3", "input": text}
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=30).json()
        return res['data'][0]['embedding']
    except Exception as e:
        print(f"向量化失败: {e}")
        return None

def process_and_store_long_doc(doc_id, full_text):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as cnt FROM document_vectors WHERE doc_id = %s", (doc_id,))
            if cursor.fetchone()['cnt'] > 0: return True 
            
            chunk_size, overlap = 500, 50
            chunks = [full_text[i:i + chunk_size] for i in range(0, len(full_text), chunk_size - overlap) if len(full_text[i:i + chunk_size]) > 50]
            
            for chunk in chunks:
                vec = get_text_embedding(chunk)
                if vec:
                    vec_str = "[" + ",".join(map(str, vec)) + "]"
                    cursor.execute("INSERT INTO document_vectors (doc_id, chunk_text, embedding) VALUES (%s, %s, %s)", (doc_id, chunk, vec_str))
            conn.commit()
            return True
    except Exception as e:
        print(f"长文档存储失败: {e}")
        return False
    finally:
        conn.close()

def search_relevant_doc_chunks(user_question, limit=5):
    question_vec = get_text_embedding(user_question)
    if not question_vec: return ""
    vec_str = "[" + ",".join(map(str, question_vec)) + "]"
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = f"""SELECT chunk_text, VEC_COSINE_DISTANCE(embedding, '{vec_str}') as distance 
                      FROM document_vectors ORDER BY distance ASC LIMIT {limit}"""
            cursor.execute(sql)
            results = cursor.fetchall()
            return "\n...\n".join([row['chunk_text'] for row in results])
    except Exception as e:
        return ""
    finally:
        conn.close()

# ================= 5. 数据主动同步模块 =================
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
        records, has_more, page_token = [], True, ""
        
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

@app.api_route("/force-sync", methods=["GET", "POST", "HEAD"])
async def manual_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_full_sync)
    return {"status": "success", "message": "全量拉取同步已在后台启动！"}

# ================= 6. Xavier AI 大脑中枢 & 日报引擎 =================
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

def send_feishu_message(receive_id, text, receive_id_type="chat_id"):
    tenant_token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
    payload = {"receive_id": receive_id, "msg_type": "text", "content": json.dumps({"text": text})}
    requests.post(url, headers={"Authorization": f"Bearer {tenant_token}", "Content-Type": "application/json"}, json=payload)

def call_ai_api(sys_instruction, history):
    url = "https://api.siliconflow.cn/v1/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {SILICONFLOW_API_KEY}"}
    messages = [{"role": "system", "content": sys_instruction}]
    for msg in history:
        role = "assistant" if msg["role"] == "model" else "user"
        messages.append({"role": role, "content": msg["parts"][0]["text"]})

    payload = {
        "model": "deepseek-ai/DeepSeek-V3", 
        "messages": messages,
        "temperature": 0.1 
    }

    for _ in range(3):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            if response.status_code == 200: return response.json()['choices'][0]['message']['content']
            time.sleep(2)
        except Exception as e: pass
    return "Xavier 的大脑正在高速运转，API 通道稍微拥堵，请尝试拆分您的提问！"

def generate_and_send_daily_report(target_chat_id=None):
    print("⏰ [任务] 开始生成工作日报...")
    today_str = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    current_month_str = datetime.now(timezone(timedelta(hours=8))).strftime('%Y年%m月')
    
    conn = get_db_connection()
    sales_data_raw = ""
    chat_records_raw = ""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 产品名称, 目标, 合计 FROM kunlun_sales WHERE 月份 = %s", (current_month_str,))
            sales_data_raw += f"【昆仑系列底表数据】: {cursor.fetchall()}\n"
            
            cursor.execute("SELECT 产品名称, 目标, 合计 FROM dragons1_sales WHERE 月份 = %s", (current_month_str,))
            sales_data_raw += f"【小京龙系列底表数据】: {cursor.fetchall()}\n"
            
            cursor.execute("SELECT 品类, 合计, 目标, 达成率 FROM month_gsv_data WHERE 月份 = %s", (current_month_str,))
            sales_data_raw += f"【大盘分类底表数据】: {cursor.fetchall()}\n"
            
            cursor.execute("SELECT message_text FROM chat_records WHERE DATE(create_time) = %s", (today_str,))
            chats = cursor.fetchall()
            chat_records_raw = "\n".join([row['message_text'] for row in chats])
    except Exception as e:
        print(f"日报拉取底表数据失败: {e}")
        return
    finally:
        conn.close()

# 3. 构建极其严厉的模板 Prompt，剥夺 AI 的发散与排版权力
    report_prompt = f"""
【最高级别系统指令：格式绝对锁定】
你现在的唯一身份是一个“无情的填表机器”。我将为你提供一份固定的【空白日报表单】，你必须【逐字逐句】照抄这份表单的原始骨架，只允许在方括号 [ ] 的位置填入你分析出的真实数据。

【🚨 绝对禁止的违规行为】（一旦违反，将直接导致系统严重报错）：
1. 严禁改变“一、二、三”和“1、2、3”的序号排版！
2. 严禁自己发明任何新段落或新标题（绝对不许出现“核心指标”、“分店铺表现”等字眼）！
3. 严禁使用任何 Emoji 表情符号（绝对不许出现 📊、🔥、🏆、🚨 等）！
4. 严禁生成开头问候语和结尾总结废话！

【👇你需要严格复制并填写的空白表单】（请直接从“一、销售数据”开始输出）：
一、销售数据（不需要分开店铺）
1、月出库进度：[计算底表数据并填入：月度累计达成/目标，达成率是多少？分品类各是多少？达成率分别是多少？]数据可在“AI大战”飞书群中获取其它人提供的
2、月零售进度：[计算底表数据并填入：月度累计达成/目标，达成率是多少？分品类各是多少金额？达成率分别是多少？]
3、昆仑系列达成进度：[计算底表数据并填入：月度累计达成合计/昆仑系列总目标，达成率是多少？分型号各多少台？]
4、小京龙系列达成进度：[计算底表数据并填入：月度累计达成合计/小京龙系列总目标，达成率是多少？分型号各多少台？]

二、日重点工作闭环：
1、运营动作：
[提炼下方群聊记录中关于小京龙和昆仑系列的运营动作。如无记录，必须原样输出“今日暂无相关群聊记录”]
2、推广动作：
[提炼下方群聊记录中关于小京龙和昆仑系列的推广动作。如无记录，必须原样输出“今日暂无相关群聊记录”]
3、其它工作简报：
[提炼下方群聊记录中的其他工作。如无记录，必须原样输出“今日暂无相关群聊记录”]

三、待办事项（待完成工作）及规划完成时间
[提炼下方群聊记录中的待办事项。如无记录，必须原样输出“今日暂无相关群聊记录”]

---
【你的唯一数据提取来源】：

今日真实数据库底表：
{sales_data_raw}

京东渠道内部沟通群及AI大战群的群聊记录：
{chat_records_raw}
"""
    ai_report = call_ai_api(report_prompt, [])
    
    # ⚠️ 极度关键：填入你真实的默认群聊 chat_id
    DEFAULT_GROUP_CHAT_ID = "请在此处填入日报群的chat_id" 
    
    send_to_id = target_chat_id if target_chat_id else DEFAULT_GROUP_CHAT_ID
    send_feishu_message(send_to_id, ai_report, receive_id_type="chat_id")
    print(f"✅ 工作日报已成功推送至 {send_to_id}！")

def process_message(message_id, user_text, chat_id):
    if chat_id not in history_memory:
        history_memory[chat_id] = []
    
    link_match = re.search(r'https://[a-zA-Z0-9-]+\.feishu\.cn/(docx|wiki)/([a-zA-Z0-9]+)', user_text)
    if link_match:
        link_type = link_match.group(1)
        doc_id_or_token = link_match.group(2)
        doc_id = get_real_app_token(doc_id_or_token, get_tenant_access_token()) if link_type == 'wiki' else doc_id_or_token
        
        full_text = read_feishu_docx(doc_id)
        if "抱歉，我无法读取" not in full_text:
            success = process_and_store_long_doc(doc_id, full_text)
            if success:
                user_text += f"\n\n【系统提示】文档已成功拆解并存入知识库。请直接告诉用户：'我已经把这篇文档学习完毕存入记忆库了，你可以向我提问关于文档的任何细节。'"
        else:
            user_text += f"\n\n【系统提示】读取失败，提醒用户开通权限。"

    clean_query = re.sub(r'https://[a-zA-Z0-9-]+\.feishu\.cn/\S+', '', user_text).strip()
    if clean_query: 
        relevant_context = search_relevant_doc_chunks(clean_query, limit=5)
        if relevant_context:
            user_text += f"\n\n【专属知识检索提取】为了回答用户的这个问题，系统检索到了以下极其相关的文档历史片段，请务必严格参考它们作答：\n---\n{relevant_context}\n---"

    history = history_memory[chat_id][-10:]
    history.append({"role": "user", "parts": [{"text": user_text}]})
    
    db_schema = get_database_schema()
    today_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    yesterday_date = (datetime.now(timezone(timedelta(hours=8))) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    sys_instruction = f"""你的名字叫Xavier，是家电行业电商资深运营专家。
直连 TiDB。当前北京时间：{today_date}，昨天日期是：{yesterday_date}。

当前表结构：
{db_schema}

【🔥 业务字典与防错铁律】
1. 【大盘流水表】：`daily_category_gsv` 存储的是【金额(元)】！问大盘总额、各品类销售额时查此表。查“某日至某日”时，必须转化为 YYYY-MM-DD 使用 BETWEEN 语法。
2. 【特定系列宽表（⚠️极其重要）】：昆仑或小京龙必须查 `kunlun_sales` 或 `dragons1_sales`。
   - 绝密警告1：这两张表存储的是【销量(台)】！里面的数字都是卖了多少台，【绝对不允许】输出为“元”！
   - 绝密警告2：这两张表是【宽表】，日期是【列名】（如 `1日`），行标识是【月份】（如 '2026年06月'）。
   - 查时间段（如5日至11日），必须横向相加：SELECT 店铺, SUM(IFNULL(`5日`,0)+IFNULL(`6日`,0)+IFNULL(`7日`,0)+IFNULL(`8日`,0)+IFNULL(`9日`,0)+IFNULL(`10日`,0)+IFNULL(`11日`,0)) AS 销量, SUM(目标) AS 总目标 FROM dragons1_sales WHERE 月份='2026年06月' GROUP BY 店铺

【🚨 零幻觉与实体封杀令（生死铁律）】
1. SQL 返回了什么店铺，你就【只允许】输出什么店铺！如果数据库只返回了“云米净水自营店2024”，你绝不允许画蛇添足地去补充列表中不存在的店并给它标 0！
2. 绝不允许在回答中发明或出现“天猫”、“拼多多”、“抖音”等任何非数据库存在的渠道！
3. 绝不允许出现“空气炸锅”、“扫地机器人”等完全不存在于你底表中的品类！
4. 严格遵守单位：宽表=台，大盘=元。如果发现自己要把宽表数据写成“元”，立刻自己掌嘴并纠正为“台”！

【🤖 工作流纪律】
阶段一：只要用户的指令是在索要销售数据或报表，你的第一次思考和输出【只能且必须】是一段被 ```sql ``` 包裹的代码！
阶段二：收到系统查回的真实数据后，严格按照真实数据汇报，只说查到的内容，绝不加戏。

【⚙️ 物理限制】每次仅支持单条 SQL！多维度合并请用 UNION ALL。
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
        second_prompt = f"系统已执行你的SQL，数据库返回的真实数据如下:\n{db_data}\n\n请严格基于上述数据向用户汇报。绝不能再输出 SQL 代码！"
        history.append({"role": "user", "parts": [{"text": second_prompt}]})
        
        ai_reply = call_ai_api(sys_instruction, history)

    if "拥堵" not in ai_reply:
        history_memory[chat_id].append({"role": "user", "parts": [{"text": user_text}]})
        history_memory[chat_id].append({"role": "model", "parts": [{"text": ai_reply}]})
        
    reply_feishu_message(message_id, ai_reply)

# ================= 7. 飞书 Webhook 接收入口 (精准防干扰 + 快捷指令拦截) =================
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
        
        # 🌟 第一层动作：潜水模式（不管谁说话，统统存入记忆库）
        background_tasks.add_task(save_chat_history, chat_id, sender_id, user_text)
        
        # 🌟 第二层动作：身份甄别（判断是否需要大脑开口回复）
        should_reply = False
        if chat_type == "p2p":
            should_reply = True
        elif chat_type == "group":
            mentions = msg.get("mentions", [])
            for m in mentions:
                if "bot_id" in m.get("id", {}) or m.get("name", "").lower() in ["xavier", "机器人"]:
                    should_reply = True
                    break
                
        # 🌟 第三层动作：处理回复与快捷拦截
        if should_reply:
            clean_text = re.sub(r'@_user_\w+', '', user_text).strip()
            
            # 日报专属快捷指令嗅探拦截
            if "日报" in clean_text and any(kw in clean_text for kw in ["发", "写", "生成", "输出", "看"]):
                background_tasks.add_task(reply_feishu_message, message_id, "收到指令！Xavier 正在为您火速盘点今日真实数据与群聊记录，请稍候约 30-60 秒...")
                background_tasks.add_task(generate_and_send_daily_report, chat_id)
            else:
                background_tasks.add_task(process_message, message_id, clean_text, chat_id)
            
    return {"status": "ok"}
