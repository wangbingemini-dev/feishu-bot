import time
import os
import json
import requests
import pymysql
import re
import threading
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, BackgroundTasks, Header, HTTPException, Request

# 👇 引入飞书官方 SDK（用于长链接）
import lark_oapi as lark

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
daily_report_lock = threading.Lock()

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

# ================= 4. 记忆系统与知识库 =================
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
    except Exception as e:
        print(f"创建表失败: {e}")
    finally:
        conn.close()

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
        print(f"✅ [记忆存入成功] {clean_text[:15]}...") # 新增监控
    except Exception as e:
        print(f"❌ [存入数据库报错]: {e}") # 暴露致命错误
    finally:
        conn.close()

def read_feishu_docx(doc_id):
    tenant_token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{doc_id}/blocks"
    headers = {"Authorization": f"Bearer {tenant_token}", "Content-Type": "application/json"}
    full_text, page_token, has_more = "", "", True
    try:
        while has_more:
            req_url = url + f"?page_token={page_token}" if page_token else url
            response = requests.get(req_url, headers=headers).json()
            if response.get("code") != 0: return "抱歉，无法读取文档。"
            for block in response.get("data", {}).get("items", []):
                if block.get("block_type") in [1, 2, 3, 4, 5, 6, 7, 8, 9]: 
                    for element in block.get("text", {}).get("elements", []):
                        full_text += element.get("text_run", {}).get("content", "")
                    full_text += "\n"
            has_more = response.get("data", {}).get("has_more", False)
            page_token = response.get("data", {}).get("page_token", "")
        return full_text.strip()
    except Exception: return "读取文档异常"

def get_text_embedding(text):
    url = "https://api.siliconflow.cn/v1/embeddings"
    headers = {"Authorization": f"Bearer {SILICONFLOW_API_KEY}", "Content-Type": "application/json"}
    try:
        res = requests.post(url, headers=headers, json={"model": "BAAI/bge-m3", "input": text}, timeout=30).json()
        return res['data'][0]['embedding']
    except Exception: return None

def process_and_store_long_doc(doc_id, full_text):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as cnt FROM document_vectors WHERE doc_id = %s", (doc_id,))
            if cursor.fetchone()['cnt'] > 0: return True 
            chunks = [full_text[i:i + 500] for i in range(0, len(full_text), 450) if len(full_text[i:i + 500]) > 50]
            for chunk in chunks:
                vec = get_text_embedding(chunk)
                if vec:
                    cursor.execute("INSERT INTO document_vectors (doc_id, chunk_text, embedding) VALUES (%s, %s, %s)", 
                                   (doc_id, chunk, "[" + ",".join(map(str, vec)) + "]"))
            conn.commit()
            return True
    except Exception: return False
    finally: conn.close()

def search_relevant_doc_chunks(user_question):
    vec = get_text_embedding(user_question)
    if not vec: return ""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT chunk_text FROM document_vectors ORDER BY VEC_COSINE_DISTANCE(embedding, '[" + ",".join(map(str, vec)) + "]') ASC LIMIT 5")
            return "\n...\n".join([row['chunk_text'] for row in cursor.fetchall()])
    except Exception: return ""
    finally: conn.close()

# ================= 5. AI 大脑中枢与 SQL 执行 =================
def get_database_schema():
    conn = get_db_connection()
    schema_info = ""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            for table in [list(row.values())[0] for row in cursor.fetchall()]:
                cursor.execute(f"SHOW COLUMNS FROM {table}")
                schema_info += f"表名: {table}, 字段: {', '.join([row['Field'] for row in cursor.fetchall()])}\n"
    except Exception: pass
    finally: conn.close()
    return schema_info

def execute_ai_sql(sql_query):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            return str(cursor.fetchall())
    except Exception as e: return f"SQL执行报错: {e}"
    finally: conn.close()

def reply_feishu_message(message_id, text):
    tenant_token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
    requests.post(url, headers={"Authorization": f"Bearer {tenant_token}", "Content-Type": "application/json"}, 
                  json={"msg_type": "text", "content": json.dumps({"text": text})})

def send_feishu_message(receive_id, text, receive_id_type="chat_id"):
    tenant_token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
    requests.post(url, headers={"Authorization": f"Bearer {tenant_token}", "Content-Type": "application/json"}, 
                  json={"receive_id": receive_id, "msg_type": "text", "content": json.dumps({"text": text})})

def call_ai_api(sys_instruction, history):
    url, headers = "https://api.siliconflow.cn/v1/chat/completions", {"Content-Type": "application/json", "Authorization": f"Bearer {SILICONFLOW_API_KEY}"}
    messages = [{"role": "system", "content": sys_instruction}] + [{"role": "assistant" if msg["role"] == "model" else "user", "content": msg["parts"][0]["text"]} for msg in history]
    for _ in range(3):
        try:
            res = requests.post(url, headers=headers, json={"model": "deepseek-ai/DeepSeek-V3", "messages": messages, "temperature": 0.1}, timeout=60)
            if res.status_code == 200: return res.json()['choices'][0]['message']['content']
            time.sleep(2)
        except Exception: pass
    return "Xavier 的大脑正在高速运转，API 通道稍微拥堵，请稍后再试！"

def generate_and_send_daily_report(target_chat_id=None):
    today_str = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    current_month_str = datetime.now(timezone(timedelta(hours=8))).strftime('%Y年%m月')
    conn = get_db_connection()
    sales_data_raw, chat_records_raw = "", ""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 产品名称, 目标, 合计 FROM kunlun_sales WHERE 月份 = %s", (current_month_str,))
            sales_data_raw += f"【昆仑系列销量】: {cursor.fetchall()}\n"
            cursor.execute("SELECT 产品名称, 目标, 合计 FROM dragons1_sales WHERE 月份 = %s", (current_month_str,))
            sales_data_raw += f"【小京龙系列销量】: {cursor.fetchall()}\n"
            cursor.execute("SELECT 品类, GSV, 目标, 达成率 FROM month_gsv_data WHERE 月份 = %s", (current_month_str,))
            sales_data_raw += f"【大盘分类销售额】: {cursor.fetchall()}\n"
            cursor.execute("SELECT message_text FROM chat_records WHERE DATE(create_time) = %s", (today_str,))
            chat_records_raw = "\n".join([row['message_text'] for row in cursor.fetchall()])
    except Exception: return
    finally: conn.close()

    report_prompt = f"""
【最高级别指令：无情填表机器】
你必须逐字照抄下方的空白表单骨架，只能在 [] 的位置填入真实数据。严禁加 Emoji，严禁编造数据和标题！

一、销售数据
1、月出库进度：[填入大盘分类销售额计算结果]
2、月零售进度：[填入大盘分类销售额计算结果]
3、昆仑系列达成进度：[填入昆仑系列销量计算结果]
4、小京龙系列达成进度：[填入小京龙系列销量计算结果]

二、日重点工作闭环：
1、运营动作：\n[提炼群聊]
2、推广动作：\n[提炼群聊]
3、其它工作简报：\n[提炼群聊]

三、待办事项（待完成工作）及规划完成时间\n[提炼群聊]

--- 真实素材来源 ---
【底表】：{sales_data_raw}
【聊天】：{chat_records_raw}
"""
    ai_report = call_ai_api(report_prompt, [])
    send_to_id = target_chat_id if target_chat_id else "请在此处填入日报群的chat_id" 
    send_feishu_message(send_to_id, ai_report, receive_id_type="chat_id")

def process_message(message_id, user_text, chat_id):
    if chat_id not in history_memory: history_memory[chat_id] = []
    
    clean_query = re.sub(r'https://[a-zA-Z0-9-]+\.feishu\.cn/\S+', '', user_text).strip()
    if clean_query: 
        relevant_context = search_relevant_doc_chunks(clean_query)
        if relevant_context: user_text += f"\n\n【知识检索库】请参考：\n{relevant_context}"

    history = history_memory[chat_id][-10:]
    history.append({"role": "user", "parts": [{"text": user_text}]})
    
    today_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    yesterday_date = (datetime.now(timezone(timedelta(hours=8))) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    sys_instruction = f"""你的名字叫Xavier，是家电行业电商数据参谋。
直连 TiDB。当前北京时间：{today_date}，昨天日期是：{yesterday_date}。
当前表结构：{get_database_schema()}

【🔥 业务字典与防错铁律】
1. 【大盘】：`daily_category_gsv`是【金额(元)】！查“某日至某日”，必须转化 YYYY-MM-DD 用 BETWEEN。
2. 【宽表】：昆仑/小京龙查 `kunlun_sales`/`dragons1_sales`。是【销量(台)】绝对不能输出元！横向相加：SUM(IFNULL(`5日`,0)+IFNULL(`6日`,0))。严禁在宽表用 BETWEEN。

【🚨 零幻觉铁律】
1. 只输出 SQL 返回的店铺和品类！
2. 绝不发明天猫、拼多多、空气炸锅等不存在的数据！

【🤖 工作流】第一步必须输出被 ```sql 
``` 包裹的查询代码！第二步根据返回真实数据汇报。
"""
    ai_reply = call_ai_api(sys_instruction, history)
    
    sql_query = None
    if "```sql" in ai_reply.lower() and "```" in ai_reply:
        match = re.search(r'```sql(.*?)```', ai_reply, re.DOTALL | re.IGNORECASE)
        if match: sql_query = match.group(1).strip()

    if sql_query:
        db_data = execute_ai_sql(sql_query)
        history.append({"role": "model", "parts": [{"text": ai_reply}]})
        history.append({"role": "user", "parts": [{"text": f"数据库真实返回:\n{db_data}\n\n请汇报。绝不再输出SQL！"}]})
        ai_reply = call_ai_api(sys_instruction, history)

    history_memory[chat_id].extend([{"role": "user", "parts": [{"text": user_text}]}, {"role": "model", "parts": [{"text": ai_reply}]}])
    reply_feishu_message(message_id, ai_reply)


# ================= 6. 飞书长链接 WebSocket 引擎 (核心改造区) =================

def handle_ws_message(data) -> None:
    """这是接收飞书长链接消息的处理中枢"""
    try:
        # 新增监控：看看有没有数据流进来
        print(f"📦 [WS 接收到消息，开始解析...]") 
        
        raw_json = lark.JSON.marshal(data)
        event_dict = json.loads(raw_json)
        
        event = event_dict.get("event", {})
        msg = event.get("message", {})
        
        message_id = msg.get("message_id")
        chat_id = msg.get("chat_id")
        chat_type = msg.get("chat_type", "p2p")
        sender_id = event.get("sender", {}).get("sender_id", {}).get("open_id", "unknown")
        
        if msg.get("message_type") == "text":
            if message_id in processed_message_ids: return
            processed_message_ids.add(message_id)
            
            content = json.loads(msg.get("content", "{}"))
            user_text = content.get("text", "")
            
            # 🌟 1. 潜水模式：存入 TiDB
            threading.Thread(target=save_chat_history, args=(chat_id, sender_id, user_text)).start()
            
            # 🌟 2. 身份甄别
            should_reply = False
            if chat_type == "p2p":
                should_reply = True
            elif chat_type == "group":
                mentions = msg.get("mentions", [])
                for m in mentions:
                    if "bot_id" in m.get("id", {}) or m.get("name", "").lower() in ["xavier", "机器人"]:
                        should_reply = True
                        break
                        
            # 🌟 3. 拦截器与大模型处理
            if should_reply:
                clean_text = re.sub(r'@_user_\w+', '', user_text).strip()
                if "日报" in clean_text and any(kw in clean_text for kw in ["发", "写", "生成", "输出", "看"]):
                    threading.Thread(target=reply_feishu_message, args=(message_id, "收到指令！Xavier 正在通过长链接为您提取今日数据与潜水记忆，请稍候约 30-60 秒...")).start()
                    threading.Thread(target=generate_and_send_daily_report, args=(chat_id,)).start()
                else:
                    threading.Thread(target=process_message, args=(message_id, clean_text, chat_id)).start()
    except Exception as e:
        print(f"❌ [WS 消息处理极其严重报错]: {e}")

def start_feishu_ws_client():
    """在后台独立运行的 WebSocket 守护进程"""
    print("🚀 正在初始化飞书 WebSocket 长链接...")
    event_handler = lark.EventDispatcherHandler.builder("", "") \
        .register_p2_im_message_receive_v1(handle_ws_message) \
        .build()
    
    # 构建并启动客户端，它会一直保持连接
    cli = lark.ws.Client(FEISHU_APP_ID, FEISHU_APP_SECRET, event_handler=event_handler)
    cli.start()


# ================= 7. 服务启动与防休眠 =================

scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

@app.on_event("startup")
def on_startup():
    init_databases()
    
    # 云端正式日报由 Render Cron Job 执行；仅在显式开启时保留进程内定时，避免重复发送。
    if os.environ.get("ENABLE_IN_PROCESS_DAILY_REPORT", "false").lower() in {"1", "true", "yes"}:
        scheduler.add_job(generate_and_send_daily_report, 'cron', hour=22, minute=30)
    
    # 🌟 注册自动生物钟：每隔 2 小时自动同步一次飞书多维表格
    scheduler.add_job(execute_full_sync, 'interval', hours=2)
    
    scheduler.start()
    
    # 启动长链接线程
    ws_thread = threading.Thread(target=start_feishu_ws_client, daemon=True)
    ws_thread.start()

# ================= 5.5 新增：飞书多维表格全量同步中枢 =================

def execute_full_sync():
    """从飞书多维表格拉取全量数据并批量写入 TiDB"""
    print("🔄 [同步任务] 开始从飞书多维表格拉取最新数据...")
    tenant_token = get_tenant_access_token()
    if not tenant_token:
        print("❌ [同步任务] 获取 tenant_access_token 失败，终止同步。")
        return False
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            for config in TABLE_CONFIGS:
                table_id = config["table_id"]
                db_table = config["db_table"]
                mapping = config["mapping"]
                
                # 过滤未配置或占位符的表格
                if "REPLACE_WITH" in table_id:
                    continue
                    
                print(f"⏳ 正在同步底表: {db_table} ...")
                
                # 1. 流式分页读取飞书多维表格数据
                url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{WIKI_TOKEN}/tables/{table_id}/records"
                headers = {"Authorization": f"Bearer {tenant_token}"}
                
                all_records = []
                page_token = ""
                has_more = True
                
                while has_more:
                    params = {"page_size": 500}
                    if page_token:
                        params["page_token"] = page_token
                    
                    res = requests.get(url, headers=headers, params=params).json()
                    if res.get("code") != 0:
                        print(f"❌ 读取飞书表 {db_table} 失败: {res.get('msg')}")
                        break
                        
                    data_page = res.get("data", {})
                    all_records.extend(data_page.get("items", []))
                    has_more = data_page.get("has_more", False)
                    page_token = data_page.get("page_token", "")
                
                if not all_records:
                    print(f"⚠️ 飞书表 {db_table} 未检测到任何真实数据，跳过写入。")
                    continue
                    
                # 2. 清空旧数据防止数据重叠
                cursor.execute(f"TRUNCATE TABLE {db_table}")
                
                # 3. 映射字段并批量构造插入
                fields_db = list(mapping.values())
                fields_fs = list(mapping.keys())
                
                # 构造标准 SQL 插入语句
                sql_insert = f"INSERT INTO {db_table} ({', '.join([f'`{f}`' for f in fields_db])}) VALUES ({', '.join(['%s'] * len(fields_db))})"
                
                batch_data = []
                batch_data = []
                for record in all_records:
                    fields = record.get("fields", {})
                    row = []
                    for fs_key in fields_fs:
                        val = fields.get(fs_key)
                        
                        # 🌟 新增核心修复：拦截飞书的 13 位毫秒时间戳并翻译
                        if isinstance(val, int) and len(str(val)) == 13:
                            try:
                                # 将毫秒时间戳转化为北京时间的标准日期字符串
                                val = datetime.fromtimestamp(val / 1000.0, timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
                            except Exception:
                                pass # 如果转换失败，保持原样
                                
                        # 原有的复杂结构转化逻辑
                        elif isinstance(val, list) or isinstance(val, dict):
                            val = json.dumps(val, ensure_ascii=False)
                            
                        row.append(val)
                    batch_data.append(row)
                
                if batch_data:
                    cursor.executemany(sql_insert, batch_data)
                    print(f"✅ 表 {db_table} 同步成功，顺畅写入 {len(batch_data)} 条记录！")
                    
            conn.commit()
            print("🎉 [同步任务] 恭喜！飞书多维表格全量数据已完美同步至 TiDB 数据库！")
            return True
    except Exception as e:
        print(f"❌ [同步任务] 发生严重异常崩溃: {e}")
        return False
    finally:
        conn.close()


@app.get("/force-sync")
def force_sync_endpoint(background_tasks: BackgroundTasks):
    """公开手动触发接口：支持随时随地在浏览器输入网址拉取全量同步"""
    # 使用 FastAPI 的 BackgroundTasks 异步执行，防止浏览器等待超时导致报错
    background_tasks.add_task(execute_full_sync)
    return {
        "status": "success", 
        "message": "全量数据同步指令已下发！Xavier 正在后台火速搬运飞书多维表格，请静候 10 秒后刷新数据库查看数据。"
    }

@app.post("/tasks/daily-report")
async def daily_report_endpoint(request: Request, x_task_token: str | None = Header(default=None)):
    """GitHub Actions 调用的云端日报任务入口。"""
    expected_token = os.environ.get("DAILY_REPORT_TASK_TOKEN") or os.environ.get("FEISHU_APP_SECRET")
    if not expected_token or x_task_token != expected_token:
        raise HTTPException(status_code=403, detail="invalid task token")

    from cloud_daily_report import DailyReportError, run_daily_report

    payload = await request.json()
    env_overrides = payload.get("env", {}) if isinstance(payload, dict) else {}
    if not isinstance(env_overrides, dict):
        raise HTTPException(status_code=400, detail="env must be an object")

    allowed_env_keys = {
        "REPORT_DATE",
        "TIDB_HOST",
        "TIDB_PORT",
        "TIDB_USER",
        "TIDB_PASSWORD",
        "TIDB_DATABASE",
        "FEISHU_APP_ID",
        "FEISHU_APP_SECRET",
        "FEISHU_REPORT_RECEIVE_ID",
        "FEISHU_REPORT_RECEIVE_ID_TYPE",
        "LLM_BASE_URL",
        "LLM_MODEL",
        "OBSIDIAN_GITHUB_REPO",
        "OBSIDIAN_GITHUB_BRANCH",
        "OBSIDIAN_GITHUB_TOKEN",
        "OBSIDIAN_REPORT_DIR",
        "DAILY_REPORT_STABILITY_DEADLINE_MINUTES",
        "DAILY_REPORT_STABILITY_INTERVAL_SECONDS",
    }
    sanitized_overrides = {
        key: str(value)
        for key, value in env_overrides.items()
        if key in allowed_env_keys and value not in (None, "")
    }

    try:
        with daily_report_lock:
            return run_daily_report(sanitized_overrides)
    except DailyReportError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        print(f"❌ [云端日报任务失败]: {exc}")
        raise HTTPException(status_code=500, detail="daily report task failed") from exc
    
@app.get("/")
@app.head("/")
def health_check():
    """保留这个根目录用于响应 Render 的存活健康检查"""
    return {"status": "Xavier is alive and WebSocket is connected!"}
