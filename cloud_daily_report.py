import base64
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pymysql
import requests
from dotenv import load_dotenv


TZ = timezone(timedelta(hours=8))
DEFAULT_DOTENV_PATH = "/Users/wangbin/Desktop/智能体/Codex/.env"


load_dotenv(os.environ.get("DAILY_REPORT_ENV_FILE", DEFAULT_DOTENV_PATH), override=False)


class DailyReportError(RuntimeError):
    pass


def env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if required and not value:
        raise DailyReportError(f"缺少必要环境变量: {name}")
    return value


def env_any(names: list[str], default: str | None = None, required: bool = False) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    if required:
        raise DailyReportError(f"缺少必要环境变量: {' 或 '.join(names)}")
    return default


def get_report_date() -> str:
    return env("REPORT_DATE") or datetime.now(TZ).strftime("%Y-%m-%d")


def get_db_connection():
    return pymysql.connect(
        host=env_any(["DB_HOST", "TIDB_HOST"], required=True),
        port=int(env_any(["DB_PORT", "TIDB_PORT"], "4000") or "4000"),
        user=env_any(["DB_USER", "TIDB_USER"], required=True),
        password=env_any(["DB_PASS", "TIDB_PASSWORD"], required=True),
        database=env_any(["DB_NAME", "TIDB_DATABASE"], "test") or "test",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime,)):
        return value.isoformat()
    return str(value)


def normalized_hash(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=json_default)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def fetch_all(cursor, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    cursor.execute(sql, params)
    return cursor.fetchall()


def collect_sales_snapshot(report_date: str) -> dict[str, Any]:
    report_dt = datetime.strptime(report_date, "%Y-%m-%d")
    current_month = report_dt.strftime("%Y年%m月")
    day_col = f"{report_dt.day}日"

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            daily_gsv = fetch_all(
                cursor,
                """
                SELECT `时间`, `店铺`, `品类`, `GSV`, `同期GSV`, `同比`, `目标`, `目标达成率`
                FROM daily_category_gsv
                WHERE DATE(`时间`) = %s
                ORDER BY `店铺`, `品类`
                """,
                (report_date,),
            )
            month_gsv = fetch_all(
                cursor,
                """
                SELECT `月份`, `店铺`, `品类`, `目标`, `合计`
                FROM month_gsv_data
                WHERE `月份` = %s
                ORDER BY `店铺`, `品类`
                """,
                (current_month,),
            )
            kunlun = fetch_all(
                cursor,
                f"""
                SELECT `月份`, `店铺`, `商品id`, `产品名称`, `目标`, `达成率`, `合计`, `{day_col}` AS `今日销量`
                FROM kunlun_sales
                WHERE `月份` = %s
                ORDER BY `店铺`, `产品名称`
                """,
                (current_month,),
            )
            dragons = fetch_all(
                cursor,
                f"""
                SELECT `月份`, `店铺`, `商品id`, `产品名称`, `目标`, `达成率`, `合计`, `{day_col}` AS `今日销量`
                FROM dragons1_sales
                WHERE `月份` = %s
                ORDER BY `店铺`, `产品名称`
                """,
                (current_month,),
            )

        payload = {
            "report_date": report_date,
            "source_queried_at": datetime.now(TZ).isoformat(),
            "current_month": current_month,
            "daily_category_gsv": daily_gsv,
            "month_gsv_data": month_gsv,
            "kunlun_sales": kunlun,
            "dragons1_sales": dragons,
        }
        payload["source_row_count"] = sum(
            len(payload[key])
            for key in ["daily_category_gsv", "month_gsv_data", "kunlun_sales", "dragons1_sales"]
        )
        payload["snapshot_sha256"] = normalized_hash(
            {
                "daily_category_gsv": daily_gsv,
                "month_gsv_data": month_gsv,
                "kunlun_sales": kunlun,
                "dragons1_sales": dragons,
            }
        )
        return payload
    finally:
        conn.close()


def wait_for_stable_snapshot(report_date: str) -> dict[str, Any]:
    deadline_minutes = int(env("DAILY_REPORT_STABILITY_DEADLINE_MINUTES", "15") or "15")
    interval_seconds = int(env("DAILY_REPORT_STABILITY_INTERVAL_SECONDS", "60") or "60")
    deadline = time.time() + max(deadline_minutes, 0) * 60
    previous = collect_sales_snapshot(report_date)

    while time.time() < deadline:
        time.sleep(max(interval_seconds, 1))
        current = collect_sales_snapshot(report_date)
        if current["snapshot_sha256"] == previous["snapshot_sha256"]:
            current["reconciliation"] = {"status": "matched", "checked_at": datetime.now(TZ).isoformat()}
            return current
        previous = current

    previous["reconciliation"] = {
        "status": "deadline_reached",
        "checked_at": datetime.now(TZ).isoformat(),
        "note": "快照在等待窗口内仍有变化，按截止时间生成。",
    }
    return previous


def collect_chat_messages(report_date: str) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            rows = fetch_all(
                cursor,
                """
                SELECT chat_id, sender_id, message_text, create_time
                FROM chat_records
                WHERE DATE(create_time) = %s
                ORDER BY create_time ASC
                """,
                (report_date,),
            )
        return {
            "report_date": report_date,
            "source_queried_at": datetime.now(TZ).isoformat(),
            "source_row_count": len(rows),
            "messages": rows,
        }
    finally:
        conn.close()


def summarize_rows(rows: list[dict[str, Any]], limit: int = 80) -> str:
    if not rows:
        return "[]"
    clipped = rows[:limit]
    suffix = f"\n... 已截断展示 {len(rows) - limit} 行" if len(rows) > limit else ""
    return json.dumps(clipped, ensure_ascii=False, indent=2, default=json_default) + suffix


def call_ai_api(prompt: str) -> str:
    api_key = env_any(["SILICONFLOW_API_KEY", "LLM_API_KEY"], required=True)
    if api_key == "your_llm_api_key":
        raise DailyReportError("LLM_API_KEY 仍是占位符，请在 Render 中配置真实模型 API Key。")
    base_url = (env_any(["SILICONFLOW_BASE_URL", "LLM_BASE_URL"], "https://api.siliconflow.cn/v1") or "").rstrip("/")
    model = env_any(["DAILY_REPORT_MODEL", "LLM_MODEL"], "deepseek-ai/DeepSeek-V3") or "deepseek-ai/DeepSeek-V3"
    response = requests.post(
        f"{base_url}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": "你是严谨的电商运营日报分析师。只基于用户给出的真实 TiDB 数据写日报，不编造。",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
        },
        timeout=90,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"].strip()


def build_report_markdown(sales_snapshot: dict[str, Any], chat_snapshot: dict[str, Any]) -> str:
    report_date = sales_snapshot["report_date"]
    chat_text = "\n".join(
        f"- {row.get('create_time')}: {row.get('message_text')}"
        for row in chat_snapshot.get("messages", [])
        if row.get("message_text")
    )
    if not chat_text:
        chat_text = "暂无明确记录"

    prompt = f"""
请生成一份 Markdown 工作日报，必须严格使用以下结构：

# {report_date} 工作日报

## 一、销售数据

## 二、当天运营动作汇总

## 三、明天工作规划

要求：
1. 只使用下方 TiDB 快照与聊天记录，不得编造不存在的店铺、品类、商品或金额。
2. 金额类数据说明单位为元；昆仑/小京龙销量说明单位为台。
3. 如果聊天记录为空，运营动作写“暂无明确记录”。
4. 保持中文，语气专业、简洁，适合直接发送飞书。
5. 不要输出代码块。

--- TiDB 销售快照元数据 ---
source_queried_at: {sales_snapshot["source_queried_at"]}
source_row_count: {sales_snapshot["source_row_count"]}
snapshot_sha256: {sales_snapshot["snapshot_sha256"]}
reconciliation: {json.dumps(sales_snapshot.get("reconciliation", {}), ensure_ascii=False)}

--- daily_category_gsv ---
{summarize_rows(sales_snapshot["daily_category_gsv"])}

--- month_gsv_data ---
{summarize_rows(sales_snapshot["month_gsv_data"])}

--- kunlun_sales ---
{summarize_rows(sales_snapshot["kunlun_sales"])}

--- dragons1_sales ---
{summarize_rows(sales_snapshot["dragons1_sales"])}

--- 当天聊天记录 ---
{chat_text}
"""
    markdown = call_ai_api(prompt)
    if not markdown.startswith(f"# {report_date} 工作日报"):
        markdown = f"# {report_date} 工作日报\n\n{markdown}"
    return markdown.rstrip() + "\n"


def get_tenant_access_token() -> str:
    response = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        headers={"Content-Type": "application/json; charset=utf-8"},
        json={
            "app_id": env("FEISHU_APP_ID", required=True),
            "app_secret": env("FEISHU_APP_SECRET", required=True),
        },
        timeout=30,
    )
    response.raise_for_status()
    token = response.json().get("tenant_access_token")
    if not token:
        raise DailyReportError(f"获取飞书 tenant_access_token 失败: {response.text}")
    return token


def send_feishu_message(text: str) -> dict[str, Any]:
    chat_id = env_any(["DAILY_REPORT_CHAT_ID", "FEISHU_REPORT_RECEIVE_ID", "FEISHU_CHAT_ID"], required=True)
    token = get_tenant_access_token()
    response = requests.post(
        "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"},
        json={"receive_id": chat_id, "msg_type": "text", "content": json.dumps({"text": text}, ensure_ascii=False)},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("code") != 0:
        raise DailyReportError(f"飞书发送失败: {data}")
    return data


def github_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {env('OBSIDIAN_GITHUB_TOKEN', required=True)}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def write_obsidian_markdown(report_date: str, markdown: str) -> dict[str, Any]:
    repo = env("OBSIDIAN_GITHUB_REPO", required=True)
    branch = env("OBSIDIAN_GITHUB_BRANCH", "main") or "main"
    report_dir = (env("OBSIDIAN_REPORT_DIR", "工作日报") or "工作日报").strip("/")
    path = f"{report_dir}/{report_date}_工作日报.md"
    url = f"https://api.github.com/repos/{repo}/contents/{requests.utils.quote(path)}"
    headers = github_headers()
    get_response = requests.get(url, headers=headers, params={"ref": branch}, timeout=30)

    sha = None
    if get_response.status_code == 200:
        sha = get_response.json().get("sha")
    elif get_response.status_code != 404:
        raise DailyReportError(f"读取 Obsidian GitHub 文件失败: {get_response.status_code} {get_response.text}")

    payload = {
        "message": f"Update daily report {report_date}",
        "content": base64.b64encode(markdown.encode("utf-8")).decode("ascii"),
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha

    put_response = requests.put(url, headers=headers, json=payload, timeout=30)
    if put_response.status_code not in (200, 201):
        raise DailyReportError(f"写入 Obsidian GitHub 文件失败: {put_response.status_code} {put_response.text}")

    verify_response = requests.get(url, headers=headers, params={"ref": branch}, timeout=30)
    verify_response.raise_for_status()
    content = base64.b64decode(verify_response.json()["content"]).decode("utf-8")
    if content != markdown:
        raise DailyReportError("Obsidian GitHub 文件写入后复核不一致，已停止飞书发送。")

    return {"repo": repo, "branch": branch, "path": path, "commit": put_response.json().get("commit", {})}


def ensure_run_table() -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_report_runs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    report_date DATE NOT NULL,
                    report_sha256 CHAR(64) NOT NULL,
                    snapshot_sha256 CHAR(64) NOT NULL,
                    obsidian_path VARCHAR(255) NOT NULL,
                    feishu_message_id VARCHAR(128),
                    status VARCHAR(32) NOT NULL,
                    error_text TEXT,
                    created_at DATETIME NOT NULL,
                    UNIQUE KEY uniq_report_hash (report_date, report_sha256)
                )
                """
            )
        conn.commit()
    finally:
        conn.close()


def already_sent(report_date: str, report_sha256: str) -> bool:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id FROM daily_report_runs
                WHERE report_date = %s AND report_sha256 = %s AND status = 'sent'
                LIMIT 1
                """,
                (report_date, report_sha256),
            )
            return cursor.fetchone() is not None
    finally:
        conn.close()


def record_run(
    report_date: str,
    report_sha256: str,
    snapshot_sha256: str,
    obsidian_path: str,
    status: str,
    feishu_message_id: str | None = None,
    error_text: str | None = None,
) -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO daily_report_runs
                    (report_date, report_sha256, snapshot_sha256, obsidian_path, feishu_message_id, status, error_text, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    snapshot_sha256 = VALUES(snapshot_sha256),
                    obsidian_path = VALUES(obsidian_path),
                    feishu_message_id = VALUES(feishu_message_id),
                    status = VALUES(status),
                    error_text = VALUES(error_text),
                    created_at = VALUES(created_at)
                """,
                (
                    report_date,
                    report_sha256,
                    snapshot_sha256,
                    obsidian_path,
                    feishu_message_id,
                    status,
                    error_text,
                    datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def maybe_sync_from_feishu() -> None:
    if (env("DAILY_REPORT_RUN_SYNC_FIRST", "false") or "false").lower() not in {"1", "true", "yes"}:
        return
    from main import execute_full_sync

    if not execute_full_sync():
        raise DailyReportError("日报前置飞书多维表格同步失败。")


def run_daily_report() -> dict[str, Any]:
    report_date = get_report_date()
    ensure_run_table()
    maybe_sync_from_feishu()

    sales_snapshot = wait_for_stable_snapshot(report_date)
    if sales_snapshot["source_row_count"] <= 0:
        raise DailyReportError("sales_metrics 等价快照为空，已停止生成和发送，避免使用旧数据或伪数据。")

    chat_snapshot = collect_chat_messages(report_date)
    markdown = build_report_markdown(sales_snapshot, chat_snapshot)
    report_sha256 = hashlib.sha256(markdown.encode("utf-8")).hexdigest()

    obsidian_result = write_obsidian_markdown(report_date, markdown)
    if already_sent(report_date, report_sha256) and (env("FORCE_DAILY_REPORT_SEND", "false") or "false").lower() not in {"1", "true", "yes"}:
        return {
            "status": "skipped",
            "reason": "same report already sent",
            "report_date": report_date,
            "report_sha256": report_sha256,
            "snapshot_sha256": sales_snapshot["snapshot_sha256"],
            "obsidian": obsidian_result,
        }

    try:
        feishu_result = send_feishu_message(markdown)
        message_id = feishu_result.get("data", {}).get("message_id")
        record_run(
            report_date,
            report_sha256,
            sales_snapshot["snapshot_sha256"],
            obsidian_result["path"],
            "sent",
            feishu_message_id=message_id,
        )
        return {
            "status": "sent",
            "report_date": report_date,
            "report_sha256": report_sha256,
            "snapshot_sha256": sales_snapshot["snapshot_sha256"],
            "obsidian": obsidian_result,
            "feishu": {"message_id": message_id},
        }
    except Exception as exc:
        record_run(
            report_date,
            report_sha256,
            sales_snapshot["snapshot_sha256"],
            obsidian_result["path"],
            "obsidian_written_feishu_failed",
            error_text=str(exc),
        )
        raise


def main() -> int:
    try:
        result = run_daily_report()
        print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))
        return 0
    except Exception as exc:
        print(f"日报任务失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
