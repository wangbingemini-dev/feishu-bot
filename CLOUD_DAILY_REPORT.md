# 云端日报部署说明

本项目新增 `cloud_daily_report.py`，用于在 Render Cron Job 中每天自动执行：

1. 从 TiDB 读取当天销售快照和聊天记录。
2. 等待快照稳定后生成 Markdown 日报。
3. 先写入 Obsidian Vault 对应的 GitHub 仓库并复核内容。
4. 写入成功后发送飞书消息。
5. 在 TiDB 的 `daily_report_runs` 表中记录发送状态，避免同一份日报重复发送。

## Render Cron Job

`render.yaml` 已包含一个 Cron Job：

```yaml
schedule: "30 14 * * *"
startCommand: python cloud_daily_report.py
```

Render Cron 使用 UTC 时间；`30 14 * * *` 对应北京时间每天 22:30。

## 必要环境变量

Web Service 和 Cron Job 都需要：

- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `SILICONFLOW_API_KEY`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASS`
- `DB_NAME`

Cron Job 额外需要：

- `DAILY_REPORT_CHAT_ID`: 日报接收群的 `chat_id`
- `OBSIDIAN_GITHUB_TOKEN`: 有目标 Obsidian 仓库 contents 读写权限的 GitHub token
- `OBSIDIAN_GITHUB_REPO`: Obsidian Vault 仓库，例如 `owner/obsidian-vault`
- `OBSIDIAN_GITHUB_BRANCH`: 默认 `main`
- `OBSIDIAN_REPORT_DIR`: 默认 `工作日报`

可选：

- `REPORT_DATE`: 手动补跑指定日期，格式 `YYYY-MM-DD`
- `FORCE_DAILY_REPORT_SEND`: 设为 `true` 时允许同一份日报重复发送
- `DAILY_REPORT_RUN_SYNC_FIRST`: 设为 `true` 时，日报前先调用现有飞书多维表格同步
- `DAILY_REPORT_STABILITY_DEADLINE_MINUTES`: 默认 `15`
- `DAILY_REPORT_STABILITY_INTERVAL_SECONDS`: 默认 `60`

## 手动本地验证

```bash
python -m py_compile main.py cloud_daily_report.py
REPORT_DATE=2026-06-27 python cloud_daily_report.py
```

如果缺少任何密钥或目标仓库配置，脚本会失败并停止，不会生成伪数据，也不会发送飞书。
