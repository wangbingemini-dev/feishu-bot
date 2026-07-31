import ast
import unittest
from pathlib import Path

from runtime_flags import env_flag


class FeishuChatWebSocketToggleTests(unittest.TestCase):
    def test_chat_websocket_is_disabled_by_default(self):
        self.assertFalse(env_flag("ENABLE_FEISHU_CHAT_WS", environ={}))

    def test_chat_websocket_can_be_explicitly_enabled(self):
        for value in ("1", "true", "TRUE", " yes "):
            with self.subTest(value=value):
                self.assertTrue(
                    env_flag(
                        "ENABLE_FEISHU_CHAT_WS",
                        environ={"ENABLE_FEISHU_CHAT_WS": value},
                    )
                )

    def test_chat_websocket_stays_disabled_for_other_values(self):
        for value in ("0", "false", "no", ""):
            with self.subTest(value=value):
                self.assertFalse(
                    env_flag(
                        "ENABLE_FEISHU_CHAT_WS",
                        environ={"ENABLE_FEISHU_CHAT_WS": value},
                    )
                )

    def test_startup_keeps_bitable_to_tidb_sync_enabled(self):
        source_path = Path(__file__).resolve().parents[1] / "main.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        startup = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "on_startup"
        )
        startup_source = ast.unparse(startup)
        self.assertIn(
            "scheduler.add_job(execute_full_sync, 'interval', hours=2)",
            startup_source,
        )
        self.assertIn("scheduler.start()", startup_source)
        self.assertIn("start_feishu_chat_ws_if_enabled()", startup_source)

    def test_render_explicitly_disables_the_old_chat_websocket(self):
        render_path = Path(__file__).resolve().parents[1] / "render.yaml"
        render_text = render_path.read_text(encoding="utf-8")
        self.assertIn("key: ENABLE_FEISHU_CHAT_WS", render_text)
        self.assertIn('value: "false"', render_text)

    def test_health_check_reports_chat_and_sync_status_separately(self):
        source_path = Path(__file__).resolve().parents[1] / "main.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        health_check = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "health_check"
        )
        health_source = ast.unparse(health_check)
        self.assertIn("'legacy_chat_websocket_enabled'", health_source)
        self.assertIn("'bitable_tidb_sync_enabled': True", health_source)


if __name__ == "__main__":
    unittest.main()
