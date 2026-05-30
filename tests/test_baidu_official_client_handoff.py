import sys
import types
import unittest
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import selenium  # noqa: F401
except ModuleNotFoundError:
    selenium_module = types.ModuleType("selenium")
    webdriver_module = types.ModuleType("selenium.webdriver")
    edge_module = types.ModuleType("selenium.webdriver.edge")
    options_module = types.ModuleType("selenium.webdriver.edge.options")
    support_module = types.ModuleType("selenium.webdriver.support")
    ui_module = types.ModuleType("selenium.webdriver.support.ui")

    class FakeOptions:
        pass

    class FakeEdge:
        pass

    class FakeWebDriverWait:
        pass

    webdriver_module.Edge = FakeEdge
    options_module.Options = FakeOptions
    ui_module.WebDriverWait = FakeWebDriverWait
    selenium_module.webdriver = webdriver_module
    webdriver_module.edge = edge_module
    edge_module.options = options_module
    support_module.ui = ui_module

    sys.modules.setdefault("selenium", selenium_module)
    sys.modules.setdefault("selenium.webdriver", webdriver_module)
    sys.modules.setdefault("selenium.webdriver.edge", edge_module)
    sys.modules.setdefault("selenium.webdriver.edge.options", options_module)
    sys.modules.setdefault("selenium.webdriver.support", support_module)
    sys.modules.setdefault("selenium.webdriver.support.ui", ui_module)

from modules import baidu_official_client_handoff as handoff


class BaiduOfficialClientHandoffTests(unittest.TestCase):
    def test_account_uk_helpers_ignore_zero_and_extract_client_uk(self):
        self.assertEqual(handoff.normalize_account_uk("0"), "")
        self.assertEqual(handoff.normalize_account_uk("4095174782"), "4095174782")
        self.assertEqual(
            handoff.extract_uk_from_command_line(
                r'"D:\BaiduNetdisk\module\BrowserEngine\baidunetdiskhost.exe" --plugin_id=1000 --uk=4095174782'
            ),
            "4095174782",
        )

    def test_ensure_local_client_service_launches_main_when_service_is_ready_but_client_is_not_running(self):
        main_exe = Path(r"D:\BaiduNetdisk\BaiduNetdisk.exe")
        detect_exe = Path(r"D:\BaiduNetdisk\YunDetectService.exe")

        with mock.patch.object(handoff, "try_get_local_client_version", return_value={"version": "8.3.11.105"}), \
            mock.patch.object(handoff, "list_running_baidu_main_processes", return_value=[]), \
            mock.patch.object(handoff, "launch_command", return_value=4321) as launch_command:
            result = handoff.ensure_local_client_service(
                main_exe,
                detect_exe,
                "ua",
                "https://pan.baidu.com/s/abc",
            )

        self.assertTrue(result["ready"])
        self.assertEqual(result["version"], "8.3.11.105")
        self.assertEqual(result["launched_main_pid"], 4321)
        self.assertEqual(result["launched_detect_pid"], 0)
        launch_command.assert_called_once_with([str(main_exe)], main_exe.parent)

    def test_ensure_local_client_service_keeps_dry_run_side_effect_free(self):
        main_exe = Path(r"D:\BaiduNetdisk\BaiduNetdisk.exe")
        detect_exe = Path(r"D:\BaiduNetdisk\YunDetectService.exe")

        with mock.patch.object(handoff, "try_get_local_client_version", return_value={"version": "8.3.11.105"}), \
            mock.patch.object(handoff, "list_running_baidu_main_processes", return_value=[]), \
            mock.patch.object(handoff, "launch_command") as launch_command:
            result = handoff.ensure_local_client_service(
                main_exe,
                detect_exe,
                "ua",
                "https://pan.baidu.com/s/abc",
                launch_if_missing=False,
            )

        self.assertTrue(result["ready"])
        self.assertEqual(result["version"], "8.3.11.105")
        self.assertEqual(result["launched_main_pid"], 0)
        self.assertEqual(result["launched_detect_pid"], 0)
        launch_command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
