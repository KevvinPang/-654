from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any

import requests
import urllib3

try:
    import winreg
except ImportError:  # pragma: no cover - Windows only helper
    winreg = None

try:
    from pywinauto import Desktop
    from pywinauto.keyboard import send_keys
except Exception:  # pragma: no cover - optional desktop automation
    Desktop = None
    send_keys = None


MODULE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_ROOT.parent
BAIDU_DOWNLOADER_PATH = MODULE_ROOT / "baidu_share_downloader" / "baidu_share_downloader.py"
CREATE_NO_WINDOW = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
LOCAL_CLIENT_BASE_URL = "https://localhost.pan.baidu.com:10000"
CLIENT_INVOKER_SOURCE = "wp-download_web_share"
CLIENT_INVOKER_TYPE = "web_sharelink_page"
CLIENT_LOCAL_SOURCE = "web_https"
COMMON_DOWNLOAD_DIR_NAMES = ("BaiduNetdiskDownload", "百度网盘下载", "Downloads", "下载", "Download")
DOWNLOAD_MOVE_TIMEOUT_SECONDS = 8 * 60 * 60
DOWNLOAD_MOVE_POLL_SECONDS = 6
DOWNLOAD_STABLE_POLLS_REQUIRED = 2
DOWNLOAD_CANDIDATE_LOOKBACK_SECONDS = 15 * 60
CLIENT_UI_CONFIRM_TIMEOUT_SECONDS = 20
CLIENT_UI_CONFIRM_POLL_SECONDS = 1
CLIENT_UI_CONFIRM_TITLE_TOKENS = ("下载", "传输", "保存", "提示", "确认", "百度网盘")
CLIENT_UI_CONFIRM_BUTTON_TOKENS = (
    "下载",
    "立即下载",
    "开始下载",
    "确定",
    "确认",
    "继续",
    "保存",
    "加入传输",
    "转存并下载",
)
CLIENT_UI_CONFIRM_KEYBOARD_TITLE_TOKENS = ("下载", "保存", "提示", "确认")
REGISTRY_ROOTS = []
if winreg is not None:
    REGISTRY_ROOTS = [
        winreg.HKEY_CURRENT_USER,
        winreg.HKEY_LOCAL_MACHINE,
    ]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def load_baidu_downloader_module() -> Any:
    spec = importlib.util.spec_from_file_location("server_auto_clip.baidu_share_downloader_runtime", BAIDU_DOWNLOADER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载百度网盘模块：{BAIDU_DOWNLOADER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


BAIDU_DOWNLOADER = load_baidu_downloader_module()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Queue Baidu share files into the official Baidu Netdisk client.")
    parser.add_argument("share_url", help="Baidu share URL that should be transferred and queued.")
    parser.add_argument("--workspace-name", default="", help="Workspace label for logging.")
    parser.add_argument("--preferred-output-dir", default="", help="Expected local output root.")
    parser.add_argument("--target-spec-file", default="", help="JSON file that stores precise target specs.")
    parser.add_argument("--target-name", action="append", default=[], help="Fallback target filename for legacy mode.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve targets and print the queue plan without writing into the client database.")
    parser.add_argument(
        "--handoff-mode",
        choices=("queue", "invoker"),
        default="queue",
        help="queue: transfer to own netdisk and write the official client queue directly; invoker: hand off via browser-triggered client download.",
    )
    return parser.parse_args(argv)


def print_json(tag: str, payload: dict[str, object]) -> None:
    print(tag, json.dumps(payload, ensure_ascii=False))


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def share_referer_url(share_url: str) -> str:
    normalized = str(share_url or "").strip()
    if not normalized:
        return "https://pan.baidu.com/"
    return normalized.split("#", 1)[0]


def share_page_url(share_url: str) -> str:
    referer = share_referer_url(share_url)
    if "#list/path=%2F" in referer:
        return referer
    return f"{referer}#list/path=%2F"


def build_pan_headers(user_agent: str, referer: str, *, with_ajax: bool = False) -> dict[str, str]:
    headers = {
        "User-Agent": str(user_agent or BAIDU_DOWNLOADER.BAIDU_DOWNLOAD_USER_AGENT),
        "Accept": "application/json, text/plain, */*",
        "Referer": share_referer_url(referer),
        "Origin": "https://pan.baidu.com",
    }
    if with_ajax:
        headers["X-Requested-With"] = "XMLHttpRequest"
    return headers


def build_local_client_headers(user_agent: str, referer: str) -> dict[str, str]:
    return {
        "User-Agent": str(user_agent or BAIDU_DOWNLOADER.BAIDU_DOWNLOAD_USER_AGENT),
        "Accept": "application/json, text/plain, */*",
        "Referer": share_referer_url(referer),
        "Origin": "https://pan.baidu.com",
    }


def get_logged_in_user_uk(driver: Any) -> str:
    try:
        value = driver.execute_script(
            """
            const getVal = (key) => window.locals && window.locals.get ? window.locals.get(key) : null;
            const userInfo = getVal('userInfo') || window.yunData?.userInfo || window.locals?.userInfo || null;
            const candidates = [
                getVal('uk'),
                getVal('current_uk'),
                getVal('currentUserUk'),
                getVal('user_uk'),
                userInfo?.uk,
                userInfo?.user_uk,
                window.yunData?.uk,
                window.yunData?.user_uk,
                window.PageData?.uk,
                window.PageData?.user_uk,
                window.context?.uk,
                window.context?.user_uk,
            ];
            for (const item of candidates) {
                const text = String(item ?? '').trim();
                if (text) return text;
            }
            return '';
            """
        )
    except Exception:
        return ""
    return str(value or "").strip()


def try_get_local_client_version(user_agent: str, referer: str) -> dict[str, Any]:
    try:
        response = requests.get(
            f"{LOCAL_CLIENT_BASE_URL}/guanjia",
            params={"method": "getversion", "t": "1"},
            headers=build_local_client_headers(user_agent, referer),
            timeout=5,
            verify=False,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return {}
    if as_int(payload.get("errorno"), -1) != 0:
        return {}
    return payload


def ensure_local_client_service(
    main_exe: Path | None,
    detect_exe: Path | None,
    user_agent: str,
    referer: str,
) -> dict[str, Any]:
    version_payload = try_get_local_client_version(user_agent, referer)
    launched_main_pid = 0
    launched_detect_pid = 0
    if version_payload:
        return {
            "ready": True,
            "version": str(version_payload.get("version") or ""),
            "launched_main_pid": launched_main_pid,
            "launched_detect_pid": launched_detect_pid,
        }

    if detect_exe is not None:
        launched_detect_pid = launch_command([str(detect_exe)], detect_exe.parent)
    if main_exe is not None:
        launched_main_pid = launch_command([str(main_exe)], main_exe.parent)

    deadline = time.time() + 20
    while time.time() < deadline:
        time.sleep(1)
        version_payload = try_get_local_client_version(user_agent, referer)
        if version_payload:
            return {
                "ready": True,
                "version": str(version_payload.get("version") or ""),
                "launched_main_pid": launched_main_pid,
                "launched_detect_pid": launched_detect_pid,
            }

    raise RuntimeError("未能连接到百度网盘客户端本地下载服务，请确认官方客户端已经安装并能正常打开。")


def build_sharedownload_payload(
    session: requests.Session,
    runtime: dict[str, Any],
    share_url: str,
    share_items: list[dict[str, Any]],
    user_agent: str,
) -> dict[str, Any]:
    share_short_id = BAIDU_DOWNLOADER.extract_share_short_id(share_url, runtime)
    sign_data = BAIDU_DOWNLOADER.get_sign_and_timestamp(session, str(runtime.get("bdstoken") or ""), share_short_id)
    request_url = (
        "https://pan.baidu.com/api/sharedownload"
        "?channel=chunlei&clienttype=0&web=1&app_id=250528"
        f"&sign={sign_data['sign']}"
        f"&timestamp={sign_data['timestamp']}"
        f"&bdstoken={runtime['bdstoken']}"
        f"&logid={sign_data['logid']}"
    )
    body = {
        "encrypt": "1",
        "product": "share",
        "uk": str(runtime["share_uk"]),
        "primaryid": str(runtime["shareid"]),
        "fid_list": json.dumps([int(item["fs_id"]) for item in share_items], ensure_ascii=False),
        "path_list": json.dumps([str(item.get("path") or "").strip() for item in share_items], ensure_ascii=False),
    }
    if runtime.get("sekey"):
        body["extra"] = json.dumps({"sekey": runtime["sekey"]}, ensure_ascii=False)
    response = session.post(
        request_url,
        data=body,
        headers=build_pan_headers(user_agent, share_url, with_ajax=True),
        timeout=BAIDU_DOWNLOADER.REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = BAIDU_DOWNLOADER.load_json_response(response)
    if as_int(payload.get("errno"), -1) != 0:
        raise RuntimeError(f"分享下载请求失败：{payload}")
    encrypted_list = str(payload.get("list") or "").strip()
    if not encrypted_list:
        raise RuntimeError("分享下载请求没有返回可交给客户端的文件列表。")
    return {
        "payload": payload,
        "sign_data": sign_data,
        "encrypted_list": encrypted_list,
    }


def fetch_browser_id(session: requests.Session, share_url: str, user_agent: str) -> str:
    response = session.get(
        "https://pan.baidu.com/api/invoker/get",
        params={"t": str(int(time.time() * 1000))},
        headers=build_pan_headers(user_agent, share_url),
        timeout=BAIDU_DOWNLOADER.REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = BAIDU_DOWNLOADER.load_json_response(response)
    browser_id = str(payload.get("browserId") or payload.get("browser_id") or "").strip()
    if not browser_id:
        raise RuntimeError(f"未拿到浏览器会话标识：{payload}")
    return browser_id


def send_invoker_download(
    session: requests.Session,
    *,
    browser_id: str,
    login_uk: str,
    encrypted_list: str,
    share_url: str,
    user_agent: str,
) -> dict[str, Any]:
    download_info = {
        "method": "DownloadShareItems",
        "uk": login_uk,
        "checkuser": False,
        "filelist": encrypted_list,
        "share_url": share_page_url(share_url),
        "src_from": CLIENT_INVOKER_SOURCE,
        "src_type": CLIENT_INVOKER_TYPE,
    }
    response = session.post(
        "https://pan.baidu.com/api/invoker/send",
        data={
            "browserId": browser_id,
            "downloadInfo": json.dumps(download_info, ensure_ascii=False, separators=(",", ":")),
        },
        headers=build_pan_headers(user_agent, share_url, with_ajax=True),
        timeout=BAIDU_DOWNLOADER.REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = BAIDU_DOWNLOADER.load_json_response(response)
    if as_int(payload.get("errno"), -1) != 0:
        raise RuntimeError(f"发送客户端下载接管请求失败：{payload}")
    sequence = str(payload.get("seq") or "").strip()
    if not sequence:
        raise RuntimeError(f"客户端下载接管请求没有返回 sequence：{payload}")
    return {
        "payload": payload,
        "sequence": sequence,
        "download_info": download_info,
    }


def notify_local_client_download(browser_id: str, sequence: str, user_agent: str, share_url: str) -> dict[str, Any]:
    response = requests.get(
        f"{LOCAL_CLIENT_BASE_URL}/downloadpc",
        params=[
            ("browserId", browser_id),
            ("sequence", sequence),
            ("src_from", CLIENT_LOCAL_SOURCE),
            ("t", str(int(time.time() * 1000))),
            ("src_from", CLIENT_INVOKER_SOURCE),
            ("src_type", CLIENT_INVOKER_TYPE),
        ],
        headers=build_local_client_headers(user_agent, share_url),
        timeout=10,
        verify=False,
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except Exception:
        payload = {"raw": (response.text or "").strip()}
    if as_int(payload.get("errorno"), -1) != 0:
        raise RuntimeError(f"本地客户端下载接管失败：{payload}")
    return payload


def normalize_ui_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return re.sub(r"\s+", "", text)


def ui_text_matches(value: Any, tokens: tuple[str, ...]) -> bool:
    normalized = normalize_ui_text(value)
    if not normalized:
        return False
    return any(normalize_ui_text(token) in normalized for token in tokens if token)


def control_type_name(control: Any) -> str:
    try:
        return str(control.element_info.control_type or "").strip()
    except Exception:
        pass
    try:
        return str(control.friendly_class_name() or "").strip()
    except Exception:
        return ""


def control_text(control: Any) -> str:
    try:
        return str(control.window_text() or "").strip()
    except Exception:
        return ""


def get_control_process_id(control: Any) -> int:
    try:
        return int(control.process_id())
    except Exception:
        return 0


def focus_window(control: Any) -> None:
    for action_name in ("restore", "set_focus"):
        action = getattr(control, action_name, None)
        if action is None:
            continue
        try:
            action()
        except Exception:
            continue


def click_control(control: Any) -> bool:
    for action_name in ("invoke", "click_input", "click"):
        action = getattr(control, action_name, None)
        if action is None:
            continue
        try:
            action()
            return True
        except Exception:
            continue
    return False


def iter_client_top_windows() -> list[tuple[str, Any]]:
    if Desktop is None:
        return []
    process_ids = {
        int(item.get("ProcessId") or 0)
        for item in list_running_baidu_processes()
        if int(item.get("ProcessId") or 0) > 0
    }
    if not process_ids:
        return []

    windows: list[tuple[str, Any]] = []
    seen_handles: set[int] = set()
    for backend in ("uia", "win32"):
        try:
            desktop_windows = Desktop(backend=backend).windows()
        except Exception:
            continue
        for window in desktop_windows:
            try:
                handle = int(window.handle)
            except Exception:
                handle = 0
            if handle and handle in seen_handles:
                continue
            if get_control_process_id(window) not in process_ids:
                continue
            if handle:
                seen_handles.add(handle)
            windows.append((backend, window))
    return windows


def iter_client_descendants(window: Any) -> list[Any]:
    descendants: list[Any] = []
    try:
        descendants.extend(window.descendants())
    except Exception:
        return descendants
    return descendants


def confirm_button_priority(text: Any) -> int:
    normalized = normalize_ui_text(text)
    if not normalized:
        return 999
    priority_tokens = (
        "转存并下载",
        "立即下载",
        "开始下载",
        "下载",
        "加入传输",
        "继续",
        "确定",
        "确认",
        "保存",
    )
    for index, token in enumerate(priority_tokens):
        if normalize_ui_text(token) in normalized:
            return index
    return 999


def prepare_share_window_for_download(window: Any) -> dict[str, Any] | None:
    if send_keys is None:
        return None
    window_title = control_text(window)
    normalized = normalize_ui_text(window_title)
    if "资源分享" not in normalized and "分享" not in normalized:
        return None
    focus_window(window)
    try:
        send_keys("^a")
    except Exception:
        return None
    return {
        "action": "select_all",
        "window_title": window_title,
        "button_text": "",
        "control_type": control_type_name(window),
    }


def click_window_relative(window: Any, x_ratio: float, y_ratio: float) -> bool:
    try:
        rect = window.rectangle()
        offset_x = max(1, int(rect.width() * x_ratio))
        offset_y = max(1, int(rect.height() * y_ratio))
    except Exception:
        return False
    for action_name in ("click_input", "click"):
        action = getattr(window, action_name, None)
        if action is None:
            continue
        try:
            action(coords=(offset_x, offset_y))
            return True
        except TypeError:
            try:
                action()
                return True
            except Exception:
                continue
        except Exception:
            continue
    return False


def try_handle_download_storage_dialog(window: Any) -> dict[str, Any] | None:
    window_title = control_text(window)
    normalized = normalize_ui_text(window_title)
    if "设置下载存储路径" not in normalized and "下载存储路径" not in normalized:
        return None
    focus_window(window)
    if not click_window_relative(window, 0.88, 0.86):
        return None
    return {
        "action": "dialog_download_click",
        "window_title": window_title,
        "button_text": "下载",
        "control_type": control_type_name(window),
    }


def try_close_intro_window(window: Any) -> dict[str, Any] | None:
    if send_keys is None:
        return None
    window_title = control_text(window)
    normalized = normalize_ui_text(window_title)
    if "欢迎使用百度网盘" not in normalized and "同步空间" not in normalized:
        return None
    focus_window(window)
    try:
        send_keys("%{F4}")
    except Exception:
        return None
    return {
        "action": "close_intro_window",
        "window_title": window_title,
        "button_text": "",
        "control_type": control_type_name(window),
    }


def try_click_client_confirm_controls(window: Any) -> dict[str, Any] | None:
    window_title = control_text(window)
    candidates = [window]
    candidates.extend(iter_client_descendants(window))
    ranked_controls: list[tuple[int, int, Any, str, str]] = []
    for order_index, control in enumerate(candidates):
        text = control_text(control)
        if not ui_text_matches(text, CLIENT_UI_CONFIRM_BUTTON_TOKENS):
            continue
        control_type = control_type_name(control)
        if control is not window and control_type and "button" not in normalize_ui_text(control_type):
            continue
        ranked_controls.append((confirm_button_priority(text), order_index, control, text, control_type))

    for _, _, control, text, control_type in sorted(ranked_controls, key=lambda item: (item[0], item[1])):
        focus_window(window)
        if click_control(control):
            return {
                "action": "click",
                "window_title": window_title,
                "button_text": text,
                "control_type": control_type,
            }
    return None


def try_keyboard_confirm_window(window: Any) -> dict[str, Any] | None:
    if send_keys is None:
        return None
    window_title = control_text(window)
    if not ui_text_matches(window_title, CLIENT_UI_CONFIRM_KEYBOARD_TITLE_TOKENS):
        return None
    focus_window(window)
    try:
        send_keys("{ENTER}")
    except Exception:
        return None
    return {
        "action": "keyboard_enter",
        "window_title": window_title,
        "button_text": "",
        "control_type": control_type_name(window),
    }


def maybe_confirm_client_download_ui(workspace_name: str) -> dict[str, Any]:
    if Desktop is None:
        return {
            "available": False,
            "confirmed": False,
            "reason": "pywinauto_unavailable",
            "clicked": [],
            "seen_windows": [],
        }

    deadline = time.time() + CLIENT_UI_CONFIRM_TIMEOUT_SECONDS
    clicked: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    prepared_windows: set[int | str] = set()
    confirmed = False
    idle_rounds = 0
    while time.time() < deadline:
        acted_this_round = False
        windows = iter_client_top_windows()
        for _, window in windows:
            window_title = control_text(window)
            normalized_title = normalize_ui_text(window_title)
            if normalized_title:
                seen_titles.add(window_title)
            if window_title and not ui_text_matches(window_title, CLIENT_UI_CONFIRM_TITLE_TOKENS):
                continue
            close_intro_result = try_close_intro_window(window)
            if close_intro_result is not None:
                close_intro_result["workspace_name"] = workspace_name
                clicked.append(close_intro_result)
                print_json("OFFICIAL_CLIENT_UI_CONFIRM", close_intro_result)
                confirmed = True
                acted_this_round = True
                break
            dialog_result = try_handle_download_storage_dialog(window)
            if dialog_result is not None:
                dialog_result["workspace_name"] = workspace_name
                clicked.append(dialog_result)
                print_json("OFFICIAL_CLIENT_UI_CONFIRM", dialog_result)
                confirmed = True
                acted_this_round = True
                break
            try:
                prepare_key: int | str = int(window.handle)
            except Exception:
                prepare_key = window_title or id(window)
            if prepare_key not in prepared_windows:
                prepare_result = prepare_share_window_for_download(window)
                if prepare_result is not None:
                    prepare_result["workspace_name"] = workspace_name
                    clicked.append(prepare_result)
                    prepared_windows.add(prepare_key)
                    print_json("OFFICIAL_CLIENT_UI_PREPARE", prepare_result)
                    acted_this_round = True
                    break
            click_result = try_click_client_confirm_controls(window)
            if click_result is not None:
                click_result["workspace_name"] = workspace_name
                clicked.append(click_result)
                print_json("OFFICIAL_CLIENT_UI_CONFIRM", click_result)
                confirmed = True
                acted_this_round = True
                break
            keyboard_result = try_keyboard_confirm_window(window)
            if keyboard_result is not None:
                keyboard_result["workspace_name"] = workspace_name
                clicked.append(keyboard_result)
                print_json("OFFICIAL_CLIENT_UI_CONFIRM", keyboard_result)
                confirmed = True
                acted_this_round = True
                break
        if acted_this_round:
            idle_rounds = 0
            time.sleep(CLIENT_UI_CONFIRM_POLL_SECONDS)
            continue
        idle_rounds += 1
        if confirmed and idle_rounds >= 2:
            break
        time.sleep(CLIENT_UI_CONFIRM_POLL_SECONDS)

    result = {
        "available": True,
        "confirmed": confirmed,
        "reason": "handled_dialogs" if confirmed else "no_confirm_dialog_detected",
        "clicked": clicked,
        "seen_windows": sorted(seen_titles),
    }
    print_json(
        "OFFICIAL_CLIENT_UI_CONFIRM_SKIP",
        {
            "workspace_name": workspace_name,
            "reason": result["reason"],
            "seen_windows": result["seen_windows"],
        },
    )
    return result


def unique_paths(paths: list[Path]) -> list[Path]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        try:
            key = str(path.resolve()).lower()
        except Exception:
            key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(path)
    return result


def client_users_root(main_exe: Path | None) -> Path | None:
    if main_exe is None:
        return None
    users_root = main_exe.parent / "module" / "BrowserEngine" / "users"
    return users_root if users_root.exists() else None


def iter_transmission_databases(main_exe: Path | None) -> list[Path]:
    users_root = client_users_root(main_exe)
    if users_root is None:
        return []
    databases: list[Path] = []
    for directory in users_root.iterdir():
        db_path = directory / "transmission.db"
        if directory.is_dir() and db_path.exists():
            databases.append(db_path)
    return databases


def read_recent_download_records(main_exe: Path | None, *, limit: int = 400) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for db_path in iter_transmission_databases(main_exe):
        try:
            connection = sqlite3.connect(str(db_path), timeout=5)
            cursor = connection.cursor()
            rows = cursor.execute(
                "select server_path, local_path, file_size, add_time, status "
                "from download_file where local_path != '' order by add_time desc limit ?",
                (max(1, int(limit)),),
            ).fetchall()
            connection.close()
        except Exception:
            continue
        for server_path, local_path, file_size, add_time, status in rows:
            local_path_text = str(local_path or "").strip()
            if not local_path_text:
                continue
            records.append(
                {
                    "db_path": str(db_path),
                    "server_path": str(server_path or "").strip(),
                    "local_path": local_path_text,
                    "file_size": as_int(file_size, 0),
                    "add_time": as_int(add_time, 0),
                    "status": as_int(status, 0),
                }
            )
    records.sort(key=lambda item: (as_int(item.get("add_time"), 0), str(item.get("local_path") or "")), reverse=True)
    return records


def existing_drive_roots(*paths: Path) -> list[Path]:
    wanted_letters: set[str] = {"C", "D", "E"}
    for path in paths:
        drive = str(path.drive or "").strip().upper().replace(":", "")
        if drive:
            wanted_letters.add(drive)
    roots: list[Path] = []
    for letter in sorted(wanted_letters):
        root = Path(f"{letter}:\\")
        if root.exists():
            roots.append(root)
    return roots


def build_local_path_from_share_item(output_dir: Path, share_item: dict[str, Any]) -> Path:
    share_path = str(share_item.get("path") or "").strip()
    filename = str(share_item.get("server_filename") or "download.mp4").strip() or "download.mp4"
    parts = [part for part in PurePosixPath(share_path).parts if part not in {"", "/"}]
    if len(parts) >= 3:
        return output_dir.joinpath(*parts[2:])
    return output_dir / filename


def resolve_local_target_path(item: dict[str, Any], preferred_output_dir: str = "") -> Path | None:
    local_path_text = str(item.get("local_path") or "").strip()
    if local_path_text:
        return Path(local_path_text).expanduser().resolve()
    output_root = str(preferred_output_dir or "").strip()
    if not output_root:
        return None
    return build_local_path_from_share_item(Path(output_root).expanduser().resolve(), dict(item.get("share_item") or {}))


def collect_download_search_roots(
    main_exe: Path | None,
    preferred_output_dir: str,
    move_targets: list[dict[str, Any]],
) -> list[Path]:
    roots: list[Path] = []
    recent_records = read_recent_download_records(main_exe)
    for record in recent_records[:120]:
        local_path = Path(str(record.get("local_path") or "")).expanduser()
        parent = local_path.parent if local_path.suffix else local_path
        for _ in range(3):
            if not str(parent):
                break
            roots.append(parent)
            if parent.parent == parent or not parent.parent.name:
                break
            parent = parent.parent

    home = Path.home()
    for candidate in [home / "Downloads", home / "下载"]:
        if candidate.exists():
            roots.append(candidate)

    preferred_path = Path(str(preferred_output_dir or "")).expanduser() if str(preferred_output_dir or "").strip() else None
    drives = existing_drive_roots(*(list(filter(None, [preferred_path, home])) + [Path(target["target_path"]) for target in move_targets]))
    for drive_root in drives:
        for folder_name in COMMON_DOWNLOAD_DIR_NAMES:
            candidate = drive_root / folder_name
            if candidate.exists():
                roots.append(candidate)

    return unique_paths([path for path in roots if path.exists() and path.is_dir()])


def build_move_targets(selected_items: list[dict[str, Any]], preferred_output_dir: str) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for item in selected_items:
        share_item = dict(item.get("share_item") or {})
        target_path = resolve_local_target_path(item, preferred_output_dir)
        filename = str(share_item.get("server_filename") or item.get("label") or "").strip()
        if not filename or target_path is None:
            continue
        share_path = str(share_item.get("path") or "").strip()
        share_parts = [part.lower() for part in PurePosixPath(share_path).parts if part not in {"", "/"}]
        targets.append(
            {
                "label": str(item.get("label") or filename).strip(),
                "filename": filename,
                "filename_lower": filename.lower(),
                "share_path": share_path,
                "share_parts": share_parts,
                "expected_size": max(as_int(item.get("target_size"), 0), as_int(share_item.get("size"), 0)),
                "target_path": target_path,
            }
        )
    return targets


def destination_ready(target: dict[str, Any]) -> bool:
    path = Path(target["target_path"])
    if not path.exists() or not path.is_file():
        return False
    expected_size = as_int(target.get("expected_size"), 0)
    if expected_size <= 0:
        return path.stat().st_size > 0
    return path.stat().st_size == expected_size


def path_looks_like_current_download(path: Path, handoff_started_at: float) -> bool:
    try:
        stat = path.stat()
    except OSError:
        return False
    newest_local_ts = max(float(stat.st_ctime), float(stat.st_mtime))
    return newest_local_ts >= (handoff_started_at - DOWNLOAD_CANDIDATE_LOOKBACK_SECONDS)


def gather_record_path_candidates(
    target: dict[str, Any],
    recent_records: list[dict[str, Any]],
    handoff_started_at: float,
) -> list[Path]:
    filename = str(target["filename"]).lower()
    share_path = str(target.get("share_path") or "").strip().lower()
    exact_path_candidates: list[Path] = []
    candidates: list[Path] = []
    for record in recent_records:
        local_path = Path(str(record.get("local_path") or "")).expanduser()
        if local_path.name.lower() != filename:
            continue
        server_path = str(record.get("server_path") or "").strip().lower()
        if share_path and server_path == share_path:
            exact_path_candidates.append(local_path)
            continue
        if as_int(record.get("add_time"), 0) < int(handoff_started_at - DOWNLOAD_CANDIDATE_LOOKBACK_SECONDS):
            continue
        candidates.append(local_path)
    return unique_paths(exact_path_candidates + candidates)


def scan_download_roots(roots: list[Path], wanted_names: set[str]) -> dict[str, list[Path]]:
    results: dict[str, list[Path]] = {name: [] for name in wanted_names}
    normalized = {name.lower() for name in wanted_names if name}
    if not normalized:
        return results
    for root in roots:
        try:
            for current_root, _, files in os.walk(root):
                for filename in files:
                    lowered = filename.lower()
                    if lowered not in normalized:
                        continue
                    results.setdefault(lowered, []).append((Path(current_root) / filename).resolve())
        except Exception:
            continue
    for key in list(results.keys()):
        results[key] = unique_paths([path for path in results[key] if path.exists() and path.is_file()])
    return results


def candidate_path_score(
    path: Path,
    target: dict[str, Any],
    handoff_started_at: float,
    record_candidates: set[str],
) -> tuple[int, int, int, float]:
    expected_size = as_int(target.get("expected_size"), 0)
    try:
        stat = path.stat()
    except OSError:
        return (-1, -1, -1, 0.0)
    size_match = 1 if expected_size > 0 and stat.st_size == expected_size else 0
    record_match = 1 if str(path).lower() in record_candidates else 0
    recent_match = 1 if stat.st_mtime >= (handoff_started_at - 600) else 0
    path_text = str(path.parent).lower()
    token_match = sum(1 for token in list(target.get("share_parts") or [])[-4:-1] if token and token in path_text)
    return (record_match, size_match, recent_match + token_match, stat.st_mtime)


def choose_best_download_candidate(
    target: dict[str, Any],
    root_candidates: list[Path],
    record_candidates: list[Path],
    handoff_started_at: float,
) -> Path | None:
    recent_root_candidates = [path for path in root_candidates if path_looks_like_current_download(path, handoff_started_at)]
    candidates = unique_paths(record_candidates + recent_root_candidates)
    if not candidates:
        return None
    record_set = {str(path).lower() for path in record_candidates}
    ranked = sorted(
        candidates,
        key=lambda path: candidate_path_score(path, target, handoff_started_at, record_set),
        reverse=True,
    )
    return ranked[0] if ranked else None


def candidate_is_complete(
    path: Path,
    target: dict[str, Any],
    size_state: dict[str, tuple[int, int]],
) -> bool:
    try:
        current_size = path.stat().st_size
    except OSError:
        return False
    key = str(path).lower()
    previous_size, stable_count = size_state.get(key, (-1, 0))
    stable_count = stable_count + 1 if previous_size == current_size else 1
    size_state[key] = (current_size, stable_count)
    expected_size = as_int(target.get("expected_size"), 0)
    if expected_size > 0:
        return current_size == expected_size and stable_count >= DOWNLOAD_STABLE_POLLS_REQUIRED
    return current_size > 0 and stable_count >= (DOWNLOAD_STABLE_POLLS_REQUIRED + 1)


def move_download_to_target(source_path: Path, target_path: Path) -> dict[str, Any]:
    source = source_path.expanduser().resolve()
    target = target_path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if source == target:
        return {"action": "already_in_place", "source": str(source), "target": str(target)}
    if target.exists():
        try:
            if source.exists() and source.stat().st_size == target.stat().st_size:
                source.unlink(missing_ok=True)
                return {"action": "duplicate_removed", "source": str(source), "target": str(target)}
        except OSError:
            pass
        if target.is_file():
            target.unlink()
    moved_path = Path(shutil.move(str(source), str(target)))
    return {"action": "moved", "source": str(source), "target": str(moved_path.resolve())}


def wait_and_move_client_downloads(
    main_exe: Path | None,
    preferred_output_dir: str,
    selected_items: list[dict[str, Any]],
    *,
    workspace_name: str,
    handoff_started_at: float,
) -> dict[str, Any]:
    move_targets = build_move_targets(selected_items, preferred_output_dir)
    if not move_targets:
        print("OFFICIAL_CLIENT_MOVE_SKIP 没有可用于自动搬运的目标路径，已跳过搬运等待。")
        return {"moved_count": 0, "skipped_count": 0, "roots": []}

    pending: list[dict[str, Any]] = []
    skipped_count = 0
    for target in move_targets:
        if destination_ready(target):
            skipped_count += 1
            print_json(
                "OFFICIAL_CLIENT_MOVE_READY",
                {
                    "workspace_name": workspace_name,
                    "label": target["label"],
                    "target_path": str(target["target_path"]),
                },
            )
            continue
        pending.append(target)

    roots = collect_download_search_roots(main_exe, preferred_output_dir, move_targets)
    print_json(
        "OFFICIAL_CLIENT_MOVE_PLAN",
        {
            "workspace_name": workspace_name,
            "search_roots": [str(path) for path in roots],
            "pending_count": len(pending),
            "skipped_count": skipped_count,
        },
    )
    if not pending:
        return {"moved_count": 0, "skipped_count": skipped_count, "roots": [str(path) for path in roots]}

    size_state: dict[str, tuple[int, int]] = {}
    moved_count = 0
    wait_round = 0
    deadline = time.time() + DOWNLOAD_MOVE_TIMEOUT_SECONDS

    while pending and time.time() < deadline:
        wait_round += 1
        recent_records = read_recent_download_records(main_exe)
        roots = collect_download_search_roots(main_exe, preferred_output_dir, move_targets)
        root_matches = scan_download_roots(roots, {target["filename_lower"] for target in pending})

        next_pending: list[dict[str, Any]] = []
        for target in pending:
            if destination_ready(target):
                skipped_count += 1
                continue
            record_candidates = gather_record_path_candidates(target, recent_records, handoff_started_at)
            source_path = choose_best_download_candidate(
                target,
                root_matches.get(target["filename_lower"], []),
                record_candidates,
                handoff_started_at,
            )
            if source_path is None or not candidate_is_complete(source_path, target, size_state):
                next_pending.append(target)
                continue
            move_result = move_download_to_target(source_path, Path(target["target_path"]))
            moved_count += 1
            print_json(
                "OFFICIAL_CLIENT_MOVED",
                {
                    "workspace_name": workspace_name,
                    "label": target["label"],
                    "source_path": move_result["source"],
                    "target_path": move_result["target"],
                    "action": move_result["action"],
                },
            )

        pending = next_pending
        if not pending:
            break

        if wait_round == 1 or wait_round % 5 == 0:
            print_json(
                "OFFICIAL_CLIENT_WAIT",
                {
                    "workspace_name": workspace_name,
                    "pending_count": len(pending),
                    "pending_labels": [str(item["label"]) for item in pending],
                    "search_root_count": len(roots),
                    "wait_round": wait_round,
                },
            )
        time.sleep(DOWNLOAD_MOVE_POLL_SECONDS)

    if pending:
        root_preview = "、".join(str(path) for path in roots[:8])
        raise RuntimeError(
            "官方客户端下载已经接管，但在监控目录里一直没有发现这次下载完成后的新文件，无法自动搬运到工作间目录："
            + "、".join(str(item["label"]) for item in pending)
            + (f"。当前监控目录：{root_preview}" if root_preview else "")
        )

    return {
        "moved_count": moved_count,
        "skipped_count": skipped_count,
        "roots": [str(path) for path in roots],
    }


def wait_for_client_target_downloads(
    preferred_output_dir: str,
    selected_items: list[dict[str, Any]],
    *,
    workspace_name: str,
) -> dict[str, Any]:
    move_targets = build_move_targets(selected_items, preferred_output_dir)
    if not move_targets:
        print("OFFICIAL_CLIENT_MOVE_SKIP 没有可用于等待下载完成的目标路径，已跳过等待。")
        return {"moved_count": 0, "skipped_count": 0, "roots": []}

    pending: list[dict[str, Any]] = []
    skipped_count = 0
    for target in move_targets:
        if destination_ready(target):
            skipped_count += 1
            print_json(
                "OFFICIAL_CLIENT_MOVE_READY",
                {
                    "workspace_name": workspace_name,
                    "label": target["label"],
                    "target_path": str(target["target_path"]),
                },
            )
            continue
        pending.append(target)

    roots = unique_paths(
        [Path(str(preferred_output_dir)).expanduser().resolve()] if str(preferred_output_dir or "").strip() else []
        + [Path(target["target_path"]).parent.resolve() for target in move_targets]
    )
    print_json(
        "OFFICIAL_CLIENT_MOVE_PLAN",
        {
            "workspace_name": workspace_name,
            "search_roots": [str(path) for path in roots],
            "pending_count": len(pending),
            "skipped_count": skipped_count,
        },
    )
    if not pending:
        return {"moved_count": 0, "skipped_count": skipped_count, "roots": [str(path) for path in roots]}

    size_state: dict[str, tuple[int, int]] = {}
    moved_count = 0
    wait_round = 0
    deadline = time.time() + DOWNLOAD_MOVE_TIMEOUT_SECONDS

    while pending and time.time() < deadline:
        wait_round += 1
        next_pending: list[dict[str, Any]] = []
        for target in pending:
            target_path = Path(target["target_path"])
            if not target_path.exists() or not target_path.is_file() or not candidate_is_complete(target_path, target, size_state):
                next_pending.append(target)
                continue
            moved_count += 1
            print_json(
                "OFFICIAL_CLIENT_MOVED",
                {
                    "workspace_name": workspace_name,
                    "label": target["label"],
                    "source_path": str(target_path),
                    "target_path": str(target_path),
                    "action": "downloaded_in_place",
                },
            )

        pending = next_pending
        if not pending:
            break

        if wait_round == 1 or wait_round % 5 == 0:
            print_json(
                "OFFICIAL_CLIENT_WAIT",
                {
                    "workspace_name": workspace_name,
                    "pending_count": len(pending),
                    "pending_labels": [str(item["label"]) for item in pending],
                    "search_root_count": len(roots),
                    "wait_round": wait_round,
                },
            )
        time.sleep(DOWNLOAD_MOVE_POLL_SECONDS)

    if pending:
        root_preview = "、".join(str(path) for path in roots[:8])
        raise RuntimeError(
            "官方客户端下载队列已经写入，但目标目录里一直没有看到这些文件下载完成："
            + "、".join(str(item["label"]) for item in pending)
            + (f"。当前监控目录：{root_preview}" if root_preview else "")
        )

    return {
        "moved_count": moved_count,
        "skipped_count": skipped_count,
        "roots": [str(path) for path in roots],
    }


def execute_invoker_handoff(
    args: argparse.Namespace,
    driver: Any,
    session: requests.Session,
    user_agent: str,
    runtime: dict[str, Any],
    selected_items: list[dict[str, Any]],
    main_exe: Path | None,
    detect_exe: Path | None,
) -> int:
    service_info = ensure_local_client_service(main_exe, detect_exe, user_agent, args.share_url)
    print_json(
        "OFFICIAL_CLIENT_SERVICE",
        {
            "workspace_name": args.workspace_name,
            "client_version": str(service_info.get("version") or ""),
            "launched_main_pid": int(service_info.get("launched_main_pid") or 0),
            "launched_detect_pid": int(service_info.get("launched_detect_pid") or 0),
            "dry_run": bool(args.dry_run),
        },
    )
    if args.dry_run:
        print_json(
            "OFFICIAL_CLIENT_QUEUED",
            {
                "workspace_name": args.workspace_name,
                "queued_count": len(selected_items),
                "skipped_count": 0,
                "browser_login_uk": "",
                "client_version": str(service_info.get("version") or ""),
                "launched_main_pid": int(service_info.get("launched_main_pid") or 0),
                "launched_detect_pid": int(service_info.get("launched_detect_pid") or 0),
                "running_processes": list_running_baidu_processes(),
                "dry_run": True,
                "client_controls_download_dir": True,
            },
        )
        print("OFFICIAL_CLIENT_NOTE 已完成官方客户端接管下载的预演，尚未真正发起下载。")
        return 0

    login_uk = get_logged_in_user_uk(driver)
    if not login_uk:
        raise RuntimeError("未能识别当前登录百度账号的 uk，请重新打开登录窗口确认已经登录成功。")
    print_json(
        "OFFICIAL_CLIENT_ACCOUNT",
        {
            "workspace_name": args.workspace_name,
            "login_uk": login_uk,
            "share_uk": str(runtime.get("share_uk") or ""),
        },
    )

    share_items = [dict(item["share_item"]) for item in selected_items]
    sharedownload_result = build_sharedownload_payload(session, runtime, args.share_url, share_items, user_agent)
    print_json(
        "OFFICIAL_CLIENT_SHAREDOWNLOAD",
        {
            "workspace_name": args.workspace_name,
            "file_count": len(share_items),
            "encrypted_list_length": len(str(sharedownload_result["encrypted_list"])),
            "sign_timestamp": str(sharedownload_result["sign_data"].get("timestamp") or ""),
        },
    )

    browser_id = fetch_browser_id(session, args.share_url, user_agent)
    invoker_result = send_invoker_download(
        session,
        browser_id=browser_id,
        login_uk=login_uk,
        encrypted_list=str(sharedownload_result["encrypted_list"]),
        share_url=args.share_url,
        user_agent=user_agent,
    )
    print_json(
        "OFFICIAL_CLIENT_INVOKER",
        {
            "workspace_name": args.workspace_name,
            "browser_id": browser_id,
            "sequence": invoker_result["sequence"],
        },
    )

    handoff_started_at = time.time()
    local_client_result = notify_local_client_download(
        browser_id,
        str(invoker_result["sequence"]),
        user_agent,
        args.share_url,
    )
    ui_confirm_result = maybe_confirm_client_download_ui(args.workspace_name)
    move_result = wait_and_move_client_downloads(
        main_exe,
        args.preferred_output_dir,
        selected_items,
        workspace_name=args.workspace_name,
        handoff_started_at=handoff_started_at,
    )
    print_json(
        "OFFICIAL_CLIENT_QUEUED",
        {
            "workspace_name": args.workspace_name,
            "queued_count": len(selected_items),
            "skipped_count": 0,
            "browser_login_uk": login_uk,
            "client_version": str(service_info.get("version") or ""),
            "browser_id": browser_id,
            "sequence": str(invoker_result["sequence"]),
            "local_client_result": local_client_result,
            "ui_confirm_result": ui_confirm_result,
            "launched_main_pid": int(service_info.get("launched_main_pid") or 0),
            "launched_detect_pid": int(service_info.get("launched_detect_pid") or 0),
            "moved_count": int(move_result.get("moved_count") or 0),
            "move_skipped_count": int(move_result.get("skipped_count") or 0),
            "move_search_roots": move_result.get("roots") or [],
            "running_processes": list_running_baidu_processes(),
            "dry_run": False,
            "client_controls_download_dir": True,
        },
    )
    print("OFFICIAL_CLIENT_NOTE 已把选中的分享视频交给百度网盘官方客户端继续下载；如果客户端弹出确认/下载窗口，脚本会尝试自动确认，然后继续等待下载完成并自动搬运到当前工作间目录。")
    return 0


def execute_queue_handoff(
    args: argparse.Namespace,
    driver: Any,
    session: requests.Session,
    runtime: dict[str, Any],
    selected_items: list[dict[str, Any]],
    main_exe: Path | None,
    current_user_uk: str = "",
) -> int:
    if args.dry_run:
        client_user_dir = detect_client_user_dir(main_exe, current_user_uk) if main_exe else None
        print_json(
            "OFFICIAL_CLIENT_QUEUED",
            {
                "workspace_name": args.workspace_name,
                "client_user_dir": str(client_user_dir) if client_user_dir else "",
                "database_path": str(client_user_dir / "transmission.db") if client_user_dir else "",
                "queued_count": len(selected_items),
                "skipped_count": 0,
                "launched_pid": 0,
                "running_processes": list_running_baidu_processes(),
                "dry_run": True,
                "client_controls_download_dir": False,
            },
        )
        print("OFFICIAL_CLIENT_NOTE 已完成官方客户端下载队列模式的预演，未实际转存，也未写入客户端下载队列。")
        return 0

    access_token = BAIDU_DOWNLOADER.get_access_token(session)
    if not access_token:
        access_token = BAIDU_DOWNLOADER.get_access_token_in_browser(driver)
    if not access_token:
        raise RuntimeError("未能获取百度网盘授权，请重新登录百度专用窗口后再试。")

    transferred_queue: list[dict[str, Any]] = []
    used_root_paths: set[str] = set()
    for item in selected_items:
        share_item = dict(item["share_item"])
        transfer = BAIDU_DOWNLOADER.transfer_to_own_netdisk(session, runtime, share_item, args.share_url)
        if int(transfer.get("errno") or 0) != 0:
            raise RuntimeError(f"转存失败：{transfer}")

        root_items = BAIDU_DOWNLOADER.get_root_file_list(session, access_token)
        own_file = find_transferred_file(root_items, share_item, used_root_paths)
        dlink = BAIDU_DOWNLOADER.get_own_file_dlink(session, access_token, int(own_file["fs_id"]))
        local_path = str(item["local_path"] or "").strip()
        if not local_path:
            raise RuntimeError(f"未收到本地下载路径：{item['label']}")
        resolved_local_path = str(Path(local_path).expanduser().resolve())
        transferred_queue.append(
            {
                "label": item["label"],
                "server_path": str(own_file.get("path") or "").strip(),
                "local_path": resolved_local_path,
                "file_size": int(own_file.get("size") or share_item.get("size") or 0),
                "md5": str(own_file.get("md5") or share_item.get("md5") or "").strip(),
                "download_url": dlink,
            }
        )
        print_json(
            "OFFICIAL_CLIENT_TRANSFER",
            {
                "label": item["label"],
                "share_path": str(share_item.get("path") or ""),
                "own_path": str(own_file.get("path") or ""),
                "local_path": resolved_local_path,
            },
        )

    if main_exe is None:
        raise RuntimeError("未检测到官方百度网盘主程序，无法写入客户端下载队列。")

    client_user_dir = detect_client_user_dir(main_exe, current_user_uk)
    database_path = client_user_dir / "transmission.db"
    print_json(
        "OFFICIAL_CLIENT_QUEUE_DB",
        {
            "workspace_name": args.workspace_name,
            "client_user_dir": str(client_user_dir),
            "database_path": str(database_path),
            "current_user_uk": current_user_uk,
        },
    )
    queue_result = enqueue_client_downloads(database_path, transferred_queue, dry_run=args.dry_run)

    running_before = list_running_baidu_processes()
    launched_pid = 0
    if int(queue_result.get("queued_count") or 0) > 0 and not running_before:
        launched_pid = launch_command([str(main_exe)], main_exe.parent)
        time.sleep(2)

    wait_result = wait_for_client_target_downloads(
        args.preferred_output_dir,
        selected_items,
        workspace_name=args.workspace_name,
    )
    running_processes = list_running_baidu_processes()
    print_json(
        "OFFICIAL_CLIENT_QUEUED",
        {
            "workspace_name": args.workspace_name,
            "client_user_dir": str(client_user_dir),
            "database_path": str(database_path),
            "queued_count": int(queue_result.get("queued_count") or 0),
            "skipped_count": int(queue_result.get("skipped_count") or 0),
            "launched_pid": launched_pid,
            "running_processes": running_processes,
            "dry_run": False,
            "client_controls_download_dir": False,
            "moved_count": int(wait_result.get("moved_count") or 0),
            "move_skipped_count": int(wait_result.get("skipped_count") or 0),
            "move_search_roots": wait_result.get("roots") or [],
        },
    )
    print("OFFICIAL_CLIENT_NOTE 已自动转存并写入官方百度网盘客户端下载队列，客户端会直接下载到目标目录，脚本会等待下载完成。")
    return 0


def parse_executable_from_command(raw_command: str) -> Path | None:
    command = str(raw_command or "").strip()
    if not command:
        return None
    if command.startswith('"'):
        match = re.match(r'"([^"]+)"', command)
        if match:
            candidate = Path(match.group(1))
            return candidate if candidate.exists() else None
    first_token = command.split(" ", 1)[0].strip()
    if first_token:
        candidate = Path(first_token)
        if candidate.exists():
            return candidate
    return None


def iter_registry_candidates() -> list[Path]:
    candidates: list[Path] = []
    if winreg is None:
        return candidates

    registry_paths = [
        r"Software\Classes\Baiduyunguanjia\shell\open\command",
        r"Software\Classes\Baiduyunguanjia",
        r"Software\Classes\baiduyunguanjia\shell\open\command",
        r"Software\Classes\baiduyunguanjia",
    ]
    for root in REGISTRY_ROOTS:
        for subkey in registry_paths:
            try:
                with winreg.OpenKey(root, subkey) as key:
                    raw_value, _ = winreg.QueryValueEx(key, "")
                    candidate = parse_executable_from_command(str(raw_value))
                    if candidate is not None:
                        candidates.append(candidate)
            except OSError:
                continue
    return candidates


def iter_filesystem_candidates() -> list[Path]:
    paths = [
        Path(r"D:\BaiduNetdisk\BaiduNetdisk.exe"),
        Path(r"D:\BaiduNetdisk\YunDetectService.exe"),
        Path(r"C:\Program Files\BaiduNetdisk\BaiduNetdisk.exe"),
        Path(r"C:\Program Files\BaiduNetdisk\YunDetectService.exe"),
        Path(r"C:\Program Files (x86)\BaiduNetdisk\BaiduNetdisk.exe"),
        Path(r"C:\Program Files (x86)\BaiduNetdisk\YunDetectService.exe"),
    ]
    return [path for path in paths if path.exists()]


def dedupe_paths(paths: list[Path]) -> list[Path]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        normalized = str(path.resolve()).lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(path.resolve())
    return result


def detect_official_client() -> tuple[Path | None, Path | None]:
    registry_candidates = iter_registry_candidates()
    filesystem_candidates = iter_filesystem_candidates()
    candidates = dedupe_paths(registry_candidates + filesystem_candidates)

    main_exe: Path | None = None
    detect_exe: Path | None = None
    for path in candidates:
        lowered = path.name.lower()
        if lowered == "baidunetdisk.exe" and main_exe is None:
            main_exe = path
        elif lowered == "yundetectservice.exe" and detect_exe is None:
            detect_exe = path
            sibling = path.with_name("BaiduNetdisk.exe")
            if sibling.exists() and main_exe is None:
                main_exe = sibling.resolve()
    return main_exe, detect_exe


def launch_command(command: list[str], cwd: Path) -> int:
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=CREATE_NO_WINDOW,
    )
    return process.pid


def list_running_baidu_processes() -> list[dict[str, Any]]:
    if os.name != "nt":
        return []
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            (
                "Get-CimInstance Win32_Process | "
                "Where-Object { $_.Name -match 'BaiduNetdisk|BaiduNetdiskUnite|YunDetectService|baidunetdiskhost' } | "
                "Select-Object Name,ProcessId,ExecutablePath,CommandLine | ConvertTo-Json -Compress"
            ),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        creationflags=CREATE_NO_WINDOW,
    )
    payload = (result.stdout or "").strip() or "[]"
    try:
        data = json.loads(payload)
    except Exception:
        return []
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def load_target_specs(path_text: str, fallback_names: list[str]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    raw_path = str(path_text or "").strip()
    if raw_path:
        spec_path = Path(raw_path)
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
        for item in payload.get("targets") or []:
            if isinstance(item, dict):
                specs.append(dict(item))
    if not specs:
        for name in fallback_names:
            normalized = str(name or "").strip()
            if normalized:
                specs.append({"label": normalized, "target_filename": normalized})

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in specs:
        key = (
            str(item.get("target_fsid") or "").strip()
            or str(item.get("target_path") or "").strip()
            or str(item.get("target_filename") or "").strip()
            or str(item.get("label") or "").strip()
        )
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def select_target_from_spec(files: list[dict[str, Any]], spec: dict[str, Any]) -> dict[str, Any]:
    target_fsid = str(spec.get("target_fsid") or "").strip()
    if target_fsid:
        for item in files:
            if str(item.get("fs_id") or "").strip() == target_fsid:
                return item

    target_path = str(spec.get("target_path") or "").strip()
    if target_path:
        for item in files:
            if str(item.get("path") or "").strip() == target_path:
                return item

    target_filename = str(spec.get("target_filename") or spec.get("label") or "").strip()
    if target_filename:
        lowered = target_filename.lower()
        for item in files:
            if str(item.get("server_filename") or "").strip().lower() == lowered:
                return item

    raise RuntimeError(f"未在分享链接里找到目标文件：{target_filename or target_path or target_fsid}")


def detect_client_user_dir(main_exe: Path, user_uk: str = "") -> Path:
    users_root = main_exe.parent / "module" / "BrowserEngine" / "users"
    if not users_root.exists():
        raise RuntimeError(f"未找到百度网盘客户端用户目录：{users_root}")

    candidates = [item for item in users_root.iterdir() if item.is_dir() and (item / "transmission.db").exists()]
    if not candidates:
        raise RuntimeError(f"未找到百度网盘客户端下载数据库：{users_root}")

    def token_hit_score(directory: Path) -> int:
        if not user_uk:
            return 0
        score = 0
        for name in ("recentcache.db-wal", "recentcache.db", "transmission.db"):
            path = directory / name
            if not path.exists() or path.stat().st_size > 5 * 1024 * 1024:
                continue
            try:
                data = path.read_bytes()
            except OSError:
                continue
            if user_uk.encode("utf-8") in data or user_uk.encode("utf-16le") in data:
                score += 10
        return score

    def freshness_score(directory: Path) -> float:
        score = 0.0
        for name in ("recentcache.db-wal", "recentcache.db", "transmission.db"):
            path = directory / name
            if path.exists():
                score = max(score, path.stat().st_mtime)
        return score

    selected = max(candidates, key=lambda item: (freshness_score(item), token_hit_score(item), item.name))
    return selected


def find_transferred_file(root_items: list[dict[str, Any]], target_item: dict[str, Any], used_paths: set[str]) -> dict[str, Any]:
    expected_name = str(target_item.get("server_filename") or "").strip()
    expected_stem = Path(expected_name).stem.lower()
    expected_size = int(target_item.get("size") or 0)
    expected_md5 = str(target_item.get("md5") or "").strip().lower()

    candidates: list[dict[str, Any]] = []
    for item in root_items:
        if int(item.get("isdir") or 0) == 1:
            continue
        item_path = str(item.get("path") or "").strip()
        if item_path in used_paths:
            continue
        item_name = str(item.get("server_filename") or "").strip()
        item_stem = Path(item_name).stem.lower()
        if item_name == expected_name:
            candidates.append(item)
            continue
        item_size = int(item.get("size") or 0)
        item_md5 = str(item.get("md5") or "").strip().lower()
        if expected_size > 0 and item_size == expected_size:
            if expected_md5 and item_md5 and item_md5 == expected_md5:
                candidates.append(item)
                continue
            if item_stem == expected_stem or item_stem.startswith(expected_stem):
                candidates.append(item)
                continue
        if item_stem.startswith(expected_stem):
            candidates.append(item)

    if not candidates:
        raise RuntimeError(f"转存后未在我的网盘根目录里找到文件：{expected_name}")

    candidates.sort(key=lambda item: (int(item.get("server_mtime") or 0), int(item.get("size") or 0)), reverse=True)
    selected = candidates[0]
    used_paths.add(str(selected.get("path") or "").strip())
    return selected


def build_remote_parent(path_text: str) -> str:
    normalized = str(path_text or "").strip() or "/"
    parent = str(PurePosixPath(normalized).parent)
    return parent if parent and parent != "." else "/"


def enqueue_client_downloads(database_path: Path, queue_items: list[dict[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    queue_preview = [
        {
            "server_path": item["server_path"],
            "local_path": item["local_path"],
            "file_size": item["file_size"],
        }
        for item in queue_items
    ]
    if dry_run:
        return {"queued_count": len(queue_items), "skipped_count": 0, "preview": queue_preview}

    connection = sqlite3.connect(str(database_path), timeout=30)
    try:
        cursor = connection.cursor()
        existing_keys = {
            (str(server_path or "").strip(), str(local_path or "").strip())
            for server_path, local_path in cursor.execute("select server_path, local_path from download_file").fetchall()
        }
        max_task_id = int(cursor.execute("select coalesce(max(task_id), 0) from download_file").fetchone()[0] or 0)
        base_task_id = max(int(time.time()), max_task_id + 1)
        add_time = int(time.time())
        batch_id = uuid.uuid4().hex

        queued_count = 0
        skipped_count = 0
        insert_sql = (
            "insert into download_file ("
            "task_id, server_path, local_path, status, file_size, complete_size, isdir, error_code, "
            "add_time, status_changetime, download_url, cmd_type, priority, md5, context, server_root_path, "
            "batch_id, trans_id, root_trans_id, reserved1, reserved2, reserved3, reserved4, reserved5"
            ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        )

        for offset, item in enumerate(queue_items):
            server_path = str(item["server_path"]).strip()
            local_path = str(item["local_path"]).strip()
            if (server_path, local_path) in existing_keys:
                skipped_count += 1
                continue

            Path(local_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
            cursor.execute(
                insert_sql,
                (
                    base_task_id + offset,
                    server_path,
                    local_path,
                    1,
                    int(item.get("file_size") or 0),
                    0,
                    0,
                    0,
                    add_time,
                    None,
                    str(item.get("download_url") or "").strip(),
                    1,
                    0,
                    str(item.get("md5") or "").strip(),
                    "",
                    build_remote_parent(server_path),
                    batch_id,
                    str(uuid.uuid4()),
                    "",
                    1,
                    0,
                    "",
                    "",
                    "",
                ),
            )
            queued_count += 1

        connection.commit()
        return {"queued_count": queued_count, "skipped_count": skipped_count, "preview": queue_preview}
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    main_exe, detect_exe = detect_official_client()
    target_specs = load_target_specs(args.target_spec_file, args.target_name)

    print_json(
        "OFFICIAL_CLIENT_DETECTED",
        {
            "workspace_name": args.workspace_name,
            "preferred_output_dir": args.preferred_output_dir,
            "main_exe": str(main_exe) if main_exe else "",
            "detect_exe": str(detect_exe) if detect_exe else "",
            "target_count": len(target_specs),
            "dry_run": bool(args.dry_run),
        },
    )

    if main_exe is None and detect_exe is None:
        print("OFFICIAL_CLIENT_ERROR 未检测到官方百度网盘客户端，请先安装客户端。")
        return 1
    if not target_specs:
        print("OFFICIAL_CLIENT_ERROR 没有收到要转存和排队下载的目标文件。")
        return 1

    driver = BAIDU_DOWNLOADER.build_driver(
        str(BAIDU_DOWNLOADER.DEFAULT_EDGE_BINARY),
        str(BAIDU_DOWNLOADER.DEFAULT_EDGE_USER_DATA),
        BAIDU_DOWNLOADER.DEFAULT_EDGE_PROFILE,
    )
    temp_profile_context = getattr(driver, "_temp_profile_context", None)
    try:
        runtime = BAIDU_DOWNLOADER.fetch_runtime_info(driver, args.share_url)
        session, user_agent = BAIDU_DOWNLOADER.build_session(driver)
        login_state = {
            "logged_in": BAIDU_DOWNLOADER.session_has_baidu_login_cookie(session),
            "cookie_names": BAIDU_DOWNLOADER.session_baidu_cookie_names(session),
            "launch_mode": getattr(driver, "_profile_launch_mode", "unknown"),
            "user_agent": user_agent,
            "current_user_uk": get_logged_in_user_uk(driver),
            "share_uk": str(runtime.get("share_uk") or ""),
        }
        print_json("OFFICIAL_CLIENT_LOGIN", login_state)
        if not login_state["logged_in"]:
            raise RuntimeError("百度专用登录窗口还没有登录，请先在控制台里点击“登录百度”完成登录。")

        file_list = BAIDU_DOWNLOADER.get_share_list(session, runtime)
        share_files = [
            item
            for item in file_list
            if int(item.get("isdir") or 0) == 0
            and BAIDU_DOWNLOADER.supported_share_file_kind(item)
        ]
        if not share_files:
            raise RuntimeError("未在分享链接中找到可下载的文件。")

        selected_items: list[dict[str, Any]] = []
        for spec in target_specs:
            target_item = select_target_from_spec(share_files, spec)
            selected_items.append(
                {
                    "label": str(spec.get("label") or target_item.get("server_filename") or "").strip(),
                    "share_item": target_item,
                    "local_path": str(spec.get("local_path") or "").strip(),
                    "target_size": as_int(spec.get("target_size"), 0),
                }
            )
        print_json(
            "OFFICIAL_CLIENT_TARGETS",
            {
                "workspace_name": args.workspace_name,
                "count": len(selected_items),
                "targets": [
                    {
                        "label": item["label"],
                        "share_path": str(item["share_item"].get("path") or ""),
                        "local_path": item["local_path"],
                    }
                    for item in selected_items
                ],
            },
        )
        print_json(
            "OFFICIAL_CLIENT_HANDOFF_MODE",
            {
                "workspace_name": args.workspace_name,
                "mode": args.handoff_mode,
            },
        )
        if args.handoff_mode == "invoker":
            return execute_invoker_handoff(
                args,
                driver,
                session,
                user_agent,
                runtime,
                selected_items,
                main_exe,
                detect_exe,
            )
        return execute_queue_handoff(
            args,
            driver,
            session,
            runtime,
            selected_items,
            main_exe,
            str(login_state.get("current_user_uk") or ""),
        )
    finally:
        driver.quit()
        if temp_profile_context is not None:
            temp_profile_context.cleanup()


if __name__ == "__main__":
    sys.exit(main())
