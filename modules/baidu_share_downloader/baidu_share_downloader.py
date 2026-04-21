import argparse
import base64
import concurrent.futures
import json
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait


AUTHORIZE_URL = (
    "https://openapi.baidu.com/oauth/2.0/authorize"
    "?response_type=token&scope=basic,netdisk"
    "&client_id=omiOnr2tYnN9vSyDErcVFWpPU2mZA7YO"
    "&redirect_uri=oob&confirm_login=0"
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EDGE_BINARY = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Microsoft" / "Edge" / "Application" / "msedge.exe"
SYSTEM_EDGE_USER_DATA = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))) / "Microsoft" / "Edge" / "User Data"
DEFAULT_EDGE_PROFILE = "Default"
DEFAULT_DOWNLOAD_DIR = PROJECT_ROOT / "runtime" / "baidu_downloads"
EDGE_RUNTIME_ROOT = PROJECT_ROOT / "runtime" / "edge_profiles"
DEFAULT_EDGE_USER_DATA = EDGE_RUNTIME_ROOT / "baidu_login_profile"
REQUEST_TIMEOUT = 30
BAIDU_LOGIN_COOKIE_NAMES = {"BDUSS", "BDUSS_BFESS"}
BAIDU_DOWNLOAD_USER_AGENT = "pan.baidu.com"
SUPPORTED_VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".avi", ".m4v"}
SUPPORTED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
SINGLE_DOWNLOAD_READ_TIMEOUT = 60
SEGMENT_DOWNLOAD_READ_TIMEOUT = 20
SINGLE_DOWNLOAD_PROGRESS_INTERVAL = 2 * 1024 * 1024
SEGMENT_DOWNLOAD_PROGRESS_INTERVAL = 2 * 1024 * 1024

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception:
    pass

EDGE_PROFILE_SKIP_DIRS = {
    "Cache",
    "Code Cache",
    "GPUCache",
    "GrShaderCache",
    "ShaderCache",
    "DawnCache",
    "DawnGraphiteCache",
    "Crashpad",
    "Service Worker",
    "Session Storage",
    "Storage",
    "BrowserMetrics",
    "OptimizationGuidePredictionModels",
}
EDGE_PROFILE_SKIP_FILES = {
    "LOCK",
    "SingletonCookie",
    "SingletonLock",
    "SingletonSocket",
    "Last Browser",
    "Last Version",
}


def query_sqlite_rows(database_path: Path, sql: str) -> list[tuple]:
    if not database_path.exists():
        return []

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix="baidu_sqlite_", suffix=".db", delete=False) as handle:
            temp_path = Path(handle.name)
        shutil.copy2(database_path, temp_path)
        connection = sqlite3.connect(str(temp_path))
        try:
            return connection.execute(sql).fetchall()
        finally:
            connection.close()
    except Exception:
        try:
            uri = "file:" + database_path.as_posix() + "?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
            try:
                return connection.execute(sql).fetchall()
            finally:
                connection.close()
        except Exception:
            return []
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def get_baidu_cookie_names(user_data_root: Path, edge_profile: str) -> list[str]:
    profile_root = user_data_root / edge_profile
    cookie_names: set[str] = set()
    for cookie_db in [profile_root / "Network" / "Cookies", profile_root / "Cookies"]:
        rows = query_sqlite_rows(
            cookie_db,
            "select distinct name from cookies where host_key like '%baidu%' order by name",
        )
        for row in rows:
            if row and row[0]:
                cookie_names.add(str(row[0]))
    return sorted(cookie_names)


def has_baidu_login_cookie(user_data_root: Path, edge_profile: str) -> bool:
    cookie_names = get_baidu_cookie_names(user_data_root, edge_profile)
    return any(name in BAIDU_LOGIN_COOKIE_NAMES for name in cookie_names)


def sync_edge_profile(source_root: Path, edge_profile: str, destination_root: Path) -> None:
    destination_root.mkdir(parents=True, exist_ok=True)
    safe_copy_file(source_root / "Local State", destination_root / "Local State")
    clone_profile_tree(source_root / edge_profile, destination_root / edge_profile)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download a Baidu Netdisk share file using the current Edge login session.")
    parser.add_argument("share_url", help="Baidu share link, for example: https://pan.baidu.com/s/xxxx?pwd=xxxx")
    parser.add_argument("--output-dir", default=str(DEFAULT_DOWNLOAD_DIR), help="Directory where the downloaded file will be saved.")
    parser.add_argument("--target-filename", default="", help="Exact filename to download, such as 7.mp4. Default: smallest mp4 in the share.")
    parser.add_argument("--target-path", default="", help="Exact share path to download, for example: /sharelink.../folder/7.mp4")
    parser.add_argument("--target-fsid", default="", help="Exact Baidu fs_id to download.")
    parser.add_argument("--edge-binary", default=str(DEFAULT_EDGE_BINARY), help="Path to msedge.exe.")
    parser.add_argument("--edge-user-data", default=str(DEFAULT_EDGE_USER_DATA), help="Edge user data directory.")
    parser.add_argument("--edge-profile", default=DEFAULT_EDGE_PROFILE, help="Edge profile name, usually Default.")
    parser.add_argument("--download-threads", type=int, default=4, help="Segment threads for a single file download.")
    parser.add_argument("--list-only", action="store_true", help="List available mp4 files and exit without downloading.")
    return parser.parse_args(argv)


def safe_copy_file(source: Path, destination: Path) -> None:
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    except Exception:
        pass


def clone_profile_tree(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    for root_text, dir_names, file_names in os.walk(source):
        root = Path(root_text)
        dir_names[:] = [name for name in dir_names if name not in EDGE_PROFILE_SKIP_DIRS]
        relative = root.relative_to(source)
        target_root = destination / relative
        target_root.mkdir(parents=True, exist_ok=True)
        for file_name in file_names:
            if file_name in EDGE_PROFILE_SKIP_FILES:
                continue
            safe_copy_file(root / file_name, target_root / file_name)


def prepare_edge_user_data(edge_user_data: str, edge_profile: str) -> tuple[str, tempfile.TemporaryDirectory[str] | None]:
    source_root = Path(edge_user_data)
    source_profile = source_root / edge_profile
    if not source_root.exists() or not source_profile.exists():
        return edge_user_data, None

    EDGE_RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
    temp_context = tempfile.TemporaryDirectory(prefix="baidu_edge_", dir=str(EDGE_RUNTIME_ROOT))
    temp_root = Path(temp_context.name)
    safe_copy_file(source_root / "Local State", temp_root / "Local State")
    clone_profile_tree(source_profile, temp_root / edge_profile)
    return str(temp_root), temp_context


def ensure_managed_edge_user_data(edge_profile: str = DEFAULT_EDGE_PROFILE) -> Path:
    managed_root = Path(DEFAULT_EDGE_USER_DATA)
    managed_profile = managed_root / edge_profile
    source_root = SYSTEM_EDGE_USER_DATA
    source_profile = source_root / edge_profile
    managed_root.mkdir(parents=True, exist_ok=True)
    managed_has_login = has_baidu_login_cookie(managed_root, edge_profile)
    system_has_login = has_baidu_login_cookie(source_root, edge_profile)

    if not managed_profile.exists():
        if source_profile.exists():
            sync_edge_profile(source_root, edge_profile, managed_root)
        managed_profile.mkdir(parents=True, exist_ok=True)
        return managed_root

    if not managed_has_login and system_has_login and source_profile.exists():
        sync_edge_profile(source_root, edge_profile, managed_root)
    return managed_root


def list_edge_processes(target_text: str | None = None) -> list[dict]:
    if os.name != "nt":
        return []

    if target_text:
        escaped = str(target_text).replace("'", "''")
        where_clause = f'| Where-Object {{ $_.CommandLine -like "*{escaped}*" }}'
    else:
        where_clause = ""
    script = f"""
$items = Get-CimInstance Win32_Process -Filter "Name='msedge.exe'" {where_clause} |
  Select-Object ProcessId, ParentProcessId, CommandLine
if ($items) {{
  $items | ConvertTo-Json -Compress
}} else {{
  '[]'
}}
"""
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=20,
        check=False,
        creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
    )
    if result.returncode != 0:
        return []

    output = (result.stdout or "").strip()
    if not output:
        return []
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def list_edge_processes_for_user_data(user_data_root: Path) -> list[dict]:
    return list_edge_processes(str(user_data_root))


def collect_edge_root_process_ids(user_data_root: Path) -> list[int]:
    matched_processes = list_edge_processes_for_user_data(user_data_root)
    if not matched_processes:
        return []

    all_edge_processes = {
        int(item.get("ProcessId") or 0): item
        for item in list_edge_processes()
        if int(item.get("ProcessId") or 0) > 0
    }
    root_ids: set[int] = set()
    for item in matched_processes:
        process_id = int(item.get("ProcessId") or 0)
        if process_id <= 0:
            continue
        root_id = process_id
        seen: set[int] = set()
        while root_id and root_id not in seen:
            seen.add(root_id)
            parent_id = int((all_edge_processes.get(root_id) or {}).get("ParentProcessId") or 0)
            if parent_id <= 0 or parent_id not in all_edge_processes:
                break
            root_id = parent_id
        root_ids.add(root_id)
    return sorted(root_ids)


def terminate_edge_processes_for_user_data(user_data_root: Path) -> list[int]:
    if os.name != "nt":
        return []

    terminated_ids: list[int] = []
    for _ in range(3):
        targets = collect_edge_root_process_ids(user_data_root)
        if not targets:
            break
        for process_id in targets:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(process_id)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=20,
                check=False,
                creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
            )
        terminated_ids.extend(targets)
        time.sleep(1)
    return sorted({process_id for process_id in terminated_ids if process_id > 0})


def launch_driver(edge_binary: str, edge_user_data: str, edge_profile: str) -> webdriver.Edge:
    options = Options()
    options.binary_location = str(edge_binary)
    options.add_argument(f"--user-data-dir={str(edge_user_data)}")
    options.add_argument(f"--profile-directory={str(edge_profile)}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-features=msEdgeTranslate")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--remote-debugging-port=0")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-background-networking")
    options.add_argument("--hide-crash-restore-bubble")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-component-extensions-with-background-pages")
    options.add_argument("--log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    return webdriver.Edge(options=options)


def launch_cloned_driver(edge_binary: str, edge_user_data: str, edge_profile: str) -> webdriver.Edge:
    runtime_user_data, temp_context = prepare_edge_user_data(edge_user_data, edge_profile)
    if temp_context is None:
        raise RuntimeError("unable to prepare cloned Edge profile")
    try:
        driver = launch_driver(edge_binary, runtime_user_data, edge_profile)
    except Exception:
        temp_context.cleanup()
        raise
    setattr(driver, "_temp_profile_context", temp_context)
    setattr(driver, "_profile_launch_mode", "cloned")
    return driver


def build_driver(edge_binary: str, edge_user_data: str, edge_profile: str) -> webdriver.Edge:
    if Path(edge_user_data).resolve() == Path(DEFAULT_EDGE_USER_DATA).resolve():
        edge_user_data = str(ensure_managed_edge_user_data(edge_profile))

    active_processes = list_edge_processes_for_user_data(Path(edge_user_data))
    if active_processes:
        for _ in range(3):
            terminated_ids = terminate_edge_processes_for_user_data(Path(edge_user_data))
            if terminated_ids:
                print("EDGE_PROFILE_CLEANUP", json.dumps({"terminated": terminated_ids}, ensure_ascii=False))
            time.sleep(1)
            active_processes = list_edge_processes_for_user_data(Path(edge_user_data))
            if not active_processes:
                break
        if active_processes:
            raise RuntimeError("百度专用登录窗口仍在运行，请先关闭相关窗口后再重试。")

    try:
        driver = launch_driver(edge_binary, edge_user_data, edge_profile)
        setattr(driver, "_temp_profile_context", None)
        setattr(driver, "_profile_launch_mode", "direct")
        return driver
    except Exception as direct_error:
        active_processes = list_edge_processes_for_user_data(Path(edge_user_data))
        if active_processes:
            raise RuntimeError("百度专用登录窗口还在运行，请先关闭那个窗口，再重新提取或开始处理。") from direct_error
        try:
            return launch_cloned_driver(edge_binary, edge_user_data, edge_profile)
        except Exception as cloned_error:
            raise cloned_error from direct_error


def fetch_runtime_info(driver: webdriver.Edge, share_url: str) -> dict:
    driver.get(share_url)

    def ready(drv: webdriver.Edge) -> bool:
        try:
            title = drv.title or ""
            if "请输入提取码" in title:
                return False
            share_id = drv.execute_script(
                """
                const getVal = (key) => window.locals && window.locals.get ? window.locals.get(key) : null;
                return getVal('shareid') || window.yunData?.shareid || null;
                """
            )
            return bool(share_id)
        except Exception:
            return False

    WebDriverWait(driver, 45).until(ready)
    time.sleep(2)

    return driver.execute_script(
        """
        const getVal = (key) => window.locals && window.locals.get ? window.locals.get(key) : null;
        return {
            title: document.title,
            href: location.href,
            shareid: getVal('shareid') || window.yunData?.shareid || null,
            share_uk: getVal('share_uk') || window.yunData?.share_uk || null,
            bdstoken: getVal('bdstoken') || window.yunData?.bdstoken || '',
            jsToken: window.jsToken || null,
            sekey: window.currentSekey || window.cache?.list?.config?.params?.sekey || null,
            file_list: getVal('file_list') || [],
        };
        """
    )


def build_session(driver: webdriver.Edge) -> tuple[requests.Session, str]:
    session = requests.Session()
    user_agent = driver.execute_script("return navigator.userAgent")
    session.headers.update({"User-Agent": user_agent, "Accept": "*/*"})
    for cookie in driver.get_cookies():
        session.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain"), path=cookie.get("path", "/"))
    return session, user_agent


def clone_session(session: requests.Session) -> requests.Session:
    cloned = requests.Session()
    cloned.headers.update(session.headers)
    cloned.cookies.update(session.cookies)
    return cloned


def build_download_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "User-Agent": BAIDU_DOWNLOAD_USER_AGENT,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Referer": "https://pan.baidu.com/",
    }
    if extra:
        headers.update(extra)
    return headers


def describe_http_exception(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if response is None:
        return str(exc)

    content_type = str(response.headers.get("Content-Type") or "").strip()
    snippet = ""
    try:
        text = (response.text or "").strip()
        if text:
            snippet = text[:240].replace("\r", " ").replace("\n", " ")
    except Exception:
        snippet = ""

    if snippet:
        return f"{exc} | content_type={content_type} | body={snippet}"
    if content_type:
        return f"{exc} | content_type={content_type}"
    return str(exc)


def load_json_response(response: requests.Response) -> dict:
    tried: list[str] = []
    for encoding in [
        response.encoding,
        response.apparent_encoding,
        "utf-8",
        "utf-8-sig",
        "gb18030",
        "gbk",
        "latin-1",
    ]:
        if not encoding or encoding in tried:
            continue
        tried.append(encoding)
        try:
            return json.loads(response.content.decode(encoding))
        except Exception:
            continue
    return response.json()


def select_target_file(files: list[dict], target_filename: str, target_path: str = "", target_fsid: str = "") -> dict:
    if target_fsid:
        target_fsid_text = str(target_fsid).strip()
        for item in files:
            if str(item.get("fs_id", "")).strip() == target_fsid_text:
                return item
        raise RuntimeError(f"target fs_id not found: {target_fsid_text}")
    if target_path:
        target_path_text = str(target_path).strip()
        for item in files:
            if str(item.get("path", "")).strip() == target_path_text:
                return item
        raise RuntimeError(f"target path not found: {target_path_text}")
    if target_filename:
        lowered = target_filename.lower()
        for item in files:
            if str(item.get("server_filename", "")).lower() == lowered:
                return item
        raise RuntimeError(f"target file not found: {target_filename}")
    return min(files, key=lambda item: item.get("size") or 0)


def supported_share_file_kind(file_item: dict) -> str:
    suffix = Path(str(file_item.get("server_filename") or "")).suffix.lower()
    if suffix in SUPPORTED_VIDEO_EXTENSIONS:
        return "video"
    if suffix in SUPPORTED_IMAGE_EXTENSIONS:
        return "image"
    return ""


def fetch_share_dir_page(session: requests.Session, runtime: dict, dir_path: str, page: int = 1, num: int = 200) -> list[dict]:
    list_url = (
        "https://pan.baidu.com/share/list?"
        + urlencode(
            {
                "uk": str(runtime["share_uk"]),
                "shareid": str(runtime["shareid"]),
                "order": "other",
                "desc": "1",
                "showempty": "0",
                "clienttype": "0",
                "page": str(page),
                "num": str(num),
                "dir": dir_path,
            }
        )
    )
    response = session.get(list_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = load_json_response(response)
    if payload.get("errno") != 0:
        raise RuntimeError(f"share/list failed: {payload}")
    return payload.get("list") or []


def get_share_list(session: requests.Session, runtime: dict) -> list[dict]:
    root_item = runtime["file_list"][0]
    root_path = str(root_item["path"])
    all_items: list[dict] = []
    queue: list[str] = [root_path]
    seen_dirs: set[str] = set()
    while queue:
        current_dir = queue.pop(0)
        if current_dir in seen_dirs:
            continue
        seen_dirs.add(current_dir)
        page = 1
        while True:
            page_items = fetch_share_dir_page(session, runtime, current_dir, page=page, num=200)
            if not page_items:
                break
            all_items.extend(page_items)
            for item in page_items:
                if int(item.get("isdir") or 0) == 1:
                    child_path = str(item.get("path") or "").strip()
                    if child_path and child_path not in seen_dirs:
                        queue.append(child_path)
            if len(page_items) < 200:
                break
            page += 1
    return all_items


def build_output_path(output_dir: Path, file_item: dict, root_dir: str) -> Path:
    server_filename = str(file_item.get("server_filename") or "download.mp4").strip() or "download.mp4"
    share_path = str(file_item.get("path") or "").strip()
    root_dir = str(root_dir or "").rstrip("/")
    if not share_path:
        return output_dir / server_filename
    relative_path = share_path
    if root_dir and share_path.startswith(root_dir):
        relative_path = share_path[len(root_dir):]
    relative_path = relative_path.lstrip("/").replace("/", os.sep)
    if not relative_path:
        relative_path = server_filename
    return output_dir / relative_path


def extract_share_short_id(share_url: str, runtime: dict | None = None) -> str:
    candidates: list[str] = []
    if isinstance(runtime, dict):
        candidates.extend(
            [
                str(runtime.get("shorturl") or "").strip(),
                str(runtime.get("surl") or "").strip(),
            ]
        )
        href = str(runtime.get("href") or "").strip()
        if href:
            candidates.append(href)
    candidates.append(str(share_url or "").strip())

    for candidate in candidates:
        if not candidate:
            continue
        parsed = urlparse(candidate)
        query = parse_qs(parsed.query)
        for key in ("surl", "shorturl"):
            value = str((query.get(key) or [""])[0]).strip()
            if value:
                return value
        match = re.search(r"/s/([^/?#]+)", parsed.path or candidate)
        if match:
            return str(match.group(1)).strip()
    raise RuntimeError(f"could not determine share short id from url: {share_url}")


def get_sign_and_timestamp(session: requests.Session, bdstoken: str, share_short_id: str) -> dict:
    baiduid = session.cookies.get("BAIDUID", domain=".baidu.com") or session.cookies.get("BAIDUID")
    if not baiduid:
        raise RuntimeError("BAIDUID cookie not found")
    logid = base64.b64encode(baiduid.encode("utf-8")).decode("ascii")
    sign_url = (
        "https://pan.baidu.com/share/tplconfig"
        "?fields=sign,timestamp&channel=chunlei&web=1&app_id=250528&clienttype=0&view_mode=1"
        f"&surl={share_short_id}"
        f"&bdstoken={bdstoken}"
        f"&logid={logid}"
    )
    response = session.get(sign_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = load_json_response(response)
    if payload.get("errno") != 0:
        raise RuntimeError(f"tplconfig failed: {payload}")
    data = payload["data"]
    data["logid"] = logid
    return data


def try_sharedownload_with_requests(
    session: requests.Session,
    runtime: dict,
    file_item: dict,
    sign_data: dict,
    share_url: str,
) -> dict:
    url = (
        "https://pan.baidu.com/api/sharedownload"
        "?channel=chunlei&clienttype=0&web=1&app_id=250528"
        f"&sign={sign_data['sign']}"
        f"&timestamp={sign_data['timestamp']}"
        f"&bdstoken={runtime['bdstoken']}"
        f"&logid={sign_data['logid']}"
    )
    body = {
        "encrypt": "0",
        "product": "share",
        "uk": str(runtime["share_uk"]),
        "primaryid": str(runtime["shareid"]),
        "fid_list": json.dumps([file_item["fs_id"]]),
    }
    if runtime.get("sekey"):
        body["extra"] = json.dumps({"sekey": runtime["sekey"]})
    response = session.post(
        url,
        data=body,
        headers={"Referer": share_url, "X-Requested-With": "XMLHttpRequest"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return load_json_response(response)


def try_sharedownload_in_browser(driver: webdriver.Edge, runtime: dict, file_item: dict, sign_data: dict) -> dict:
    script = """
    const done = arguments[arguments.length - 1];
    const runtime = arguments[0];
    const fileItem = arguments[1];
    const signData = arguments[2];
    const url = new URL('https://pan.baidu.com/api/sharedownload');
    url.searchParams.set('channel', 'chunlei');
    url.searchParams.set('clienttype', '0');
    url.searchParams.set('web', '1');
    url.searchParams.set('app_id', '250528');
    url.searchParams.set('sign', signData.sign);
    url.searchParams.set('timestamp', String(signData.timestamp));
    url.searchParams.set('bdstoken', runtime.bdstoken || '');
    url.searchParams.set('logid', signData.logid);

    const body = new URLSearchParams();
    body.set('encrypt', '0');
    body.set('product', 'share');
    body.set('uk', String(runtime.share_uk));
    body.set('primaryid', String(runtime.shareid));
    body.set('fid_list', JSON.stringify([fileItem.fs_id]));
    if (runtime.sekey) {
        body.set('extra', JSON.stringify({ sekey: runtime.sekey }));
    }

    fetch(url.toString(), {
        method: 'POST',
        credentials: 'include',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest'
        },
        body: body.toString()
    }).then(r => r.json()).then(done).catch(err => done({ errno: -999, errmsg: String(err) }));
    """
    return driver.execute_async_script(script, runtime, file_item, sign_data)


def transfer_to_own_netdisk(session: requests.Session, runtime: dict, file_item: dict, share_url: str) -> dict:
    url = (
        "https://pan.baidu.com/share/transfer"
        f"?shareid={runtime['shareid']}"
        f"&from={runtime['share_uk']}"
        "&async=1&channel=chunlei&clienttype=0&web=1&app_id=250528"
        f"&bdstoken={runtime['bdstoken']}"
        "&ondup=newcopy"
    )
    body = {
        "fsidlist": json.dumps([file_item["fs_id"]]),
        "path": "/",
    }
    if runtime.get("sekey"):
        body["sekey"] = runtime["sekey"]
    response = session.post(
        url,
        data=body,
        headers={"Referer": share_url, "X-Requested-With": "XMLHttpRequest"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return load_json_response(response)


def get_access_token(session: requests.Session) -> str:
    response = session.get(AUTHORIZE_URL, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    if "access_token=" in response.url:
        fragment = urlparse(response.url).fragment
        return parse_qs(fragment).get("access_token", [""])[0]

    html = response.text
    bdstoken_match = re.search(r'name="bdstoken"\\s+value="([^"]+)"', html)
    client_id_match = re.search(r'name="client_id"\\s+value="([^"]+)"', html)
    if not (bdstoken_match and client_id_match):
        return ""

    form = {
        "grant_permissions_arr": "netdisk",
        "bdstoken": bdstoken_match.group(1),
        "client_id": client_id_match.group(1),
        "response_type": "token",
        "display": "page",
        "grant_permissions": "basic,netdisk",
    }
    session.post(AUTHORIZE_URL, data=form, timeout=REQUEST_TIMEOUT)
    second = session.get(AUTHORIZE_URL, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    if "access_token=" not in second.url:
        return ""
    fragment = urlparse(second.url).fragment
    return parse_qs(fragment).get("access_token", [""])[0]


def wait_for_access_token(driver: webdriver.Edge, timeout_seconds: int) -> str:
    deadline = time.time() + max(1, timeout_seconds)
    while time.time() < deadline:
        current_url = driver.current_url
        if "access_token=" in current_url:
            fragment = urlparse(current_url).fragment
            return parse_qs(fragment).get("access_token", [""])[0]
        time.sleep(1)
    return ""


def try_click_authorize_button(driver: webdriver.Edge) -> bool:
    try:
        clicked = driver.execute_script(
            """
            const labelPattern = /同意|授权|允许|继续|确认|授权并登录|登录并授权/;
            const nodes = Array.from(
                document.querySelectorAll('button, input[type="submit"], a, [role="button"], .pass-button')
            );
            for (const node of nodes) {
                const text = (node.innerText || node.value || node.getAttribute('aria-label') || '').trim();
                if (!text || !labelPattern.test(text) || node.disabled) {
                    continue;
                }
                node.click();
                return true;
            }
            return false;
            """
        )
        return bool(clicked)
    except Exception:
        return False


def get_access_token_in_browser(driver: webdriver.Edge) -> str:
    driver.get(AUTHORIZE_URL)
    token = wait_for_access_token(driver, 12)
    if token:
        return token

    if try_click_authorize_button(driver):
        token = wait_for_access_token(driver, 20)
        if token:
            return token

    html = driver.page_source
    bdstoken_match = re.search(r'name="bdstoken"\s+value="([^"]+)"', html)
    client_id_match = re.search(r'name="client_id"\s+value="([^"]+)"', html)
    if not (bdstoken_match and client_id_match):
        return ""

    fields = {
        "grant_permissions_arr": "netdisk",
        "bdstoken": bdstoken_match.group(1),
        "client_id": client_id_match.group(1),
        "response_type": "token",
        "display": "page",
        "grant_permissions": "basic,netdisk",
    }
    driver.execute_script(
        """
        const action = arguments[0];
        const fields = arguments[1];
        const form = document.createElement('form');
        form.method = 'POST';
        form.action = action;
        for (const [key, value] of Object.entries(fields)) {
            const input = document.createElement('input');
            input.type = 'hidden';
            input.name = key;
            input.value = value;
            form.appendChild(input);
        }
        document.body.appendChild(form);
        form.submit();
        """,
        AUTHORIZE_URL,
        fields,
    )

    return wait_for_access_token(driver, 20)


def session_has_baidu_login_cookie(session: requests.Session) -> bool:
    for cookie in session.cookies:
        domain = str(cookie.domain or "").lower()
        if "baidu" not in domain:
            continue
        if cookie.name in BAIDU_LOGIN_COOKIE_NAMES:
            return True
    return False


def session_baidu_cookie_names(session: requests.Session) -> list[str]:
    cookie_names = {
        str(cookie.name)
        for cookie in session.cookies
        if "baidu" in str(cookie.domain or "").lower()
    }
    return sorted(cookie_names)


def attach_access_token(url: str, token: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["access_token"] = [token]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def get_root_file_list(session: requests.Session, access_token: str) -> list[dict]:
    response = session.get(
        "https://pan.baidu.com/rest/2.0/xpan/file",
        params={"method": "list", "showempty": "1", "dir": "/", "access_token": access_token},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = load_json_response(response)
    if payload.get("errno") != 0:
        raise RuntimeError(f"xpan file list failed: {payload}")
    return payload.get("list", [])


def get_own_file_dlink(session: requests.Session, access_token: str, fs_id: int) -> str:
    response = session.get(
        "https://pan.baidu.com/rest/2.0/xpan/multimedia",
        params={
            "method": "filemetas",
            "dlink": "1",
            "fsids": json.dumps([fs_id]),
            "access_token": access_token,
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = load_json_response(response)
    if payload.get("errno") != 0:
        raise RuntimeError(f"filemetas failed: {payload}")
    return payload["list"][0]["dlink"]


def parse_total_size_from_headers(headers: dict[str, str]) -> int:
    content_range = str(headers.get("Content-Range") or headers.get("content-range") or "").strip()
    if "/" in content_range:
        total_text = content_range.rsplit("/", 1)[-1].strip()
        if total_text.isdigit():
            return int(total_text)
    content_length = str(headers.get("Content-Length") or headers.get("content-length") or "").strip()
    return int(content_length or 0)


def probe_download_capability(session: requests.Session, url: str) -> tuple[bool, int]:
    with session.get(
        url,
        headers=build_download_headers({"Range": "bytes=0-0"}),
        stream=True,
        timeout=(REQUEST_TIMEOUT, SEGMENT_DOWNLOAD_READ_TIMEOUT),
    ) as response:
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "text/html" in content_type.lower():
            raise RuntimeError("download returned HTML instead of binary content")
        total_size = parse_total_size_from_headers(dict(response.headers))
        range_supported = response.status_code == 206 and total_size > 0
        return range_supported, total_size


def download_file_single(session: requests.Session, url: str, target: Path) -> None:
    max_attempts = 10
    chunk_size = 256 * 1024
    flush_interval = 1024 * 1024
    for attempt in range(1, max_attempts + 1):
        existing_size = target.stat().st_size if target.exists() else 0
        headers = build_download_headers()
        if existing_size:
            headers["Range"] = f"bytes={existing_size}-"
            print("DOWNLOAD_RESUME", target.name, existing_size, attempt)

        try:
            with session.get(
                url,
                headers=headers,
                stream=True,
                timeout=(REQUEST_TIMEOUT, SINGLE_DOWNLOAD_READ_TIMEOUT),
            ) as response:
                if existing_size and response.status_code == 200:
                    target.unlink(missing_ok=True)
                    print("DOWNLOAD_RESTART", target.name, existing_size)
                    continue

                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type.lower():
                    raise RuntimeError("download returned HTML instead of binary content")

                response_size = int(response.headers.get("Content-Length", "0") or 0)
                is_resumed = existing_size > 0 and response.status_code == 206
                total_size = existing_size + response_size if is_resumed else response_size
                mode = "ab" if is_resumed else "wb"
                bytes_written = existing_size if is_resumed else 0
                buffered_bytes = 0
                next_report = max(
                    ((bytes_written // SINGLE_DOWNLOAD_PROGRESS_INTERVAL) + 1) * SINGLE_DOWNLOAD_PROGRESS_INTERVAL,
                    SINGLE_DOWNLOAD_PROGRESS_INTERVAL,
                )

                print("DOWNLOAD_START", target.name, total_size, attempt, bytes_written)
                print(
                    "DOWNLOAD_RESPONSE",
                    target.name,
                    response.status_code,
                    response.headers.get("Content-Length", ""),
                    response.headers.get("Content-Range", ""),
                )
                with target.open(mode) as fh:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        bytes_written += len(chunk)
                        buffered_bytes += len(chunk)
                        if buffered_bytes >= flush_interval:
                            fh.flush()
                            buffered_bytes = 0
                        if bytes_written >= next_report:
                            fh.flush()
                            os.fsync(fh.fileno())
                            print("DOWNLOAD_PROGRESS", target.name, bytes_written)
                            next_report += SINGLE_DOWNLOAD_PROGRESS_INTERVAL
                    fh.flush()
                    os.fsync(fh.fileno())

                print("DOWNLOAD_DONE", target.name, bytes_written)
                return
        except requests.exceptions.RequestException as exc:
            print("DOWNLOAD_RETRY", target.name, attempt, describe_http_exception(exc))
            if attempt >= max_attempts:
                raise


def download_range_part(
    source_session: requests.Session,
    url: str,
    part_path: Path,
    start: int,
    end: int,
    part_index: int,
    part_total: int,
) -> int:
    expected_size = end - start + 1
    chunk_size = 256 * 1024
    flush_interval = 1024 * 1024
    progress_interval = SEGMENT_DOWNLOAD_PROGRESS_INTERVAL
    existing_size = part_path.stat().st_size if part_path.exists() else 0
    if existing_size > expected_size:
        part_path.unlink(missing_ok=True)
        existing_size = 0
    if existing_size == expected_size:
        print("DOWNLOAD_PART_SKIP", part_path.name, part_index, part_total, expected_size)
        return expected_size

    request_start = start + existing_size
    headers = build_download_headers({"Range": f"bytes={request_start}-{end}"})
    mode = "ab" if existing_size else "wb"
    client = clone_session(source_session)
    try:
        with client.get(
            url,
            headers=headers,
            stream=True,
            timeout=(REQUEST_TIMEOUT, SEGMENT_DOWNLOAD_READ_TIMEOUT),
        ) as response:
            response.raise_for_status()
            if response.status_code != 206:
                raise RuntimeError(f"range request not honored for part {part_index}")
            print("DOWNLOAD_PART_START", part_path.name, part_index, part_total, request_start, end)
            print(
                "DOWNLOAD_PART_RESPONSE",
                part_path.name,
                part_index,
                part_total,
                response.status_code,
                response.headers.get("Content-Length", ""),
                response.headers.get("Content-Range", ""),
            )
            with part_path.open(mode) as fh:
                buffered_bytes = 0
                written_bytes = existing_size
                next_report = max((((written_bytes // progress_interval) + 1) * progress_interval), progress_interval)
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fh.write(chunk)
                        written_bytes += len(chunk)
                        buffered_bytes += len(chunk)
                        if buffered_bytes >= flush_interval:
                            fh.flush()
                            buffered_bytes = 0
                        if written_bytes >= next_report:
                            fh.flush()
                            print("DOWNLOAD_PART_PROGRESS", part_path.name, part_index, part_total, written_bytes)
                            next_report += progress_interval
                fh.flush()
                os.fsync(fh.fileno())
    finally:
        client.close()

    final_size = part_path.stat().st_size if part_path.exists() else 0
    if final_size != expected_size:
        raise RuntimeError(f"part size mismatch: expected {expected_size}, got {final_size}")
    print("DOWNLOAD_PART_DONE", part_path.name, part_index, part_total, final_size)
    return final_size


def download_file_segmented(session: requests.Session, url: str, target: Path, download_threads: int, total_size: int) -> None:
    min_part_size = 32 * 1024 * 1024
    part_total = max(1, min(download_threads, math.ceil(total_size / min_part_size)))
    if part_total <= 1:
        download_file_single(session, url, target)
        return

    part_dir = target.parent / f"{target.name}.parts"
    part_dir.mkdir(parents=True, exist_ok=True)
    chunk_size = math.ceil(total_size / part_total)
    ranges: list[tuple[int, int, Path, int]] = []
    for index in range(part_total):
        start = index * chunk_size
        end = min(total_size - 1, ((index + 1) * chunk_size) - 1)
        part_path = part_dir / f"{target.name}.part{index + 1:02d}"
        ranges.append((start, end, part_path, index + 1))

    print("DOWNLOAD_MULTI_START", target.name, total_size, part_total)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=part_total) as executor:
            futures = [
                executor.submit(download_range_part, session, url, part_path, start, end, index, part_total)
                for start, end, part_path, index in ranges
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()

        with target.open("wb") as output_handle:
            for _, _, part_path, index in ranges:
                with part_path.open("rb") as part_handle:
                    shutil.copyfileobj(part_handle, output_handle, 1024 * 1024)
                print("DOWNLOAD_PART_MERGED", target.name, index, part_total)
            output_handle.flush()
            os.fsync(output_handle.fileno())
        print("DOWNLOAD_MULTI_DONE", target.name, total_size, part_total)
    finally:
        shutil.rmtree(part_dir, ignore_errors=True)


def download_file(session: requests.Session | None, url: str, target: Path, download_threads: int = 1) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    close_session = session is None
    client = session or requests.Session()
    try:
        normalized_threads = max(1, int(download_threads or 1))
        if normalized_threads > 1 and not target.exists():
            try:
                range_supported, total_size = probe_download_capability(client, url)
                if range_supported and total_size > 32 * 1024 * 1024:
                    try:
                        download_file_segmented(client, url, target, normalized_threads, total_size)
                        return
                    except Exception as exc:
                        print("DOWNLOAD_MULTI_RETRY_SINGLE", target.name, normalized_threads, describe_http_exception(exc))
                print("DOWNLOAD_MULTI_UNAVAILABLE", target.name, normalized_threads, total_size, int(range_supported))
            except Exception as exc:
                print("DOWNLOAD_MULTI_FALLBACK", target.name, normalized_threads, describe_http_exception(exc))
        download_file_single(client, url, target)
    finally:
        if close_session:
            client.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    driver = build_driver(args.edge_binary, args.edge_user_data, args.edge_profile)
    try:
        runtime = fetch_runtime_info(driver, args.share_url)
        print("RUNTIME", json.dumps(runtime, ensure_ascii=False))
        session, user_agent = build_session(driver)
        print("USER_AGENT", user_agent)
        login_state = {
            "logged_in": session_has_baidu_login_cookie(session),
            "cookie_names": session_baidu_cookie_names(session),
            "launch_mode": getattr(driver, "_profile_launch_mode", "unknown"),
        }
        print("LOGIN_STATE", json.dumps(login_state, ensure_ascii=False))
        if not args.list_only and not login_state["logged_in"]:
            raise RuntimeError("百度专用登录窗口还没有登录，请先点击“登录百度”，在弹出的专用窗口里完成登录后关闭窗口，再重新开始处理。")

        file_list = get_share_list(session, runtime)
        files = [
            item
            for item in file_list
            if not item.get("isdir") and supported_share_file_kind(item)
        ]
        if not files:
            raise RuntimeError("no supported video or image files found")
        print(
            "SHARE_FILES",
            json.dumps(
                [
                    {
                        "name": item["server_filename"],
                        "size": item.get("size", 0),
                        "fs_id": item.get("fs_id"),
                        "path": item.get("path"),
                        "file_type": supported_share_file_kind(item),
                    }
                    for item in files
                ],
                ensure_ascii=False,
            ),
        )
        if args.list_only:
            return 0

        root_dir = str(runtime["file_list"][0]["path"])
        if args.target_filename or args.target_path or args.target_fsid:
            target_pool = files
        else:
            target_pool = [item for item in files if supported_share_file_kind(item) == "video"]
            if not target_pool:
                raise RuntimeError("no video files found for default download target")
        target_item = select_target_file(target_pool, args.target_filename, args.target_path, args.target_fsid)
        print("TARGET", json.dumps(target_item, ensure_ascii=False))

        share_short_id = extract_share_short_id(args.share_url, runtime)
        print("SHARE_SHORT_ID", share_short_id)
        sign_data = get_sign_and_timestamp(session, runtime.get("bdstoken", ""), share_short_id)
        print("SIGN", json.dumps(sign_data, ensure_ascii=False))

        shared = try_sharedownload_with_requests(session, runtime, target_item, sign_data, args.share_url)
        print("SHAREDOWNLOAD_REQUESTS", json.dumps(shared, ensure_ascii=False))
        if shared.get("errno") != 0 or isinstance(shared.get("list"), str):
            shared = try_sharedownload_in_browser(driver, runtime, target_item, sign_data)
            print("SHAREDOWNLOAD_BROWSER", json.dumps(shared, ensure_ascii=False))
        if shared.get("errno") != 0:
            raise RuntimeError(f"sharedownload failed: {shared}")
        target_path = build_output_path(output_dir, target_item, root_dir)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(shared.get("list"), list):
            dlink = shared["list"][0]["dlink"]
            try:
                download_file(session, dlink, target_path, download_threads=args.download_threads)
            except Exception as first_error:
                print("DIRECT_DOWNLOAD_FAILED", str(first_error))
                token = get_access_token(session)
                print("ACCESS_TOKEN", token[:24] + "..." if token else "")
                if not token:
                    raise
                dlink = attach_access_token(dlink, token)
                download_file(session, dlink, target_path, download_threads=args.download_threads)
        else:
            print("SHAREDOWNLOAD_NEEDS_TRANSFER", shared.get("list"))
            token = get_access_token(session)
            if not token:
                token = get_access_token_in_browser(driver)
            print("ACCESS_TOKEN", token[:24] + "..." if token else "")
            if not token:
                raise RuntimeError("百度登录态未能完成转存授权，请重新点击“登录百度”，在弹出的专用窗口里登录后关闭，再重新开始处理。")
            transfer = transfer_to_own_netdisk(session, runtime, target_item, args.share_url)
            print("TRANSFER", json.dumps(transfer, ensure_ascii=False))
            root_list = get_root_file_list(session, token)
            candidates = [item for item in root_list if item.get("server_filename", "").startswith(target_item["server_filename"].rsplit(".", 1)[0])]
            if not candidates:
                raise RuntimeError("could not find transferred file in root directory")
            own_file = max(candidates, key=lambda item: item.get("server_mtime") or 0)
            print("OWN_FILE", json.dumps(own_file, ensure_ascii=False))
            dlink = get_own_file_dlink(session, token, own_file["fs_id"])
            dlink = attach_access_token(dlink, token)
            download_file(session, dlink, target_path, download_threads=args.download_threads)

        print("DOWNLOADED", str(target_path), target_path.stat().st_size)
        return 0
    finally:
        temp_profile_context = getattr(driver, "_temp_profile_context", None)
        driver.quit()
        if temp_profile_context is not None:
            temp_profile_context.cleanup()


# Override the earlier definitions with a clean UTF-8 version so runtime
# messages and title checks stay readable even if the source file previously
# contained mojibake strings.
def build_driver(edge_binary: str, edge_user_data: str, edge_profile: str) -> webdriver.Edge:
    if Path(edge_user_data).resolve() == Path(DEFAULT_EDGE_USER_DATA).resolve():
        edge_user_data = str(ensure_managed_edge_user_data(edge_profile))

    active_processes = list_edge_processes_for_user_data(Path(edge_user_data))
    if active_processes:
        for _ in range(3):
            terminated_ids = terminate_edge_processes_for_user_data(Path(edge_user_data))
            if terminated_ids:
                print("EDGE_PROFILE_CLEANUP", json.dumps({"terminated": terminated_ids}, ensure_ascii=False))
            time.sleep(1)
            active_processes = list_edge_processes_for_user_data(Path(edge_user_data))
            if not active_processes:
                break
        if active_processes:
            raise RuntimeError("百度专用登录窗口仍在运行，请先关闭相关窗口后再重试。")

    try:
        driver = launch_driver(edge_binary, edge_user_data, edge_profile)
        setattr(driver, "_temp_profile_context", None)
        setattr(driver, "_profile_launch_mode", "direct")
        return driver
    except Exception as direct_error:
        active_processes = list_edge_processes_for_user_data(Path(edge_user_data))
        if active_processes:
            raise RuntimeError("百度专用登录窗口还在运行，请先关闭那个窗口，再重新提取或开始处理。") from direct_error
        try:
            return launch_cloned_driver(edge_binary, edge_user_data, edge_profile)
        except Exception as cloned_error:
            raise cloned_error from direct_error


def fetch_runtime_info(driver: webdriver.Edge, share_url: str) -> dict:
    driver.get(share_url)

    def ready(drv: webdriver.Edge) -> bool:
        try:
            title = drv.title or ""
            if "请输入提取码" in title:
                return False
            share_id = drv.execute_script(
                """
                const getVal = (key) => window.locals && window.locals.get ? window.locals.get(key) : null;
                return getVal('shareid') || window.yunData?.shareid || null;
                """
            )
            return bool(share_id)
        except Exception:
            return False

    WebDriverWait(driver, 45).until(ready)
    time.sleep(2)

    return driver.execute_script(
        """
        const getVal = (key) => window.locals && window.locals.get ? window.locals.get(key) : null;
        return {
            title: document.title,
            href: location.href,
            shareid: getVal('shareid') || window.yunData?.shareid || null,
            share_uk: getVal('share_uk') || window.yunData?.share_uk || null,
            bdstoken: getVal('bdstoken') || window.yunData?.bdstoken || '',
            jsToken: window.jsToken || null,
            sekey: window.currentSekey || window.cache?.list?.config?.params?.sekey || null,
            file_list: getVal('file_list') || [],
        };
        """
    )


if __name__ == "__main__":
    sys.exit(main())
