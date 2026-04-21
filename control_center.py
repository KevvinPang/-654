from __future__ import annotations

import argparse
import collections
import importlib.util
import json
import mimetypes
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import textwrap
import urllib.parse
import webbrowser
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = PROJECT_ROOT / "runtime" / "workspaces"
BATCH_RUNNER = PROJECT_ROOT / "batch_runner.py"
MODULE_MANIFEST = PROJECT_ROOT / "module_manifest.json"
CONTROL_CENTER_MANIFEST = PROJECT_ROOT / "control_center_manifest.json"
CONTROL_CENTER_UI = PROJECT_ROOT / "control_center_ui.html"
CONTROL_CENTER_ASSETS_DIR = PROJECT_ROOT / "assets"
CONTROL_CENTER_APP_LOGO_PNG = CONTROL_CENTER_ASSETS_DIR / "app_logo.png"
CONTROL_CENTER_APP_LOGO_ICO = CONTROL_CENTER_ASSETS_DIR / "app_logo.ico"
SUPPORTED_VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".avi", ".m4v"}
SUPPORTED_AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus"}
SUPPORTED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
SUPPORTED_SUBTITLE_EXTENSIONS = {".srt", ".txt", ".ass", ".vtt"}
COVER_ARTIFACT_STEM = "selected_cover"
CONTROL_CENTER_RUNTIME_DIR = PROJECT_ROOT / "runtime" / "control_center"
CONTROL_CENTER_PID_FILE = CONTROL_CENTER_RUNTIME_DIR / "control_center.pid"
CONTROL_CENTER_NOTICE_FILE = CONTROL_CENTER_RUNTIME_DIR / "last_system_notice.json"
BAIDU_LOGIN_URL = "https://pan.baidu.com/disk/main"
BAIDU_EDGE_RUNTIME_ROOT = PROJECT_ROOT / "runtime" / "edge_profiles"
BAIDU_MANAGED_EDGE_USER_DATA = BAIDU_EDGE_RUNTIME_ROOT / "baidu_login_profile"
BAIDU_EDGE_PROFILE = "Default"
BAIDU_LOGIN_COOKIE_NAMES = {"BDUSS", "BDUSS_BFESS"}
BAIDU_OFFICIAL_HANDOFF_PATH = PROJECT_ROOT / "modules" / "baidu_official_client_handoff.py"
LOG_LINE_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[([A-Z]+)\](?: \[([^\]]+)\])? (.*)$")
TASK_SCHEMA_VERSION = 3
TASK_SCHEMA_VERSION_KEY = "_control_center_task_version"
UI_SESSION_HEARTBEAT_TIMEOUT_SECONDS = 15.0
UI_SESSION_SHUTDOWN_GRACE_SECONDS = 60.0

DEFAULT_CONCURRENCY = {
    "baidu_share": 1,
    "douyin_download": 3,
    "subtitle_extract": 1,
    "auto_clip": 1,
}
KNOWN_MOJIBAKE_REPLACEMENTS = {
    "E:\\鏍风墖": "E:\\样片",
    "E:\\鎴愮墖": "E:\\成片",
    "E:\\�������Զ�����": "E:\\服务器自动剪辑",
    "E:\\��Ƭ": "E:\\样片",
}
KNOWN_WORKSPACE_MOJIBAKE = (
    "鏂╂儏褰撳ぉ浠栦滑鎮旂柉浜?",
    "鏂╂儏褰撳ぉ浠栦滑鎮旂柉浜",
    "ն�鵱�����ǻڷ���",
)


@dataclass
class JobState:
    job_id: str
    workspace: str
    command: list[str]
    log_path: str
    started_at: float
    status: str = "running"
    return_code: int | None = None
    recent_lines: collections.deque[str] = field(default_factory=lambda: collections.deque(maxlen=300))
    pid: int | None = None
    process: subprocess.Popen[Any] | None = None
    workspace_members: list[str] = field(default_factory=list)
    stop_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "workspace": self.workspace,
            "command": self.command,
            "log_path": self.log_path,
            "started_at": self.started_at,
            "status": self.status,
            "return_code": self.return_code,
            "pid": self.pid,
            "recent_lines": list(self.recent_lines)[-80:],
            "workspace_members": list(self.workspace_members),
            "stop_reason": self.stop_reason,
        }


JOB_LOCK = threading.Lock()
JOBS: dict[str, JobState] = {}
JOB_COUNTER = 0
BAIDU_OFFICIAL_HANDOFF_MODULE: Any | None = None
BAIDU_OFFICIAL_HANDOFF_LOAD_ERROR = ""
UI_SESSION_LOCK = threading.Lock()
UI_SESSIONS: dict[str, float] = {}
UI_SESSION_LAST_EMPTY_AT: float | None = None
UI_SESSION_EVER_CONNECTED = False
SERVER_SHUTDOWN_LOCK = threading.Lock()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local control center for server_auto_clip.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host.")
    parser.add_argument("--port", type=int, default=19081, help="Bind port.")
    parser.add_argument("--open-browser", action="store_true", help="Open the browser automatically after startup.")
    return parser.parse_args(argv)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def runtime_timestamp_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + f",{int((time.time() % 1) * 1000):03d}"


def append_text_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip("\n") + "\n")


def system_notice_message(reason: str, stopped_jobs: int = 0) -> str:
    normalized = str(reason or "").strip()
    if normalized == "ui-session-disconnected":
        return (
            f"前端界面断开超过 {int(UI_SESSION_SHUTDOWN_GRACE_SECONDS)} 秒，"
            f"控制台已自动停止 {stopped_jobs} 个后台任务。"
            "这次不是 AI 自己卡死，而是界面断开保护触发了自动中止。"
        )
    if normalized == "manual-ui-shutdown":
        return f"控制台已按手动关闭请求停止 {stopped_jobs} 个后台任务。"
    if normalized:
        return normalized
    return f"后台任务已被停止，共 {stopped_jobs} 个。"


def persist_system_notice(message: str, *, tone: str = "warning", reason: str = "") -> None:
    if not str(message or "").strip():
        return
    notice_id = f"{int(time.time())}_{abs(hash((message, tone, reason))) % 1000000}"
    write_json(
        CONTROL_CENTER_NOTICE_FILE,
        {
            "id": notice_id,
            "message": str(message),
            "tone": str(tone or "warning"),
            "reason": str(reason or ""),
            "timestamp": time.time(),
            "time_text": runtime_timestamp_text(),
        },
    )


def append_job_stop_notice(job: JobState, message: str) -> None:
    text = str(message or "").strip()
    if not text:
        return
    line = f"{runtime_timestamp_text()} [WARN] [系统] {text}"
    log_path = Path(str(job.log_path or "")).expanduser()
    try:
        if log_path:
            append_text_line(log_path, line)
    except Exception:
        pass
    workspace_names = list(job.workspace_members or ([job.workspace] if job.workspace else []))
    for workspace_name in workspace_names:
        try:
            workspace_dir = resolve_workspace_dir(workspace_name, create=False)
        except Exception:
            continue
        try:
            append_text_line(workspace_dir / "logs" / "workspace.log", line)
        except Exception:
            pass
    with JOB_LOCK:
        job.stop_reason = text
        job.recent_lines.append(line)


def repair_known_mojibake_text(value: str, workspace_name: str = "") -> str:
    text = str(value or "")
    if not text:
        return text
    repaired = text
    for broken, fixed in KNOWN_MOJIBAKE_REPLACEMENTS.items():
        repaired = repaired.replace(broken, fixed)
    if workspace_name:
        for broken in KNOWN_WORKSPACE_MOJIBAKE:
            repaired = repaired.replace(broken, workspace_name)
    best = repaired
    best_score = _text_quality_score(best)
    for candidate in iter_mojibake_candidates(repaired):
        score = _text_quality_score(candidate)
        if score > best_score:
            best = candidate
            best_score = score
    return best


def iter_mojibake_candidates(text: str) -> list[str]:
    candidates = [text]
    for source_encoding, target_encoding in (
        ("gb18030", "utf-8"),
        ("cp936", "utf-8"),
        ("latin1", "utf-8"),
        ("cp1252", "utf-8"),
    ):
        try:
            candidate = text.encode(source_encoding).decode(target_encoding)
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _text_quality_score(text: str) -> int:
    value = str(text or "")
    if not value:
        return 0
    cjk_count = sum(1 for char in value if "\u4e00" <= char <= "\u9fff")
    printable_ascii = sum(1 for char in value if char.isascii() and (char.isalnum() or char in " .:/\\-_?=&()[]{}"))
    replacement_penalty = value.count("\ufffd") * 20
    marker_penalty = sum(value.count(marker) for marker in ("锟斤拷", "鈻", "�?", "��")) * 12
    return cjk_count * 4 + printable_ascii - replacement_penalty - marker_penalty


def has_obviously_broken_text(value: str) -> bool:
    text = str(value or "")
    if not text:
        return False
    return "\ufffd" in text or "锟斤拷" in text or "��" in text


def sanitize_baidu_share_entries(entries: list[Any]) -> list[Any]:
    cleaned: list[Any] = []
    for item in entries:
        if not isinstance(item, dict):
            cleaned.append(item)
            continue
        current = dict(item)
        target_path = str(current.get("target_path", "") or "")
        if has_obviously_broken_text(target_path):
            current["target_path"] = ""
        target_filename = str(current.get("target_filename", "") or "")
        if has_obviously_broken_text(target_filename) and target_path:
            current["target_filename"] = Path(target_path).name
        cleaned.append(current)
    return cleaned


def normalize_task_payload(value: Any, workspace_name: str) -> Any:
    if isinstance(value, dict):
        return {key: normalize_task_payload(item, workspace_name) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_task_payload(item, workspace_name) for item in value]
    if isinstance(value, str):
        return repair_known_mojibake_text(value, workspace_name)
    return value


def decode_subprocess_output_line(raw_line: bytes | str, workspace_name: str = "") -> str:
    if isinstance(raw_line, str):
        return repair_known_mojibake_text(raw_line.rstrip("\r\n"), workspace_name)

    payload = raw_line.rstrip(b"\r\n")
    if not payload:
        return ""

    for encoding in ("utf-8", "gb18030", "cp936"):
        try:
            return repair_known_mojibake_text(payload.decode(encoding), workspace_name)
        except UnicodeDecodeError:
            continue
    return repair_known_mojibake_text(payload.decode("utf-8", errors="replace"), workspace_name)


def ensure_workspace_root() -> None:
    WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)
    CONTROL_CENTER_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def resolve_workspace_dir(workspace_name: str, *, create: bool = False) -> Path:
    ensure_workspace_root()
    normalized = str(workspace_name or "").strip()
    if not normalized:
        raise ValueError("workspace is required")
    if "/" in normalized or "\\" in normalized:
        raise ValueError("workspace name cannot contain path separators")

    workspace_dir = (WORKSPACE_ROOT / normalized).resolve()
    try:
        workspace_dir.relative_to(WORKSPACE_ROOT.resolve())
    except ValueError as exc:
        raise ValueError("workspace must stay inside runtime/workspaces") from exc

    if create:
        workspace_dir.mkdir(parents=True, exist_ok=True)
    return workspace_dir


def default_workspace_task(workspace_name: str) -> dict[str, Any]:
    return {
        TASK_SCHEMA_VERSION_KEY: TASK_SCHEMA_VERSION,
        "workspace_name": workspace_name,
        "concurrency": dict(DEFAULT_CONCURRENCY),
        "settings": {
            "ai_api_key": "",
            "ai_api_url": "https://ark.cn-beijing.volces.com/api/v3/chat/completions",
                    "ai_model": "doubao-seed-character-251128",
            "ai_fallback_models": [],
            "prefer_funasr_audio_subtitles": True,
            "disable_ai_subtitle_review": False,
            "disable_ai_narration_rewrite": False,
            "prefer_funasr_sentence_pauses": True,
            "force_no_narration_mode": False,
            "narration_background_percent": 3,
            "output_watermark_text": "",
            "enable_random_episode_flip": True,
            "random_episode_flip_ratio": 0.4,
            "enable_random_visual_filter": True,
            "reference_speed_factor": 1.0,
            "cover_image_path": "",
            "cover_image_name": "",
            "cover_image_share_key": "",
            "bgm_audio_path": "",
            "bgm_volume_percent": 12,
            "clip_output_root": "",
            "tts_voice": "zh-CN-YunxiNeural",
            "tts_rate": "+8%",
            "enable_backup_tts": False,
            "azure_tts_key": "",
            "azure_tts_region": "",
            "azure_tts_voice": "",
        },
        "baidu_share": [],
        "douyin_download": [],
        "subtitle_extract": [],
        "auto_clip": [],
    }


def apply_workspace_task_defaults(payload: dict[str, Any], workspace_name: str) -> dict[str, Any]:
    task = dict(payload)
    task["workspace_name"] = workspace_name
    task[TASK_SCHEMA_VERSION_KEY] = int(task.get(TASK_SCHEMA_VERSION_KEY, TASK_SCHEMA_VERSION) or TASK_SCHEMA_VERSION)
    concurrency = task.get("concurrency")
    if not isinstance(concurrency, dict):
        concurrency = {}
    task["concurrency"] = {**DEFAULT_CONCURRENCY, **concurrency}
    settings = task.get("settings")
    if not isinstance(settings, dict):
        settings = {}
    task["settings"] = settings
    settings.setdefault("ai_fallback_models", [])
    settings.setdefault("prefer_funasr_audio_subtitles", True)
    settings.setdefault("disable_ai_subtitle_review", False)
    settings.setdefault("disable_ai_narration_rewrite", False)
    settings.setdefault("prefer_funasr_sentence_pauses", True)
    settings.setdefault("force_no_narration_mode", False)
    settings.setdefault("narration_background_percent", 3)
    settings.setdefault("output_watermark_text", "")
    settings.setdefault("enable_random_episode_flip", True)
    settings.setdefault("random_episode_flip_ratio", 0.4)
    settings.setdefault("enable_random_visual_filter", True)
    settings.setdefault("reference_speed_factor", 1.0)
    settings.setdefault("cover_image_path", "")
    settings.setdefault("cover_image_name", "")
    settings.setdefault("cover_image_share_key", "")
    settings.setdefault("bgm_audio_path", "")
    settings.setdefault("bgm_volume_percent", 12)
    settings.setdefault("clip_output_root", "")
    settings.setdefault("tts_voice", "zh-CN-YunxiNeural")
    settings.setdefault("tts_rate", "+8%")
    settings.setdefault("enable_backup_tts", False)
    settings.setdefault("azure_tts_key", "")
    settings.setdefault("azure_tts_region", "")
    settings.setdefault("azure_tts_voice", "")
    for key in ("baidu_share", "douyin_download", "subtitle_extract"):
        if not isinstance(task.get(key), list):
            task[key] = []
    raw_auto_clip_entries = task.get("auto_clip")
    if not isinstance(raw_auto_clip_entries, list):
        raw_auto_clip_entries = []
    auto_clip_entries: list[Any] = []
    for item in raw_auto_clip_entries:
        if not isinstance(item, dict):
            auto_clip_entries.append(item)
            continue
        current = dict(item)
        current.setdefault("skip_existing", False)
        auto_clip_entries.append(current)
    task["auto_clip"] = auto_clip_entries
    task["baidu_share"] = sanitize_baidu_share_entries(task.get("baidu_share") or [])
    return task


def migrate_legacy_workspace_task(payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    task = dict(payload)
    raw_version = task.get(TASK_SCHEMA_VERSION_KEY, 0)
    try:
        version = int(raw_version)
    except (TypeError, ValueError):
        version = 0
    if version >= TASK_SCHEMA_VERSION:
        return task, False

    changed = False
    settings = task.get("settings")
    if not isinstance(settings, dict):
        settings = {}
        task["settings"] = settings
        changed = True
    if settings.get("enable_random_episode_flip") is False:
        settings["enable_random_episode_flip"] = True
        changed = True
    if settings.get("enable_random_visual_filter") is False:
        settings["enable_random_visual_filter"] = True
        changed = True
    if settings.get("disable_ai_narration_rewrite") is True:
        settings["disable_ai_narration_rewrite"] = False
        changed = True
    narration_background_percent = settings.get("narration_background_percent")
    try:
        narration_background_percent_value = float(narration_background_percent)
    except (TypeError, ValueError):
        narration_background_percent_value = None
    if narration_background_percent_value == 15.0:
        settings["narration_background_percent"] = 3
        changed = True
    auto_clip_entries = task.get("auto_clip")
    if isinstance(auto_clip_entries, list):
        for item in auto_clip_entries:
            if not isinstance(item, dict):
                continue
            if item.get("skip_existing", True):
                item["skip_existing"] = False
                changed = True
    task[TASK_SCHEMA_VERSION_KEY] = TASK_SCHEMA_VERSION
    return task, changed or version != TASK_SCHEMA_VERSION


def prune_expired_ui_sessions(now: float | None = None) -> int:
    global UI_SESSION_LAST_EMPTY_AT
    current_time = time.time() if now is None else now
    with UI_SESSION_LOCK:
        expired_ids = [
            session_id
            for session_id, last_seen_at in UI_SESSIONS.items()
            if current_time - last_seen_at > UI_SESSION_HEARTBEAT_TIMEOUT_SECONDS
        ]
        for session_id in expired_ids:
            UI_SESSIONS.pop(session_id, None)
        if UI_SESSIONS:
            UI_SESSION_LAST_EMPTY_AT = None
        elif UI_SESSION_EVER_CONNECTED and UI_SESSION_LAST_EMPTY_AT is None:
            UI_SESSION_LAST_EMPTY_AT = current_time
        return len(UI_SESSIONS)


def register_ui_session(session_id: str) -> int:
    global UI_SESSION_EVER_CONNECTED, UI_SESSION_LAST_EMPTY_AT
    normalized = str(session_id or "").strip()
    if not normalized:
        raise ValueError("session_id is required")
    current_time = time.time()
    prune_expired_ui_sessions(current_time)
    with UI_SESSION_LOCK:
        UI_SESSIONS[normalized] = current_time
        UI_SESSION_EVER_CONNECTED = True
        UI_SESSION_LAST_EMPTY_AT = None
        return len(UI_SESSIONS)


def disconnect_ui_session(session_id: str) -> int:
    global UI_SESSION_LAST_EMPTY_AT
    normalized = str(session_id or "").strip()
    current_time = time.time()
    with UI_SESSION_LOCK:
        if normalized:
            UI_SESSIONS.pop(normalized, None)
        if UI_SESSIONS:
            UI_SESSION_LAST_EMPTY_AT = None
        elif UI_SESSION_EVER_CONNECTED:
            UI_SESSION_LAST_EMPTY_AT = current_time
        return len(UI_SESSIONS)


def stop_all_jobs(*, reason: str = "") -> int:
    with JOB_LOCK:
        running_job_ids = [
            job_id
            for job_id, job in JOBS.items()
            if job.process is not None and job.status in {"running", "stopping"}
        ]
    stopped = 0
    for job_id in running_job_ids:
        if stop_job(job_id, reason=reason):
            stopped += 1
    return stopped


def request_server_shutdown(
    server: ThreadingHTTPServer,
    *,
    stop_jobs: bool = False,
    reason: str = "",
) -> dict[str, Any]:
    stop_reason_message = ""
    if reason == "ui-session-disconnected":
        stop_reason_message = (
            f"前端界面断开超过 {int(UI_SESSION_SHUTDOWN_GRACE_SECONDS)} 秒，"
            "控制台正在自动停止当前后台任务。"
        )
    elif reason == "manual-ui-shutdown":
        stop_reason_message = "控制台正在按手动关闭请求停止当前后台任务。"
    elif reason:
        stop_reason_message = reason
    stopped_jobs = stop_all_jobs(reason=stop_reason_message) if stop_jobs else 0
    if stop_jobs and stopped_jobs > 0:
        persist_system_notice(
            system_notice_message(reason, stopped_jobs),
            tone="warning" if reason == "ui-session-disconnected" else "info",
            reason=reason,
        )
    with SERVER_SHUTDOWN_LOCK:
        already_requested = bool(getattr(server, "_shutdown_requested", False))
        if not already_requested:
            setattr(server, "_shutdown_requested", True)
            setattr(server, "_shutdown_reason", reason)
            threading.Thread(target=server.shutdown, daemon=True).start()
    return {
        "accepted": not already_requested,
        "stopped_jobs": stopped_jobs,
        "reason": reason,
    }


def start_ui_session_watchdog(server: ThreadingHTTPServer) -> None:
    if getattr(server, "_ui_session_watchdog_started", False):
        return
    setattr(server, "_ui_session_watchdog_started", True)

    def worker() -> None:
        while not getattr(server, "_shutdown_requested", False):
            time.sleep(2.0)
            prune_expired_ui_sessions()
            with UI_SESSION_LOCK:
                should_shutdown = (
                    UI_SESSION_EVER_CONNECTED
                    and not UI_SESSIONS
                    and UI_SESSION_LAST_EMPTY_AT is not None
                    and (time.time() - UI_SESSION_LAST_EMPTY_AT) >= UI_SESSION_SHUTDOWN_GRACE_SECONDS
                )
            if should_shutdown:
                request_server_shutdown(server, stop_jobs=True, reason="ui-session-disconnected")
                return

    threading.Thread(target=worker, daemon=True).start()


def summarize_task(task_data: dict[str, Any]) -> dict[str, int]:
    return {
        "baidu_share": len(task_data.get("baidu_share") or []),
        "douyin_download": len(task_data.get("douyin_download") or []),
        "subtitle_extract": len(task_data.get("subtitle_extract") or []),
        "auto_clip": len(task_data.get("auto_clip") or []),
    }


def collect_workspace_files(root: Path, extensions: set[str]) -> list[Path]:
    if not root.exists():
        return []
    files: list[Path] = []
    try:
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if extensions and path.suffix.lower() not in extensions:
                continue
            files.append(path)
    except OSError:
        return []
    def safe_mtime(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0
    return sorted(files, key=safe_mtime, reverse=True)


def workspace_cover_dir(workspace_dir: Path) -> Path:
    return workspace_dir / "covers"


def workspace_bgm_dir(workspace_dir: Path) -> Path:
    return workspace_dir / "bgm"


def resolve_workspace_optional_path(workspace_dir: Path, raw_path: str | None) -> Path | None:
    normalized = str(raw_path or "").strip()
    if not normalized:
        return None
    candidate = Path(normalized).expanduser()
    if not candidate.is_absolute():
        candidate = workspace_dir / candidate
    return candidate.resolve()


def safe_workspace_relative_path(workspace_dir: Path, target_path: Path) -> str:
    try:
        return target_path.resolve().relative_to(workspace_dir.resolve()).as_posix()
    except ValueError:
        return str(target_path.resolve())


def summarize_workspace_assets(workspace_dir: Path) -> dict[str, Any]:
    source_root = workspace_dir / "downloads" / "baidu"
    reference_root = workspace_dir / "downloads" / "douyin"
    cover_root = workspace_cover_dir(workspace_dir)
    bgm_root = workspace_bgm_dir(workspace_dir)
    subtitle_root = workspace_dir / "subtitles"
    clip_root = workspace_dir / "clips"

    def pack(root: Path, files: list[Path]) -> dict[str, Any]:
        recent = []
        for item in files[:3]:
            try:
                recent.append(item.relative_to(root).as_posix())
            except ValueError:
                recent.append(item.name)
        items = []
        for item in files[:200]:
            try:
                relative_path = item.relative_to(root).as_posix()
            except ValueError:
                relative_path = item.name
            try:
                size = item.stat().st_size
            except OSError:
                size = 0
            items.append(
                {
                    "name": item.name,
                    "relative_path": relative_path,
                    "size": size,
                }
            )
        return {
            "count": len(files),
            "recent": recent,
            "items": items,
            "hidden_count": max(0, len(files) - len(items)),
            "path": str(root),
        }

    source_files = collect_workspace_files(source_root, SUPPORTED_VIDEO_EXTENSIONS)
    reference_files = collect_workspace_files(reference_root, SUPPORTED_VIDEO_EXTENSIONS)
    cover_files = collect_workspace_files(cover_root, SUPPORTED_IMAGE_EXTENSIONS)
    bgm_files = collect_workspace_files(bgm_root, SUPPORTED_AUDIO_EXTENSIONS)
    subtitle_files = collect_workspace_files(subtitle_root, SUPPORTED_SUBTITLE_EXTENSIONS)
    clip_files = collect_workspace_files(clip_root, SUPPORTED_VIDEO_EXTENSIONS)
    return {
        "source": pack(source_root, source_files),
        "reference": pack(reference_root, reference_files),
        "cover": pack(cover_root, cover_files),
        "bgm": pack(bgm_root, bgm_files),
        "subtitle": pack(subtitle_root, subtitle_files),
        "clip": pack(clip_root, clip_files),
    }


def list_workspaces() -> list[dict[str, Any]]:
    ensure_workspace_root()
    workspaces: list[dict[str, Any]] = []
    for workspace_dir in sorted(WORKSPACE_ROOT.iterdir()):
        if not workspace_dir.is_dir():
            continue
        task_path = workspace_dir / "task.json"
        if task_path.exists():
            task_data = read_json(task_path, default_workspace_task(workspace_dir.name))
        else:
            task_data = default_workspace_task(workspace_dir.name)
        workspaces.append(
            {
                "name": workspace_dir.name,
                "path": str(workspace_dir),
                "task_path": str(task_path),
                "has_task": task_path.exists(),
                "task_summary": summarize_task(task_data),
                "asset_summary": summarize_workspace_assets(workspace_dir),
            }
        )
    return workspaces


def get_workspace_task(workspace_name: str) -> dict[str, Any]:
    workspace_dir = resolve_workspace_dir(workspace_name, create=False)
    task_path = workspace_dir / "task.json"
    if not task_path.exists():
        return default_workspace_task(workspace_dir.name)
    raw_task = read_json(task_path, default_workspace_task(workspace_dir.name))
    if not isinstance(raw_task, dict):
        return default_workspace_task(workspace_dir.name)
    normalized_task = normalize_task_payload(raw_task, workspace_dir.name)
    migrated_task, migrated = migrate_legacy_workspace_task(normalized_task)
    task = apply_workspace_task_defaults(migrated_task, workspace_dir.name)
    if migrated or task != raw_task:
        task_path.write_text(json.dumps(task, ensure_ascii=False, indent=2), encoding="utf-8")
    return task


def save_workspace_task(workspace_name: str, payload: dict[str, Any]) -> Path:
    workspace_dir = resolve_workspace_dir(workspace_name, create=True)
    task_path = workspace_dir / "task.json"
    payload = normalize_task_payload(dict(payload), workspace_dir.name)
    payload = apply_workspace_task_defaults(payload, workspace_dir.name)
    task_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return task_path


def path_exists(raw_path: str | None) -> bool:
    if not raw_path:
        return False
    try:
        return Path(raw_path).exists()
    except Exception:
        return False


def load_baidu_official_handoff_module() -> Any:
    global BAIDU_OFFICIAL_HANDOFF_MODULE, BAIDU_OFFICIAL_HANDOFF_LOAD_ERROR
    if BAIDU_OFFICIAL_HANDOFF_MODULE is not None:
        return BAIDU_OFFICIAL_HANDOFF_MODULE
    if BAIDU_OFFICIAL_HANDOFF_LOAD_ERROR:
        raise RuntimeError(BAIDU_OFFICIAL_HANDOFF_LOAD_ERROR)
    try:
        spec = importlib.util.spec_from_file_location(
            "server_auto_clip.baidu_official_handoff_runtime",
            BAIDU_OFFICIAL_HANDOFF_PATH,
        )
        if spec is None or spec.loader is None:
            raise RuntimeError(f"无法加载百度官方客户端下载模块：{BAIDU_OFFICIAL_HANDOFF_PATH}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        BAIDU_OFFICIAL_HANDOFF_MODULE = module
        return module
    except Exception as exc:
        BAIDU_OFFICIAL_HANDOFF_LOAD_ERROR = str(exc)
        raise


def parse_runtime_log_line(line: str, workspace_name: str = "") -> dict[str, str]:
    raw = repair_known_mojibake_text(str(line or "").rstrip("\r\n"), workspace_name)
    matched = LOG_LINE_PATTERN.match(raw)
    if not matched:
        return {"timestamp": "", "level": "", "scope": "", "message": raw, "raw": raw}
    return {
        "timestamp": matched.group(1),
        "level": matched.group(2),
        "scope": matched.group(3) or "",
        "message": matched.group(4),
        "raw": raw,
    }


def read_runtime_log_records(path: Path, workspace_name: str = "") -> list[dict[str, str]]:
    if not path.exists() or not path.is_file():
        return []
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    return [parse_runtime_log_line(line, workspace_name) for line in content.splitlines() if str(line).strip()]


def latest_workspace_job(workspace_name: str) -> dict[str, Any] | None:
    with JOB_LOCK:
        matches = [
            job.to_dict()
            for job in sorted(JOBS.values(), key=lambda item: item.started_at, reverse=True)
            if workspace_name in (job.workspace_members or [job.workspace])
        ]
    return matches[0] if matches else None


def latest_workspace_log_path(workspace_dir: Path, workspace_name: str) -> Path | None:
    latest_job = latest_workspace_job(workspace_name)
    if latest_job:
        log_path = Path(str(latest_job.get("log_path") or "")).expanduser()
        if log_path.exists() and log_path.is_file():
            return log_path

    logs_dir = workspace_dir / "logs"
    if not logs_dir.exists():
        return None

    candidates = sorted(logs_dir.glob("job_*.log"), key=lambda item: item.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]

    workspace_log = logs_dir / "workspace.log"
    if workspace_log.exists():
        return workspace_log
    return None


def latest_prefixed_json_payload(records: list[dict[str, str]], prefix: str) -> dict[str, Any] | None:
    for record in reversed(records):
        message = str(record.get("message") or "")
        if not message.startswith(prefix):
            continue
        try:
            payload = json.loads(message[len(prefix) :].strip())
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def prefixed_message_count(records: list[dict[str, str]], prefix: str) -> int:
    return sum(1 for record in records if str(record.get("message") or "").startswith(prefix))


def format_duration_text(seconds: int | float) -> str:
    total = max(0, int(seconds or 0))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}小时{minutes}分钟" if minutes else f"{hours}小时"
    if minutes:
        return f"{minutes}分钟{secs}秒" if secs else f"{minutes}分钟"
    return f"{secs}秒"


def format_file_timestamp(timestamp: float | int) -> str:
    if not timestamp:
        return ""
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(timestamp)))
    except Exception:
        return ""


def resolve_workspace_baidu_output_dir(task: dict[str, Any], workspace_dir: Path) -> Path:
    entries = task.get("baidu_share") or []
    if not entries:
        return workspace_dir / "downloads" / "baidu"
    raw_output = str((entries[0] or {}).get("output_subdir") or "").strip()
    if not raw_output:
        return workspace_dir / "downloads" / "baidu"
    output_path = Path(raw_output).expanduser()
    if output_path.is_absolute():
        return output_path if output_path.name == workspace_dir.name else output_path / workspace_dir.name
    return (workspace_dir / output_path).resolve()


def existing_open_target(raw_path: str, *, select_file: bool = False) -> tuple[Path, bool]:
    requested = Path(str(raw_path or "").strip()).expanduser()
    if requested.exists():
        resolved = requested.resolve()
        if requested.is_file():
            return (resolved if select_file else resolved.parent), bool(select_file)
        return resolved, False
    parent = requested.parent
    if parent.exists():
        return parent.resolve(), False
    raise FileNotFoundError(f"路径不存在：{requested}")


def open_local_path_in_explorer(raw_path: str, *, select_file: bool = False) -> dict[str, Any]:
    normalized = str(raw_path or "").strip()
    if not normalized:
        raise ValueError("path is required")

    target, should_select = existing_open_target(normalized, select_file=select_file)

    if os.name == "nt":
        if should_select:
            subprocess.Popen(
                ["explorer", "/select,", str(target)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
            )
        else:
            os.startfile(str(target))  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", str(target)])
    else:
        subprocess.Popen(["xdg-open", str(target)])

    return {
        "requested_path": normalized,
        "opened_path": str(target),
        "selected": should_select,
    }


def diagnose_workspace_baidu_official_wait(workspace_name: str) -> dict[str, Any]:
    workspace_dir = resolve_workspace_dir(workspace_name, create=False)
    task = get_workspace_task(workspace_name)
    latest_job = latest_workspace_job(workspace_name)
    log_path = latest_workspace_log_path(workspace_dir, workspace_name)
    if log_path is None:
        raise FileNotFoundError("当前工作间还没有可诊断的运行日志。")

    records = read_runtime_log_records(log_path, workspace_name)
    wait_payload = latest_prefixed_json_payload(records, "OFFICIAL_CLIENT_WAIT")
    if wait_payload is None:
        raise RuntimeError("当前日志里没有百度官方客户端下载等待记录。")
    move_plan_payload = latest_prefixed_json_payload(records, "OFFICIAL_CLIENT_MOVE_PLAN") or {}

    pending_labels = [repair_known_mojibake_text(str(item), workspace_name) for item in wait_payload.get("pending_labels") or []]
    pending_labels = [item for item in pending_labels if item]
    pending_count = int(wait_payload.get("pending_count") or len(pending_labels) or 0)
    wait_round = int(wait_payload.get("wait_round") or 0)
    wait_seconds = wait_round * 6
    search_roots = [
        repair_known_mojibake_text(str(item), workspace_name)
        for item in move_plan_payload.get("search_roots") or []
        if str(item or "").strip()
    ]
    total_targets = max(len(task.get("baidu_share") or []), pending_count)
    ready_count = max(0, total_targets - pending_count)
    moved_events = prefixed_message_count(records, "OFFICIAL_CLIENT_MOVED")
    skipped_in_plan = int(move_plan_payload.get("skipped_count") or 0)

    handoff_started_at = float(latest_job.get("started_at") or 0.0) if latest_job else 0.0
    if handoff_started_at <= 0:
        try:
            handoff_started_at = log_path.stat().st_mtime
        except OSError:
            handoff_started_at = time.time()

    search_root_details: list[dict[str, Any]] = []
    existing_roots: list[Path] = []
    for path_text in search_roots:
        path = Path(path_text).expanduser()
        exists = path.exists() and path.is_dir()
        if exists:
            try:
                existing_roots.append(path.resolve())
            except OSError:
                existing_roots.append(path)
        search_root_details.append(
            {
                "path": str(path),
                "exists": exists,
                "name": repair_known_mojibake_text(path.name or str(path), workspace_name),
            }
        )

    process_items: list[dict[str, Any]] = []
    process_count = 0
    client_records: list[dict[str, Any]] = []
    client_error = ""
    pending_details: list[dict[str, Any]] = []
    root_matches: dict[str, list[Path]] = {item.lower(): [] for item in pending_labels}

    try:
        handoff_module = load_baidu_official_handoff_module()
        try:
            main_exe, _ = handoff_module.detect_official_client()
        except Exception:
            main_exe = None
        try:
            process_items = handoff_module.list_running_baidu_processes()
            process_count = len(process_items)
        except Exception:
            process_items = []
            process_count = 0
        try:
            client_records = handoff_module.read_recent_download_records(main_exe, limit=240)
        except Exception:
            client_records = []
        if existing_roots and pending_labels:
            try:
                root_matches = handoff_module.scan_download_roots(existing_roots, {item.lower() for item in pending_labels})
            except Exception:
                root_matches = {item.lower(): [] for item in pending_labels}
    except Exception as exc:
        client_error = str(exc)

    lookback_seconds = 15 * 60
    old_same_name_labels: list[str] = []
    recent_match_labels: list[str] = []
    record_only_labels: list[str] = []
    missing_labels: list[str] = []
    missing_root_count = sum(1 for item in search_root_details if not item["exists"])

    for label in pending_labels:
        lowered = label.lower()
        matched_records = [
            item
            for item in client_records
            if Path(str(item.get("local_path") or "")).name.lower() == lowered
        ][:5]
        expected_size = max((int(item.get("file_size") or 0) for item in matched_records), default=0)
        raw_paths = root_matches.get(lowered) or []
        candidate_items: list[dict[str, Any]] = []
        recent_match_count = 0
        old_match_count = 0

        for candidate_path in raw_paths[:5]:
            try:
                stat = candidate_path.stat()
            except OSError:
                continue
            newest_ts = max(float(stat.st_mtime), float(stat.st_ctime))
            is_recent = newest_ts >= (handoff_started_at - lookback_seconds)
            if is_recent:
                recent_match_count += 1
            else:
                old_match_count += 1

            size_hint = ""
            if expected_size > 0:
                if stat.st_size == expected_size:
                    size_hint = "大小已达到客户端下载记录"
                elif stat.st_size < expected_size:
                    size_hint = f"大小 {stat.st_size} / {expected_size}"
                else:
                    size_hint = f"大小 {stat.st_size}，高于记录值 {expected_size}"

            candidate_items.append(
                {
                    "path": str(candidate_path),
                    "size": stat.st_size,
                    "modified_at": format_file_timestamp(newest_ts),
                    "is_recent": is_recent,
                    "time_hint": "属于本次任务附近的新文件" if is_recent else "时间明显早于本次任务，更像历史同名文件",
                    "size_hint": size_hint,
                }
            )

        record_items = [
            {
                "local_path": repair_known_mojibake_text(str(item.get("local_path") or ""), workspace_name),
                "file_size": int(item.get("file_size") or 0),
                "added_at": format_file_timestamp(int(item.get("add_time") or 0)),
                "status_code": int(item.get("status") or 0),
            }
            for item in matched_records
        ]

        if recent_match_count > 0:
            state = "recent_match_found"
            state_label = "已发现本次任务的新文件"
            recent_match_labels.append(label)
        elif old_match_count > 0:
            state = "only_old_same_name"
            state_label = "只发现旧的同名历史文件"
            old_same_name_labels.append(label)
        elif record_items:
            state = "record_only"
            state_label = "客户端下载记录里有它，但监控目录里没看到新文件"
            record_only_labels.append(label)
        else:
            state = "not_found"
            state_label = "监控目录和客户端下载记录里都没看到它"
            missing_labels.append(label)

        pending_details.append(
            {
                "label": label,
                "state": state,
                "state_label": state_label,
                "root_matches": candidate_items,
                "client_records": record_items,
            }
        )

    root_hit_counts: dict[str, int] = {}
    for detail in pending_details:
        for candidate in detail.get("root_matches") or []:
            path_text = str(candidate.get("path") or "")
            for root in search_root_details:
                root_path = str(root.get("path") or "")
                if path_text.lower().startswith(root_path.lower()):
                    root_hit_counts[root_path] = root_hit_counts.get(root_path, 0) + 1
                    break
    for root in search_root_details:
        root["match_count"] = root_hit_counts.get(str(root["path"]), 0)

    suggestions: list[str] = []
    if process_count <= 0:
        suggestions.append("当前没有检测到百度网盘官方客户端进程，请先确认官方客户端没有被关闭。")
    if missing_root_count > 0:
        suggestions.append("有部分监控目录当前不存在，说明客户端下载路径可能已经改过，和这次任务接管时不一致。")
    if old_same_name_labels:
        preview = "、".join(old_same_name_labels[:4])
        suffix = " 等多个文件" if len(old_same_name_labels) > 4 else ""
        suggestions.append(f"监控目录里发现了旧的同名文件：{preview}{suffix}。这些文件时间早于本次任务，脚本不会误搬运它们。")
    if recent_match_labels:
        preview = "、".join(recent_match_labels[:4])
        suffix = " 等多个文件" if len(recent_match_labels) > 4 else ""
        suggestions.append(f"监控目录里已经出现本次任务的新文件：{preview}{suffix}。这通常说明客户端下载仍在继续，或刚下载完还在等待稳定后搬运。")
    if record_only_labels and not recent_match_labels:
        preview = "、".join(record_only_labels[:4])
        suffix = " 等多个文件" if len(record_only_labels) > 4 else ""
        suggestions.append(f"客户端下载记录里有这些文件：{preview}{suffix}，但监控目录里没看到新文件。常见原因是客户端下载目录变了，或下载没有真正开始。")
    if missing_labels and not recent_match_labels and not record_only_labels:
        preview = "、".join(missing_labels[:4])
        suffix = " 等多个文件" if len(missing_labels) > 4 else ""
        suggestions.append(f"脚本监控的目录和客户端下载记录里都没发现这些待完成文件：{preview}{suffix}。请优先检查百度网盘客户端当前实际下载位置。")
    if wait_seconds >= 1800:
        suggestions.append("这一步已经等待很久了。如果客户端下载器里显示已经下完但界面还是不动，可以把对应文件确认放到目标目录后再重跑。")
    if client_error:
        suggestions.append(f"本次高级诊断没有完整读到客户端下载状态：{client_error}")
    if not suggestions:
        suggestions.append("目前更像是客户端下载仍在继续，建议先看官方客户端里的传输列表和实际下载目录。")

    summary = (
        f"当前原素材已到位 {ready_count}/{total_targets} 个，剩余 {pending_count} 个等待百度网盘官方客户端下载并搬运。"
        if total_targets
        else f"当前还有 {pending_count} 个文件在等待百度网盘官方客户端下载并搬运。"
    )
    process_summary = (
        f"已检测到 {process_count} 个百度网盘相关进程。"
        if process_count > 0
        else "当前未检测到百度网盘相关进程。"
    )

    action_paths: list[dict[str, str]] = [
        {"label": "工作间目录", "path": str(workspace_dir)},
        {"label": "日志目录", "path": str(log_path.parent)},
        {"label": "原素材目录", "path": str(resolve_workspace_baidu_output_dir(task, workspace_dir))},
    ]
    for index, root in enumerate(search_root_details[:4], start=1):
        if root["exists"]:
            action_paths.append({"label": f"监控目录 {index}", "path": str(root["path"])})

    deduped_actions: list[dict[str, str]] = []
    seen_action_paths: set[str] = set()
    for item in action_paths:
        path_text = str(item.get("path") or "").strip()
        if not path_text or path_text in seen_action_paths:
            continue
        seen_action_paths.add(path_text)
        deduped_actions.append({"label": str(item.get("label") or "路径"), "path": path_text})

    process_preview = [
        {
            "name": repair_known_mojibake_text(str(item.get("Name") or ""), workspace_name),
            "pid": int(item.get("ProcessId") or 0),
            "path": repair_known_mojibake_text(str(item.get("ExecutablePath") or ""), workspace_name),
        }
        for item in process_items[:6]
    ]

    return {
        "workspace": workspace_name,
        "blocked": wait_seconds >= 900 and pending_count > 0,
        "log_path": str(log_path),
        "summary": summary,
        "process_summary": process_summary,
        "wait_round": wait_round,
        "wait_seconds": wait_seconds,
        "wait_text": format_duration_text(wait_seconds),
        "pending_count": pending_count,
        "pending_labels": pending_labels,
        "total_count": total_targets,
        "ready_count": ready_count,
        "moved_count": moved_events,
        "skipped_count": skipped_in_plan,
        "process_count": process_count,
        "processes": process_preview,
        "search_root_count": len(search_root_details),
        "search_roots": search_root_details,
        "pending_details": pending_details,
        "suggestions": suggestions,
        "action_paths": deduped_actions,
        "client_error": client_error,
    }


def load_control_center_html() -> str:
    if CONTROL_CENTER_UI.exists():
        return CONTROL_CENTER_UI.read_text(encoding="utf-8")
    return HTML_PAGE


def unique_destination_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    for index in range(2, 10_000):
        candidate = path.with_name(f"{stem}_{index}{suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"too many duplicate files for {path.name}")


def collect_local_files(raw_paths: list[str], allowed_extensions: set[str]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for raw_path in raw_paths:
        if not str(raw_path or "").strip():
            continue
        source = Path(str(raw_path).strip()).expanduser()
        if not source.exists():
            raise FileNotFoundError(f"local path not found: {source}")
        candidates: list[Path]
        if source.is_dir():
            candidates = [
                item
                for item in sorted(source.rglob("*"))
                if item.is_file() and item.suffix.lower() in allowed_extensions
            ]
        elif source.is_file() and source.suffix.lower() in allowed_extensions:
            candidates = [source]
        else:
            continue
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved not in seen:
                seen.add(resolved)
                files.append(resolved)
    return files


def collect_local_video_files(raw_paths: list[str]) -> list[Path]:
    return collect_local_files(raw_paths, SUPPORTED_VIDEO_EXTENSIONS)


def collect_local_audio_files(raw_paths: list[str]) -> list[Path]:
    return collect_local_files(raw_paths, SUPPORTED_AUDIO_EXTENSIONS)


def copy_local_files(target_dir: Path, source_files: list[Path]) -> dict[str, Any]:
    if not source_files:
        raise ValueError("提供的路径里没有找到可导入的有效文件")
    target_dir.mkdir(parents=True, exist_ok=True)
    copied: list[dict[str, str]] = []
    for source in source_files:
        destination = unique_destination_path(target_dir / source.name)
        shutil.copy2(source, destination)
        copied.append({"from": str(source), "to": str(destination)})
    return {"copied": copied, "count": len(copied), "target_dir": str(target_dir)}


def target_dir_for_import_kind(workspace_dir: Path, kind: str) -> Path:
    if kind == "source":
        return workspace_dir / "downloads" / "baidu"
    if kind in {"reference", "subtitle_video"}:
        return workspace_dir / "downloads" / "douyin"
    if kind == "bgm":
        return workspace_bgm_dir(workspace_dir)
    if kind == "subtitle_file":
        return workspace_dir / "subtitles"
    raise ValueError("kind must be source, reference, subtitle_video, bgm, or subtitle_file")


def import_local_files(workspace_name: str, kind: str, raw_paths: list[str]) -> dict[str, Any]:
    workspace_dir = resolve_workspace_dir(workspace_name, create=True)
    target_dir = target_dir_for_import_kind(workspace_dir, kind)
    if kind in {"source", "reference", "subtitle_video"}:
        source_files = collect_local_video_files(raw_paths)
        return copy_local_files(target_dir, source_files)
    if kind == "bgm":
        source_files = collect_local_audio_files(raw_paths)
        return copy_local_files(target_dir, source_files)
    if kind == "subtitle_file":
        source_files = collect_local_files(raw_paths, SUPPORTED_SUBTITLE_EXTENSIONS)
        return copy_local_files(target_dir, source_files)
    raise ValueError("kind must be source, reference, subtitle_video, bgm, or subtitle_file")


def sanitize_uploaded_filename(filename: str) -> str:
    raw_name = Path(str(filename or "").replace("\\", "/")).name.strip()
    if not raw_name:
        raise ValueError("upload filename is required")
    normalized = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", raw_name)
    normalized = normalized.strip(" .")
    if not normalized:
        raise ValueError("upload filename is invalid")
    return normalized


def upload_file_to_workspace(
    workspace_name: str,
    kind: str,
    filename: str,
    stream: Any,
    content_length: int,
) -> dict[str, Any]:
    if content_length <= 0:
        raise ValueError("upload body is empty")

    workspace_dir = resolve_workspace_dir(workspace_name, create=True)
    target_dir = target_dir_for_import_kind(workspace_dir, kind)
    safe_name = sanitize_uploaded_filename(filename)
    suffix = Path(safe_name).suffix.lower()

    if kind in {"source", "reference", "subtitle_video"} and suffix not in SUPPORTED_VIDEO_EXTENSIONS:
        raise ValueError(f"unsupported video file type: {safe_name}")
    if kind == "bgm" and suffix not in SUPPORTED_AUDIO_EXTENSIONS:
        raise ValueError(f"unsupported audio file type: {safe_name}")
    if kind == "subtitle_file" and suffix not in SUPPORTED_SUBTITLE_EXTENSIONS:
        raise ValueError(f"unsupported subtitle file type: {safe_name}")

    target_dir.mkdir(parents=True, exist_ok=True)
    destination = unique_destination_path(target_dir / safe_name)
    remaining = int(content_length)
    with destination.open("wb") as fh:
        while remaining > 0:
            chunk = stream.read(min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError("upload body is truncated")
            fh.write(chunk)
            remaining -= len(chunk)

    return {
        "count": 1,
        "workspace": workspace_dir.name,
        "target_dir": str(target_dir),
        "saved_name": destination.name,
        "saved_path": str(destination),
        "bytes": int(content_length),
    }


def pick_local_folder(initial_path: str = "") -> dict[str, Any]:
    if os.name != "nt":
        raise RuntimeError("当前系统暂不支持原生文件夹选择器")

    initial = str(initial_path or "").strip()
    script = """
Add-Type -AssemblyName System.Windows.Forms | Out-Null
$dialog = New-Object System.Windows.Forms.FolderBrowserDialog
$dialog.Description = '选择文件夹'
$dialog.ShowNewFolderButton = $true
$initial = $args[0]
if ($initial) {
  try {
    if (Test-Path -LiteralPath $initial) {
      $dialog.SelectedPath = (Resolve-Path -LiteralPath $initial).Path
    }
  } catch {}
}
$result = $dialog.ShowDialog()
if ($result -eq [System.Windows.Forms.DialogResult]::OK) {
  Write-Output $dialog.SelectedPath
  exit 0
}
exit 2
"""
    result = subprocess.run(
        ["powershell", "-NoProfile", "-STA", "-Command", script, initial],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=180,
        check=False,
        creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
    )
    if result.returncode == 2:
        return {"selected_path": "", "cancelled": True}
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip().splitlines()
        raise RuntimeError(detail[-1] if detail else "文件夹选择失败")
    selected_path = (result.stdout or "").strip().splitlines()
    return {
        "selected_path": selected_path[-1].strip() if selected_path else "",
        "cancelled": False,
    }


def set_workspace_cover_from_baidu_share(
    workspace_name: str,
    *,
    share_url: str,
    target_fsid: str = "",
    target_path: str = "",
    target_filename: str = "",
    share_key: str = "",
) -> dict[str, Any]:
    workspace_dir = resolve_workspace_dir(workspace_name, create=True)
    covers_dir = workspace_cover_dir(workspace_dir)
    temp_root = workspace_dir / "temp"
    covers_dir.mkdir(parents=True, exist_ok=True)
    temp_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="cover_select_", dir=str(temp_root)) as temp_dir_text:
        temp_dir = Path(temp_dir_text)
        download_result = download_baidu_share_file_to_path(
            share_url,
            target_fsid=target_fsid,
            target_path=target_path,
            target_filename=target_filename,
            output_dir=temp_dir,
            download_threads=1,
        )
        downloaded_path = Path(str(download_result["downloaded_path"]))
        original_name = downloaded_path.name
        suffix = downloaded_path.suffix.lower()
        if suffix not in SUPPORTED_IMAGE_EXTENSIONS:
            raise RuntimeError(f"所选文件不是受支持的封面图片：{downloaded_path.name}")

        final_cover_path = covers_dir / f"{COVER_ARTIFACT_STEM}{suffix}"
        for candidate in covers_dir.glob(f"{COVER_ARTIFACT_STEM}.*"):
            if candidate.resolve() != final_cover_path.resolve():
                candidate.unlink(missing_ok=True)
        if final_cover_path.exists():
            final_cover_path.unlink(missing_ok=True)
        shutil.move(str(downloaded_path), str(final_cover_path))

    task = get_workspace_task(workspace_dir.name)
    settings = dict(task.get("settings") or {})
    settings["cover_image_path"] = safe_workspace_relative_path(workspace_dir, final_cover_path)
    settings["cover_image_name"] = str(target_filename or original_name).strip() or original_name
    settings["cover_image_share_key"] = str(share_key or target_fsid or target_path or target_filename).strip()
    task["settings"] = settings
    save_workspace_task(workspace_dir.name, task)
    return {
        "workspace": workspace_dir.name,
        "cover_image_path": settings["cover_image_path"],
        "cover_image_name": settings["cover_image_name"],
        "cover_image_share_key": settings["cover_image_share_key"],
    }


def clear_workspace_cover(workspace_name: str) -> dict[str, Any]:
    workspace_dir = resolve_workspace_dir(workspace_name, create=True)
    task = get_workspace_task(workspace_dir.name)
    settings = dict(task.get("settings") or {})
    settings["cover_image_path"] = ""
    settings["cover_image_name"] = ""
    settings["cover_image_share_key"] = ""
    task["settings"] = settings
    save_workspace_task(workspace_dir.name, task)
    return {"workspace": workspace_dir.name, "cleared": True}


def resolve_workspace_cover_image(workspace_name: str, raw_path: str = "") -> Path:
    workspace_dir = resolve_workspace_dir(workspace_name, create=False)
    cover_path = resolve_workspace_optional_path(workspace_dir, raw_path)
    if cover_path is None:
        task = get_workspace_task(workspace_dir.name)
        settings = task.get("settings") or {}
        cover_path = resolve_workspace_optional_path(workspace_dir, str(settings.get("cover_image_path") or ""))
    if cover_path is None or not cover_path.exists() or not cover_path.is_file():
        raise FileNotFoundError("当前工作间还没有可预览的封面图片")
    if cover_path.suffix.lower() not in SUPPORTED_IMAGE_EXTENSIONS:
        raise ValueError("当前封面文件不是受支持的图片格式")
    return cover_path


def test_ai_api_connection(
    *,
    ai_api_key: str,
    ai_model: str,
    ai_api_url: str,
    ai_fallback_models: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    api_key = str(ai_api_key or "").strip()
    model = str(ai_model or "").strip()
    api_url = str(ai_api_url or "").strip()
    if not api_key:
        raise ValueError("AI API Key 不能为空")
    if not model:
        raise ValueError("AI 主模型不能为空")
    if not api_url:
        raise ValueError("AI API 地址不能为空")
    python_path = resolve_python([
        PROJECT_ROOT / "modules" / "auto_clip_engine" / ".venv" / "Scripts" / "python.exe",
    ])

    payload = {
        "ai_api_key": api_key,
        "ai_model": model,
        "ai_api_url": api_url,
        "ai_fallback_models": ai_fallback_models or [],
    }
    script = textwrap.dedent(
        f"""
        import json
        from modules.auto_clip_engine.drama_clone_core import AINarrationGenerator

        payload = {json.dumps(payload, ensure_ascii=False)}
        tester = AINarrationGenerator(
            api_key=payload["ai_api_key"],
            model=payload["ai_model"],
            api_url=payload["ai_api_url"],
            fallback_models=payload["ai_fallback_models"],
        )
        response_text = tester.request_text_completion(
            system_prompt={json.dumps("你是 API 连通性测试助手。请只返回简短结果，不要输出多余解释。", ensure_ascii=False)},
            user_prompt={json.dumps("请只回复：连接成功", ensure_ascii=False)},
            temperature=0.0,
            label="AI API test",
            max_tokens=32,
            timeout=45,
        )
        if not response_text:
            detail = (tester.last_rewrite_issue or tester.last_ai_issue or "AI 接口未返回有效内容").strip()
            raise RuntimeError(detail)

        active_index = int(getattr(tester, "_active_config_index", 0) or 0)
        configs = list(getattr(tester, "_configs", []) or [])
        active_config = configs[active_index] if 0 <= active_index < len(configs) else {{}}
        preview = str(response_text or "").strip().replace("\\r", " ").replace("\\n", " ")
        if len(preview) > 80:
            preview = preview[:80] + "..."
        result = {{
            "active_model": str(tester.model or payload["ai_model"]),
            "active_api_url": str(tester.api_url or payload["ai_api_url"]),
            "active_label": str(active_config.get("label") or ""),
            "used_fallback": active_index > 0,
            "response_preview": preview,
        }}
        print("__AI_TEST_RESULT__" + json.dumps(result, ensure_ascii=False))
        """
    ).strip()

    result = subprocess.run(
        [python_path, "-c", script],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        check=False,
        env=build_subprocess_env(),
    )
    output = (result.stdout or "").strip()
    if result.returncode != 0:
        detail = output.splitlines()[-1] if output else ""
        raise RuntimeError(detail or "AI API test failed")

    result_line = ""
    for line in reversed(output.splitlines()):
        if line.startswith("__AI_TEST_RESULT__"):
            result_line = line.removeprefix("__AI_TEST_RESULT__")
            break
    if not result_line:
        raise RuntimeError(output.splitlines()[-1] if output else "AI API test returned no result")

    payload = json.loads(result_line)
    return {
        "active_model": str(payload.get("active_model") or model),
        "active_api_url": str(payload.get("active_api_url") or api_url),
        "active_label": str(payload.get("active_label") or ""),
        "used_fallback": bool(payload.get("used_fallback")),
        "response_preview": str(payload.get("response_preview") or ""),
    }


def enrich_module_entry(item: dict[str, Any], module_lookup: dict[str, Any]) -> dict[str, Any]:
    result = dict(item)
    source = module_lookup.get(result.get("id"), {})
    result.setdefault("type", source.get("type", "unknown"))
    result.setdefault("entry", source.get("entry", ""))
    result.setdefault("path", source.get("path", ""))
    available = path_exists(result.get("path")) or path_exists(result.get("entry"))
    result["available"] = available
    if result.get("status") == "ready" and not available:
        result["status"] = "missing"
    return result


def load_module_views() -> dict[str, Any]:
    module_manifest = read_json(
        MODULE_MANIFEST,
        {
            "project_name": "server_auto_clip",
            "project_root": str(PROJECT_ROOT),
            "runtime_root": str(PROJECT_ROOT / "runtime"),
            "workspace_root": str(WORKSPACE_ROOT),
            "modules": {},
        },
    )
    control_manifest = read_json(CONTROL_CENTER_MANIFEST, {"current_modules": [], "planned_modules": []})
    module_lookup = module_manifest.get("modules") or {}
    current_modules = [enrich_module_entry(item, module_lookup) for item in control_manifest.get("current_modules", [])]
    planned_modules = [dict(item) for item in control_manifest.get("planned_modules", [])]
    return {
        "module_manifest": module_manifest,
        "control_center_manifest": {
            "current_modules": current_modules,
            "planned_modules": planned_modules,
        },
    }


def next_job_id() -> str:
    global JOB_COUNTER
    with JOB_LOCK:
        JOB_COUNTER += 1
        return f"job_{int(time.time())}_{JOB_COUNTER:03d}"


def resolve_python(candidates: list[Path] | None = None) -> str:
    for candidate in candidates or []:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def build_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    return env


def run_checked(command: list[str], *, cwd: Path, timeout: int, error_prefix: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stdout or "").strip()
        detail = detail.splitlines()[-1] if detail else ""
        raise RuntimeError(f"{error_prefix}: {detail or subprocess.list2cmdline(command)}")
    return result


def ensure_baidu_share_python() -> str:
    module_dir = PROJECT_ROOT / "modules" / "baidu_share_downloader"
    requirements_path = module_dir / "requirements.txt"
    venv_dir = module_dir / ".venv"
    python_path = venv_dir / "Scripts" / "python.exe"

    if not python_path.exists():
        run_checked(
            [sys.executable, "-m", "venv", str(venv_dir)],
            cwd=module_dir,
            timeout=600,
            error_prefix="创建百度网盘下载环境失败",
        )

    check_result = subprocess.run(
        [str(python_path), "-c", "import requests, selenium"],
        cwd=str(module_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        check=False,
    )
    if check_result.returncode == 0:
        return str(python_path)

    run_checked(
        [str(python_path), "-m", "ensurepip", "--upgrade"],
        cwd=module_dir,
        timeout=600,
        error_prefix="初始化百度网盘下载 pip 失败",
    )
    if requirements_path.exists():
        run_checked(
            [
                str(python_path),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "-r",
                str(requirements_path),
            ],
            cwd=module_dir,
            timeout=1800,
            error_prefix="安装百度网盘下载依赖失败",
        )
    else:
        run_checked(
            [
                str(python_path),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "requests",
                "selenium",
            ],
            cwd=module_dir,
            timeout=1800,
            error_prefix="安装百度网盘下载依赖失败",
        )
    run_checked(
        [str(python_path), "-c", "import requests, selenium"],
        cwd=module_dir,
        timeout=120,
        error_prefix="百度网盘下载依赖校验失败",
    )
    return str(python_path)


def get_default_edge_binary() -> Path | None:
    candidates = [
        Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def get_default_edge_user_data_dir() -> Path:
    return Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))) / "Microsoft" / "Edge" / "User Data"


def ensure_managed_baidu_edge_user_data() -> Path:
    managed_root = BAIDU_MANAGED_EDGE_USER_DATA
    managed_profile = managed_root / BAIDU_EDGE_PROFILE
    source_root = get_default_edge_user_data_dir()
    source_profile = source_root / BAIDU_EDGE_PROFILE
    managed_root.mkdir(parents=True, exist_ok=True)

    managed_cookie_names: set[str] = set()
    for cookie_db in [managed_profile / "Network" / "Cookies", managed_profile / "Cookies"]:
        rows = query_sqlite_rows(
            cookie_db,
            "select distinct name from cookies where host_key like '%baidu%' order by name",
        )
        for row in rows:
            if row and row[0]:
                managed_cookie_names.add(str(row[0]))

    system_cookie_names: set[str] = set()
    for cookie_db in [source_profile / "Network" / "Cookies", source_profile / "Cookies"]:
        rows = query_sqlite_rows(
            cookie_db,
            "select distinct name from cookies where host_key like '%baidu%' order by name",
        )
        for row in rows:
            if row and row[0]:
                system_cookie_names.add(str(row[0]))

    managed_has_login = any(name in BAIDU_LOGIN_COOKIE_NAMES for name in managed_cookie_names)
    system_has_login = any(name in BAIDU_LOGIN_COOKIE_NAMES for name in system_cookie_names)

    if not managed_profile.exists():
        if (source_root / "Local State").exists():
            shutil.copy2(source_root / "Local State", managed_root / "Local State")
        if source_profile.exists():
            shutil.copytree(source_profile, managed_profile, dirs_exist_ok=True)
        managed_profile.mkdir(parents=True, exist_ok=True)
    elif not managed_has_login and system_has_login:
        if (source_root / "Local State").exists():
            shutil.copy2(source_root / "Local State", managed_root / "Local State")
        if source_profile.exists():
            shutil.copytree(source_profile, managed_profile, dirs_exist_ok=True)
    return managed_root


def query_sqlite_rows(database_path: Path, sql: str) -> list[tuple[Any, ...]]:
    if not database_path.exists():
        return []

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix="control_center_sqlite_", suffix=".db", delete=False) as handle:
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


def list_managed_baidu_edge_processes() -> list[dict[str, Any]]:
    if os.name != "nt":
        return []

    target = str(BAIDU_MANAGED_EDGE_USER_DATA).replace("'", "''")
    script = f"""
$target = '{target}'
$items = Get-CimInstance Win32_Process -Filter "Name='msedge.exe'" |
  Where-Object {{ $_.CommandLine -like "*$target*" }} |
  Select-Object ProcessId, CommandLine
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


def get_baidu_login_state() -> dict[str, Any]:
    managed_root = ensure_managed_baidu_edge_user_data()
    profile_root = managed_root / BAIDU_EDGE_PROFILE
    cookie_names: set[str] = set()
    for cookie_db in [profile_root / "Network" / "Cookies", profile_root / "Cookies"]:
        rows = query_sqlite_rows(
            cookie_db,
            "select distinct name from cookies where host_key like '%baidu%' order by name",
        )
        for row in rows:
            if row and row[0]:
                cookie_names.add(str(row[0]))

    logged_in = any(name in BAIDU_LOGIN_COOKIE_NAMES for name in cookie_names)
    processes = list_managed_baidu_edge_processes()
    return {
        "logged_in": logged_in,
        "cookie_names": sorted(cookie_names),
        "profile_root": str(profile_root),
        "profile_in_use": bool(processes),
        "process_count": len(processes),
    }


def open_baidu_login_window() -> dict[str, Any]:
    login_state = get_baidu_login_state()
    if login_state.get("profile_in_use"):
        return {
            "url": BAIDU_LOGIN_URL,
            "via": "edge_default_profile",
            "already_open": True,
            "login_state": login_state,
        }
    edge_binary = get_default_edge_binary()
    if edge_binary is None:
        webbrowser.open(BAIDU_LOGIN_URL)
        return {"url": BAIDU_LOGIN_URL, "via": "default_browser", "login_state": login_state}

    edge_user_data = ensure_managed_baidu_edge_user_data()
    command = [
        str(edge_binary),
        f"--user-data-dir={edge_user_data}",
        f"--profile-directory={BAIDU_EDGE_PROFILE}",
        "--new-window",
        BAIDU_LOGIN_URL,
    ]
    subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return {"url": BAIDU_LOGIN_URL, "via": "edge_default_profile", "command": command, "login_state": login_state}


def remove_control_center_pid_file() -> None:
    try:
        if CONTROL_CENTER_PID_FILE.exists():
            raw = CONTROL_CENTER_PID_FILE.read_text(encoding="ascii", errors="ignore").strip()
            if not raw or raw == str(os.getpid()):
                CONTROL_CENTER_PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def list_baidu_share_files(share_url: str) -> dict[str, Any]:
    share_url = str(share_url or "").strip()
    if not share_url:
        raise ValueError("share_url is required")
    ensure_baidu_login_ready(auto_open_login=True)

    script_path = PROJECT_ROOT / "modules" / "baidu_share_downloader" / "baidu_share_downloader.py"
    python_path = ensure_baidu_share_python()
    command = [
        python_path,
        str(script_path),
        share_url,
        "--list-only",
    ]
    result = subprocess.run(
        command,
        cwd=str(script_path.parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
        check=False,
        env=build_subprocess_env(),
    )

    output = result.stdout or ""
    files: list[dict[str, Any]] = []
    for line in output.splitlines():
        prefix = ""
        if line.startswith("SHARE_FILES "):
            prefix = "SHARE_FILES "
        elif line.startswith("MP4_FILES "):
            prefix = "MP4_FILES "
        if not prefix:
            continue
        payload = line[len(prefix) :].strip()
        parsed = json.loads(payload)
        if isinstance(parsed, list):
            files = [item for item in parsed if isinstance(item, dict)]
            break

    if result.returncode != 0:
        detail = output.strip().splitlines()[-1] if output.strip() else "baidu share listing failed"
        raise RuntimeError(detail)
    if not files:
        raise RuntimeError("未能从网盘链接中提取到可下载文件")

    return {
        "share_url": share_url,
        "files": files,
        "command": command,
    }


def download_baidu_share_file_to_path(
    share_url: str,
    *,
    target_fsid: str = "",
    target_path: str = "",
    target_filename: str = "",
    output_dir: Path,
    download_threads: int = 1,
) -> dict[str, Any]:
    ensure_baidu_login_ready(auto_open_login=True)

    script_path = PROJECT_ROOT / "modules" / "baidu_share_downloader" / "baidu_share_downloader.py"
    python_path = ensure_baidu_share_python()
    command = [
        python_path,
        str(script_path),
        share_url,
        "--output-dir",
        str(output_dir),
    ]
    if target_fsid:
        command.extend(["--target-fsid", target_fsid])
    if target_path:
        command.extend(["--target-path", target_path])
    if target_filename:
        command.extend(["--target-filename", target_filename])
    command.extend(["--download-threads", str(max(1, int(download_threads or 1)))])

    result = subprocess.run(
        command,
        cwd=str(script_path.parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=1800,
        check=False,
        env=build_subprocess_env(),
    )

    output = result.stdout or ""
    downloaded_path: Path | None = None
    downloaded_size = 0
    for line in output.splitlines():
        if not line.startswith("DOWNLOADED "):
            continue
        parts = line.split(" ")
        if len(parts) < 3:
            continue
        candidate_path = " ".join(parts[1:-1]).strip()
        size_text = parts[-1].strip()
        candidate = Path(candidate_path)
        if candidate.exists():
            downloaded_path = candidate
        try:
            downloaded_size = int(size_text)
        except ValueError:
            downloaded_size = 0

    if result.returncode != 0:
        detail = output.strip().splitlines()[-1] if output.strip() else "baidu share download failed"
        raise RuntimeError(detail)
    if downloaded_path is None or not downloaded_path.exists():
        raise RuntimeError("未能确认百度网盘文件下载结果")
    return {
        "downloaded_path": str(downloaded_path),
        "downloaded_size": downloaded_size or downloaded_path.stat().st_size,
        "command": command,
    }


def workspace_has_active_jobs(workspace_name: str) -> bool:
    with JOB_LOCK:
        return any(
            workspace_name in (job.workspace_members or [job.workspace]) and job.status in {"running", "stopping"}
            for job in JOBS.values()
        )


def clear_workspace_related_job_cache(workspace_dir: Path) -> dict[str, Any]:
    removed_job_ids: list[str] = []
    removed_log_paths: list[str] = []
    workspace_name = workspace_dir.name
    workspace_root = workspace_dir.resolve()

    with JOB_LOCK:
        removable_ids = [
            job_id
            for job_id, job in JOBS.items()
            if workspace_name in (job.workspace_members or [job.workspace]) and job.status not in {"running", "stopping"}
        ]
        for job_id in removable_ids:
            job = JOBS.pop(job_id)
            removed_job_ids.append(job_id)

            raw_log_path = str(job.log_path or "").strip()
            if not raw_log_path:
                continue
            log_path = Path(raw_log_path).expanduser()
            try:
                resolved_log_path = log_path.resolve()
            except OSError:
                resolved_log_path = log_path

            # 工作间目录里的日志会随着整个工作间一起删除；这里只补清理额外的全局日志。
            if workspace_root in resolved_log_path.parents:
                continue
            if not resolved_log_path.exists() or not resolved_log_path.is_file():
                continue
            try:
                resolved_log_path.unlink()
            except OSError:
                continue
            removed_log_paths.append(str(resolved_log_path))

    return {
        "cleared_job_cache_count": len(removed_job_ids),
        "cleared_job_ids": removed_job_ids,
        "cleared_external_log_count": len(removed_log_paths),
        "cleared_external_logs": removed_log_paths,
    }


def delete_workspace(workspace_name: str) -> dict[str, Any]:
    workspace_dir = resolve_workspace_dir(workspace_name, create=False)
    if workspace_has_active_jobs(workspace_dir.name):
        raise RuntimeError("当前工作间还有运行中的任务，不能删除")
    cache_result = clear_workspace_related_job_cache(workspace_dir)
    shutil.rmtree(workspace_dir)
    return {
        "workspace": workspace_dir.name,
        "deleted_path": str(workspace_dir),
        **cache_result,
    }


def rename_workspace(workspace_name: str, new_name: str) -> dict[str, Any]:
    source_dir = resolve_workspace_dir(workspace_name, create=False)
    if workspace_has_active_jobs(source_dir.name):
        raise RuntimeError("当前工作间还有运行中的任务，不能改名")

    normalized_new_name = str(new_name or "").strip()
    if not normalized_new_name:
        raise ValueError("new_name is required")
    if normalized_new_name == source_dir.name:
        return {
            "workspace": source_dir.name,
            "old_workspace": source_dir.name,
            "renamed_path": str(source_dir),
        }

    target_dir = resolve_workspace_dir(normalized_new_name, create=False)
    if target_dir.exists():
        raise RuntimeError("目标工作间名称已存在，请换一个名字")

    shutil.move(str(source_dir), str(target_dir))

    task_path = target_dir / "task.json"
    if task_path.exists():
        task = read_json(task_path, default_workspace_task(target_dir.name))
        if not isinstance(task, dict):
            task = default_workspace_task(target_dir.name)
        task["workspace_name"] = target_dir.name
        task = normalize_task_payload(task, target_dir.name)
        task["baidu_share"] = sanitize_baidu_share_entries(task.get("baidu_share") or [])
        task_path.write_text(json.dumps(task, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "workspace": target_dir.name,
        "old_workspace": source_dir.name,
        "renamed_path": str(target_dir),
    }


def ensure_baidu_login_ready(*, auto_open_login: bool) -> dict[str, Any]:
    login_state = get_baidu_login_state()
    if login_state.get("profile_in_use"):
        raise RuntimeError("百度专用登录窗口还在运行，请先关闭那个窗口，再提取网盘或开始处理。")
    if login_state["logged_in"]:
        return login_state
    if auto_open_login:
        open_baidu_login_window()
        raise RuntimeError("百度专用登录窗口还没有登录，已为你打开登录窗口。请先在弹出的窗口里完成登录并关闭，再重新操作。")
    raise RuntimeError("百度专用登录窗口还没有登录，请先点击“登录百度”。")


def ensure_baidu_login_for_workspaces(workspace_names: list[str]) -> None:
    needs_baidu = False
    for workspace_name in workspace_names:
        task = get_workspace_task(workspace_name)
        if task.get("baidu_share"):
            needs_baidu = True
            break
    if not needs_baidu:
        return

    ensure_baidu_login_ready(auto_open_login=True)


def start_batch_job(workspace_names: list[str], *, workspace_parallel: int | None = None) -> JobState:
    normalized_names: list[str] = []
    seen: set[str] = set()
    for raw_name in workspace_names:
        workspace_dir = resolve_workspace_dir(raw_name, create=False)
        if workspace_dir.name not in seen:
            seen.add(workspace_dir.name)
            normalized_names.append(workspace_dir.name)
    if not normalized_names:
        raise ValueError("at least one workspace is required")
    ensure_baidu_login_for_workspaces(normalized_names)

    command = [resolve_python(), "-u", str(BATCH_RUNNER)]
    for workspace_name in normalized_names:
        command.extend(["--workspace", workspace_name])
    if workspace_parallel is not None:
        command.extend(["--workspace-parallel", str(max(1, int(workspace_parallel)))])

    if len(normalized_names) == 1:
        log_dir = resolve_workspace_dir(normalized_names[0], create=True) / "logs"
        workspace_label = normalized_names[0]
    else:
        log_dir = PROJECT_ROOT / "logs"
        workspace_label = "批量: " + ", ".join(normalized_names)
    log_dir.mkdir(parents=True, exist_ok=True)
    job_id = next_job_id()
    log_path = log_dir / f"{job_id}.log"
    process = subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        bufsize=0,
        env=build_subprocess_env(),
    )
    job = JobState(
        job_id=job_id,
        workspace=workspace_label,
        command=command,
        log_path=str(log_path),
        started_at=time.time(),
        pid=process.pid,
        process=process,
        workspace_members=list(normalized_names),
    )
    with JOB_LOCK:
        JOBS[job_id] = job

    def reader() -> None:
        assert process.stdout is not None
        workspace_hint = normalized_names[0] if len(normalized_names) == 1 else ""
        with log_path.open("a", encoding="utf-8") as fh:
            for raw_line in iter(process.stdout.readline, b""):
                stripped = decode_subprocess_output_line(raw_line, workspace_hint)
                fh.write(stripped + "\n")
                fh.flush()
                with JOB_LOCK:
                    job.recent_lines.append(stripped)
            return_code = process.wait()
            with JOB_LOCK:
                job.return_code = return_code
                job.status = "completed" if return_code == 0 else "failed"
                job.process = None

    threading.Thread(target=reader, daemon=True).start()
    return job


def start_workspace_job(workspace_name: str) -> JobState:
    return start_batch_job([workspace_name])


def stop_job(job_id: str, *, reason: str = "") -> bool:
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job or not job.process:
            return False
        process = job.process
        pid = job.pid or process.pid
        job.status = "stopping"
    if reason:
        append_job_stop_notice(job, reason)
    if os.name == "nt" and pid:
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
        )
    else:
        process.terminate()
    return True


def list_jobs() -> list[dict[str, Any]]:
    with JOB_LOCK:
        return [job.to_dict() for job in sorted(JOBS.values(), key=lambda item: item.started_at, reverse=True)]


def get_job(job_id: str) -> dict[str, Any] | None:
    with JOB_LOCK:
        job = JOBS.get(job_id)
        return None if job is None else job.to_dict()


def build_status_payload(server: ThreadingHTTPServer) -> dict[str, Any]:
    return {
        "server": {
            "host": server.server_address[0],
            "port": server.server_address[1],
            "pid": os.getpid(),
            "project_root": str(PROJECT_ROOT),
            "workspace_root": str(WORKSPACE_ROOT),
            "pid_file": str(CONTROL_CENTER_PID_FILE),
        },
        "system_notice": read_json(CONTROL_CENTER_NOTICE_FILE, None),
        "workspaces": list_workspaces(),
        "jobs": list_jobs(),
        "modules": load_module_views(),
    }


HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>服务器自动剪辑控制台</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {
      --bg: #f4f1e8;
      --panel: rgba(255, 250, 240, 0.93);
      --ink: #221b16;
      --muted: #6d6359;
      --line: #d8cbbb;
      --accent: #c56022;
      --accent-soft: #f2d8c4;
      --good: #2f7d4a;
      --warn: #9d6b08;
      --bad: #a63b3b;
      --shadow: 0 18px 44px rgba(61, 41, 20, 0.08);
      --mono: "Cascadia Mono", "Consolas", monospace;
      --sans: "Microsoft YaHei UI", "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: var(--sans);
      background:
        radial-gradient(circle at top right, #f8ddbb 0, transparent 28%),
        linear-gradient(180deg, #f8f4ec 0%, #f3eee5 55%, #ece2d3 100%);
    }
    .shell { max-width: 1480px; margin: 0 auto; padding: 28px; }
    .hero {
      display: flex;
      justify-content: space-between;
      align-items: end;
      gap: 18px;
      margin-bottom: 20px;
    }
    .hero h1 { margin: 0 0 8px; font-size: 34px; }
    .hero p { margin: 0; color: var(--muted); max-width: 760px; }
    .badge {
      display: inline-block;
      margin-bottom: 10px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-weight: 700;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: var(--shadow);
      padding: 18px;
      backdrop-filter: blur(8px);
    }
    .panel h2 { margin: 0 0 10px; font-size: 20px; }
    .panel h3 { margin: 0 0 8px; font-size: 16px; }
    .sub { color: var(--muted); font-size: 13px; }
    .modules { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 12px; }
    .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
    .module-card, .workspace-card, .job-card {
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      background: rgba(255, 255, 255, 0.72);
    }
    .row { display: flex; justify-content: space-between; align-items: center; gap: 12px; }
    .meta { display: flex; gap: 14px; flex-wrap: wrap; margin-top: 8px; color: var(--muted); font-size: 13px; }
    .actions { display: flex; gap: 8px; flex-wrap: wrap; }
    button {
      border: 0;
      border-radius: 12px;
      padding: 10px 14px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary { background: #eadfd1; color: var(--ink); }
    button.warn { background: var(--bad); }
    textarea {
      width: 100%;
      min-height: 380px;
      border-radius: 16px;
      border: 1px solid var(--line);
      padding: 14px;
      font-family: var(--mono);
      font-size: 13px;
      line-height: 1.5;
      background: #fffdfa;
    }
    select {
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #fffdfa;
    }
    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: var(--mono);
      font-size: 12px;
      background: #2a241e;
      color: #f8efe2;
      border-radius: 14px;
      padding: 12px;
      max-height: 240px;
      overflow: auto;
    }
    .module-state-ready { color: var(--good); }
    .module-state-planned { color: var(--warn); }
    .module-state-missing { color: var(--bad); }
    .empty { color: var(--muted); font-size: 13px; }
    .hint {
      margin-top: 12px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(198, 96, 34, 0.08);
      color: var(--muted);
      font-size: 13px;
    }
    .summary-strip {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0;
    }
    .stat-card {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px 16px;
      background: rgba(255, 255, 255, 0.75);
      box-shadow: var(--shadow);
    }
    .stat-label { color: var(--muted); font-size: 12px; }
    .stat-value { margin-top: 8px; font-size: 28px; font-weight: 800; }
    .stat-detail { margin-top: 6px; color: var(--muted); font-size: 12px; line-height: 1.5; }
    .guide-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-top: 12px;
    }
    .guide-card {
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      background: rgba(255, 255, 255, 0.72);
    }
    .guide-card strong {
      display: inline-flex;
      min-width: 30px;
      height: 30px;
      align-items: center;
      justify-content: center;
      margin-bottom: 10px;
      border-radius: 10px;
      background: var(--ink);
      color: #fff;
    }
    .guide-card h3 { margin: 0 0 8px; font-size: 15px; }
    .guide-card p { margin: 0; color: var(--muted); font-size: 13px; line-height: 1.6; }
    .module-groups {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .module-lane {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      background: rgba(255, 255, 255, 0.72);
    }
    .module-lane h3 { margin: 0 0 6px; font-size: 16px; }
    .module-lane .sub { margin-bottom: 10px; }
    .module-list { display: grid; gap: 10px; }
    .status-pill {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 5px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }
    .status-ready { background: rgba(47, 125, 74, 0.12); color: var(--good); }
    .status-planned { background: rgba(157, 107, 8, 0.14); color: var(--warn); }
    .status-missing, .status-failed { background: rgba(166, 59, 59, 0.12); color: var(--bad); }
    .status-running { background: rgba(37, 95, 115, 0.12); color: #255f73; }
    .status-idle { background: #eadfd1; color: var(--ink); }
    .workspace-create {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      margin: 12px 0;
    }
    input[type="text"] {
      width: 100%;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #fffdfa;
    }
    .workspace-card.selected {
      border-color: var(--accent);
      box-shadow: inset 0 0 0 1px rgba(198, 96, 34, 0.18);
      background: rgba(255, 247, 237, 0.9);
    }
    .task-tools {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 12px 0;
    }
    .editor-grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 320px;
      gap: 14px;
      align-items: start;
    }
    .inspector {
      display: grid;
      gap: 10px;
    }
    .inspect-card {
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      background: rgba(255, 255, 255, 0.72);
      font-size: 13px;
      line-height: 1.6;
    }
    .inspect-card h3 { margin: 0 0 8px; font-size: 14px; }
    .check-good { background: rgba(47, 125, 74, 0.1); color: var(--good); }
    .check-warn { background: rgba(157, 107, 8, 0.12); color: var(--warn); }
    .check-info { background: rgba(37, 95, 115, 0.1); color: #255f73; }
    .path-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-top: 12px;
    }
    .path-card {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 10px;
      background: rgba(255, 255, 255, 0.72);
      font-size: 12px;
      color: var(--muted);
    }
    .path-card code { display: inline-block; margin-top: 6px; }
    @media (max-width: 1100px) {
      .two-col { grid-template-columns: 1fr; }
      .hero { align-items: start; flex-direction: column; }
      .summary-strip, .guide-grid, .module-groups, .editor-grid, .path-grid { grid-template-columns: 1fr; }
      .workspace-create { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="hero">
      <div>
        <div class="badge">本地电脑当服务器</div>
        <h1>服务器自动剪辑控制台</h1>
        <p>把页面改成“先建工作间、再填链接和配置、最后保存启动”的顺序。你不用一次记住全部字段，按步骤走就能把下载、字幕和成片链路跑起来。</p>
      </div>
      <div class="actions">
        <button class="secondary" onclick="refreshAll()">刷新状态</button>
        <div class="sub" id="serverInfo">正在加载控制台状态...</div>
      </div>
    </div>

    <div id="noticeBar"></div>

    <div class="summary-strip" id="summaryStrip"></div>

    <div class="panel">
      <h2>上手顺序</h2>
      <div class="sub">第一次用，照着 1 到 4 做就行。素材已经在本地的话，可以直接跳过下载模块。</div>
      <div class="guide-grid">
        <div class="guide-card">
          <strong>1</strong>
          <h3>新建工作间</h3>
          <p>一个工作间就是一套独立任务，会自动区分下载目录、字幕目录、成片目录和日志。</p>
        </div>
        <div class="guide-card">
          <strong>2</strong>
          <h3>填下载任务</h3>
          <p>原素材放 <code>baidu_share</code>，爆款参考视频放 <code>douyin_download</code>。</p>
        </div>
        <div class="guide-card">
          <strong>3</strong>
          <h3>补字幕和剪辑</h3>
          <p>要提字幕就配 <code>subtitle_extract</code>；要自动出片就补 <code>settings</code> 和 <code>auto_clip</code>。</p>
        </div>
        <div class="guide-card">
          <strong>4</strong>
          <h3>保存后启动</h3>
          <p>右侧主按钮会先保存当前编辑器内容，再启动当前工作间，你只需要盯日志区看输出。</p>
        </div>
      </div>
      <div class="hint">字幕提取默认就是精准模式、每秒 5 帧，并且优先自动识别字幕区。自动剪辑会按参考视频、参考字幕和原素材目录批量生成成片。</div>
    </div>

    <div class="panel">
      <h2>模块分区</h2>
      <div class="sub">不再把模块平铺成一大堆卡片，而是按你真正的操作顺序分成下载、字幕、成片和调度。</div>
      <div class="module-groups" id="moduleGrid"></div>
    </div>

    <div class="two-col">
      <div class="panel">
        <h2>工作间</h2>
        <div class="sub">左边只负责选工作间、新建工作间和启动已保存配置。</div>
        <div class="workspace-create">
          <input type="text" id="newWorkspaceName" placeholder="例如：短剧推广_0410">
          <button onclick="createWorkspace()">新建工作间</button>
        </div>
        <div id="workspaceList"></div>
      </div>

      <div class="panel">
        <h2>当前工作间</h2>
        <div class="sub" id="workspaceMeta">选中一个工作间后，这里会显示当前保存配置的概况和默认输出目录。</div>
        <div class="task-tools">
          <button class="secondary" onclick="loadTask()">重新载入</button>
          <button onclick="saveTask()">保存配置</button>
          <button class="secondary" onclick="saveAndRunCurrentWorkspace()">保存后启动</button>
        </div>
        <div class="actions" id="workspaceBadges" style="margin-bottom: 12px;"></div>
        <div class="path-grid" id="workspacePaths"></div>
      </div>
    </div>

    <div class="panel">
      <div class="row">
        <div>
          <h2>任务配置</h2>
          <div class="sub">这里编辑的就是当前工作间的 <code>task.json</code>。先套模板，再把示例链接和文件名改成你自己的。</div>
        </div>
        <div class="actions">
          <select id="workspaceSelect" onchange="loadTask()"></select>
          <button class="secondary" onclick="useTemplate('blank')">空白模板</button>
          <button class="secondary" onclick="useTemplate('download')">下载+字幕模板</button>
          <button class="secondary" onclick="useTemplate('full')">完整成片模板</button>
          <button class="secondary" onclick="formatTaskJson()">格式化 JSON</button>
        </div>
      </div>
      <div class="actions" style="margin: 12px 0;">
        <span class="status-pill status-idle"><code>baidu_share</code></span>
        <span class="status-pill status-idle"><code>douyin_download</code></span>
        <span class="status-pill status-idle"><code>subtitle_extract</code></span>
        <span class="status-pill status-idle"><code>settings</code></span>
        <span class="status-pill status-idle"><code>auto_clip</code></span>
      </div>
      <div class="editor-grid">
        <textarea id="taskEditor" spellcheck="false"></textarea>
        <div class="inspector" id="taskInspector"></div>
      </div>
    </div>

    <div class="two-col">
      <div class="panel">
        <h2>运行任务</h2>
        <div class="sub">这里只看运行状态。更详细的输出去右边日志区。</div>
        <div id="jobList"></div>
      </div>
      <div class="panel">
        <div class="row">
          <div>
            <h2>最近日志</h2>
            <div class="sub" id="logMeta">点击左侧运行卡片的“查看日志”即可切换。</div>
          </div>
          <button class="secondary" onclick="clearLogView()">清空显示</button>
        </div>
        <pre id="logViewer">暂时还没有日志。</pre>
      </div>
    </div>
  </div>

  <script>
    let currentWorkspace = "";
    let latestStatus = null;
    let currentLogJobId = "";
    let noticeTimer = null;

    const DEFAULT_CONCURRENCY = {
      baidu_share: 1,
      douyin_download: 3,
      subtitle_extract: 1,
      auto_clip: 1
    };

    const DEFAULT_SETTINGS = {
      ai_api_key: "",
      ai_api_url: "https://ark.cn-beijing.volces.com/api/v3/chat/completions",
                ai_model: "doubao-seed-character-251128",
      tts_voice: "zh-CN-YunxiNeural",
      tts_rate: "+8%",
      enable_backup_tts: false,
      azure_tts_key: "",
      azure_tts_region: "",
      azure_tts_voice: ""
    };

    const MODULE_GROUPS = [
      { title: "下载模块", description: "先把原素材和参考爆款拉到本地。", ids: ["baidu_share_downloader", "douyin_batch_downloader"] },
      { title: "字幕模块", description: "提 srt 字幕，并自动识别字幕区。", ids: ["subtitle_region_detector", "subtitle_batch_runner"] },
      { title: "成片模块", description: "AI 改写、配音和最终成片。", ids: ["auto_clip_engine"] },
      { title: "控制与调度", description: "中控界面和多工作间批量调度。", ids: ["control_center", "batch_runner"] }
    ];

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function cloneJson(value) {
      return JSON.parse(JSON.stringify(value));
    }

    function hasText(value) {
      return typeof value === "string" && value.trim().length > 0;
    }

    function showNotice(message, tone = "info") {
      const bar = document.getElementById("noticeBar");
      if (noticeTimer) {
        window.clearTimeout(noticeTimer);
        noticeTimer = null;
      }
      const toneClass = tone === "success" ? "check-good" : (tone === "error" ? "check-warn" : "check-info");
      bar.innerHTML = `<div class="inspect-card ${toneClass}" style="margin-bottom: 18px;">${escapeHtml(message)}</div>`;
      noticeTimer = window.setTimeout(() => {
        bar.innerHTML = "";
      }, 5000);
    }

    function resetTaskEditor() {
      document.getElementById("taskEditor").value = "";
      renderTaskInspector();
    }

    async function api(path, options = {}) {
      const response = await fetch(path, {
        headers: { "Content-Type": "application/json" },
        ...options
      });
      const text = await response.text();
      const data = text ? JSON.parse(text) : {};
      if (!response.ok) {
        throw new Error(data.error || response.statusText);
      }
      return data;
    }

    function normalizeTask(task, workspaceName = currentWorkspace || task?.workspace_name || "workspace") {
      const rawTask = (task && typeof task === "object" && !Array.isArray(task)) ? cloneJson(task) : {};
      rawTask.workspace_name = workspaceName;
      rawTask.concurrency = { ...DEFAULT_CONCURRENCY, ...(rawTask.concurrency || {}) };
      rawTask.settings = { ...DEFAULT_SETTINGS, ...(rawTask.settings || {}) };
      rawTask.baidu_share = Array.isArray(rawTask.baidu_share) ? rawTask.baidu_share : [];
      rawTask.douyin_download = Array.isArray(rawTask.douyin_download) ? rawTask.douyin_download : [];
      rawTask.subtitle_extract = Array.isArray(rawTask.subtitle_extract) ? rawTask.subtitle_extract : [];
      rawTask.auto_clip = Array.isArray(rawTask.auto_clip) ? rawTask.auto_clip : [];
      return rawTask;
    }

    function summarizeTask(task) {
      return {
        baidu_share: (task.baidu_share || []).length,
        douyin_download: (task.douyin_download || []).length,
        subtitle_extract: (task.subtitle_extract || []).length,
        auto_clip: (task.auto_clip || []).length
      };
    }

    function totalTaskCount(summary) {
      return Object.values(summary || {}).reduce((sum, value) => sum + Number(value || 0), 0);
    }

    function statusLabel(status) {
      return {
        ready: "已就绪",
        planned: "待接入",
        missing: "缺失",
        running: "运行中",
        stopping: "停止中",
        completed: "已完成",
        failed: "失败",
        idle: "空闲"
      }[status] || status;
    }

    function statusClass(status) {
      if (status === "ready" || status === "completed") return "status-ready";
      if (status === "running" || status === "stopping") return "status-running";
      if (status === "planned") return "status-planned";
      if (status === "missing" || status === "failed") return "status-failed";
      return "status-idle";
    }

    function renderSummaryStrip(workspaces, jobs, modules) {
      const wrap = document.getElementById("summaryStrip");
      const readyModules = modules.filter(item => item.status === "ready").length;
      const runningJobs = jobs.filter(item => item.status === "running" || item.status === "stopping").length;
      const clipWorkspaces = workspaces.filter(item => (item.task_summary?.auto_clip || 0) > 0).length;
      const configured = workspaces.filter(item => totalTaskCount(item.task_summary || {}) > 0).length;
      wrap.innerHTML = `
        <div class="stat-card"><div class="stat-label">工作间数量</div><div class="stat-value">${workspaces.length}</div><div class="stat-detail">每个工作间都是一条独立链路</div></div>
        <div class="stat-card"><div class="stat-label">运行中任务</div><div class="stat-value">${runningJobs}</div><div class="stat-detail">详细实时输出在日志区</div></div>
        <div class="stat-card"><div class="stat-label">已就绪模块</div><div class="stat-value">${readyModules}/${modules.length || 0}</div><div class="stat-detail">界面只展示现在能直接用的模块</div></div>
        <div class="stat-card"><div class="stat-label">已配自动剪辑</div><div class="stat-value">${clipWorkspaces}</div><div class="stat-detail">${configured} 个工作间已经写过任务配置</div></div>
      `;
    }

    function moduleCard(module, planned = false) {
      const status = planned ? "planned" : (module.status || "ready");
      return `
        <div class="module-card">
          <div class="row">
            <strong>${escapeHtml(module.name)}</strong>
            <span class="status-pill ${statusClass(status)}">${escapeHtml(statusLabel(status))}</span>
          </div>
          <div class="sub" style="margin-top: 8px;">${escapeHtml(module.description || "")}</div>
        </div>
      `;
    }

    function renderModules(data) {
      const grid = document.getElementById("moduleGrid");
      const current = data.control_center_manifest.current_modules || [];
      const planned = data.control_center_manifest.planned_modules || [];
      if (!current.length && !planned.length) {
        grid.innerHTML = '<div class="empty">还没有可展示的模块清单。</div>';
        return;
      }
      const lookup = new Map(current.map(item => [item.id, item]));
      const lanes = MODULE_GROUPS.map(group => {
        const items = group.ids.map(id => lookup.get(id)).filter(Boolean);
        if (!items.length) {
          return "";
        }
        return `
          <div class="module-lane">
            <h3>${escapeHtml(group.title)}</h3>
            <div class="sub">${escapeHtml(group.description)}</div>
            <div class="module-list">${items.map(item => moduleCard(item, false)).join("")}</div>
          </div>
        `;
      }).join("");
      const plannedLane = planned.length ? `
        <div class="module-lane">
          <h3>后续模块</h3>
          <div class="sub">这些模块还没正式接进来，但已经预留了位置。</div>
          <div class="module-list">${planned.map(item => moduleCard(item, true)).join("")}</div>
        </div>
      ` : "";
      grid.innerHTML = lanes + plannedLane;
    }

    function runtimeStateForWorkspace(workspaceName) {
      const jobs = latestStatus?.jobs || [];
      if (jobs.some(job => job.workspace === workspaceName && (job.status === "running" || job.status === "stopping"))) {
        return { label: "运行中", className: "status-running" };
      }
      if (jobs.some(job => job.workspace === workspaceName && job.status === "failed")) {
        return { label: "最近失败", className: "status-failed" };
      }
      if (jobs.some(job => job.workspace === workspaceName && job.status === "completed")) {
        return { label: "最近完成", className: "status-ready" };
      }
      return { label: "空闲", className: "status-idle" };
    }

    function renderWorkspaceMeta() {
      const workspace = (latestStatus?.workspaces || []).find(item => item.name === currentWorkspace);
      const badges = document.getElementById("workspaceBadges");
      const meta = document.getElementById("workspaceMeta");
      const paths = document.getElementById("workspacePaths");
      if (!workspace) {
        badges.innerHTML = "";
        meta.textContent = "选中一个工作间后，这里会显示当前保存配置的概况和默认输出目录。";
        paths.innerHTML = "";
        return;
      }
      const summary = workspace.task_summary || {};
      const runtime = runtimeStateForWorkspace(workspace.name);
      meta.textContent = `${workspace.path} | 右侧“保存后启动”会先保存编辑器内容再运行。`;
      badges.innerHTML = `
        <span class="status-pill status-idle">百度 ${summary.baidu_share || 0}</span>
        <span class="status-pill status-idle">抖音 ${summary.douyin_download || 0}</span>
        <span class="status-pill status-idle">字幕 ${summary.subtitle_extract || 0}</span>
        <span class="status-pill status-idle">剪辑 ${summary.auto_clip || 0}</span>
        <span class="status-pill ${runtime.className}">${escapeHtml(runtime.label)}</span>
      `;
      paths.innerHTML = [
        ["原素材目录", "downloads/baidu"],
        ["参考视频目录", "downloads/douyin"],
        ["字幕目录", "subtitles"],
        ["成片目录", "clips"],
        ["日志目录", "logs"],
        ["临时目录", "temp"]
      ].map(([label, path]) => `<div class="path-card">${escapeHtml(label)}<br><code>${escapeHtml(path)}</code></div>`).join("");
    }

    function workspaceCard(item) {
      const summary = item.task_summary || {};
      const total = totalTaskCount(summary);
      const runtime = runtimeStateForWorkspace(item.name);
      const selected = item.name === currentWorkspace ? " selected" : "";
      return `
        <div class="workspace-card${selected}">
          <div class="row">
            <div>
              <h3>${escapeHtml(item.name)}</h3>
              <div class="sub">${escapeHtml(item.path)}</div>
            </div>
            <span class="status-pill ${runtime.className}">${escapeHtml(runtime.label)}</span>
          </div>
          <div class="meta">
            <span>百度 ${summary.baidu_share || 0}</span>
            <span>抖音 ${summary.douyin_download || 0}</span>
            <span>字幕 ${summary.subtitle_extract || 0}</span>
            <span>剪辑 ${summary.auto_clip || 0}</span>
            <span>${total > 0 ? `共 ${total} 段任务` : "空白工作间"}</span>
          </div>
          <div class="actions" style="margin-top: 12px;">
            <button class="secondary" onclick="selectWorkspace('${encodeURIComponent(item.name)}')">编辑</button>
            <button onclick="runWorkspace('${encodeURIComponent(item.name)}')">启动已保存配置</button>
          </div>
        </div>
      `;
    }

    function renderWorkspaces(items) {
      const list = document.getElementById("workspaceList");
      const select = document.getElementById("workspaceSelect");
      if (!items.length) {
        currentWorkspace = "";
        list.innerHTML = '<div class="empty">当前还没有工作间。先新建一个工作间，再去右边填模板和配置。</div>';
        select.innerHTML = "";
        resetTaskEditor();
        renderWorkspaceMeta();
        return;
      }
      const names = items.map(item => item.name);
      if (!currentWorkspace || !names.includes(currentWorkspace)) {
        currentWorkspace = items[0].name;
      }
      list.innerHTML = items.map(workspaceCard).join("");
      select.innerHTML = items.map(item => `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name)}</option>`).join("");
      select.value = currentWorkspace;
      renderWorkspaceMeta();
    }

    function parseEditorTask() {
      const text = document.getElementById("taskEditor").value.trim();
      if (!text) {
        return { state: "empty" };
      }
      try {
        const parsed = JSON.parse(text);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          throw new Error("task.json 顶层必须是对象。");
        }
        return { state: "ok", task: normalizeTask(parsed, currentWorkspace || parsed.workspace_name || "workspace") };
      } catch (error) {
        return { state: "error", message: error.message || "JSON 解析失败。" };
      }
    }

    function containsPlaceholder(value) {
      if (typeof value === "string") return value.includes("请替换");
      if (Array.isArray(value)) return value.some(item => containsPlaceholder(item));
      if (value && typeof value === "object") return Object.values(value).some(item => containsPlaceholder(item));
      return false;
    }

    function collectChecks(task) {
      const summary = summarizeTask(task);
      const checks = [];
      if (totalTaskCount(summary) === 0) {
        checks.push({ tone: "check-info", title: "当前还是空白配置", body: "先点上面的模板按钮生成一份起步配置。" });
      }
      if ((summary.baidu_share || 0) + (summary.douyin_download || 0) === 0) {
        checks.push({ tone: "check-info", title: "还没配置下载任务", body: "如果素材还没在本地，先填 baidu_share 或 douyin_download。" });
      }
      if ((task.subtitle_extract || []).some(item => !hasText(item.input_glob) && !hasText(item.input_path) && !(Array.isArray(item.input_paths) && item.input_paths.length > 0))) {
        checks.push({ tone: "check-warn", title: "字幕任务缺输入", body: "subtitle_extract 至少要填 input_glob、input_path 或 input_paths。" });
      }
      if ((task.auto_clip || []).length === 0) {
        checks.push({ tone: "check-info", title: "还没配置自动剪辑", body: "想直接生成成片，就把 auto_clip 段补上。" });
      }
      if ((task.auto_clip || []).some(item => !hasText(item.reference_video) && !hasText(item.reference_video_glob))) {
        checks.push({ tone: "check-warn", title: "自动剪辑缺参考视频", body: "auto_clip 至少要有 reference_video 或 reference_video_glob。" });
      }
      if ((task.auto_clip || []).some(item => !hasText(item.reference_subtitle) && !hasText(item.reference_subtitle_glob))) {
        checks.push({ tone: "check-warn", title: "自动剪辑缺参考字幕", body: "auto_clip 至少要有 reference_subtitle 或 reference_subtitle_glob。" });
      }
      if ((task.auto_clip || []).some(item => !hasText(item.source_dir))) {
        checks.push({ tone: "check-warn", title: "自动剪辑缺原素材目录", body: "auto_clip 任务必须指定 source_dir。" });
      }
      if ((task.auto_clip || []).length > 0 && !hasText(task.settings?.ai_api_key) && !(task.auto_clip || []).some(item => hasText(item.ai_api_key))) {
        checks.push({ tone: "check-warn", title: "自动剪辑缺 AI Key", body: "settings.ai_api_key 还是空的，启动后没法完成 AI 改写。" });
      }
      if (containsPlaceholder(task)) {
        checks.push({ tone: "check-warn", title: "模板占位还没改", body: "配置里还有“请替换”字样，启动前记得改成真实链接。" });
      }
      if (!checks.length) {
        checks.push({ tone: "check-good", title: "草稿结构看起来可以启动", body: "先点右上角“保存后启动”，再到日志区看实时输出。" });
      }
      return checks;
    }

    function renderTaskInspector(task = null, parseResult = null) {
      const box = document.getElementById("taskInspector");
      if (!currentWorkspace) {
        box.innerHTML = '<div class="inspect-card">先在左侧选中一个工作间，这里就会显示当前草稿的缺项提醒。</div>';
        return;
      }
      if (parseResult?.state === "empty") {
        box.innerHTML = '<div class="inspect-card">当前编辑器还是空的。你可以先套模板，再把链接和文件名改成你自己的。</div>';
        return;
      }
      if (parseResult?.state === "error") {
        box.innerHTML = `<div class="inspect-card check-warn"><h3>JSON 格式不正确</h3>${escapeHtml(parseResult.message)}</div>`;
        return;
      }
      if (!task) {
        box.innerHTML = '<div class="inspect-card">这里会显示当前草稿的任务段数量、关键设置和下一步建议。</div>';
        return;
      }
      const summary = summarizeTask(task);
      const checks = collectChecks(task);
      box.innerHTML = `
        <div class="inspect-card">
          <h3>任务段数量</h3>
          <div class="meta">
            <span>百度 ${summary.baidu_share || 0}</span>
            <span>抖音 ${summary.douyin_download || 0}</span>
            <span>字幕 ${summary.subtitle_extract || 0}</span>
            <span>剪辑 ${summary.auto_clip || 0}</span>
          </div>
        </div>
        <div class="inspect-card">
          <h3>关键设置</h3>
          <div class="meta">
            <span>AI Key ${hasText(task.settings?.ai_api_key) ? "已填" : "未填"}</span>
            <span>AI 模型 ${escapeHtml(task.settings?.ai_model || "未填")}</span>
            <span>TTS ${escapeHtml(task.settings?.tts_voice || "未填")}</span>
          </div>
        </div>
        ${checks.map(check => `<div class="inspect-card ${escapeHtml(check.tone)}"><h3>${escapeHtml(check.title)}</h3>${escapeHtml(check.body)}</div>`).join("")}
      `;
    }

    function setEditorTask(task) {
      document.getElementById("taskEditor").value = JSON.stringify(task, null, 2);
      renderTaskInspector(task, { state: "ok" });
    }

    function jobCard(job) {
      const preview = (job.recent_lines || []).slice(-2).join(" / ") || "暂无输出";
      return `
        <div class="job-card">
          <div class="row">
            <div>
              <h3>${escapeHtml(job.workspace)}</h3>
              <div class="sub">${escapeHtml(job.log_path)}</div>
            </div>
            <span class="status-pill ${statusClass(job.status)}">${escapeHtml(statusLabel(job.status))}</span>
          </div>
          <div class="job-preview">${escapeHtml(preview)}</div>
          <div class="actions" style="margin-top: 12px;">
            <button class="secondary" onclick="showLog('${encodeURIComponent(job.job_id)}')">查看日志</button>
            ${(job.status === "running" || job.status === "stopping") ? `<button class="warn" onclick="stopJob('${encodeURIComponent(job.job_id)}')">停止</button>` : ""}
          </div>
        </div>
      `;
    }

    function renderJobs(items) {
      const list = document.getElementById("jobList");
      if (!items.length) {
        list.innerHTML = '<div class="empty">当前还没有运行记录。等你启动工作间后，这里会出现运行卡片。</div>';
        clearLogView();
        return;
      }
      list.innerHTML = items.map(jobCard).join("");
      if (currentLogJobId && !items.some(item => item.job_id === currentLogJobId)) {
        clearLogView();
      } else if (currentLogJobId) {
        updateLogViewFromCache();
      }
    }

    async function loadStatus() {
      const data = await api("/api/status");
      latestStatus = data;
      document.getElementById("serverInfo").textContent =
        `${data.server.host}:${data.server.port} | 工作间 ${data.workspaces.length} | 运行任务 ${data.jobs.length}`;
      renderSummaryStrip(data.workspaces, data.jobs, data.modules.control_center_manifest.current_modules || []);
      renderModules(data.modules);
      renderWorkspaces(data.workspaces);
      renderJobs(data.jobs);
    }

    async function loadTask() {
      const select = document.getElementById("workspaceSelect");
      if (!select.value) {
        currentWorkspace = "";
        resetTaskEditor();
        renderWorkspaceMeta();
        return;
      }
      currentWorkspace = select.value;
      const data = await api(`/api/workspaces/${encodeURIComponent(currentWorkspace)}/task`);
      setEditorTask(normalizeTask(data.task, currentWorkspace));
      renderWorkspaceMeta();
    }

    function selectWorkspace(encodedName) {
      currentWorkspace = decodeURIComponent(encodedName);
      document.getElementById("workspaceSelect").value = currentWorkspace;
      loadTask().catch(error => showNotice(error.message, "error"));
    }

    async function persistCurrentTask(showSuccess = true) {
      if (!currentWorkspace) {
        showNotice("请先选择一个工作间。", "error");
        return false;
      }
      const parsed = parseEditorTask();
      if (parsed.state === "empty") {
        showNotice("当前编辑器是空的，不能保存。", "error");
        return false;
      }
      if (parsed.state === "error") {
        renderTaskInspector(null, parsed);
        showNotice("JSON 格式不正确，不能保存。", "error");
        return false;
      }
      setEditorTask(parsed.task);
      await api(`/api/workspaces/${encodeURIComponent(currentWorkspace)}/task`, {
        method: "POST",
        body: JSON.stringify({ task: parsed.task })
      });
      await loadStatus();
      if (showSuccess) {
        showNotice(`已保存 ${currentWorkspace} 的 task.json。`, "success");
      }
      return true;
    }

    async function saveTask() {
      await persistCurrentTask(true);
    }

    async function saveAndRunCurrentWorkspace() {
      const saved = await persistCurrentTask(false);
      if (!saved) return;
      await api("/api/run-workspace", {
        method: "POST",
        body: JSON.stringify({ workspace: currentWorkspace })
      });
      await loadStatus();
      showNotice(`已保存并启动 ${currentWorkspace}。`, "success");
    }

    async function runWorkspace(encodedName) {
      const workspace = decodeURIComponent(encodedName);
      currentWorkspace = workspace;
      await api("/api/run-workspace", {
        method: "POST",
        body: JSON.stringify({ workspace })
      });
      await loadStatus();
      showNotice(`已启动 ${workspace} 的已保存配置。`, "success");
    }

    async function stopJob(encodedJobId) {
      const jobId = decodeURIComponent(encodedJobId);
      await api("/api/stop-job", {
        method: "POST",
        body: JSON.stringify({ job_id: jobId })
      });
      await loadStatus();
      showNotice("停止请求已经发出。", "info");
    }

    function updateLogViewFromCache() {
      if (!currentLogJobId) return;
      const job = (latestStatus?.jobs || []).find(item => item.job_id === currentLogJobId);
      if (!job) return;
      document.getElementById("logViewer").textContent = (job.recent_lines || []).join("\\n") || "暂时还没有日志。";
      document.getElementById("logMeta").textContent = `${job.workspace} | ${statusLabel(job.status)}${job.return_code !== null ? ` / exit ${job.return_code}` : ""}`;
    }

    function clearLogView() {
      currentLogJobId = "";
      document.getElementById("logViewer").textContent = "暂时还没有日志。";
      document.getElementById("logMeta").textContent = "点击左侧运行卡片的“查看日志”即可切换。";
    }

    function buildTemplate(kind, workspaceName) {
      const task = normalizeTask({ workspace_name: workspaceName }, workspaceName);
      if (kind === "blank") {
        return task;
      }
      task.baidu_share = [
        {
          share_url: "https://pan.baidu.com/s/请替换成你的原素材链接?pwd=提取码",
          download_mode: "api",
          target_filename: "episode01.mp4",
          output_subdir: "downloads/baidu",
          skip_existing: true
        }
      ];
      task.douyin_download = [
        {
          url: "https://v.douyin.com/请替换成你的爆款视频链接/",
          output_subdir: "downloads/douyin",
          with_watermark: false
        }
      ];
      task.subtitle_extract = [
        {
          input_glob: "downloads/douyin/*.mp4",
          output_subdir: "subtitles",
          temp_subdir: "temp/subtitle",
          auto_detect_subtitle_area: true,
          language: "ch",
          mode: "accurate",
          extract_frequency: 5,
          probe_extract_frequency: 5,
          generate_txt: true,
          skip_existing: true
        }
      ];
      if (kind === "full") {
        task.auto_clip = [
          {
            reference_video_glob: "downloads/douyin/*.mp4",
            reference_subtitle_glob: "subtitles/*.srt",
            source_dir: "downloads/baidu",
            output_subdir: "clips",
            temp_subdir: "temp/auto_clip",
            title: `${workspaceName}_final`,
            skip_existing: true
          }
        ];
      }
      return task;
    }

    function useTemplate(kind) {
      if (!currentWorkspace) {
        showNotice("请先新建或选择一个工作间，再使用模板。", "error");
        return;
      }
      const editor = document.getElementById("taskEditor");
      if (editor.value.trim() && !window.confirm("这会用模板覆盖当前编辑器内容，确定继续吗？")) {
        return;
      }
      setEditorTask(buildTemplate(kind, currentWorkspace));
      showNotice("模板已经载入到编辑器里，记得把示例链接改成你自己的。", "info");
    }

    function formatTaskJson() {
      const parsed = parseEditorTask();
      if (parsed.state === "empty") {
        showNotice("当前编辑器还是空的。", "error");
        return;
      }
      if (parsed.state === "error") {
        renderTaskInspector(null, parsed);
        showNotice("JSON 格式不正确，不能格式化。", "error");
        return;
      }
      setEditorTask(parsed.task);
      showNotice("JSON 已格式化。", "success");
    }

    async function createWorkspace() {
      const input = document.getElementById("newWorkspaceName");
      const name = input.value.trim();
      if (!name) {
        showNotice("先输入一个工作间名字。", "error");
        return;
      }
      if (name.includes("/") || name.includes(String.fromCharCode(92))) {
        showNotice("工作间名字里不能带路径分隔符。", "error");
        return;
      }
      await api(`/api/workspaces/${encodeURIComponent(name)}/task`, {
        method: "POST",
        body: JSON.stringify({ task: buildTemplate("blank", name) })
      });
      input.value = "";
      currentWorkspace = name;
      await loadStatus();
      await loadTask();
      showNotice(`已新建工作间 ${name}。你现在可以直接套模板了。`, "success");
    }

    async function showLog(encodedJobId) {
      currentLogJobId = decodeURIComponent(encodedJobId);
      updateLogViewFromCache();
    }

    async function refreshAll() {
      await loadStatus();
      if (currentWorkspace) {
        await loadTask();
      }
    }

    document.getElementById("taskEditor").addEventListener("input", () => {
      const parsed = parseEditorTask();
      if (parsed.state === "ok") {
        renderTaskInspector(parsed.task, parsed);
      } else {
        renderTaskInspector(null, parsed);
      }
    });

    setInterval(() => { loadStatus().catch(() => {}); }, 4000);
    refreshAll().catch(error => showNotice(error.message, "error"));
  </script>
</body>
</html>
"""


class ControlCenterHandler(BaseHTTPRequestHandler):
    server_version = "ServerAutoClipControlCenter/1.1"

    def _send_common_cache_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, status: int, payload: Any) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self._send_common_cache_headers()
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, html: str) -> None:
        data = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self._send_common_cache_headers()
        self.end_headers()
        self.wfile.write(data)

    def _send_binary(self, status: int, data: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self._send_common_cache_headers()
        self.end_headers()
        self.wfile.write(data)

    def _send_empty(self, status: int = HTTPStatus.NO_CONTENT) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self._send_common_cache_headers()
        self.end_headers()

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("request body is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _content_length(self) -> int:
        try:
            return max(0, int(self.headers.get("Content-Length", "0") or 0))
        except ValueError as exc:
            raise ValueError("invalid Content-Length header") from exc

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        parts = [part for part in parsed.path.split("/") if part]

        try:
            if parsed.path == "/":
                self._send_html(load_control_center_html())
                return

            if parsed.path == "/favicon.ico":
                if not CONTROL_CENTER_APP_LOGO_ICO.exists():
                    self._send_empty()
                    return
                self._send_binary(HTTPStatus.OK, CONTROL_CENTER_APP_LOGO_ICO.read_bytes(), "image/x-icon")
                return

            if parsed.path == "/app-logo.png":
                if not CONTROL_CENTER_APP_LOGO_PNG.exists():
                    raise FileNotFoundError(f"logo file not found: {CONTROL_CENTER_APP_LOGO_PNG}")
                self._send_binary(HTTPStatus.OK, CONTROL_CENTER_APP_LOGO_PNG.read_bytes(), "image/png")
                return

            if parsed.path == "/api/status":
                self._send_json(HTTPStatus.OK, build_status_payload(self.server))
                return

            if parsed.path == "/api/baidu/login-status":
                self._send_json(HTTPStatus.OK, get_baidu_login_state())
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "task":
                workspace_name = urllib.parse.unquote(parts[2])
                self._send_json(HTTPStatus.OK, {"workspace": workspace_name, "task": get_workspace_task(workspace_name)})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "cover-preview":
                workspace_name = urllib.parse.unquote(parts[2])
                query = urllib.parse.parse_qs(parsed.query)
                raw_path = str((query.get("path") or [""])[0] or "")
                cover_path = resolve_workspace_cover_image(workspace_name, raw_path)
                mime_type = mimetypes.guess_type(cover_path.name)[0] or "application/octet-stream"
                self._send_binary(HTTPStatus.OK, cover_path.read_bytes(), mime_type)
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "baidu-official-diagnosis":
                workspace_name = urllib.parse.unquote(parts[2])
                result = diagnose_workspace_baidu_official_wait(workspace_name)
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if len(parts) == 3 and parts[0] == "api" and parts[1] == "jobs":
                job_id = urllib.parse.unquote(parts[2])
                job = get_job(job_id)
                if job is None:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return
                self._send_json(HTTPStatus.OK, {"job": job})
                return
        except FileNotFoundError as exc:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": str(exc)})
            return
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except Exception as exc:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        parts = [part for part in parsed.path.split("/") if part]

        try:
            if parsed.path == "/api/run-workspace":
                payload = self._read_json_body()
                workspace_name = str(payload.get("workspace", "")).strip()
                job = start_workspace_job(workspace_name)
                self._send_json(HTTPStatus.OK, {"job": job.to_dict()})
                return

            if parsed.path == "/api/run-batch":
                payload = self._read_json_body()
                raw_workspaces = payload.get("workspaces") or []
                if not isinstance(raw_workspaces, list):
                    raise ValueError("workspaces must be a list")
                workspace_parallel = payload.get("workspace_parallel")
                if workspace_parallel in ("", None):
                    workspace_parallel = None
                job = start_batch_job([str(item) for item in raw_workspaces], workspace_parallel=workspace_parallel)
                self._send_json(HTTPStatus.OK, {"job": job.to_dict()})
                return

            if parsed.path == "/api/stop-job":
                payload = self._read_json_body()
                job_id = str(payload.get("job_id", "")).strip()
                if not job_id:
                    raise ValueError("job_id is required")
                if not stop_job(job_id):
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not running"})
                    return
                self._send_json(HTTPStatus.OK, {"ok": True})
                return

            if parsed.path == "/api/ui-session":
                payload = self._read_json_body()
                active_sessions = register_ui_session(str(payload.get("session_id", "")).strip())
                self._send_json(HTTPStatus.OK, {"ok": True, "active_sessions": active_sessions})
                return

            if parsed.path == "/api/ui-session/disconnect":
                payload = self._read_json_body()
                active_sessions = disconnect_ui_session(str(payload.get("session_id", "")).strip())
                self._send_json(HTTPStatus.OK, {"ok": True, "active_sessions": active_sessions})
                return

            if parsed.path == "/api/baidu/list-share":
                payload = self._read_json_body()
                share_url = str(payload.get("share_url", "")).strip()
                result = list_baidu_share_files(share_url)
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if parsed.path == "/api/baidu/open-login":
                result = open_baidu_login_window()
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if parsed.path == "/api/shutdown":
                payload = self._read_json_body()
                result = request_server_shutdown(
                    self.server,
                    stop_jobs=bool(payload.get("stop_jobs")),
                    reason=str(payload.get("reason", "")).strip(),
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "message": "control center shutting down",
                        **result,
                    },
                )
                return

            if parsed.path == "/api/open-path":
                payload = self._read_json_body()
                path_text = str(payload.get("path", "")).strip()
                select_file = bool(payload.get("select_file"))
                result = open_local_path_in_explorer(path_text, select_file=select_file)
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if parsed.path == "/api/pick-folder":
                payload = self._read_json_body()
                initial_path = str(payload.get("initial_path", "")).strip()
                result = pick_local_folder(initial_path)
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if parsed.path == "/api/test-ai":
                payload = self._read_json_body()
                raw_fallback_models = payload.get("ai_fallback_models") or []
                if not isinstance(raw_fallback_models, list):
                    raise ValueError("ai_fallback_models must be a list")
                fallback_models = [item for item in raw_fallback_models if isinstance(item, dict)]
                result = test_ai_api_connection(
                    ai_api_key=str(payload.get("ai_api_key", "")).strip(),
                    ai_model=str(payload.get("ai_model", "")).strip(),
                    ai_api_url=str(payload.get("ai_api_url", "")).strip(),
                    ai_fallback_models=fallback_models,
                )
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "task":
                workspace_name = urllib.parse.unquote(parts[2])
                payload = self._read_json_body()
                task = payload.get("task")
                if not isinstance(task, dict):
                    raise ValueError("task must be a JSON object")
                task_path = save_workspace_task(workspace_name, task)
                self._send_json(HTTPStatus.OK, {"ok": True, "task_path": str(task_path)})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "delete":
                workspace_name = urllib.parse.unquote(parts[2])
                result = delete_workspace(workspace_name)
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "rename":
                workspace_name = urllib.parse.unquote(parts[2])
                payload = self._read_json_body()
                new_name = str(payload.get("new_name", "")).strip()
                result = rename_workspace(workspace_name, new_name)
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "import-local":
                workspace_name = urllib.parse.unquote(parts[2])
                payload = self._read_json_body()
                kind = str(payload.get("kind", "")).strip()
                raw_paths = payload.get("paths") or []
                if not isinstance(raw_paths, list):
                    raise ValueError("paths must be a list")
                result = import_local_files(workspace_name, kind, [str(item) for item in raw_paths])
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "cover-from-share":
                workspace_name = urllib.parse.unquote(parts[2])
                payload = self._read_json_body()
                share_url = str(payload.get("share_url", "")).strip()
                result = set_workspace_cover_from_baidu_share(
                    workspace_name,
                    share_url=share_url,
                    target_fsid=str(payload.get("target_fsid", "")).strip(),
                    target_path=str(payload.get("target_path", "")).strip(),
                    target_filename=str(payload.get("target_filename", "")).strip(),
                    share_key=str(payload.get("share_key", "")).strip(),
                )
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "clear-cover":
                workspace_name = urllib.parse.unquote(parts[2])
                result = clear_workspace_cover(workspace_name)
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "workspaces" and parts[3] == "upload-file":
                workspace_name = urllib.parse.unquote(parts[2])
                kind = str(self.headers.get("X-Upload-Kind", "")).strip()
                filename = urllib.parse.unquote(str(self.headers.get("X-Upload-Name", "")).strip())
                content_length = self._content_length()
                result = upload_file_to_workspace(workspace_name, kind, filename, self.rfile, content_length)
                self._send_json(HTTPStatus.OK, {"ok": True, **result})
                return
        except FileNotFoundError as exc:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": str(exc)})
            return
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except Exception as exc:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ensure_workspace_root()
    server = ThreadingHTTPServer((args.host, args.port), ControlCenterHandler)
    start_ui_session_watchdog(server)
    if args.open_browser:
        threading.Timer(1.0, lambda: webbrowser.open(f"http://{args.host}:{args.port}")).start()
    print(f"CONTROL_CENTER http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        remove_control_center_pid_file()
    return 0


if __name__ == "__main__":
    sys.exit(main())
