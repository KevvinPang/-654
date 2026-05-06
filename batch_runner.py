from __future__ import annotations

import argparse
import array
import concurrent.futures
import glob
import hashlib
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
RUNTIME_ROOT = PROJECT_ROOT / "runtime"
WORKSPACE_ROOT = RUNTIME_ROOT / "workspaces"
MODULES_ROOT = PROJECT_ROOT / "modules"

try:
    sys.stdout.reconfigure(errors="backslashreplace")
    sys.stderr.reconfigure(errors="backslashreplace")
except Exception:
    pass


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_frequency_value(value: Any) -> int | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw in {"", "auto", "adaptive"}:
        return None
    try:
        parsed = int(float(raw))
    except (TypeError, ValueError):
        return None
    return max(1, min(8, parsed))

SUPPORTED_VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".avi", ".m4v"}
SUPPORTED_AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus", ".wma"}
SUPPORTED_SUBTITLE_EXTENSIONS = {".srt", ".ass", ".ssa", ".txt"}
AUTO_CLIP_SETTINGS_KEYS = (
    "ai_api_key",
    "ai_api_url",
    "ai_model",
    "ai_fallback_models",
    "tts_voice",
    "tts_rate",
    "enable_backup_tts",
    "azure_tts_key",
    "azure_tts_region",
    "azure_tts_voice",
    "prefer_funasr_audio_subtitles",
    "disable_ai_subtitle_review",
    "disable_ai_narration_rewrite",
    "prefer_funasr_sentence_pauses",
    "force_no_narration_mode",
    "narration_background_percent",
    "output_watermark_text",
    "enable_random_episode_flip",
    "random_episode_flip_ratio",
    "enable_random_visual_filter",
    "reference_speed_factor",
    "cover_image_path",
    "bgm_audio_path",
    "bgm_source_mode",
    "bgm_search_query",
    "bgm_external_dirs",
    "bgm_chromaprint_fpcalc_path",
    "bgm_volume_percent",
)
BGM_SOURCE_MODES = {"none", "manual", "local_auto", "auto"}
BGM_LOCAL_LIBRARY_DEFAULT_DIRS = (Path(r"D:\BGM库"),)
BGM_LOCAL_PREFERRED_FILENAMES = {
    "苦悩の旋律 - 上田益.flac",
    "Faded异域 (变速0.9x) - DJ铁柱.mp3",
    "Fight - BeatBrothers.mp3",
    "J Balvin-X（Remix） - Speak.flac",
    "Sub Title - CHOI SEONG WOOK.mp3",
    "Time Back - Bad Style.mp3",
}
BGM_LOCAL_SIGNATURE_ANALYSIS_SECONDS = 75.0
BGM_LOCAL_SIGNATURE_FRAME_SECONDS = 0.5
BGM_LOCAL_PREFERRED_BONUS = 6.0
BGM_LOCAL_PREFERRED_CLOSE_MARGIN = 5.5
BGM_CHROMAPRINT_DEFAULT_FPCALC_PATHS = (
    PROJECT_ROOT / "runtime" / "tools" / "chromaprint" / "fpcalc.exe",
    PROJECT_ROOT / "runtime" / "tools" / "chromaprint" / "chromaprint-fpcalc-1.6.0-windows-x86_64" / "fpcalc.exe",
    Path(r"C:\Users\24995\Desktop\chromaprint-master\build\src\cmd\Release\fpcalc.exe"),
    Path(r"C:\Users\24995\Desktop\chromaprint-master\build\src\cmd\fpcalc.exe"),
)
BGM_CHROMAPRINT_ENV_NAMES = ("SERVER_AUTO_CLIP_FPCALC", "FPCALC_PATH")
BGM_CHROMAPRINT_ANALYSIS_SECONDS = 90.0
BGM_CHROMAPRINT_EXACT_MATCH_MIN_SCORE = 82.0
BGM_CHROMAPRINT_CACHE_PATH = RUNTIME_ROOT / "cache" / "bgm_chromaprint.json"
BGM_AUDIOMUSE_LOCAL_CACHE_PATH = RUNTIME_ROOT / "cache" / "bgm_audiomuse_local.json"
BGM_AUDIOMUSE_LOCAL_CACHE_VERSION = 2
BGM_AUDIOMUSE_LOCAL_ANALYSIS_SECONDS = 90.0
BGM_DEMUCS_DEFAULT_SOURCE_DIRS = (Path(r"C:\Users\24995\Desktop\demucs-main"),)
BGM_DEMUCS_DEFAULT_PYTHON_PATHS = (
    Path(r"C:\Users\24995\Desktop\ultimatevocalremovergui-master\.venv\Scripts\python.exe"),
    RUNTIME_ROOT / "tools" / "demucs_venv" / "Scripts" / "python.exe",
)
BGM_DEMUCS_SOURCE_ENV_NAMES = ("SERVER_AUTO_CLIP_DEMUCS_SOURCE_DIR", "DEMUCS_SOURCE_DIR")
BGM_DEMUCS_PYTHON_ENV_NAMES = ("SERVER_AUTO_CLIP_DEMUCS_PYTHON", "DEMUCS_PYTHON")
BGM_DEMUCS_CACHE_ROOT = RUNTIME_ROOT / "cache" / "bgm_demucs"
BGM_DEMUCS_MODEL = "htdemucs"
BGM_DEMUCS_DEVICE = "cpu"
BGM_DEMUCS_SEGMENT_SECONDS = 7
BGM_DEMUCS_SEPARATION_SECONDS = 120.0
BGM_CHROMAPRINT_SPEED_VARIANTS = (1.0, 0.90, 0.95, 1.05, 1.10)
BGM_CHROMAPRINT_VARIANT_CACHE_ROOT = RUNTIME_ROOT / "cache" / "bgm_chromaprint_variants"
BGM_REFERENCE_ANALYSIS_SECONDS = 45.0
BGM_REFERENCE_ANALYSIS_SAMPLE_RATE = 16000
BGM_REFERENCE_ANALYSIS_MAX_BYTES = int(BGM_REFERENCE_ANALYSIS_SECONDS * BGM_REFERENCE_ANALYSIS_SAMPLE_RATE * 2)
BGM_REFERENCE_VOLUME_MIN_PERCENT = 6.0
BGM_REFERENCE_VOLUME_MAX_PERCENT = 50.0
BGM_REFERENCE_VOLUME_MATCH_HEADROOM = 1.00
BGM_FFMPEG_ENV_NAMES = ("SERVER_AUTO_CLIP_FFMPEG", "FFMPEG_PATH")
BGM_FFMPEG_FALLBACKS = (
    Path(r"D:\NarratoAI_v0.7\lib\ffmpeg\ffmpeg-7.0-essentials_build\ffmpeg.exe"),
    Path(r"D:\FFmpeg\bin\ffmpeg.exe"),
    Path(r"C:\FFmpeg\bin\ffmpeg.exe"),
)
KNOWN_MOJIBAKE_REPLACEMENTS = {
    "E:\\鏍风墖": "E:\\样片",
    "E:\\鎴愮墖": "E:\\成片",
}
KNOWN_WORKSPACE_MOJIBAKE = (
    "鏂╂儏褰撳ぉ浠栦滑鎮旂柉浜?",
    "鏂╂儏褰撳ぉ浠栦滑鎮旂柉浜",
)


@dataclass
class WorkspaceContext:
    name: str
    root: Path
    config_path: Path
    config: dict[str, Any]
    logger: logging.Logger


@dataclass
class TaskSpec:
    stage: str
    label: str
    command: list[str] | None
    cwd: Path
    skip_reason: str = ""


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
    return repaired


def normalize_task_payload(value: Any, workspace_name: str) -> Any:
    if isinstance(value, dict):
        return {key: normalize_task_payload(item, workspace_name) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_task_payload(item, workspace_name) for item in value]
    if isinstance(value, str):
        return repair_known_mojibake_text(value, workspace_name)
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch download and subtitle jobs for one or more workspaces.")
    parser.add_argument("--config", action="append", default=[], help="Explicit task.json path. Can be provided multiple times.")
    parser.add_argument("--workspace", action="append", default=[], help="Workspace name under runtime/workspaces. Can be provided multiple times.")
    parser.add_argument("--all-workspaces", action="store_true", help="Run every workspace that contains task.json.")
    parser.add_argument("--workspace-parallel", type=int, default=2, help="How many workspaces can run at the same time.")
    parser.add_argument("--global-baidu-share", type=int, default=1, help="Global parallel limit for Baidu share downloads.")
    parser.add_argument("--global-douyin-download", type=int, default=3, help="Global parallel limit for Douyin downloads.")
    parser.add_argument("--global-subtitle-extract", type=int, default=1, help="Global parallel limit for subtitle extraction.")
    parser.add_argument("--global-visual-subtitle-extract", type=int, default=1, help="Global parallel limit for visual subtitle extraction.")
    parser.add_argument("--global-auto-clip", type=int, default=1, help="Global parallel limit for auto clip generation.")
    return parser.parse_args(argv)


def resolve_python(candidates: list[Path]) -> str:
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def build_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    # Force child Python processes to flush logs immediately so the UI and
    # workspace log keep showing real progress instead of looking frozen.
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    return env


def discover_config_paths(args: argparse.Namespace) -> list[Path]:
    config_paths: list[Path] = []

    for raw_path in args.config:
        config_paths.append(Path(raw_path).expanduser().resolve())

    for workspace_name in args.workspace:
        config_paths.append((WORKSPACE_ROOT / workspace_name / "task.json").resolve())

    if args.all_workspaces or not config_paths:
        for task_path in sorted(WORKSPACE_ROOT.glob("*/task.json")):
            config_paths.append(task_path.resolve())

    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in config_paths:
        if path not in seen:
            seen.add(path)
            deduped.append(path)
    return deduped


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_logger(name: str, log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger_name = f"server_auto_clip.{name}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(log_dir / "workspace.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def load_workspace(config_path: Path) -> WorkspaceContext:
    if not config_path.exists():
        raise FileNotFoundError(f"task config not found: {config_path}")
    config = normalize_task_payload(load_json(config_path), config_path.parent.name)
    workspace_root = config_path.parent
    workspace_name = workspace_root.name
    logger = build_logger(workspace_name, workspace_root / "logs")
    return WorkspaceContext(
        name=workspace_name,
        root=workspace_root,
        config_path=config_path,
        config=config,
        logger=logger,
    )


def ensure_workspace_directories(workspace_root: Path) -> None:
    for relative in [
        "downloads/baidu",
        "downloads/douyin",
        "covers",
        "bgm",
        "subtitles",
        "clips",
        "logs",
        "temp",
    ]:
        (workspace_root / relative).mkdir(parents=True, exist_ok=True)


def resolve_workspace_path(workspace_root: Path, raw_path: str | None, default_relative: str) -> Path:
    if not raw_path:
        return (workspace_root / default_relative).resolve()
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (workspace_root / candidate).resolve()


def resolve_download_output_dir(workspace: WorkspaceContext, raw_path: str | None, default_relative: str) -> Path:
    output_dir = resolve_workspace_path(workspace.root, raw_path, default_relative)
    try:
        output_dir.relative_to(workspace.root.resolve())
    except ValueError:
        if output_dir.name != workspace.name:
            output_dir = output_dir / workspace.name
    return output_dir.resolve()


def safe_output_name_from_path(input_path: Path, workspace_root: Path) -> str:
    try:
        relative = input_path.resolve().relative_to(workspace_root.resolve())
        stem_parts = list(relative.with_suffix("").parts)
        return "__".join(stem_parts) + ".srt"
    except ValueError:
        return f"{input_path.stem}.srt"


def sanitize_stem(raw: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", str(raw or "").strip())
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned or "output"


def expected_baidu_output_path(output_dir: Path, task: dict[str, Any]) -> Path | None:
    target_path = str(task.get("target_path") or "").strip()
    target_filename = str(task.get("target_filename") or "").strip()
    if target_path:
        parts = [part for part in target_path.split("/") if part]
        if len(parts) >= 3:
            return output_dir.joinpath(*parts[2:])
    if target_filename:
        return output_dir / target_filename
    return None


def normalize_baidu_download_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    if mode in {
        "official",
        "official_client",
        "official-client",
        "official_client_handoff",
        "client",
        "client_handoff",
        "baidunetdisk",
    }:
        return "official_client"
    if mode in {"api", "direct", "tool", "tool_direct"}:
        return "api"
    return "official_client"


def baidu_task_target_size_from_cache(workspace: WorkspaceContext, task: dict[str, Any]) -> int:
    cache = workspace.config.get("baidu_share_listing_cache") or {}
    files = cache.get("files") if isinstance(cache, dict) else []
    if not isinstance(files, list):
        return 0

    target_fsid = str(task.get("target_fsid") or "").strip()
    target_path = str(task.get("target_path") or "").strip()
    target_filename = str(task.get("target_filename") or "").strip()
    for item in files:
        if not isinstance(item, dict):
            continue
        if target_fsid and str(item.get("fs_id") or "").strip() == target_fsid:
            return parse_int(item.get("size"), 0)
        if target_path and str(item.get("path") or "").strip() == target_path:
            return parse_int(item.get("size"), 0)
    if target_filename:
        same_name = [
            item
            for item in files
            if isinstance(item, dict) and str(item.get("name") or item.get("server_filename") or "").strip() == target_filename
        ]
        if len(same_name) == 1:
            return parse_int(same_name[0].get("size"), 0)
    return 0


def baidu_task_target_size(workspace: WorkspaceContext, task: dict[str, Any]) -> int:
    explicit_size = parse_int(task.get("target_size"), 0)
    if explicit_size > 0:
        return explicit_size
    return baidu_task_target_size_from_cache(workspace, task)


def should_skip_existing_baidu_output(expected_output: Path, task: dict[str, Any], *, target_size: int = 0) -> bool:
    if not expected_output.exists():
        return False

    try:
        current_size = expected_output.stat().st_size
    except OSError:
        return False

    parts_dir = expected_output.with_name(expected_output.name + ".parts")
    if parts_dir.exists():
        return False

    if current_size <= 0:
        return False

    if target_size <= 0:
        return False
    if current_size != target_size:
        return False

    return True


def normalize_keyword_filters(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = re.split(r"[,\n\r;|]+", value)
    elif isinstance(value, list):
        raw_items = value
    else:
        raw_items = []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        token = str(item or "").strip()
        if not token:
            continue
        folded = token.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        normalized.append(token)
    return normalized


def baidu_task_excluded_keyword(workspace: WorkspaceContext, task: dict[str, Any]) -> str:
    shared_settings = workspace.config.get("settings") or {}
    keywords = normalize_keyword_filters(
        task.get("exclude_name_keywords", shared_settings.get("baidu_share_exclude_keywords", []))
    )
    if not keywords:
        return ""
    haystacks = [
        str(task.get("target_filename") or "").strip(),
        str(task.get("target_path") or "").strip(),
    ]
    lowered_haystacks = [item.casefold() for item in haystacks if item]
    for keyword in keywords:
        lowered_keyword = keyword.casefold()
        if any(lowered_keyword in haystack for haystack in lowered_haystacks):
            return keyword
    return ""


def collect_incomplete_baidu_targets(workspace: WorkspaceContext) -> list[str]:
    pending: list[str] = []
    for index, raw_task in enumerate(workspace.config.get("baidu_share", []), start=1):
        task = dict(raw_task or {})
        if baidu_task_excluded_keyword(workspace, task):
            continue
        output_dir = resolve_download_output_dir(workspace, task.get("output_subdir"), "downloads/baidu")
        expected_output = expected_baidu_output_path(output_dir, task)
        target_label = (
            str(task.get("target_filename") or "").strip()
            or Path(str(task.get("target_path") or "")).name
            or f"file_{index}"
        )
        target_size = baidu_task_target_size(workspace, task)
        if expected_output is None or not should_skip_existing_baidu_output(expected_output, task, target_size=target_size):
            pending.append(target_label)
    return pending


def resolve_glob_matches(workspace_root: Path, pattern: str | None, *, directories_only: bool = False) -> list[Path]:
    if not pattern:
        return []
    pattern_text = str(pattern).strip()
    if not pattern_text:
        return []
    matches: list[Path] = []
    if Path(pattern_text).is_absolute():
        iterator = [Path(item) for item in sorted(glob.glob(pattern_text))]
    else:
        iterator = sorted(workspace_root.glob(pattern_text))
    for candidate in iterator:
        if directories_only and candidate.is_dir():
            matches.append(candidate.resolve())
        elif not directories_only and candidate.is_file():
            matches.append(candidate.resolve())
    return matches


def infer_first_baidu_output_dir(workspace: WorkspaceContext) -> Path | None:
    tasks = workspace.config.get("baidu_share") or []
    if not tasks:
        return None
    return resolve_download_output_dir(workspace, tasks[0].get("output_subdir"), "downloads/baidu")


def infer_first_douyin_output_dir(workspace: WorkspaceContext) -> Path | None:
    tasks = workspace.config.get("douyin_download") or []
    if not tasks:
        return None
    return resolve_download_output_dir(workspace, tasks[0].get("output_subdir"), "downloads/douyin")


def infer_first_subtitle_output_dir(workspace: WorkspaceContext) -> Path | None:
    tasks = workspace.config.get("subtitle_extract") or []
    if tasks:
        return resolve_workspace_path(workspace.root, tasks[0].get("output_subdir"), "subtitles/audio")
    settings = workspace.config.get("settings") or {}
    if bool(settings.get("prefer_funasr_audio_subtitles", False)):
        return resolve_workspace_path(workspace.root, None, "subtitles/audio")
    return None


def infer_first_visual_subtitle_output_dir(workspace: WorkspaceContext) -> Path | None:
    tasks = workspace.config.get("visual_subtitle_extract") or []
    if tasks:
        return resolve_workspace_path(workspace.root, tasks[0].get("output_subdir"), "subtitles/visual")
    if workspace.config.get("auto_clip") or []:
        return resolve_workspace_path(workspace.root, None, "subtitles/visual")
    return None


def probe_media_duration_seconds(media_path: Path) -> float:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(media_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return 0.0
    if result.returncode != 0:
        return 0.0
    try:
        return max(0.0, float((result.stdout or "").strip()))
    except ValueError:
        return 0.0


def auto_visual_subtitle_frequency(video_path: Path) -> int:
    duration = probe_media_duration_seconds(video_path)
    if duration <= 0 or duration <= 360:
        return 5
    if duration <= 600:
        return 4
    return 3


def resolve_workspace_inputs(
    workspace_root: Path,
    raw_path: str | None,
    raw_glob: str | None = None,
    *,
    directories_only: bool = False,
) -> list[Path]:
    results: list[Path] = []
    if raw_path:
        candidate = resolve_workspace_path(workspace_root, raw_path, "")
        if directories_only and candidate.is_dir():
            results.append(candidate)
        elif not directories_only and candidate.is_file():
            results.append(candidate)
    results.extend(resolve_glob_matches(workspace_root, raw_glob, directories_only=directories_only))
    deduped: list[Path] = []
    seen: set[Path] = set()
    for item in results:
        resolved = item.resolve()
        if resolved not in seen:
            seen.add(resolved)
            deduped.append(resolved)
    return deduped


def filter_paths_by_suffix(paths: list[Path], allowed_suffixes: set[str]) -> list[Path]:
    return [path for path in paths if path.suffix.lower() in allowed_suffixes]


def setting_value(task: dict[str, Any], shared_settings: dict[str, Any], key: str, default: Any = "") -> Any:
    if key in task:
        return task.get(key)
    return shared_settings.get(key, default)


def normalize_bgm_source_mode(raw_mode: Any) -> str:
    mode = str(raw_mode or "auto").strip().lower()
    if mode in {"", "none", "off", "no", "false", "disabled", "disable"}:
        return "none"
    if mode in {"local", "local_auto", "local-auto"}:
        return "local_auto"
    if mode in {"online", "online_auto", "online-auto"}:
        return "local_auto"
    if mode in {"auto", "smart"}:
        return "auto"
    return mode if mode in BGM_SOURCE_MODES else "auto"


def sanitize_bgm_filename_part(text: str, fallback: str = "bgm") -> str:
    normalized = re.sub(r"[^\w\u4e00-\u9fff.-]+", "_", str(text or "").strip(), flags=re.UNICODE)
    normalized = normalized.strip("._-")
    return (normalized or fallback)[:80]


def infer_bgm_search_query(workspace: WorkspaceContext, reference_video: Path, raw_query: Any) -> str:
    custom_query = str(raw_query or "").strip()
    if custom_query:
        return custom_query

    context = f"{workspace.name} {reference_video.stem}".lower()
    profiles: list[tuple[tuple[str, ...], str]] = [
        (("不好惹", "战神", "复仇", "杀", "打", "霸", "逆袭", "打脸"), "dramatic cinematic tension action instrumental"),
        (("甜", "恋", "爱", "婚", "嫁", "宠", "妻", "总裁"), "romantic warm cinematic piano instrumental"),
        (("悬", "谜", "鬼", "惊", "恐", "命案"), "dark suspense cinematic instrumental"),
        (("搞笑", "喜剧", "沙雕", "爆笑"), "playful upbeat comedy instrumental"),
        (("乡下", "农村", "村", "田园"), "warm rural cinematic acoustic instrumental"),
    ]
    for needles, query in profiles:
        if any(needle.lower() in context for needle in needles):
            return query
    return "cinematic drama emotional instrumental"


def resolve_bgm_ffmpeg_path(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
) -> Path | None:
    raw_path = str(setting_value(task, shared_settings, "ffmpeg", "") or "").strip()
    if raw_path:
        candidate = resolve_workspace_path(workspace.root, raw_path, "")
        if candidate.exists():
            return candidate
    for env_name in BGM_FFMPEG_ENV_NAMES:
        env_path = os.environ.get(env_name, "").strip()
        if env_path and Path(env_path).exists():
            return Path(env_path)
    resolved = shutil.which("ffmpeg")
    if resolved:
        return Path(resolved)
    for candidate in BGM_FFMPEG_FALLBACKS:
        if candidate.exists():
            return candidate
    return None


def decode_bgm_audio_samples(
    media_path: Path,
    ffmpeg_path: Path,
    duration_seconds: float = BGM_REFERENCE_ANALYSIS_SECONDS,
) -> list[int]:
    duration = max(1.0, float(duration_seconds or BGM_REFERENCE_ANALYSIS_SECONDS))
    command = [
        str(ffmpeg_path),
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-i",
        str(media_path),
        "-vn",
        "-ac",
        "1",
        "-ar",
        str(BGM_REFERENCE_ANALYSIS_SAMPLE_RATE),
        "-t",
        f"{duration:.1f}",
        "-f",
        "s16le",
        "pipe:1",
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
        check=False,
    )
    if result.returncode != 0 or not result.stdout:
        return []
    max_bytes = int(duration * BGM_REFERENCE_ANALYSIS_SAMPLE_RATE * 2)
    raw_audio = result.stdout[:max_bytes]
    samples = array.array("h")
    samples.frombytes(raw_audio[: len(raw_audio) - (len(raw_audio) % 2)])
    if sys.byteorder != "little":
        samples.byteswap()
    return list(samples)


def decode_reference_audio_samples(reference_video: Path, ffmpeg_path: Path) -> list[int]:
    return decode_bgm_audio_samples(reference_video, ffmpeg_path)


def percentile(values: list[float], ratio: float, default: float = 0.0) -> float:
    if not values:
        return default
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * ratio))))
    return ordered[index]


def analyze_reference_audio_style(samples: list[int]) -> dict[str, float]:
    if len(samples) < BGM_REFERENCE_ANALYSIS_SAMPLE_RATE:
        return {}
    frame_size = 2048
    rms_values: list[float] = []
    zcr_values: list[float] = []
    for offset in range(0, len(samples) - frame_size + 1, frame_size):
        frame = samples[offset : offset + frame_size]
        if not frame:
            continue
        square_sum = sum(float(sample) * float(sample) for sample in frame)
        rms = math.sqrt(square_sum / len(frame)) / 32768.0
        sign_changes = 0
        previous = frame[0]
        for sample in frame[1:]:
            if (previous < 0 <= sample) or (previous >= 0 > sample):
                sign_changes += 1
            previous = sample
        rms_values.append(rms)
        zcr_values.append(sign_changes / max(1, len(frame) - 1))
    if not rms_values:
        return {}

    mean_rms = sum(rms_values) / len(rms_values)
    p30_rms = percentile(rms_values, 0.30, mean_rms)
    p90_rms = percentile(rms_values, 0.90, mean_rms)
    dynamic_ratio = p90_rms / max(0.001, p30_rms)
    mean_zcr = sum(zcr_values) / max(1, len(zcr_values))
    onset_threshold = max(mean_rms * 1.25, p30_rms * 1.55, 0.015)
    onset_count = 0
    for index in range(1, len(rms_values)):
        if rms_values[index] >= onset_threshold and rms_values[index] >= rms_values[index - 1] * 1.22:
            onset_count += 1
    duration = len(samples) / float(BGM_REFERENCE_ANALYSIS_SAMPLE_RATE)
    return {
        "mean_rms": mean_rms,
        "p90_rms": p90_rms,
        "dynamic_ratio": dynamic_ratio,
        "mean_zcr": mean_zcr,
        "onset_density": onset_count / max(1.0, duration),
    }


def reference_audio_style_query(features: dict[str, float]) -> str:
    mean_rms = float(features.get("mean_rms", 0.0) or 0.0)
    dynamic_ratio = float(features.get("dynamic_ratio", 0.0) or 0.0)
    mean_zcr = float(features.get("mean_zcr", 0.0) or 0.0)
    onset_density = float(features.get("onset_density", 0.0) or 0.0)

    if mean_rms >= 0.13 and onset_density >= 0.75:
        if mean_zcr >= 0.105:
            return "energetic electronic cinematic action tension instrumental"
        return "dramatic cinematic action percussion tension instrumental"
    if mean_rms <= 0.045 and onset_density <= 0.35:
        return "soft emotional cinematic piano gentle instrumental"
    if dynamic_ratio >= 3.0:
        return "suspense cinematic tension build dark instrumental"
    if mean_zcr >= 0.115:
        return "modern electronic suspense tension instrumental"
    if onset_density >= 0.55:
        return "upbeat cinematic drama pulse instrumental"
    return "warm emotional cinematic drama instrumental"


def load_bgm_audio_features(media_path: Path, ffmpeg_path: Path) -> dict[str, float]:
    samples = decode_bgm_audio_samples(media_path, ffmpeg_path)
    return analyze_reference_audio_style(samples)


def bgm_loudness_anchor(features: dict[str, float]) -> float:
    p90_rms = float(features.get("p90_rms", 0.0) or 0.0)
    mean_rms = float(features.get("mean_rms", 0.0) or 0.0)
    return max(p90_rms, mean_rms)


def estimate_reference_matched_bgm_volume_percent(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    reference_video: Path,
    selected_bgm: Path | None,
) -> float | None:
    if selected_bgm is None:
        return None
    ffmpeg_path = resolve_bgm_ffmpeg_path(workspace, task, shared_settings)
    if ffmpeg_path is None:
        workspace.logger.warning("[auto_bgm] cannot match BGM volume to reference: ffmpeg not found")
        return None

    reference_analysis_media = resolve_reference_bgm_analysis_media(
        workspace,
        task,
        shared_settings,
        reference_video,
        ffmpeg_path,
    )
    try:
        reference_features = load_bgm_audio_features(reference_analysis_media, ffmpeg_path)
        candidate_features = load_bgm_audio_features(selected_bgm, ffmpeg_path)
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        workspace.logger.warning("[auto_bgm] reference-matched BGM volume analysis failed: %s", exc)
        return None

    reference_anchor = bgm_loudness_anchor(reference_features)
    candidate_anchor = bgm_loudness_anchor(candidate_features)
    if reference_anchor <= 1e-5 or candidate_anchor <= 1e-5:
        workspace.logger.warning(
            "[auto_bgm] reference-matched BGM volume unavailable: reference_rms=%.5f candidate_rms=%.5f",
            reference_anchor,
            candidate_anchor,
        )
        return None

    raw_percent = reference_anchor / candidate_anchor * 100.0 * BGM_REFERENCE_VOLUME_MATCH_HEADROOM
    matched_percent = max(BGM_REFERENCE_VOLUME_MIN_PERCENT, min(BGM_REFERENCE_VOLUME_MAX_PERCENT, raw_percent))
    workspace.logger.info(
        "[auto_bgm] reference-matched BGM volume %.1f%% (raw %.1f%%, reference_rms=%.5f candidate_rms=%.5f source=%s)",
        matched_percent,
        raw_percent,
        reference_anchor,
        candidate_anchor,
        reference_analysis_media,
    )
    return matched_percent


def bgm_bool_setting(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "enable", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disable", "disabled"}:
        return False
    return default


def resolve_demucs_source_dir(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
) -> Path | None:
    candidates: list[Path] = []
    raw_path = str(setting_value(task, shared_settings, "bgm_demucs_source_dir", "") or "").strip()
    if raw_path:
        candidates.append(resolve_workspace_path(workspace.root, raw_path, ""))
    for env_name in BGM_DEMUCS_SOURCE_ENV_NAMES:
        env_path = os.environ.get(env_name, "").strip()
        if env_path:
            candidates.append(Path(env_path))
    candidates.extend(BGM_DEMUCS_DEFAULT_SOURCE_DIRS)
    for candidate in candidates:
        if (candidate / "demucs" / "separate.py").exists():
            return candidate.resolve()
    return None


def resolve_demucs_python_path(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
) -> Path | None:
    candidates: list[Path] = []
    raw_path = str(setting_value(task, shared_settings, "bgm_demucs_python_path", "") or "").strip()
    if raw_path:
        candidates.append(resolve_workspace_path(workspace.root, raw_path, ""))
    for env_name in BGM_DEMUCS_PYTHON_ENV_NAMES:
        env_path = os.environ.get(env_name, "").strip()
        if env_path:
            candidates.append(Path(env_path))
    candidates.extend(BGM_DEMUCS_DEFAULT_PYTHON_PATHS)
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()
    return None


def bgm_media_identity_key(media_path: Path, *parts: Any) -> str:
    try:
        stat = media_path.stat()
        identity = f"{media_path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}"
    except OSError:
        identity = str(media_path.resolve())
    suffix = "|".join(str(part) for part in parts)
    return hashlib.sha1(f"{identity}|{suffix}".encode("utf-8", errors="ignore")).hexdigest()


def export_reference_bgm_analysis_wav(
    reference_video: Path,
    ffmpeg_path: Path,
    output_path: Path,
    duration_seconds: float,
) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(ffmpeg_path),
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-i",
        str(reference_video),
        "-vn",
        "-ac",
        "2",
        "-ar",
        "44100",
        "-t",
        f"{max(1.0, duration_seconds):.1f}",
        str(output_path),
    ]
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=180,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 4096


def run_demucs_reference_bgm_separation(
    workspace: WorkspaceContext,
    reference_video: Path,
    ffmpeg_path: Path,
    demucs_python: Path,
    demucs_source_dir: Path,
    cache_dir: Path,
) -> Path | None:
    model_name = BGM_DEMUCS_MODEL
    input_wav = cache_dir / "reference_audio.wav"
    separated_root = cache_dir / "separated"
    no_vocals_path = cache_dir / "no_vocals.wav"
    if not export_reference_bgm_analysis_wav(
        reference_video,
        ffmpeg_path,
        input_wav,
        BGM_DEMUCS_SEPARATION_SECONDS,
    ):
        workspace.logger.warning("[auto_bgm] failed to export reference audio for Demucs; fallback to mixed reference audio")
        return None

    env = os.environ.copy()
    env["PYTHONPATH"] = str(demucs_source_dir) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["PATH"] = str(ffmpeg_path.parent) + os.pathsep + env.get("PATH", "")
    command = [
        str(demucs_python),
        "-m",
        "demucs.separate",
        "-n",
        model_name,
        "--two-stems",
        "vocals",
        "--other-method",
        "add",
        "-d",
        BGM_DEMUCS_DEVICE,
        "-j",
        "0",
        "--segment",
        str(BGM_DEMUCS_SEGMENT_SECONDS),
        "-o",
        str(separated_root),
        str(input_wav),
    ]
    workspace.logger.info("[auto_bgm] separating reference BGM with Demucs model=%s", model_name)
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=1800,
            check=False,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        workspace.logger.warning("[auto_bgm] Demucs reference BGM separation failed: %s", exc)
        return None
    if result.returncode != 0:
        message = "\n".join((result.stderr or result.stdout or "").splitlines()[-8:])
        workspace.logger.warning("[auto_bgm] Demucs reference BGM separation failed code=%s: %s", result.returncode, message)
        return None
    candidates = sorted(separated_root.glob(f"{model_name}/reference_audio/no_vocals.wav"))
    if not candidates:
        candidates = sorted(separated_root.rglob("no_vocals.wav"))
    if not candidates:
        workspace.logger.warning("[auto_bgm] Demucs finished but no no_vocals.wav was produced")
        return None
    selected = candidates[0]
    if selected.stat().st_size <= 4096:
        workspace.logger.warning("[auto_bgm] Demucs no_vocals.wav is too small; fallback to mixed reference audio")
        return None
    shutil.copy2(selected, no_vocals_path)
    metadata = {
        "reference_video": str(reference_video),
        "model": model_name,
        "duration_seconds": BGM_DEMUCS_SEPARATION_SECONDS,
        "source": str(selected),
    }
    try:
        (cache_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass
    workspace.logger.info("[auto_bgm] using separated reference BGM for matching: %s", no_vocals_path)
    return no_vocals_path


def resolve_reference_bgm_analysis_media(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    reference_video: Path,
    ffmpeg_path: Path | None,
) -> Path:
    enabled = bgm_bool_setting(setting_value(task, shared_settings, "bgm_enable_demucs_separation", True), True)
    if not enabled:
        workspace.logger.info("[auto_bgm] Demucs reference BGM separation is disabled; using mixed reference audio")
        return reference_video
    if ffmpeg_path is None:
        return reference_video
    demucs_python = resolve_demucs_python_path(workspace, task, shared_settings)
    demucs_source_dir = resolve_demucs_source_dir(workspace, task, shared_settings)
    if demucs_python is None or demucs_source_dir is None:
        workspace.logger.warning("[auto_bgm] Demucs is not configured; using mixed reference audio")
        return reference_video

    cache_key = bgm_media_identity_key(reference_video, BGM_DEMUCS_MODEL, BGM_DEMUCS_SEPARATION_SECONDS)
    cache_dir = BGM_DEMUCS_CACHE_ROOT / cache_key
    no_vocals_path = cache_dir / "no_vocals.wav"
    if no_vocals_path.exists() and no_vocals_path.stat().st_size > 4096:
        workspace.logger.info("[auto_bgm] using cached separated reference BGM: %s", no_vocals_path)
        return no_vocals_path
    cache_dir.mkdir(parents=True, exist_ok=True)
    separated_path = run_demucs_reference_bgm_separation(
        workspace,
        reference_video,
        ffmpeg_path,
        demucs_python,
        demucs_source_dir,
        cache_dir,
    )
    return separated_path if separated_path is not None else reference_video


def bgm_feature_distance(reference_features: dict[str, float], candidate_features: dict[str, float]) -> float:
    if not reference_features or not candidate_features:
        return 999.0
    specs = (
        ("mean_rms", 0.16),
        ("dynamic_ratio", 2.5),
        ("mean_zcr", 0.09),
        ("onset_density", 0.80),
    )
    total = 0.0
    weight = 0.0
    for key, scale in specs:
        if key not in reference_features or key not in candidate_features:
            continue
        total += abs(float(reference_features[key]) - float(candidate_features[key])) / max(scale, 0.001)
        weight += 1.0
    if weight <= 0.0:
        return 999.0
    return total / weight


def bgm_similarity_score(reference_features: dict[str, float], candidate_features: dict[str, float]) -> float:
    distance = bgm_feature_distance(reference_features, candidate_features)
    if distance >= 999.0:
        return 0.0
    return 100.0 / (1.0 + distance)


def normalize_bgm_signature_rows(rows: list[tuple[float, ...]]) -> list[tuple[float, ...]]:
    if not rows:
        return []
    dims = len(rows[0])
    means: list[float] = []
    stds: list[float] = []
    for dim in range(dims):
        values = [row[dim] for row in rows]
        mean = sum(values) / max(1, len(values))
        variance = sum((value - mean) * (value - mean) for value in values) / max(1, len(values))
        means.append(mean)
        stds.append(max(math.sqrt(variance), 1e-6))
    return [tuple((row[dim] - means[dim]) / stds[dim] for dim in range(dims)) for row in rows]


def build_bgm_local_signature(samples: list[int]) -> list[tuple[float, ...]]:
    frame_size = max(256, int(BGM_REFERENCE_ANALYSIS_SAMPLE_RATE * BGM_LOCAL_SIGNATURE_FRAME_SECONDS))
    if len(samples) < frame_size * 4:
        return []
    rows: list[tuple[float, float, float, float]] = []
    previous_rms = 0.0
    for offset in range(0, len(samples) - frame_size + 1, frame_size):
        frame = samples[offset : offset + frame_size]
        if not frame:
            continue
        abs_values = [abs(sample) for sample in frame]
        mean_abs = sum(abs_values) / len(abs_values) / 32768.0
        square_sum = sum(float(sample) * float(sample) for sample in frame)
        rms = math.sqrt(square_sum / len(frame)) / 32768.0
        sign_changes = 0
        diff_sum = 0.0
        previous = frame[0]
        for sample in frame[1:]:
            if (previous < 0 <= sample) or (previous >= 0 > sample):
                sign_changes += 1
            diff_sum += abs(float(sample) - float(previous))
            previous = sample
        zcr = sign_changes / max(1, len(frame) - 1)
        brightness = (diff_sum / max(1, len(frame) - 1)) / 65536.0
        peakiness = (max(abs_values) / 32768.0) / max(mean_abs, 1e-4)
        rms_delta = abs(rms - previous_rms)
        previous_rms = rms
        rows.append((rms, zcr, brightness, min(peakiness, 20.0) / 20.0 + rms_delta))
    return normalize_bgm_signature_rows(rows)


def bgm_signature_similarity_score(
    reference_signature: list[tuple[float, ...]],
    candidate_signature: list[tuple[float, ...]],
) -> float:
    if not reference_signature or not candidate_signature:
        return 0.0
    target_len = min(len(reference_signature), len(candidate_signature))
    if target_len < 8:
        return 0.0
    dims = len(reference_signature[0])
    max_offset = max(0, len(candidate_signature) - target_len)
    step = max(1, target_len // 24)
    best_distance = 999.0
    for offset in range(0, max_offset + 1, step):
        total = 0.0
        for index in range(target_len):
            ref_row = reference_signature[index]
            cand_row = candidate_signature[offset + index]
            for dim in range(dims):
                total += abs(ref_row[dim] - cand_row[dim])
        distance = total / max(1, target_len * dims)
        if distance < best_distance:
            best_distance = distance
    if best_distance >= 999.0:
        return 0.0
    return 100.0 / (1.0 + best_distance)


def resolve_chromaprint_fpcalc_path(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
) -> Path | None:
    raw_path = str(setting_value(task, shared_settings, "bgm_chromaprint_fpcalc_path", "") or "").strip()
    candidates: list[Path] = []
    if raw_path:
        candidates.append(resolve_workspace_path(workspace.root, raw_path, ""))
    for env_name in BGM_CHROMAPRINT_ENV_NAMES:
        env_path = os.environ.get(env_name, "").strip()
        if env_path:
            candidates.append(Path(env_path))
    resolved = shutil.which("fpcalc")
    if resolved:
        candidates.append(Path(resolved))
    candidates.extend(BGM_CHROMAPRINT_DEFAULT_FPCALC_PATHS)
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()
    return None


def load_bgm_chromaprint_cache() -> dict[str, Any]:
    try:
        if BGM_CHROMAPRINT_CACHE_PATH.exists():
            payload = json.loads(BGM_CHROMAPRINT_CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
    except (OSError, json.JSONDecodeError):
        return {}
    return {}


def save_bgm_chromaprint_cache(cache: dict[str, Any]) -> None:
    try:
        BGM_CHROMAPRINT_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        BGM_CHROMAPRINT_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return


def bgm_chromaprint_cache_key(media_path: Path, duration_seconds: float) -> str:
    try:
        stat = media_path.stat()
        identity = f"{media_path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}|{duration_seconds:.1f}"
    except OSError:
        identity = f"{media_path.resolve()}|{duration_seconds:.1f}"
    return hashlib.sha1(identity.encode("utf-8", errors="ignore")).hexdigest()


def load_chromaprint_fingerprint(
    media_path: Path,
    fpcalc_path: Path,
    cache: dict[str, Any],
    duration_seconds: float = BGM_CHROMAPRINT_ANALYSIS_SECONDS,
) -> list[int]:
    key = bgm_chromaprint_cache_key(media_path, duration_seconds)
    cached = cache.get(key)
    if isinstance(cached, dict):
        values = cached.get("fingerprint")
        if isinstance(values, list) and values:
            return [int(value) for value in values if isinstance(value, int)]
    command = [
        str(fpcalc_path),
        "-raw",
        "-length",
        str(max(1, int(duration_seconds))),
        str(media_path),
    ]
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=90,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if result.returncode != 0:
        return []
    fingerprint_line = ""
    for line in result.stdout.splitlines():
        if line.startswith("FINGERPRINT="):
            fingerprint_line = line.partition("=")[2].strip()
            break
    if not fingerprint_line:
        return []
    fingerprint = [int(value) for value in re.findall(r"-?\d+", fingerprint_line)]
    if fingerprint:
        cache[key] = {"path": str(media_path), "fingerprint": fingerprint}
    return fingerprint


def bgm_chromaprint_similarity_score(reference_fingerprint: list[int], candidate_fingerprint: list[int]) -> float:
    if not reference_fingerprint or not candidate_fingerprint:
        return 0.0
    target_len = min(len(reference_fingerprint), len(candidate_fingerprint))
    if target_len < 8:
        return 0.0
    max_offset = max(0, len(candidate_fingerprint) - target_len)
    step = max(1, target_len // 32)
    best_similarity = 0.0
    for offset in range(0, max_offset + 1, step):
        bit_distance = 0
        for index in range(target_len):
            bit_distance += ((reference_fingerprint[index] ^ candidate_fingerprint[offset + index]) & 0xFFFFFFFF).bit_count()
        similarity = 100.0 * (1.0 - bit_distance / max(1, target_len * 32))
        if similarity > best_similarity:
            best_similarity = similarity
    return max(0.0, min(100.0, best_similarity))


def bounded_feature(value: float, scale: float, cap: float = 1.0) -> float:
    if scale <= 0:
        return 0.0
    return max(0.0, min(cap, float(value) / scale))


def feature_std(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) * (value - mean) for value in values) / len(values))


def append_scaled_stats(vector: list[float], values: list[float], scale: float) -> None:
    if not values:
        vector.extend([0.0] * 8)
        return
    mean = sum(values) / len(values)
    stats = [
        mean,
        feature_std(values),
        percentile(values, 0.10, mean),
        percentile(values, 0.25, mean),
        percentile(values, 0.50, mean),
        percentile(values, 0.75, mean),
        percentile(values, 0.90, mean),
        max(values),
    ]
    vector.extend(bounded_feature(value, scale) for value in stats)


def append_histogram_features(vector: list[float], values: list[float], bins: tuple[float, ...]) -> None:
    if not values:
        vector.extend([0.0] * (len(bins) + 1))
        return
    counts = [0] * (len(bins) + 1)
    for value in values:
        bucket = 0
        while bucket < len(bins) and value > bins[bucket]:
            bucket += 1
        counts[bucket] += 1
    total = float(len(values))
    vector.extend(count / total for count in counts)


def append_envelope_shape_features(vector: list[float], values: list[float], bins: int = 24) -> None:
    if not values:
        vector.extend([0.0] * bins)
        return
    mean = sum(values) / len(values)
    for index in range(bins):
        start = int(index * len(values) / bins)
        end = int((index + 1) * len(values) / bins)
        chunk = values[start:max(start + 1, end)]
        chunk_mean = sum(chunk) / len(chunk)
        vector.append(max(0.0, min(1.0, chunk_mean / max(mean * 3.0, 1e-6))))


def build_bgm_audiomuse_local_vector(samples: list[int]) -> list[float]:
    frame_size = max(1024, int(BGM_REFERENCE_ANALYSIS_SAMPLE_RATE * 0.75))
    if len(samples) < frame_size * 4:
        return []
    rms_values: list[float] = []
    zcr_values: list[float] = []
    brightness_values: list[float] = []
    crest_values: list[float] = []
    delta_values: list[float] = []
    previous_rms = 0.0
    for offset in range(0, len(samples) - frame_size + 1, frame_size):
        frame = samples[offset : offset + frame_size]
        if not frame:
            continue
        abs_values = [abs(sample) for sample in frame]
        square_sum = sum(float(sample) * float(sample) for sample in frame)
        rms = math.sqrt(square_sum / len(frame)) / 32768.0
        peak = max(abs_values) / 32768.0
        sign_changes = 0
        diff_sum = 0.0
        previous = frame[0]
        for sample in frame[1:]:
            if (previous < 0 <= sample) or (previous >= 0 > sample):
                sign_changes += 1
            diff_sum += abs(float(sample) - float(previous))
            previous = sample
        zcr = sign_changes / max(1, len(frame) - 1)
        brightness = (diff_sum / max(1, len(frame) - 1)) / 65536.0
        rms_values.append(rms)
        zcr_values.append(zcr)
        brightness_values.append(brightness)
        crest_values.append(min(peak / max(rms, 1e-5), 30.0))
        delta_values.append(abs(rms - previous_rms))
        previous_rms = rms
    if not rms_values:
        return []

    mean_rms = sum(rms_values) / len(rms_values)
    std_rms = feature_std(rms_values)
    onset_threshold = max(mean_rms + std_rms * 0.65, mean_rms * 1.35, 0.012)
    onset_indices: list[int] = []
    for index in range(1, len(rms_values)):
        if rms_values[index] >= onset_threshold and rms_values[index] >= rms_values[index - 1] * 1.16:
            onset_indices.append(index)
    duration = len(samples) / float(BGM_REFERENCE_ANALYSIS_SAMPLE_RATE)
    intervals = [
        (onset_indices[index] - onset_indices[index - 1]) * 0.75
        for index in range(1, len(onset_indices))
    ]
    silence_ratio = sum(1 for value in rms_values if value < max(mean_rms * 0.25, 0.006)) / len(rms_values)

    vector: list[float] = []
    append_scaled_stats(vector, rms_values, 0.22)
    append_scaled_stats(vector, zcr_values, 0.20)
    append_scaled_stats(vector, brightness_values, 0.08)
    append_scaled_stats(vector, crest_values, 20.0)
    append_scaled_stats(vector, delta_values, 0.08)
    append_histogram_features(vector, rms_values, (0.01, 0.025, 0.05, 0.08, 0.12, 0.18, 0.25))
    append_histogram_features(vector, zcr_values, (0.015, 0.03, 0.05, 0.075, 0.10, 0.14, 0.18))
    append_histogram_features(vector, brightness_values, (0.002, 0.004, 0.008, 0.014, 0.024, 0.04, 0.07))
    append_histogram_features(vector, crest_values, (1.5, 2.0, 3.0, 5.0, 8.0, 12.0, 18.0))
    append_envelope_shape_features(vector, rms_values)
    vector.extend(
        [
            bounded_feature(len(onset_indices) / max(duration, 1.0), 2.0),
            bounded_feature(sum(intervals) / len(intervals), 8.0) if intervals else 0.0,
            bounded_feature(feature_std(intervals), 6.0) if intervals else 0.0,
            max(0.0, min(1.0, silence_ratio)),
        ]
    )
    return vector


def bgm_audiomuse_local_similarity_score(reference_vector: list[float], candidate_vector: list[float]) -> float:
    if not reference_vector or not candidate_vector:
        return 0.0
    dims = min(len(reference_vector), len(candidate_vector))
    if dims < 16:
        return 0.0
    distance = sum(abs(reference_vector[index] - candidate_vector[index]) for index in range(dims)) / dims
    return max(0.0, min(100.0, 100.0 / (1.0 + distance * 4.0)))


def load_bgm_audiomuse_local_cache() -> dict[str, Any]:
    try:
        if BGM_AUDIOMUSE_LOCAL_CACHE_PATH.exists():
            payload = json.loads(BGM_AUDIOMUSE_LOCAL_CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and payload.get("version") == BGM_AUDIOMUSE_LOCAL_CACHE_VERSION:
                entries = payload.get("entries")
                if isinstance(entries, dict):
                    return entries
    except (OSError, json.JSONDecodeError):
        return {}
    return {}


def save_bgm_audiomuse_local_cache(cache: dict[str, Any]) -> None:
    try:
        BGM_AUDIOMUSE_LOCAL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": BGM_AUDIOMUSE_LOCAL_CACHE_VERSION, "entries": cache}
        BGM_AUDIOMUSE_LOCAL_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return


def bgm_audiomuse_local_cache_key(media_path: Path) -> str:
    return bgm_media_identity_key(media_path, "audiomuse_local", BGM_AUDIOMUSE_LOCAL_CACHE_VERSION, BGM_AUDIOMUSE_LOCAL_ANALYSIS_SECONDS)


def load_bgm_audiomuse_local_vector(
    media_path: Path,
    ffmpeg_path: Path,
    cache: dict[str, Any],
) -> list[float]:
    key = bgm_audiomuse_local_cache_key(media_path)
    cached = cache.get(key)
    if isinstance(cached, dict):
        vector = cached.get("vector")
        if isinstance(vector, list) and vector:
            return [float(value) for value in vector if isinstance(value, (int, float))]
    try:
        samples = decode_bgm_audio_samples(media_path, ffmpeg_path, duration_seconds=BGM_AUDIOMUSE_LOCAL_ANALYSIS_SECONDS)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return []
    vector = build_bgm_audiomuse_local_vector(samples)
    if vector:
        try:
            stat = media_path.stat()
            cache[key] = {
                "path": str(media_path),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "analysis_seconds": BGM_AUDIOMUSE_LOCAL_ANALYSIS_SECONDS,
                "vector": vector,
            }
        except OSError:
            cache[key] = {"path": str(media_path), "vector": vector}
    return vector


def create_bgm_chromaprint_speed_variant(
    media_path: Path,
    speed: float,
    ffmpeg_path: Path | None,
    workspace: WorkspaceContext,
) -> Path | None:
    if ffmpeg_path is None or abs(speed - 1.0) < 0.001:
        return media_path
    cache_key = bgm_media_identity_key(media_path, "chromaprint_speed", f"{speed:.3f}")
    output_path = BGM_CHROMAPRINT_VARIANT_CACHE_ROOT / f"{cache_key}_{speed:.3f}.wav"
    if output_path.exists() and output_path.stat().st_size > 4096:
        return output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(ffmpeg_path),
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-i",
        str(media_path),
        "-vn",
        "-t",
        f"{BGM_CHROMAPRINT_ANALYSIS_SECONDS:.1f}",
        "-filter:a",
        f"atempo={speed:.5f}",
        "-ac",
        "2",
        "-ar",
        "44100",
        str(output_path),
    ]
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=180,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        workspace.logger.info("[auto_bgm] failed to prepare Chromaprint speed variant %.2fx: %s", speed, exc)
        return None
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 4096:
        workspace.logger.info("[auto_bgm] unusable Chromaprint speed variant %.2fx for %s", speed, media_path)
        return None
    return output_path


def load_reference_chromaprint_fingerprints(
    media_path: Path,
    fpcalc_path: Path,
    cache: dict[str, Any],
    ffmpeg_path: Path | None,
    workspace: WorkspaceContext,
) -> list[tuple[float, list[int]]]:
    fingerprints: list[tuple[float, list[int]]] = []
    for speed in BGM_CHROMAPRINT_SPEED_VARIANTS:
        variant_path = create_bgm_chromaprint_speed_variant(media_path, speed, ffmpeg_path, workspace)
        if variant_path is None:
            continue
        fingerprint = load_chromaprint_fingerprint(variant_path, fpcalc_path, cache)
        if fingerprint:
            fingerprints.append((speed, fingerprint))
    return fingerprints


def best_bgm_chromaprint_similarity_score(
    reference_fingerprints: list[tuple[float, list[int]]],
    candidate_fingerprint: list[int],
) -> tuple[float, float]:
    best_score = 0.0
    best_speed = 1.0
    for speed, reference_fingerprint in reference_fingerprints:
        score = bgm_chromaprint_similarity_score(reference_fingerprint, candidate_fingerprint)
        if score > best_score:
            best_score = score
            best_speed = speed
    return best_score, best_speed


def query_audiomuse_candidate_scores(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    query: str,
    candidates: list[Path],
    reference_samples: list[int] | None = None,
    ffmpeg_path: Path | None = None,
) -> dict[Path, float]:
    scores: dict[Path, float] = {}
    if candidates and reference_samples and ffmpeg_path is not None:
        reference_vector = build_bgm_audiomuse_local_vector(reference_samples)
        if reference_vector:
            cache = load_bgm_audiomuse_local_cache()
            local_ranked: list[tuple[float, Path]] = []
            indexed_count = 0
            for path in candidates:
                cache_key = bgm_audiomuse_local_cache_key(path)
                cached_entry = cache.get(cache_key)
                had_cached_vector = isinstance(cached_entry, dict) and isinstance(cached_entry.get("vector"), list)
                candidate_vector = load_bgm_audiomuse_local_vector(path, ffmpeg_path, cache)
                if candidate_vector and not had_cached_vector:
                    indexed_count += 1
                score = bgm_audiomuse_local_similarity_score(reference_vector, candidate_vector)
                if score > 0.0:
                    scores[path] = max(scores.get(path, 0.0), score)
                    local_ranked.append((score, path))
            save_bgm_audiomuse_local_cache(cache)
            if indexed_count:
                workspace.logger.info(
                    "[auto_bgm] local AudioMuse index added %d new/changed BGM vectors",
                    indexed_count,
                )
            if local_ranked:
                top_matches = sorted(local_ranked, key=lambda item: (item[0], item[1].stat().st_size), reverse=True)[:5]
                workspace.logger.info(
                    "[auto_bgm] local AudioMuse index top matches: %s",
                    " | ".join(f"{path.name}={score:.1f}" for score, path in top_matches),
                )
    return scores


def score_local_bgm_audio_candidate(
    candidate_path: Path,
    reference_features: dict[str, float],
    reference_signature: list[tuple[float, ...]],
    ffmpeg_path: Path | None,
) -> tuple[float, float]:
    if ffmpeg_path is None:
        return 0.0, 0.0
    try:
        candidate_samples = decode_bgm_audio_samples(
            candidate_path,
            ffmpeg_path,
            duration_seconds=BGM_LOCAL_SIGNATURE_ANALYSIS_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return 0.0, 0.0
    if not candidate_samples:
        return 0.0, 0.0
    style_sample_count = int(BGM_REFERENCE_ANALYSIS_SECONDS * BGM_REFERENCE_ANALYSIS_SAMPLE_RATE)
    candidate_features = analyze_reference_audio_style(candidate_samples[:style_sample_count])
    style_score = bgm_similarity_score(reference_features, candidate_features) if reference_features else 0.0
    candidate_signature = build_bgm_local_signature(candidate_samples)
    signature_score = bgm_signature_similarity_score(reference_signature, candidate_signature)
    return style_score, signature_score


def infer_reference_bgm_search_query(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    reference_video: Path,
) -> str:
    ffmpeg_path = resolve_bgm_ffmpeg_path(workspace, task, shared_settings)
    if ffmpeg_path is None:
        workspace.logger.warning("[auto_bgm] ffmpeg not found; cannot analyze reference BGM style")
        return ""
    try:
        samples = decode_reference_audio_samples(reference_video, ffmpeg_path)
        features = analyze_reference_audio_style(samples)
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        workspace.logger.warning("[auto_bgm] reference audio style analysis failed: %s", exc)
        return ""
    if not features:
        workspace.logger.warning("[auto_bgm] reference audio style analysis found no usable audio")
        return ""
    query = reference_audio_style_query(features)
    workspace.logger.info(
        "[auto_bgm] reference BGM style query='%s' energy=%.3f dynamic=%.2f zcr=%.3f onset=%.2f/s",
        query,
        features.get("mean_rms", 0.0),
        features.get("dynamic_ratio", 0.0),
        features.get("mean_zcr", 0.0),
        features.get("onset_density", 0.0),
    )
    return query


def build_bgm_similarity_context(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    reference_video: Path,
) -> tuple[str, dict[str, float], Path | None, Path]:
    manual_query = str(setting_value(task, shared_settings, "bgm_search_query", "") or "").strip()
    reference_query = ""
    reference_features: dict[str, float] = {}
    reference_analysis_media = reference_video
    ffmpeg_path = resolve_bgm_ffmpeg_path(workspace, task, shared_settings)
    if ffmpeg_path is None:
        workspace.logger.warning("[auto_bgm] ffmpeg not found; cannot analyze reference BGM similarity")
    else:
        reference_analysis_media = resolve_reference_bgm_analysis_media(
            workspace,
            task,
            shared_settings,
            reference_video,
            ffmpeg_path,
        )
        try:
            reference_features = load_bgm_audio_features(reference_analysis_media, ffmpeg_path)
        except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
            workspace.logger.warning("[auto_bgm] reference BGM similarity analysis failed: %s", exc)
        if reference_features:
            reference_query = reference_audio_style_query(reference_features)
            workspace.logger.info(
                "[auto_bgm] reference BGM profile query='%s' energy=%.3f dynamic=%.2f zcr=%.3f onset=%.2f/s source=%s",
                reference_query,
                reference_features.get("mean_rms", 0.0),
                reference_features.get("dynamic_ratio", 0.0),
                reference_features.get("mean_zcr", 0.0),
                reference_features.get("onset_density", 0.0),
                reference_analysis_media,
            )
        else:
            workspace.logger.warning("[auto_bgm] reference BGM profile unavailable; falling back to text query")

    if reference_query and manual_query:
        return f"{reference_query} {manual_query}", reference_features, ffmpeg_path, reference_analysis_media
    if reference_query:
        return reference_query, reference_features, ffmpeg_path, reference_analysis_media
    return infer_bgm_search_query(workspace, reference_video, manual_query), reference_features, ffmpeg_path, reference_analysis_media


def build_bgm_search_query(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    reference_video: Path,
) -> str:
    query, _reference_features, _ffmpeg_path, _reference_analysis_media = build_bgm_similarity_context(
        workspace,
        task,
        shared_settings,
        reference_video,
    )
    return query


def normalize_bgm_external_dirs(raw_dirs: Any) -> list[str]:
    if raw_dirs is None:
        return []
    if isinstance(raw_dirs, (list, tuple, set)):
        items = raw_dirs
    else:
        items = re.split(r"[\r\n]+", str(raw_dirs))
    return [str(item or "").strip() for item in items if str(item or "").strip()]


def merge_default_bgm_library_dirs(external_dirs: list[str]) -> list[str]:
    merged = list(external_dirs)
    seen = {str(resolve_workspace_path(Path.cwd(), item, "")).lower() for item in merged if str(item or "").strip()}
    for default_dir in BGM_LOCAL_LIBRARY_DEFAULT_DIRS:
        if not default_dir.exists():
            continue
        key = str(default_dir.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(str(default_dir))
    return merged


def is_preferred_local_bgm(path: Path) -> bool:
    return path.name.lower() in {name.lower() for name in BGM_LOCAL_PREFERRED_FILENAMES}


def collect_bgm_files_from_dir(directory: Path) -> list[Path]:
    if not directory.exists() or not directory.is_dir():
        return []
    return [
        path.resolve()
        for path in directory.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_AUDIO_EXTENSIONS and path.stat().st_size > 0
    ]


def is_auto_download_bgm_cache(path: Path, workspace_bgm_root: Path) -> bool:
    try:
        relative = path.resolve().relative_to(workspace_bgm_root.resolve())
    except ValueError:
        return False
    if not relative.parts:
        return False
    first_part = relative.parts[0].lower()
    return first_part in {"external_selected", "online", "lx_music"} or first_part.startswith("lx_music_")


def collect_workspace_bgm_files(workspace: WorkspaceContext, external_dirs: list[str] | None = None) -> list[Path]:
    workspace_bgm_root = workspace.root / "bgm"
    candidates = [
        path
        for path in collect_bgm_files_from_dir(workspace_bgm_root)
        if not is_auto_download_bgm_cache(path, workspace_bgm_root)
    ]
    for raw_dir in external_dirs or []:
        directory = resolve_workspace_path(workspace.root, raw_dir, "")
        external_candidates = collect_bgm_files_from_dir(directory)
        if external_candidates:
            workspace.logger.info("[auto_bgm] found %d BGM files in external dir: %s", len(external_candidates), directory)
        else:
            workspace.logger.warning("[auto_bgm] external BGM dir has no supported audio: %s", directory)
        candidates.extend(external_candidates)
    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in sorted(candidates, key=lambda item: str(item).lower()):
        if path in seen:
            continue
        seen.add(path)
        deduped.append(path)
    return deduped


def score_local_bgm_candidate(path: Path, query: str, workspace: WorkspaceContext, reference_video: Path) -> float:
    haystack = f"{path.stem} {path.parent.name}".lower()
    context = f"{workspace.name} {reference_video.stem}".lower()
    query_tokens = [
        token.lower()
        for token in re.findall(r"[\w\u4e00-\u9fff]+", query)
        if len(token.strip()) >= 2
    ]
    score = 0.0
    for token in query_tokens:
        if token in haystack:
            score += 3.0
        elif token in context and token in haystack:
            score += 2.0
    for token in re.findall(r"[\w\u4e00-\u9fff]+", context):
        if len(token) >= 2 and token in haystack:
            score += 1.5
    if "instrumental" in query.lower() and any(word in haystack for word in ("instrumental", "纯音乐", "无歌词")):
        score += 2.0
    if any(word in haystack for word in ("bgm", "配乐", "music")):
        score += 0.5
    return score


def choose_local_bgm(
    workspace: WorkspaceContext,
    reference_video: Path,
    query: str,
    reference_features: dict[str, float] | None = None,
    ffmpeg_path: Path | None = None,
    reference_analysis_media: Path | None = None,
    external_dirs: list[str] | None = None,
    task: dict[str, Any] | None = None,
    shared_settings: dict[str, Any] | None = None,
) -> Path | None:
    candidates = collect_workspace_bgm_files(workspace, external_dirs=external_dirs)
    if not candidates:
        return None
    task_settings = task or {}
    shared = shared_settings or {}
    analysis_media = reference_analysis_media or reference_video
    reference_samples: list[int] = []
    reference_signature: list[tuple[float, ...]] = []
    if ffmpeg_path is not None:
        try:
            reference_samples = decode_bgm_audio_samples(
                analysis_media,
                ffmpeg_path,
                duration_seconds=max(BGM_LOCAL_SIGNATURE_ANALYSIS_SECONDS, BGM_AUDIOMUSE_LOCAL_ANALYSIS_SECONDS),
            )
            signature_sample_count = int(BGM_LOCAL_SIGNATURE_ANALYSIS_SECONDS * BGM_REFERENCE_ANALYSIS_SAMPLE_RATE)
            reference_signature = build_bgm_local_signature(reference_samples[:signature_sample_count])
        except (OSError, subprocess.TimeoutExpired, ValueError):
            reference_signature = []
    fpcalc_path = resolve_chromaprint_fpcalc_path(workspace, task_settings, shared)
    chromaprint_cache: dict[str, Any] = {}
    reference_fingerprints: list[tuple[float, list[int]]] = []
    if fpcalc_path is not None:
        chromaprint_cache = load_bgm_chromaprint_cache()
        reference_fingerprints = load_reference_chromaprint_fingerprints(
            analysis_media,
            fpcalc_path,
            chromaprint_cache,
            ffmpeg_path,
            workspace,
        )
        if reference_fingerprints:
            workspace.logger.info(
                "[auto_bgm] Chromaprint enabled, reference fingerprints=%d source=%s fpcalc=%s",
                len(reference_fingerprints),
                analysis_media,
                fpcalc_path,
            )
        else:
            workspace.logger.info("[auto_bgm] Chromaprint available but reference fingerprint is unusable: %s", fpcalc_path)
    else:
        workspace.logger.info("[auto_bgm] Chromaprint fpcalc not found; same-song detection will use local signature only")
    audiomuse_scores = query_audiomuse_candidate_scores(
        workspace,
        task_settings,
        shared,
        query,
        candidates,
        reference_samples=reference_samples,
        ffmpeg_path=ffmpeg_path,
    )
    scored: list[tuple[float, float, float, float, float, float, float, int, Path]] = []
    for path in candidates:
        text_score = score_local_bgm_candidate(path, query, workspace, reference_video)
        style_score, signature_score = score_local_bgm_audio_candidate(
            path,
            reference_features or {},
            reference_signature,
            ffmpeg_path,
        )
        chromaprint_score = 0.0
        chromaprint_speed = 1.0
        if fpcalc_path is not None and reference_fingerprints:
            candidate_fingerprint = load_chromaprint_fingerprint(path, fpcalc_path, chromaprint_cache)
            chromaprint_score, chromaprint_speed = best_bgm_chromaprint_similarity_score(
                reference_fingerprints,
                candidate_fingerprint,
            )
        audiomuse_score = audiomuse_scores.get(path, 0.0)
        preferred = 1 if is_preferred_local_bgm(path) else 0
        preferred_bonus = BGM_LOCAL_PREFERRED_BONUS if preferred else 0.0
        combined_score = (
            signature_score * 0.40
            + style_score * 0.40
            + min(text_score * 2.0, 8.0)
            + min(audiomuse_score * 0.25, 25.0)
            + preferred_bonus
        )
        if chromaprint_score >= BGM_CHROMAPRINT_EXACT_MATCH_MIN_SCORE:
            combined_score += 100.0 + chromaprint_score
        elif chromaprint_score >= 72.0:
            combined_score += 20.0
        if audiomuse_score >= 60.0:
            combined_score += min(audiomuse_score * 0.80, 80.0)
        if signature_score >= 65.0:
            combined_score += 10.0
        scored.append((combined_score, chromaprint_score, chromaprint_speed, signature_score, style_score, text_score, audiomuse_score, preferred, path))
    if fpcalc_path is not None:
        save_bgm_chromaprint_cache(chromaprint_cache)
    exact_matches = [item for item in scored if item[1] >= BGM_CHROMAPRINT_EXACT_MATCH_MIN_SCORE]
    if exact_matches:
        selected = max(exact_matches, key=lambda item: (item[1], item[3], item[4], item[7], item[8].stat().st_size))
        _total, exact_score, exact_speed, signature_score, style_score, text_score, audiomuse_score, _preferred, selected_path = selected
        workspace.logger.info(
            "[auto_bgm] selected same-song BGM by Chromaprint exact %.1f/100 speed_variant=%.2fx, signature %.1f/100, style %.1f/100, AudioMuse %.1f: %s",
            exact_score,
            exact_speed,
            signature_score,
            style_score,
            audiomuse_score,
            selected_path,
        )
        return selected_path
    best_combined_score, best_exact_score, best_exact_speed, best_signature_score, best_style_score, best_text_score, best_audiomuse_score, _preferred, best_path = max(
        scored,
        key=lambda item: (item[0], item[1], item[3], item[4], item[5], item[6], item[7], item[8].stat().st_size, str(item[8]).lower()),
    )
    preferred_close_matches = [
        item
        for item in scored
        if item[7] and item[0] >= best_combined_score - BGM_LOCAL_PREFERRED_CLOSE_MARGIN
    ]
    if preferred_close_matches:
        selected = max(
            preferred_close_matches,
            key=lambda item: (item[4] + item[3], item[6], item[1], item[0], item[8].stat().st_size),
        )
        (
            selected_total,
            selected_exact_score,
            selected_exact_speed,
            selected_signature_score,
            selected_style_score,
            selected_text_score,
            selected_audiomuse_score,
            _selected_preferred,
            selected_path,
        ) = selected
        if selected_path != best_path:
            workspace.logger.info(
                "[auto_bgm] preferred local BGM won close match total %.1f vs best %.1f, fp %.1f/100 speed_variant=%.2fx, signature %.1f/100, style %.1f/100, text %.1f, AudioMuse %.1f: %s",
                selected_total,
                best_combined_score,
                selected_exact_score,
                selected_exact_speed,
                selected_signature_score,
                selected_style_score,
                selected_text_score,
                selected_audiomuse_score,
                selected_path,
            )
        best_combined_score = selected_total
        best_exact_score = selected_exact_score
        best_exact_speed = selected_exact_speed
        best_signature_score = selected_signature_score
        best_style_score = selected_style_score
        best_text_score = selected_text_score
        best_audiomuse_score = selected_audiomuse_score
        best_path = selected_path
    top_matches = sorted(
        scored,
        key=lambda item: (item[0], item[1], item[3], item[4], item[5], item[6], item[7]),
        reverse=True,
    )[:5]
    workspace.logger.info(
        "[auto_bgm] local library top matches: %s",
        " | ".join(
            f"{path.name} total={total:.1f} fp={exact:.1f}@{speed:.2f}x sig={signature:.1f} style={style:.1f} text={text:.1f} audiomuse={audiomuse:.1f}"
            for total, exact, speed, signature, style, text, audiomuse, _preferred_flag, path in top_matches
        ),
    )
    workspace.logger.info(
        "[auto_bgm] selected local library BGM total %.1f, fp %.1f/100 speed_variant=%.2fx, signature %.1f/100, style %.1f/100, text %.1f, AudioMuse %.1f: %s",
        best_combined_score,
        best_exact_score,
        best_exact_speed,
        best_signature_score,
        best_style_score,
        best_text_score,
        best_audiomuse_score,
        best_path,
    )
    return best_path


def resolve_auto_bgm_path(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    reference_video: Path,
) -> Path | None:
    mode = normalize_bgm_source_mode(setting_value(task, shared_settings, "bgm_source_mode", "auto"))
    if mode in {"none", "manual"}:
        return None

    query, reference_features, ffmpeg_path, reference_analysis_media = build_bgm_similarity_context(
        workspace,
        task,
        shared_settings,
        reference_video,
    )
    external_dirs = normalize_bgm_external_dirs(setting_value(task, shared_settings, "bgm_external_dirs", []))
    local_library_dirs = merge_default_bgm_library_dirs(external_dirs)

    if mode in {"auto", "local_auto"}:
        local_path = choose_local_bgm(
            workspace,
            reference_video,
            query,
            reference_features=reference_features,
            ffmpeg_path=ffmpeg_path,
            reference_analysis_media=reference_analysis_media,
            external_dirs=local_library_dirs,
            task=task,
            shared_settings=shared_settings,
        )
        if local_path is not None:
            workspace.logger.info("[auto_bgm] selected local library BGM: %s", local_path)
            return local_path
        workspace.logger.warning("[auto_bgm] no local library BGM matched query: %s", query)
        return None

    local_path = choose_local_bgm(
        workspace,
        reference_video,
        query,
        reference_features=reference_features,
        ffmpeg_path=ffmpeg_path,
        reference_analysis_media=reference_analysis_media,
        external_dirs=local_library_dirs,
        task=task,
        shared_settings=shared_settings,
    )
    if local_path is not None:
        workspace.logger.info("[auto_bgm] selected local BGM: %s", local_path)
    else:
        workspace.logger.warning("[auto_bgm] no local BGM matched query: %s", query)
    return local_path


def resolve_first_workspace_input(
    workspace_root: Path,
    raw_path: str | None,
    raw_glob: str | None = None,
    *,
    directories_only: bool = False,
) -> Path | None:
    if raw_path:
        candidate = resolve_workspace_path(workspace_root, raw_path, "")
        if directories_only and candidate.is_dir():
            return candidate
        if not directories_only and candidate.is_file():
            return candidate

    matches = resolve_glob_matches(workspace_root, raw_glob, directories_only=directories_only)
    if matches:
        return matches[0]
    return None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def expand_subtitle_inputs(task: dict[str, Any], workspace_root: Path) -> list[Path]:
    results: list[Path] = []

    for raw_path in task.get("input_paths", []):
        path = resolve_workspace_path(workspace_root, raw_path, "")
        if path.is_file():
            results.append(path.resolve())
        elif path.is_dir():
            for file_path in sorted(path.rglob("*")):
                if file_path.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS:
                    results.append(file_path.resolve())

    if task.get("input_path"):
        path = resolve_workspace_path(workspace_root, task["input_path"], "")
        if path.is_file():
            results.append(path.resolve())
        elif path.is_dir():
            for file_path in sorted(path.rglob("*")):
                if file_path.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS:
                    results.append(file_path.resolve())

    if task.get("input_glob"):
        for file_path in resolve_glob_matches(workspace_root, task.get("input_glob")):
            if file_path.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS:
                results.append(file_path.resolve())

    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in results:
        if path not in seen:
            seen.add(path)
            deduped.append(path)
    return deduped


def stream_process_output(process: subprocess.Popen[str], logger: logging.Logger, prefix: str) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        logger.info("[%s] %s", prefix, line.rstrip())


def run_process(command: list[str], cwd: Path, logger: logging.Logger, prefix: str) -> int:
    logger.info("[%s] COMMAND %s", prefix, subprocess.list2cmdline(command))
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=build_subprocess_env(),
    )
    stream_process_output(process, logger, prefix)
    return process.wait()


def run_stage_specs(
    workspace: WorkspaceContext,
    stage_name: str,
    specs: list[TaskSpec],
    per_workspace_limit: int,
    global_semaphore: threading.Semaphore,
) -> dict[str, int]:
    logger = workspace.logger
    if not specs:
        logger.info("[%s] no tasks", stage_name)
        return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    summary = {"total": len(specs), "success": 0, "failed": 0, "skipped": 0}
    max_workers = max(1, per_workspace_limit)

    def worker(spec: TaskSpec) -> str:
        try:
            if spec.skip_reason:
                logger.info("[%s] SKIP %s", spec.label, spec.skip_reason)
                return "skipped"
            assert spec.command is not None
            with global_semaphore:
                exit_code = run_process(spec.command, spec.cwd, logger, spec.label)
            if exit_code == 0:
                logger.info("[%s] finished successfully", spec.label)
                return "success"
            logger.error("[%s] failed with exit code %s", spec.label, exit_code)
            return "failed"
        except Exception as exc:
            logger.exception("[%s] crashed before completion: %s", spec.label, exc)
            return "failed"

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, spec) for spec in specs]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            summary[result] += 1

    logger.info(
        "[%s] summary total=%s success=%s failed=%s skipped=%s",
        stage_name,
        summary["total"],
        summary["success"],
        summary["failed"],
        summary["skipped"],
    )
    return summary


def build_baidu_specs(workspace: WorkspaceContext) -> list[TaskSpec]:
    script_path = MODULES_ROOT / "baidu_share_downloader" / "baidu_share_downloader.py"
    handoff_script_path = MODULES_ROOT / "baidu_official_client_handoff.py"
    python_path = resolve_python([
        MODULES_ROOT / "baidu_share_downloader" / ".venv" / "Scripts" / "python.exe",
    ])
    shared_settings = workspace.config.get("settings") or {}
    handoff_mode = str(shared_settings.get("baidu_share_handoff_mode") or "").strip().lower()
    if handoff_mode not in {"queue", "invoker"}:
        handoff_mode = ""

    specs: list[TaskSpec] = []
    official_groups: dict[tuple[str, str], dict[str, Any]] = {}
    for index, task in enumerate(workspace.config.get("baidu_share", []), start=1):
        share_url = task.get("share_url", "").strip()
        if not share_url:
            specs.append(TaskSpec("baidu_share", f"baidu#{index}", None, PROJECT_ROOT, "missing share_url"))
            continue

        output_dir = resolve_download_output_dir(workspace, task.get("output_subdir"), "downloads/baidu")
        output_dir.mkdir(parents=True, exist_ok=True)
        target_filename = (task.get("target_filename") or "").strip()
        target_path = (task.get("target_path") or "").strip()
        target_fsid = str(task.get("target_fsid") or "").strip()
        try:
            download_threads = max(1, int(task.get("download_threads") or 1))
        except (TypeError, ValueError):
            download_threads = 1
        target_label = target_filename or Path(target_path).name or f"file_{index}"
        skip_existing = bool(task.get("skip_existing", bool(target_filename)))
        download_mode = normalize_baidu_download_mode(task.get("download_mode"))
        excluded_keyword = baidu_task_excluded_keyword(workspace, task)
        if excluded_keyword:
            specs.append(
                TaskSpec(
                    "baidu_share",
                    f"baidu#{index}:{target_label}",
                    None,
                    PROJECT_ROOT,
                    f"excluded by keyword: {excluded_keyword}",
                )
            )
            continue

        expected_output = expected_baidu_output_path(output_dir, task)
        target_size = baidu_task_target_size(workspace, task)
        if expected_output is not None and skip_existing and should_skip_existing_baidu_output(expected_output, task, target_size=target_size):
            specs.append(
                TaskSpec(
                    "baidu_share",
                    f"baidu#{index}:{target_label}",
                    None,
                    PROJECT_ROOT,
                    f"target already exists and size verified: {expected_output}",
                )
            )
            continue

        if download_mode == "official_client":
            group_key = (share_url, str(output_dir))
            group = official_groups.setdefault(
                group_key,
                {
                    "share_url": share_url,
                    "output_dir": output_dir,
                    "targets": [],
                },
            )
            resolved_local_path = expected_output or (output_dir / target_label)
            group["targets"].append(
                {
                    "label": target_label,
                    "target_filename": target_filename,
                    "target_path": target_path,
                    "target_fsid": target_fsid,
                    "target_size": target_size,
                    "local_path": str(resolved_local_path),
                }
            )
            continue

        command = [
            python_path,
            str(script_path),
            share_url,
            "--output-dir",
            str(output_dir),
        ]
        if target_fsid:
            command.extend(["--target-fsid", target_fsid])
        elif target_path:
            command.extend(["--target-path", target_path])
        if target_filename:
            command.extend(["--target-filename", target_filename])
        command.extend(["--download-threads", str(download_threads)])
        specs.append(TaskSpec("baidu_share", f"baidu#{index}", command, script_path.parent))

    for group_index, group in enumerate(official_groups.values(), start=1):
        targets = group["targets"]
        preview_labels = [str(item.get("label") or "").strip() for item in targets if str(item.get("label") or "").strip()]
        preview = ", ".join(preview_labels[:3])
        if len(preview_labels) > 3:
            preview = f"{preview} ... 共 {len(preview_labels)} 个文件"
        spec_path = workspace.root / "temp" / f"official_client_group_{group_index}.json"
        spec_path.write_text(
            json.dumps(
                {
                    "workspace_name": workspace.name,
                    "share_url": group["share_url"],
                    "preferred_output_dir": str(group["output_dir"]),
                    "targets": targets,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        command = [
            python_path,
            str(handoff_script_path),
            group["share_url"],
            "--workspace-name",
            workspace.name,
            "--preferred-output-dir",
            str(group["output_dir"]),
            "--target-spec-file",
            str(spec_path),
        ]
        if handoff_mode:
            command.extend(["--handoff-mode", handoff_mode])
        specs.append(
            TaskSpec(
                "baidu_share",
                f"baidu_official#{group_index}{':' + preview if preview else ''}",
                command,
                PROJECT_ROOT,
                "" if targets else f"all targets already exist for {group['share_url']}",
            )
        )
    return specs


def build_douyin_specs(workspace: WorkspaceContext) -> list[TaskSpec]:
    script_path = MODULES_ROOT / "douyin_batch_downloader.py"
    python_path = resolve_python([
        MODULES_ROOT / "douyin_api" / ".venv" / "Scripts" / "python.exe",
    ])

    specs: list[TaskSpec] = []
    for index, task in enumerate(workspace.config.get("douyin_download", []), start=1):
        share_url = task.get("url", "").strip()
        if not share_url:
            specs.append(TaskSpec("douyin_download", f"douyin#{index}", None, PROJECT_ROOT, "missing url"))
            continue

        output_dir = resolve_download_output_dir(workspace, task.get("output_subdir"), "downloads/douyin")
        output_dir.mkdir(parents=True, exist_ok=True)

        command = [
            python_path,
            str(script_path),
            "--url",
            share_url,
            "--output-dir",
            str(output_dir),
        ]
        if task.get("output_name"):
            command.extend(["--output-name", str(task["output_name"])])
        if task.get("filename_prefix"):
            command.extend(["--filename-prefix", str(task["filename_prefix"])])
        if task.get("with_watermark"):
            command.append("--with-watermark")
        if task.get("overwrite"):
            command.append("--overwrite")

        specs.append(TaskSpec("douyin_download", f"douyin#{index}", command, PROJECT_ROOT))
    return specs


def build_subtitle_specs(workspace: WorkspaceContext) -> list[TaskSpec]:
    script_path = MODULES_ROOT / "auto_clip_engine" / "funasr_subtitle_cli.py"
    python_path = resolve_python([
        MODULES_ROOT / "auto_clip_engine" / ".venv" / "Scripts" / "python.exe",
    ])

    raw_tasks = workspace.config.get("subtitle_extract") or []
    if not raw_tasks:
        shared_settings = workspace.config.get("settings") or {}
        if bool(shared_settings.get("prefer_funasr_audio_subtitles", False)):
            raw_tasks = [{"input_glob": "downloads/douyin/*", "output_subdir": "subtitles/audio", "skip_existing": False}]

    specs: list[TaskSpec] = []
    for index, task in enumerate(raw_tasks, start=1):
        task = dict(task)
        if not task.get("input_path") and not task.get("input_paths"):
            input_glob = str(task.get("input_glob") or "").strip()
            if input_glob in {"", "downloads/douyin/*"}:
                douyin_output_dir = infer_first_douyin_output_dir(workspace)
                if douyin_output_dir is not None:
                    task["input_glob"] = str(douyin_output_dir / "*")

        input_paths = expand_subtitle_inputs(task, workspace.root)
        input_paths = filter_paths_by_suffix(input_paths, SUPPORTED_VIDEO_EXTENSIONS)
        if not input_paths:
            specs.append(TaskSpec("subtitle_extract", f"subtitle#{index}", None, PROJECT_ROOT, "no input videos matched"))
            continue

        output_dir = resolve_workspace_path(workspace.root, task.get("output_subdir"), "subtitles/audio")
        output_dir.mkdir(parents=True, exist_ok=True)

        for video_path in input_paths:
            output_name = task.get("output_name") or safe_output_name_from_path(video_path, workspace.root)
            if Path(output_name).suffix.lower() != ".srt":
                output_name = f"{Path(output_name).stem}.srt"
            output_path = output_dir / output_name
            skip_existing = bool(task.get("skip_existing", True))

            label = f"subtitle#{index}:{video_path.name}"
            if output_path.exists() and skip_existing:
                specs.append(TaskSpec("subtitle_extract", label, None, PROJECT_ROOT, f"output already exists: {output_path}"))
                continue

            command = [
                python_path,
                str(script_path),
                "--reference-video",
                str(video_path),
                "--output",
                str(output_path),
            ]
            if task.get("ffmpeg"):
                command.extend(["--ffmpeg", str(resolve_workspace_path(workspace.root, task.get("ffmpeg"), ""))])
            if task.get("ffprobe"):
                command.extend(["--ffprobe", str(resolve_workspace_path(workspace.root, task.get("ffprobe"), ""))])

            specs.append(TaskSpec("subtitle_extract", label, command, PROJECT_ROOT))
    return specs


def build_visual_subtitle_specs(workspace: WorkspaceContext) -> list[TaskSpec]:
    script_path = MODULES_ROOT / "subtitle_batch_runner.py"
    python_path = resolve_python([
        MODULES_ROOT / "subtitle_extractor_source" / "video-subtitle-extractor-main" / ".venv" / "Scripts" / "python.exe",
    ])

    raw_tasks = workspace.config.get("visual_subtitle_extract") or []
    if not raw_tasks and (workspace.config.get("auto_clip") or []):
        raw_tasks = [
            {
                "input_glob": "downloads/douyin/*",
                "output_subdir": "subtitles/visual",
                "temp_subdir": "temp/visual_subtitle",
                "auto_detect_subtitle_area": True,
                "language": "ch",
                "mode": "accurate",
                "extract_frequency": "auto",
                "probe_extract_frequency": "auto",
                "generate_txt": True,
                "skip_existing": True,
            }
        ]

    specs: list[TaskSpec] = []
    for index, task in enumerate(raw_tasks, start=1):
        task = dict(task)
        if not task.get("input_path") and not task.get("input_paths"):
            input_glob = str(task.get("input_glob") or "").strip()
            if input_glob in {"", "downloads/douyin/*"}:
                douyin_output_dir = infer_first_douyin_output_dir(workspace)
                if douyin_output_dir is not None:
                    task["input_glob"] = str(douyin_output_dir / "*")

        input_paths = expand_subtitle_inputs(task, workspace.root)
        input_paths = filter_paths_by_suffix(input_paths, SUPPORTED_VIDEO_EXTENSIONS)
        if not input_paths:
            specs.append(TaskSpec("visual_subtitle_extract", f"visual_subtitle#{index}", None, PROJECT_ROOT, "no input videos matched"))
            continue

        output_dir = resolve_workspace_path(workspace.root, task.get("output_subdir"), "subtitles/visual")
        output_dir.mkdir(parents=True, exist_ok=True)
        temp_root = resolve_workspace_path(workspace.root, task.get("temp_subdir"), "temp/visual_subtitle")
        temp_root.mkdir(parents=True, exist_ok=True)

        for video_path in input_paths:
            auto_frequency = auto_visual_subtitle_frequency(video_path)
            extract_frequency = parse_frequency_value(task.get("extract_frequency")) or auto_frequency
            probe_extract_frequency = parse_frequency_value(task.get("probe_extract_frequency")) or extract_frequency
            output_name = task.get("output_name") or safe_output_name_from_path(video_path, workspace.root)
            if Path(output_name).suffix.lower() != ".srt":
                output_name = f"{Path(output_name).stem}.srt"
            output_path = output_dir / output_name
            skip_existing = bool(task.get("skip_existing", True))

            label = f"visual_subtitle#{index}:{video_path.name}"
            if output_path.exists() and skip_existing:
                specs.append(TaskSpec("visual_subtitle_extract", label, None, PROJECT_ROOT, f"output already exists: {output_path}"))
                continue

            command = [
                python_path,
                str(script_path),
                "--input",
                str(video_path),
                "--output-dir",
                str(output_dir),
                "--output-name",
                output_name,
                "--temp-root",
                str(temp_root),
                "--temp-name",
                sanitize_stem(video_path.stem),
                "--language",
                str(task.get("language") or "ch"),
                "--mode",
                str(task.get("mode") or "accurate"),
                "--extract-frequency",
                str(extract_frequency),
                "--probe-extract-frequency",
                str(probe_extract_frequency),
            ]
            subtitle_area = task.get("subtitle_area")
            if isinstance(subtitle_area, (list, tuple)) and len(subtitle_area) == 4:
                command.extend(["--subtitle-area", ",".join(str(parse_int(value, 0)) for value in subtitle_area)])
            elif isinstance(subtitle_area, str) and subtitle_area.strip():
                command.extend(["--subtitle-area", subtitle_area.strip()])
            elif bool(task.get("auto_detect_subtitle_area", task.get("auto_subtitle_area", True))):
                command.append("--auto-subtitle-area")
            else:
                command.append("--auto-subtitle-area")
            if bool(task.get("generate_txt", True)):
                command.append("--generate-txt")
            if bool(task.get("keep_temp", False)):
                command.append("--keep-temp")
            if not skip_existing:
                command.append("--overwrite")

            specs.append(TaskSpec("visual_subtitle_extract", label, command, PROJECT_ROOT))
    return specs


def subtitle_lookup_keys(path: Path) -> list[str]:
    stem = sanitize_stem(path.stem).lower()
    keys = {stem, path.stem.lower()}
    for prefix in ("downloads__douyin__", "downloads_douyin_", "douyin__", "reference__"):
        if stem.startswith(prefix):
            keys.add(stem[len(prefix) :])
    return [key for key in keys if key]


def find_matching_subtitle(
    subtitle_paths: list[Path],
    used_subtitles: set[Path],
    video_path: Path,
    workspace_root: Path,
) -> Path | None:
    expected_stem = Path(safe_output_name_from_path(video_path, workspace_root)).stem.lower()
    direct_stem = sanitize_stem(video_path.stem).lower()
    lookup: dict[str, list[Path]] = {}
    for subtitle_path in subtitle_paths:
        for key in subtitle_lookup_keys(subtitle_path):
            lookup.setdefault(key, []).append(subtitle_path)

    for key in (expected_stem, direct_stem, video_path.stem.lower()):
        for candidate in lookup.get(key, []):
            if candidate not in used_subtitles:
                return candidate

    if len(subtitle_paths) == 1 and subtitle_paths[0] not in used_subtitles:
        return subtitle_paths[0]
    return None


def render_auto_clip_title(task: dict[str, Any], workspace: WorkspaceContext, reference_video: Path, index: int, total: int) -> str:
    template = str(task.get("title") or "").strip()
    reference_stem = sanitize_stem(reference_video.stem)
    context = {
        "workspace_name": workspace.name,
        "reference_stem": reference_stem,
        "index": index,
        "total": total,
    }
    if template:
        try:
            rendered = template.format(**context)
        except Exception:
            rendered = template
        if total > 1 and "{reference_stem}" not in template and "{index}" not in template:
            rendered = f"{rendered}_{reference_stem}"
        return sanitize_stem(rendered)
    if total > 1:
        return sanitize_stem(f"{workspace.name}_{reference_stem}")
    return reference_stem


def build_auto_clip_specs(workspace: WorkspaceContext) -> list[TaskSpec]:
    script_path = MODULES_ROOT / "auto_clip_engine" / "drama_clone_cli.py"
    python_path = resolve_python([
        MODULES_ROOT / "auto_clip_engine" / ".venv" / "Scripts" / "python.exe",
    ])

    shared_settings = workspace.config.get("settings") or {}
    specs: list[TaskSpec] = []
    for index, task in enumerate(workspace.config.get("auto_clip", []), start=1):
        task = dict(task)
        reference_video_glob = str(task.get("reference_video_glob") or "").strip()
        if not task.get("reference_video") and reference_video_glob in {"", "downloads/douyin/*"}:
            douyin_output_dir = infer_first_douyin_output_dir(workspace)
            if douyin_output_dir is not None:
                task["reference_video_glob"] = str(douyin_output_dir / "*")

        prefer_funasr_audio_subtitles = bool(
            task.get(
                "prefer_funasr_audio_subtitles",
                shared_settings.get("prefer_funasr_audio_subtitles", False),
            )
        )
        force_no_narration_mode = bool(
            task.get(
                "force_no_narration_mode",
                shared_settings.get("force_no_narration_mode", False),
            )
        )
        reference_subtitle_glob = str(task.get("reference_subtitle_glob") or "").strip()
        if (
            not prefer_funasr_audio_subtitles
            and not force_no_narration_mode
            and not task.get("reference_subtitle")
            and reference_subtitle_glob in {"", "subtitles/*.srt"}
        ):
            subtitle_output_dir = infer_first_visual_subtitle_output_dir(workspace)
            if subtitle_output_dir is not None:
                task["reference_subtitle_glob"] = str(subtitle_output_dir / "*.srt")

        reference_videos = resolve_workspace_inputs(
            workspace.root,
            task.get("reference_video"),
            task.get("reference_video_glob"),
        )
        reference_videos = filter_paths_by_suffix(reference_videos, SUPPORTED_VIDEO_EXTENSIONS)
        if not reference_videos:
            specs.append(TaskSpec("auto_clip", f"auto_clip#{index}", None, PROJECT_ROOT, "missing reference_video"))
            continue

        reference_subtitles: list[Path] = []
        explicit_reference_subtitle = bool(task.get("reference_subtitle")) or bool(task.get("reference_subtitle_glob"))
        candidate_subtitle_glob = str(task.get("reference_subtitle_glob") or "").strip()
        if not task.get("reference_subtitle") and candidate_subtitle_glob in {"", "subtitles/*.srt", "subtitles/audio/*.srt"}:
            subtitle_output_dir = (
                infer_first_subtitle_output_dir(workspace)
                if prefer_funasr_audio_subtitles
                else infer_first_visual_subtitle_output_dir(workspace)
            )
            if subtitle_output_dir is not None:
                task["reference_subtitle_glob"] = str(subtitle_output_dir / "*.srt")
        reference_subtitles = resolve_workspace_inputs(
            workspace.root,
            task.get("reference_subtitle"),
            task.get("reference_subtitle_glob"),
        )
        reference_subtitles = filter_paths_by_suffix(reference_subtitles, SUPPORTED_SUBTITLE_EXTENSIONS)
        if explicit_reference_subtitle and not reference_subtitles and not force_no_narration_mode:
            specs.append(TaskSpec("auto_clip", f"auto_clip#{index}", None, PROJECT_ROOT, "missing reference_subtitle"))
            continue

        raw_source_dir = str(task.get("source_dir") or "").strip()
        if raw_source_dir in {"", "downloads/baidu"}:
            inferred_source_dir = infer_first_baidu_output_dir(workspace)
            source_dir = inferred_source_dir or resolve_workspace_path(workspace.root, task.get("source_dir"), "downloads/baidu")
        else:
            source_dir = resolve_workspace_path(workspace.root, task.get("source_dir"), "downloads/baidu")
        if not source_dir.is_dir():
            specs.append(TaskSpec("auto_clip", f"auto_clip#{index}", None, PROJECT_ROOT, f"invalid source_dir: {source_dir}"))
            continue

        output_dir = resolve_workspace_path(workspace.root, task.get("output_subdir"), "clips")
        temp_root = resolve_workspace_path(workspace.root, task.get("temp_subdir"), "temp/auto_clip")
        output_dir.mkdir(parents=True, exist_ok=True)
        temp_root.mkdir(parents=True, exist_ok=True)

        match_all = bool(task.get("match_all_references", bool(task.get("reference_video_glob"))))
        if match_all:
            pairs: list[tuple[Path, Path | None]] = []
            used_subtitles: set[Path] = set()
            for video_path in reference_videos:
                subtitle_path = None
                if reference_subtitles and not force_no_narration_mode:
                    subtitle_path = find_matching_subtitle(reference_subtitles, used_subtitles, video_path, workspace.root)
                    if subtitle_path is not None:
                        used_subtitles.add(subtitle_path)
                pairs.append((video_path, subtitle_path))
        else:
            first_subtitle = None if force_no_narration_mode or not reference_subtitles else reference_subtitles[0]
            pairs = [(reference_videos[0], first_subtitle)]

        total_pairs = len(pairs)
        skip_existing = False
        for pair_index, (reference_video, reference_subtitle) in enumerate(pairs, start=1):
            if reference_subtitle is None and explicit_reference_subtitle and not force_no_narration_mode:
                label = f"auto_clip#{index}:{reference_video.name}"
                specs.append(TaskSpec("auto_clip", label, None, PROJECT_ROOT, "no matching subtitle for reference video"))
                continue

            title = render_auto_clip_title(task, workspace, reference_video, pair_index, total_pairs)
            expected_output = output_dir / f"{title}.mp4"
            label = f"auto_clip#{index}:{title}"
            if expected_output.exists() and skip_existing:
                specs.append(TaskSpec("auto_clip", label, None, PROJECT_ROOT, f"output already exists: {expected_output}"))
                continue

            job_payload: dict[str, Any] = {
                "reference_video": str(reference_video),
                "source_dir": str(source_dir),
                "output_dir": str(output_dir),
                "title": title,
            }
            if reference_subtitle is not None:
                job_payload["reference_subtitle"] = str(reference_subtitle)
            bgm_source_mode = normalize_bgm_source_mode(
                setting_value(task, shared_settings, "bgm_source_mode", "auto")
            )
            auto_bgm_path = (
                resolve_auto_bgm_path(workspace, task, shared_settings, reference_video)
                if bgm_source_mode not in {"none", "manual"}
                else None
            )
            auto_bgm_volume_percent = (
                estimate_reference_matched_bgm_volume_percent(
                    workspace,
                    task,
                    shared_settings,
                    reference_video,
                    auto_bgm_path,
                )
                if bgm_source_mode not in {"none", "manual"} and auto_bgm_path is not None
                else None
            )
            for key in AUTO_CLIP_SETTINGS_KEYS:
                if key in task:
                    value = task[key]
                elif key in shared_settings:
                    value = shared_settings[key]
                else:
                    continue
                if key == "bgm_audio_path" and bgm_source_mode == "none":
                    value = ""
                elif key == "bgm_audio_path" and bgm_source_mode != "manual":
                    value = str(auto_bgm_path) if auto_bgm_path is not None else ""
                elif (
                    key == "bgm_volume_percent"
                    and bgm_source_mode not in {"none", "manual"}
                    and auto_bgm_volume_percent is not None
                ):
                    value = round(float(auto_bgm_volume_percent), 1)
                elif key in {"cover_image_path", "bgm_audio_path"}:
                    value = str(resolve_workspace_path(workspace.root, str(value or "").strip(), "")) if str(value or "").strip() else ""
                job_payload[key] = value
            if bgm_source_mode not in {"none", "manual"} and auto_bgm_volume_percent is not None:
                job_payload["bgm_volume_percent"] = round(float(auto_bgm_volume_percent), 1)

            job_file = temp_root / "jobs" / f"{title}.json"
            write_json(job_file, job_payload)

            log_file = temp_root / "logs" / f"{title}.log"
            command = [
                python_path,
                str(script_path),
                "--job-file",
                str(job_file),
                "--log-file",
                str(log_file),
            ]
            if task.get("ffmpeg"):
                command.extend(["--ffmpeg", str(resolve_workspace_path(workspace.root, task.get("ffmpeg"), ""))])
            if task.get("ffprobe"):
                command.extend(["--ffprobe", str(resolve_workspace_path(workspace.root, task.get("ffprobe"), ""))])
            if task.get("keep_temp"):
                command.append("--keep-temp")

            specs.append(TaskSpec("auto_clip", label, command, script_path.parent))
    return specs


def get_stage_limit(workspace: WorkspaceContext, stage_name: str, default: int) -> int:
    concurrency = workspace.config.get("concurrency") or {}
    value = concurrency.get(stage_name, default)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


def run_workspace(workspace: WorkspaceContext, semaphores: dict[str, threading.Semaphore]) -> dict[str, dict[str, int]]:
    ensure_workspace_directories(workspace.root)
    workspace.logger.info("workspace=%s config=%s", workspace.name, workspace.config_path)

    summaries: dict[str, dict[str, int]] = {}
    summaries["baidu_share"] = run_stage_specs(
        workspace,
        "baidu_share",
        build_baidu_specs(workspace),
        get_stage_limit(workspace, "baidu_share", 1),
        semaphores["baidu_share"],
    )
    summaries["douyin_download"] = run_stage_specs(
        workspace,
        "douyin_download",
        build_douyin_specs(workspace),
        get_stage_limit(workspace, "douyin_download", 3),
        semaphores["douyin_download"],
    )
    summaries["subtitle_extract"] = run_stage_specs(
        workspace,
        "subtitle_extract",
        build_subtitle_specs(workspace),
        get_stage_limit(workspace, "subtitle_extract", 1),
        semaphores["subtitle_extract"],
    )
    summaries["visual_subtitle_extract"] = run_stage_specs(
        workspace,
        "visual_subtitle_extract",
        build_visual_subtitle_specs(workspace),
        get_stage_limit(workspace, "visual_subtitle_extract", 1),
        semaphores["visual_subtitle_extract"],
    )
    pending_baidu_targets = collect_incomplete_baidu_targets(workspace)
    if pending_baidu_targets and (workspace.config.get("auto_clip") or []):
        preview = ", ".join(pending_baidu_targets[:6])
        if len(pending_baidu_targets) > 6:
            preview = f"{preview} ..."
        workspace.logger.warning(
            "百度原素材还没有全部就绪，已暂停 auto_clip 阶段。待这些文件完成后重新运行工作间: %s",
            preview,
        )
        summaries["auto_clip"] = {"total": 0, "success": 0, "failed": 0, "skipped": 0}
    else:
        summaries["auto_clip"] = run_stage_specs(
            workspace,
            "auto_clip",
            build_auto_clip_specs(workspace),
            get_stage_limit(workspace, "auto_clip", 1),
            semaphores["auto_clip"],
        )
    return summaries


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config_paths = discover_config_paths(args)
    if not config_paths:
        print(f"No workspace task files were found under {WORKSPACE_ROOT}")
        return 1

    workspaces = [load_workspace(path) for path in config_paths]
    semaphores = {
        "baidu_share": threading.Semaphore(max(1, args.global_baidu_share)),
        "douyin_download": threading.Semaphore(max(1, args.global_douyin_download)),
        "subtitle_extract": threading.Semaphore(max(1, args.global_subtitle_extract)),
        "visual_subtitle_extract": threading.Semaphore(max(1, args.global_visual_subtitle_extract)),
        "auto_clip": threading.Semaphore(max(1, args.global_auto_clip)),
    }

    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workspace_parallel)) as executor:
        future_map = {
            executor.submit(run_workspace, workspace, semaphores): workspace
            for workspace in workspaces
        }
        for future in concurrent.futures.as_completed(future_map):
            workspace = future_map[future]
            try:
                summaries = future.result()
            except Exception as exc:
                failures += 1
                workspace.logger.exception("workspace failed: %s", exc)
                continue

            failed_tasks = sum(stage["failed"] for stage in summaries.values())
            if failed_tasks:
                failures += 1
            workspace.logger.info("workspace completed with summaries=%s", summaries)

    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
