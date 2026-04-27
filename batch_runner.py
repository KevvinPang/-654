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
import urllib.error
import urllib.parse
import urllib.request
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
    "bgm_online_provider",
    "bgm_jamendo_client_id",
    "bgm_external_dirs",
    "bgm_volume_percent",
)
BGM_SOURCE_MODES = {"none", "manual", "local_auto", "online_auto", "auto"}
BGM_ONLINE_PROVIDERS = {"jamendo"}
BGM_JAMENDO_API_URL = "https://api.jamendo.com/v3.0/tracks/"
BGM_REFERENCE_ANALYSIS_SECONDS = 45.0
BGM_REFERENCE_ANALYSIS_SAMPLE_RATE = 16000
BGM_REFERENCE_ANALYSIS_MAX_BYTES = int(BGM_REFERENCE_ANALYSIS_SECONDS * BGM_REFERENCE_ANALYSIS_SAMPLE_RATE * 2)
BGM_ONLINE_CANDIDATE_LIMIT = 4
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
    return "api"


def should_skip_existing_baidu_output(expected_output: Path, task: dict[str, Any]) -> bool:
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

    raw_target_size = task.get("target_size")
    try:
        target_size = int(raw_target_size or 0)
    except (TypeError, ValueError):
        target_size = 0
    if target_size > 0 and current_size != target_size:
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
        if expected_output is None or not should_skip_existing_baidu_output(expected_output, task):
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
        return "online_auto"
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


def decode_bgm_audio_samples(media_path: Path, ffmpeg_path: Path) -> list[int]:
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
        f"{BGM_REFERENCE_ANALYSIS_SECONDS:.1f}",
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
    raw_audio = result.stdout[:BGM_REFERENCE_ANALYSIS_MAX_BYTES]
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
) -> tuple[str, dict[str, float], Path | None]:
    manual_query = str(setting_value(task, shared_settings, "bgm_search_query", "") or "").strip()
    reference_query = ""
    reference_features: dict[str, float] = {}
    ffmpeg_path = resolve_bgm_ffmpeg_path(workspace, task, shared_settings)
    if ffmpeg_path is None:
        workspace.logger.warning("[auto_bgm] ffmpeg not found; cannot analyze reference BGM similarity")
    else:
        try:
            reference_features = load_bgm_audio_features(reference_video, ffmpeg_path)
        except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
            workspace.logger.warning("[auto_bgm] reference BGM similarity analysis failed: %s", exc)
        if reference_features:
            reference_query = reference_audio_style_query(reference_features)
            workspace.logger.info(
                "[auto_bgm] reference BGM profile query='%s' energy=%.3f dynamic=%.2f zcr=%.3f onset=%.2f/s",
                reference_query,
                reference_features.get("mean_rms", 0.0),
                reference_features.get("dynamic_ratio", 0.0),
                reference_features.get("mean_zcr", 0.0),
                reference_features.get("onset_density", 0.0),
            )
        else:
            workspace.logger.warning("[auto_bgm] reference BGM profile unavailable; falling back to text query")

    if reference_query and manual_query:
        return f"{reference_query} {manual_query}", reference_features, ffmpeg_path
    if reference_query:
        return reference_query, reference_features, ffmpeg_path
    return infer_bgm_search_query(workspace, reference_video, manual_query), reference_features, ffmpeg_path


def build_bgm_search_query(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    reference_video: Path,
) -> str:
    query, _reference_features, _ffmpeg_path = build_bgm_similarity_context(
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


def collect_bgm_files_from_dir(directory: Path) -> list[Path]:
    if not directory.exists() or not directory.is_dir():
        return []
    return [
        path.resolve()
        for path in directory.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_AUDIO_EXTENSIONS and path.stat().st_size > 0
    ]


def collect_workspace_bgm_files(workspace: WorkspaceContext, external_dirs: list[str] | None = None) -> list[Path]:
    candidates = collect_bgm_files_from_dir(workspace.root / "bgm")
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


def score_bgm_audio_candidate(
    candidate_path: Path,
    reference_features: dict[str, float],
    ffmpeg_path: Path | None,
) -> tuple[float, dict[str, float]]:
    if not reference_features or ffmpeg_path is None:
        return 0.0, {}
    try:
        candidate_features = load_bgm_audio_features(candidate_path, ffmpeg_path)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return 0.0, {}
    return bgm_similarity_score(reference_features, candidate_features), candidate_features


def choose_bgm_path_by_audio_similarity(
    paths: list[Path],
    reference_features: dict[str, float],
    ffmpeg_path: Path | None,
    workspace: WorkspaceContext,
    label: str,
) -> Path | None:
    if not paths:
        return None
    if not reference_features or ffmpeg_path is None:
        return paths[0]
    scored: list[tuple[float, Path]] = []
    for path in paths:
        audio_score, _candidate_features = score_bgm_audio_candidate(path, reference_features, ffmpeg_path)
        scored.append((audio_score, path))
    best_score, best_path = max(
        scored,
        key=lambda item: (item[0], item[1].stat().st_size, str(item[1]).lower()),
    )
    if best_score > 0.0:
        workspace.logger.info(
            "[auto_bgm] %s audio similarity selected %.1f/100: %s",
            label,
            best_score,
            best_path,
        )
    return best_path


def choose_local_bgm(
    workspace: WorkspaceContext,
    reference_video: Path,
    query: str,
    reference_features: dict[str, float] | None = None,
    ffmpeg_path: Path | None = None,
    external_dirs: list[str] | None = None,
) -> Path | None:
    candidates = collect_workspace_bgm_files(workspace, external_dirs=external_dirs)
    if not candidates:
        return None
    scored: list[tuple[float, float, Path]] = []
    for path in candidates:
        text_score = score_local_bgm_candidate(path, query, workspace, reference_video)
        audio_score, _candidate_features = score_bgm_audio_candidate(
            path,
            reference_features or {},
            ffmpeg_path,
        )
        scored.append((audio_score, text_score, path))
    best_audio_score, best_text_score, best_path = max(
        scored,
        key=lambda item: (item[0], item[1], item[2].stat().st_size, str(item[2]).lower()),
    )
    if best_audio_score > 0.0:
        workspace.logger.info(
            "[auto_bgm] local BGM audio similarity %.1f/100, text score %.1f: %s",
            best_audio_score,
            best_text_score,
            best_path,
        )
    return best_path


def read_url_json(url: str, timeout: float = 18.0) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "server-auto-clip/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
    return json.loads(raw.decode("utf-8", errors="replace"))


def download_url_to_file(url: str, output_path: Path, timeout: float = 45.0) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "server-auto-clip/1.0"})
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        with temp_path.open("wb") as file_obj:
            shutil.copyfileobj(response, file_obj)
    if temp_path.stat().st_size <= 0:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError("downloaded BGM is empty")
    temp_path.replace(output_path)


def download_jamendo_bgm(
    workspace: WorkspaceContext,
    reference_video: Path,
    query: str,
    client_id: str,
    reference_features: dict[str, float] | None = None,
    ffmpeg_path: Path | None = None,
) -> Path | None:
    client_id = str(client_id or "").strip() or os.environ.get("JAMENDO_CLIENT_ID", "").strip()
    if not client_id:
        workspace.logger.warning("[auto_bgm] Jamendo client_id is empty; skip online BGM search")
        return None

    query_hash = hashlib.sha1(f"jamendo|{query}".encode("utf-8")).hexdigest()[:10]
    online_dir = workspace.root / "bgm" / "online"
    cached_matches = sorted(online_dir.glob(f"jamendo_{query_hash}_*"))
    cached_audio = [path for path in cached_matches if path.suffix.lower() in SUPPORTED_AUDIO_EXTENSIONS and path.is_file()]
    def select_cached_audio(reason: str) -> Path | None:
        selected_cached = choose_bgm_path_by_audio_similarity(
            cached_audio,
            reference_features or {},
            ffmpeg_path,
            workspace,
            "cached Jamendo BGM",
        )
        if selected_cached is not None:
            workspace.logger.info("[auto_bgm] using cached Jamendo BGM%s: %s", reason, selected_cached)
            return selected_cached.resolve()
        return None

    if len(cached_audio) >= BGM_ONLINE_CANDIDATE_LIMIT:
        selected_cached = select_cached_audio("")
        if selected_cached is not None:
            return selected_cached

    results: list[Any] = []
    for tags in ("instrumental", ""):
        params = {
            "client_id": client_id,
            "format": "json",
            "limit": "8",
            "include": "musicinfo",
            "audioformat": "mp31",
            "search": query,
            "order": "popularity_total",
        }
        if tags:
            params["tags"] = tags
        url = f"{BGM_JAMENDO_API_URL}?{urllib.parse.urlencode(params)}"
        try:
            payload = read_url_json(url)
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            workspace.logger.warning("[auto_bgm] Jamendo search failed: %s", exc)
            if cached_audio:
                return select_cached_audio(" after search failure")
            return None
        headers = payload.get("headers") if isinstance(payload, dict) else None
        if isinstance(headers, dict) and str(headers.get("status") or "").lower() == "failed":
            workspace.logger.warning(
                "[auto_bgm] Jamendo API failed code=%s: %s",
                headers.get("code", ""),
                headers.get("error_message", "") or "unknown API error",
            )
            if cached_audio:
                return select_cached_audio(" after API failure")
            return None
        raw_results = payload.get("results") if isinstance(payload, dict) else None
        if isinstance(raw_results, list) and raw_results:
            results = raw_results
            break
    if not results:
        workspace.logger.warning("[auto_bgm] Jamendo returned no tracks for query: %s", query)
        if cached_audio:
            return select_cached_audio(" after empty search result")
        return None

    downloaded_candidates: list[Path] = [path.resolve() for path in cached_audio]
    for item in results[:BGM_ONLINE_CANDIDATE_LIMIT]:
        if not isinstance(item, dict):
            continue
        if item.get("audiodownload_allowed") is False or str(item.get("audiodownload_allowed")).lower() == "false":
            workspace.logger.info(
                "[auto_bgm] skip Jamendo track because download is not allowed: %s - %s",
                item.get("artist_name") or "unknown artist",
                item.get("name") or "unknown track",
            )
            continue
        audio_url = str(item.get("audiodownload") or item.get("audio") or "").strip()
        if not audio_url:
            continue
        track_id = sanitize_bgm_filename_part(str(item.get("id") or "track"), "track")
        track_name = sanitize_bgm_filename_part(str(item.get("name") or "jamendo"), "jamendo")
        output_path = online_dir / f"jamendo_{query_hash}_{track_id}_{track_name}.mp3"
        metadata_path = output_path.with_suffix(".json")
        if not output_path.exists() or output_path.stat().st_size <= 0:
            try:
                download_url_to_file(audio_url, output_path)
            except (OSError, urllib.error.URLError, RuntimeError) as exc:
                workspace.logger.warning("[auto_bgm] Jamendo track download failed (%s): %s", track_name, exc)
                continue
        metadata = {
            "provider": "jamendo",
            "query": query,
            "reference_video": str(reference_video),
            "id": item.get("id"),
            "name": item.get("name"),
            "artist_name": item.get("artist_name"),
            "license_ccurl": item.get("license_ccurl"),
            "source_url": item.get("shareurl"),
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        resolved_output_path = output_path.resolve()
        if resolved_output_path not in downloaded_candidates:
            downloaded_candidates.append(resolved_output_path)
        workspace.logger.info(
            "[auto_bgm] prepared Jamendo candidate: %s - %s",
            item.get("artist_name") or "unknown artist",
            item.get("name") or output_path.name,
        )

    selected = choose_bgm_path_by_audio_similarity(
        downloaded_candidates,
        reference_features or {},
        ffmpeg_path,
        workspace,
        "Jamendo BGM",
    )
    return selected.resolve() if selected is not None else None


def resolve_auto_bgm_path(
    workspace: WorkspaceContext,
    task: dict[str, Any],
    shared_settings: dict[str, Any],
    reference_video: Path,
) -> Path | None:
    mode = normalize_bgm_source_mode(setting_value(task, shared_settings, "bgm_source_mode", "auto"))
    if mode in {"none", "manual"}:
        return None

    query, reference_features, ffmpeg_path = build_bgm_similarity_context(
        workspace,
        task,
        shared_settings,
        reference_video,
    )
    provider = str(setting_value(task, shared_settings, "bgm_online_provider", "jamendo") or "jamendo").strip().lower()
    if provider not in BGM_ONLINE_PROVIDERS:
        workspace.logger.warning("[auto_bgm] unsupported online provider '%s'; using local BGM fallback", provider)
        provider = "jamendo"

    if mode in {"online_auto", "auto"}:
        online_path = download_jamendo_bgm(
            workspace,
            reference_video,
            query,
            str(setting_value(task, shared_settings, "bgm_jamendo_client_id", "") or ""),
            reference_features=reference_features,
            ffmpeg_path=ffmpeg_path,
        )
        if online_path is not None:
            return online_path
        if mode == "online_auto":
            workspace.logger.warning("[auto_bgm] online BGM unavailable; this clip will continue without auto BGM")
            return None

    local_path = choose_local_bgm(
        workspace,
        reference_video,
        query,
        reference_features=reference_features,
        ffmpeg_path=ffmpeg_path,
        external_dirs=normalize_bgm_external_dirs(setting_value(task, shared_settings, "bgm_external_dirs", [])),
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
        if expected_output is not None and skip_existing and should_skip_existing_baidu_output(expected_output, task):
            specs.append(
                TaskSpec(
                    "baidu_share",
                    f"baidu#{index}:{target_label}",
                    None,
                    PROJECT_ROOT,
                    f"target already exists: {expected_output}",
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
            raw_target_size = task.get("target_size")
            try:
                target_size = int(raw_target_size or 0)
            except (TypeError, ValueError):
                target_size = 0
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
            subtitle_output_dir = infer_first_subtitle_output_dir(workspace)
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
            subtitle_output_dir = infer_first_subtitle_output_dir(workspace)
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
                elif key in {"cover_image_path", "bgm_audio_path"}:
                    value = str(resolve_workspace_path(workspace.root, str(value or "").strip(), "")) if str(value or "").strip() else ""
                job_payload[key] = value

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
