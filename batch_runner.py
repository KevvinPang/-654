from __future__ import annotations

import argparse
import concurrent.futures
import glob
import json
import logging
import os
import re
import subprocess
import sys
import threading
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
    "enable_random_episode_flip",
    "random_episode_flip_ratio",
    "enable_random_visual_filter",
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


def collect_incomplete_baidu_targets(workspace: WorkspaceContext) -> list[str]:
    pending: list[str] = []
    for index, raw_task in enumerate(workspace.config.get("baidu_share", []), start=1):
        task = dict(raw_task or {})
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
    if not tasks:
        return None
    return resolve_workspace_path(workspace.root, tasks[0].get("output_subdir"), "subtitles")


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
    script_path = MODULES_ROOT / "subtitle_batch_runner.py"
    python_path = resolve_python([
        MODULES_ROOT / "subtitle_extractor_source" / "video-subtitle-extractor-main" / ".venv" / "Scripts" / "python.exe",
    ])

    specs: list[TaskSpec] = []
    for index, task in enumerate(workspace.config.get("subtitle_extract", []), start=1):
        task = dict(task)
        subtitle_area = task.get("subtitle_area")
        has_manual_area = isinstance(subtitle_area, list) and len(subtitle_area) == 4
        auto_detect = bool(task.get("auto_detect_subtitle_area", not has_manual_area))
        if not has_manual_area and not auto_detect:
            specs.append(TaskSpec("subtitle_extract", f"subtitle#{index}", None, PROJECT_ROOT, "missing subtitle_area"))
            continue

        if not task.get("input_path") and not task.get("input_paths"):
            input_glob = str(task.get("input_glob") or "").strip()
            if input_glob in {"", "downloads/douyin/*"}:
                douyin_output_dir = infer_first_douyin_output_dir(workspace)
                if douyin_output_dir is not None:
                    task["input_glob"] = str(douyin_output_dir / "*")

        input_paths = expand_subtitle_inputs(task, workspace.root)
        if not input_paths:
            specs.append(TaskSpec("subtitle_extract", f"subtitle#{index}", None, PROJECT_ROOT, "no input videos matched"))
            continue

        output_dir = resolve_workspace_path(workspace.root, task.get("output_subdir"), "subtitles")
        temp_root = resolve_workspace_path(workspace.root, task.get("temp_subdir"), "temp/subtitle")
        output_dir.mkdir(parents=True, exist_ok=True)
        temp_root.mkdir(parents=True, exist_ok=True)

        for video_path in input_paths:
            output_name = task.get("output_name") or safe_output_name_from_path(video_path, workspace.root)
            output_path = output_dir / output_name
            skip_existing = bool(task.get("skip_existing", True))

            label = f"subtitle#{index}:{video_path.name}"
            if output_path.exists() and skip_existing:
                specs.append(TaskSpec("subtitle_extract", label, None, PROJECT_ROOT, f"output already exists: {output_path}"))
                continue

            temp_name = Path(output_name).with_suffix("").name
            command = [
                python_path,
                str(script_path),
                "--input",
                str(video_path),
                "--output-dir",
                str(output_dir),
                "--output-name",
                output_name,
                "--language",
                str(task.get("language", "ch")),
                "--mode",
                str(task.get("mode", "accurate")),
                "--extract-frequency",
                str(task.get("extract_frequency", 5)),
                "--probe-extract-frequency",
                str(task.get("probe_extract_frequency", task.get("extract_frequency", 5))),
                "--temp-root",
                str(temp_root),
                "--temp-name",
                temp_name,
            ]
            if has_manual_area:
                command.extend(["--subtitle-area", ",".join(str(value) for value in subtitle_area)])
            elif auto_detect:
                command.append("--auto-subtitle-area")
            if task.get("generate_txt"):
                command.append("--generate-txt")
            if task.get("keep_temp"):
                command.append("--keep-temp")
            if task.get("overwrite"):
                command.append("--overwrite")

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

        reference_subtitle_glob = str(task.get("reference_subtitle_glob") or "").strip()
        if not task.get("reference_subtitle") and reference_subtitle_glob in {"", "subtitles/*.srt"}:
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

        reference_subtitles = resolve_workspace_inputs(
            workspace.root,
            task.get("reference_subtitle"),
            task.get("reference_subtitle_glob"),
        )
        reference_subtitles = filter_paths_by_suffix(reference_subtitles, SUPPORTED_SUBTITLE_EXTENSIONS)
        if not reference_subtitles:
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
                subtitle_path = find_matching_subtitle(reference_subtitles, used_subtitles, video_path, workspace.root)
                if subtitle_path is not None:
                    used_subtitles.add(subtitle_path)
                pairs.append((video_path, subtitle_path))
        else:
            pairs = [(reference_videos[0], reference_subtitles[0])]

        total_pairs = len(pairs)
        skip_existing = bool(task.get("skip_existing", True))
        for pair_index, (reference_video, reference_subtitle) in enumerate(pairs, start=1):
            if reference_subtitle is None:
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
                "reference_subtitle": str(reference_subtitle),
                "source_dir": str(source_dir),
                "output_dir": str(output_dir),
                "title": title,
            }
            for key in AUTO_CLIP_SETTINGS_KEYS:
                if key in task:
                    job_payload[key] = task[key]
                elif key in shared_settings:
                    job_payload[key] = shared_settings[key]

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
