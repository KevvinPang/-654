from __future__ import annotations

import argparse
import faulthandler
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from drama_clone_core import (
    CloneSettings,
    DEFAULT_ENABLE_RANDOM_VISUAL_FILTER,
    DEFAULT_ENABLE_RANDOM_EPISODE_FLIP,
    DEFAULT_FFMPEG,
    DEFAULT_FFPROBE,
    DEFAULT_RANDOM_EPISODE_FLIP_RATIO,
    DEFAULT_TTS_RATE,
    DEFAULT_TTS_VOICE,
    load_text_file,
    normalize_episode_flip_ratio,
    normalize_percent_value,
    parse_subtitle_content,
    run_clone_pipeline,
    sanitize_stem,
)


def _load_job_file(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except OSError as exc:
        raise SystemExit(f"Unable to read job file: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid job file JSON: {exc}") from exc


def _job_path(job: dict, key: str) -> Path | None:
    raw = job.get(key)
    if not raw:
        return None
    return Path(raw)


def _discover_log_file(argv: list[str]) -> Path | None:
    for idx, arg in enumerate(argv):
        if arg == "--log-file" and idx + 1 < len(argv):
            return Path(argv[idx + 1])
        if arg.startswith("--log-file="):
            return Path(arg.split("=", 1)[1])
    return None


def _emit_message(message: str, log_file: Path | None, *, stream=None) -> None:
    text = message or ""
    payload = text if text.endswith("\n") else text + "\n"
    target = stream or sys.stdout
    target.write(payload)
    target.flush()
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("a", encoding="utf-8-sig") as handle:
            handle.write(payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild a clean drama clip from reference frames and generate narration output."
    )
    parser.add_argument("--job-file", type=Path)
    parser.add_argument("--reference-video", type=Path)
    parser.add_argument("--reference-subtitle", type=Path)
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--title")
    parser.add_argument("--date")
    parser.add_argument("--ai-api-key")
    parser.add_argument("--ai-api-url")
    parser.add_argument("--ai-model")
    parser.add_argument("--tts-voice")
    parser.add_argument("--tts-rate")
    parser.add_argument("--enable-backup-tts", dest="enable_backup_tts", action="store_true", default=None)
    parser.add_argument("--disable-backup-tts", dest="enable_backup_tts", action="store_false")
    parser.add_argument("--azure-tts-key")
    parser.add_argument("--azure-tts-region")
    parser.add_argument("--azure-tts-voice")
    parser.add_argument("--prefer-funasr-audio-subtitles", dest="prefer_funasr_audio_subtitles", action="store_true", default=None)
    parser.add_argument("--disable-funasr-audio-subtitles", dest="prefer_funasr_audio_subtitles", action="store_false")
    parser.add_argument("--disable-ai-subtitle-review", dest="disable_ai_subtitle_review", action="store_true", default=None)
    parser.add_argument("--enable-ai-subtitle-review", dest="disable_ai_subtitle_review", action="store_false")
    parser.add_argument("--disable-ai-narration-rewrite", dest="disable_ai_narration_rewrite", action="store_true", default=None)
    parser.add_argument("--enable-ai-narration-rewrite", dest="disable_ai_narration_rewrite", action="store_false")
    parser.add_argument("--prefer-funasr-sentence-pauses", dest="prefer_funasr_sentence_pauses", action="store_true", default=None)
    parser.add_argument("--disable-funasr-sentence-pauses", dest="prefer_funasr_sentence_pauses", action="store_false")
    parser.add_argument("--force-no-narration-mode", dest="force_no_narration_mode", action="store_true", default=None)
    parser.add_argument("--disable-force-no-narration-mode", dest="force_no_narration_mode", action="store_false")
    parser.add_argument("--narration-background-percent", type=float)
    parser.add_argument("--random-flip-episodes", dest="enable_random_episode_flip", action="store_true", default=None)
    parser.add_argument("--disable-random-flip-episodes", dest="enable_random_episode_flip", action="store_false")
    parser.add_argument("--random-flip-ratio", type=float)
    parser.add_argument("--random-visual-filter", dest="enable_random_visual_filter", action="store_true", default=None)
    parser.add_argument("--disable-random-visual-filter", dest="enable_random_visual_filter", action="store_false")
    parser.add_argument("--ffmpeg", type=Path, default=DEFAULT_FFMPEG)
    parser.add_argument("--ffprobe", type=Path, default=DEFAULT_FFPROBE)
    parser.add_argument("--log-file", type=Path)
    parser.add_argument("--keep-temp", action="store_true")
    args = parser.parse_args()

    job = _load_job_file(args.job_file) if args.job_file else {}
    args.reference_video = args.reference_video or _job_path(job, "reference_video")
    args.reference_subtitle = args.reference_subtitle or _job_path(job, "reference_subtitle")
    args.source_dir = args.source_dir or _job_path(job, "source_dir")
    args.output_dir = args.output_dir or _job_path(job, "output_dir")
    args.title = args.title or job.get("title") or "output"
    args.date = args.date or job.get("date") or datetime.now().strftime("%Y%m%d")
    args.ai_api_key = args.ai_api_key or job.get("ai_api_key", "")
    args.ai_api_url = args.ai_api_url or job.get("ai_api_url", "")
    args.ai_model = args.ai_model or job.get("ai_model") or "qwen-plus"
    raw_fallback_models = job.get("ai_fallback_models") or []
    args.ai_fallback_models = raw_fallback_models if isinstance(raw_fallback_models, list) else []
    args.tts_voice = args.tts_voice or job.get("tts_voice") or DEFAULT_TTS_VOICE
    args.tts_rate = args.tts_rate or job.get("tts_rate") or DEFAULT_TTS_RATE
    if args.enable_backup_tts is None:
        args.enable_backup_tts = bool(job.get("enable_backup_tts", False))
    args.azure_tts_key = args.azure_tts_key or job.get("azure_tts_key", "")
    args.azure_tts_region = args.azure_tts_region or job.get("azure_tts_region", "")
    args.azure_tts_voice = args.azure_tts_voice or job.get("azure_tts_voice", "")
    if args.prefer_funasr_audio_subtitles is None:
        args.prefer_funasr_audio_subtitles = bool(job.get("prefer_funasr_audio_subtitles", False))
    if args.disable_ai_subtitle_review is None:
        args.disable_ai_subtitle_review = bool(job.get("disable_ai_subtitle_review", False))
    if args.disable_ai_narration_rewrite is None:
        args.disable_ai_narration_rewrite = bool(job.get("disable_ai_narration_rewrite", False))
    if args.prefer_funasr_sentence_pauses is None:
        args.prefer_funasr_sentence_pauses = bool(job.get("prefer_funasr_sentence_pauses", False))
    if args.force_no_narration_mode is None:
        args.force_no_narration_mode = bool(job.get("force_no_narration_mode", False))
    args.narration_background_percent = normalize_percent_value(
        args.narration_background_percent
        if args.narration_background_percent is not None
        else job.get("narration_background_percent", 15.0),
        15.0,
    )
    if args.enable_random_episode_flip is None:
        args.enable_random_episode_flip = bool(
            job.get("enable_random_episode_flip", DEFAULT_ENABLE_RANDOM_EPISODE_FLIP)
        )
    if args.enable_random_visual_filter is None:
        args.enable_random_visual_filter = bool(
            job.get("enable_random_visual_filter", DEFAULT_ENABLE_RANDOM_VISUAL_FILTER)
        )
    args.random_flip_ratio = normalize_episode_flip_ratio(
        args.random_flip_ratio
        if args.random_flip_ratio is not None
        else job.get("random_episode_flip_ratio", DEFAULT_RANDOM_EPISODE_FLIP_RATIO)
    )

    required = {
        "reference_video": "--reference-video",
        "source_dir": "--source-dir",
        "output_dir": "--output-dir",
    }
    if not bool(args.prefer_funasr_audio_subtitles) and not bool(args.force_no_narration_mode):
        required["reference_subtitle"] = "--reference-subtitle"
    missing = [flag for attr, flag in required.items() if getattr(args, attr) is None]
    if missing:
        parser.error("missing required inputs: " + ", ".join(missing))

    return args


def main() -> None:
    args = parse_args()
    log_file = args.log_file

    def emit(message: str) -> None:
        _emit_message(message, log_file)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("", encoding="utf-8-sig")
        with log_file.open("a", encoding="utf-8-sig") as handle:
            faulthandler.enable(handle)

    subtitle_entries = []
    if args.reference_subtitle is not None:
        subtitle_content = load_text_file(args.reference_subtitle)
        subtitle_entries = parse_subtitle_content(subtitle_content, args.reference_subtitle.suffix)
        if not subtitle_entries and not bool(args.prefer_funasr_audio_subtitles):
            raise SystemExit("No subtitle entries were parsed from the reference subtitle file.")

    settings = CloneSettings(
        reference_video=args.reference_video,
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        subtitle_entries=subtitle_entries,
        output_stem=sanitize_stem(args.title),
        ai_api_key=args.ai_api_key,
        ai_model=args.ai_model,
        ai_api_url=args.ai_api_url,
        ai_fallback_models=args.ai_fallback_models,
        tts_voice=args.tts_voice,
        tts_rate=args.tts_rate,
        enable_backup_tts=bool(args.enable_backup_tts),
        azure_tts_key=args.azure_tts_key or "",
        azure_tts_region=args.azure_tts_region or "",
        azure_tts_voice=args.azure_tts_voice or "",
        prefer_funasr_audio_subtitles=bool(args.prefer_funasr_audio_subtitles),
        disable_ai_subtitle_review=bool(args.disable_ai_subtitle_review),
        disable_ai_narration_rewrite=bool(args.disable_ai_narration_rewrite),
        prefer_funasr_sentence_pauses=bool(args.prefer_funasr_sentence_pauses),
        force_no_narration_mode=bool(args.force_no_narration_mode),
        narration_background_percent=args.narration_background_percent,
        enable_random_episode_flip=bool(args.enable_random_episode_flip),
        random_episode_flip_ratio=args.random_flip_ratio,
        enable_random_visual_filter=bool(args.enable_random_visual_filter),
        keep_temp=args.keep_temp,
    )
    result = run_clone_pipeline(
        settings,
        ffmpeg=args.ffmpeg,
        ffprobe=args.ffprobe,
        log_func=emit,
        progress_func=lambda value, text: emit(f"[{value:>5.1f}%] {text}"),
    )
    emit(f"Output video: {result.video_path}")


def _main_entry() -> int:
    try:
        main()
    except SystemExit as exc:
        code = exc.code
        if code in (None, 0):
            return 0
        detail = code if isinstance(code, str) else ""
        if detail:
            _emit_message(detail, _discover_log_file(sys.argv[1:]), stream=sys.stderr)
        return int(code) if isinstance(code, int) else 1
    except KeyboardInterrupt:
        raise
    except Exception:
        detail = traceback.format_exc().rstrip()
        _emit_message(detail, _discover_log_file(sys.argv[1:]), stream=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main_entry())
