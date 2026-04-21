from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from drama_clone_core import (  # noqa: E402
    DEFAULT_FFMPEG,
    DEFAULT_FFPROBE,
    VideoProcessor,
    run_funasr_reference_transcription,
    write_srt,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate audio-first SRT subtitles from a reference video with FunASR.")
    parser.add_argument("--reference-video", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ffmpeg", type=Path, default=DEFAULT_FFMPEG)
    parser.add_argument("--ffprobe", type=Path, default=DEFAULT_FFPROBE)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.reference_video.exists():
        raise SystemExit(f"Reference video not found: {args.reference_video}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    video_processor = VideoProcessor(args.ffmpeg, args.ffprobe)
    entries = run_funasr_reference_transcription(
        args.reference_video,
        video_processor,
        log_func=lambda message: print(message, flush=True),
    )
    if not entries:
        raise SystemExit("FunASR did not produce any subtitle entries.")
    write_srt(args.output, entries)
    print(f"FunASR audio subtitles written: {args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
