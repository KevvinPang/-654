from __future__ import annotations

import asyncio
import difflib
import hashlib
import html
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import requests

try:
    from PIL import Image, ImageFilter, ImageOps

    PIL_AVAILABLE = True
except ImportError:
    Image = None
    ImageFilter = None
    ImageOps = None
    PIL_AVAILABLE = False

try:
    import numpy as np

    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

try:
    import cv2

    CV2_AVAILABLE = True
except ImportError:
    cv2 = None
    CV2_AVAILABLE = False

try:
    import edge_tts

    TTS_AVAILABLE = True
except ImportError:
    edge_tts = None
    TTS_AVAILABLE = False


FRAME_INTERVAL = 0.5
SOURCE_FRAME_CACHE_VERSION = "20260406_flip_v1"
AUDIT_SAMPLE_STEP = 4
AUDIT_LOW_SCORE = 0.18
LEGACY_DEFAULT_TTS_VOICE = "zh-CN-YunxiNeural"
DEFAULT_TTS_VOICE = "zh-CN-YunxiNeural"
DEFAULT_TTS_RATE = "+8%"
DEFAULT_TTS_VOLUME = "+0%"
DEFAULT_TTS_PITCH = "+0Hz"
DEFAULT_DUCK_VOLUME = 0.32
TTS_PROVIDER_EDGE = "edge"
TTS_PROVIDER_AZURE = "azure"
TTS_PROVIDER_CACHE = "cache"
AZURE_TTS_DEFAULT_OUTPUT_FORMAT = "audio-24khz-160kbitrate-mono-mp3"
AZURE_TTS_WAV_OUTPUT_FORMAT = "riff-24khz-16bit-mono-pcm"
AZURE_TTS_REQUEST_TIMEOUT_SECONDS = 90
MAX_SUBTITLE_CHARS = 18
MIN_SUBTITLE_CHARS = 6
TARGET_SUBTITLE_CPS = 4.8
TARGET_TTS_CPS = 4.6
ESTIMATED_TTS_CPS = 4.2
MIN_TTS_SPEED_FACTOR = 0.94
MAX_TTS_SPEED_FACTOR = 1.28
MAX_TTS_SYNTH_RATE_FACTOR = 1.75
MIN_TTS_RATE_BOOST_MULTIPLIER = 1.04
MAX_TTS_RESYNTH_PASSES = 3
MAX_TTS_TIMELINE_OVERFLOW_SECONDS = 0.45
MAX_TTS_GROUP_REFINEMENT_PASSES = 12
LOCAL_TTS_MICRO_SPEED_FACTOR = 1.10
MIN_AUDIO_STRETCH_SPEED = 0.88
NARRATION_REWRITE_SOFT_OVERFLOW_UNITS = 3
TTS_REQUEST_TIMEOUT_SECONDS = 45
MAX_TTS_SEGMENT_SPLIT_DEPTH = 3
TTS_MIN_VALID_DURATION_SECONDS = 0.18
TTS_MIN_VALID_DURATION_RATIO = 0.32
TTS_MIN_VALID_UNITS_FOR_DURATION_CHECK = 8
TTS_MIN_VALID_MAX_VOLUME_DB = -40.0
TTS_MIN_VALID_MEAN_VOLUME_DB = -46.0
TTS_GROUP_OVERFLOW_SPLIT_RATIO = 1.12
TTS_GROUP_OVERFLOW_SPLIT_MARGIN_SECONDS = 0.16
MIN_REFERENCE_GAP_SECONDS = 0.06
MAX_REFERENCE_GAP_SECONDS = 0.18
SHORT_GAP_MERGE_SECONDS = 1.6
SHORT_GAP_FRAGMENT_SECONDS = 1.25
MAX_MERGED_NARRATION_DURATION = 5.8
MAX_TTS_GROUP_DURATION = 6.6
MAX_TTS_GROUP_UNITS = 38
MAX_TTS_GROUP_ENTRIES = 4
SUBTITLE_MASK_SAMPLE_INTERVAL = 0.9
SUBTITLE_MASK_MIN_SAMPLES = 18
SUBTITLE_MASK_MAX_SAMPLES = 72
SUBTITLE_MASK_LEFT_RATIO = 0.05
SUBTITLE_MASK_RIGHT_RATIO = 0.95
SUBTITLE_MASK_TOP_RATIO = 0.64
SUBTITLE_MASK_BOTTOM_RATIO = 0.96
SUBTITLE_MASK_MIN_HEIGHT_RATIO = 0.032
SUBTITLE_MASK_MAX_HEIGHT_RATIO = 0.090
SUBTITLE_MASK_MIN_CONFIDENCE = 0.18
SUBTITLE_MASK_FALLBACK_SIGNAL = 0.09
SUBTITLE_MASK_DEFAULT_X_MARGIN_RATIO = 0.06
SUBTITLE_MASK_DEFAULT_TOP_RATIO = 0.84
SUBTITLE_MASK_DEFAULT_HEIGHT_RATIO = 0.085
SUBTITLE_MASK_COMPONENT_MIN_FRAMES = 5
SUBTITLE_MASK_COMPONENT_MIN_WIDTH_RATIO = 0.11
SUBTITLE_MASK_COMPONENT_MAX_HEIGHT_RATIO = 0.24
SUBTITLE_MASK_DYNAMIC_ALPHA = 0.99
SUBTITLE_MASK_BASE_ALPHA = 0.24
SUBTITLE_MASK_BOX_HOLD_FRAMES = 3
SUBTITLE_MASK_OUTPUT_MIN_HEIGHT_RATIO = 0.062
SUBTITLE_MASK_OUTPUT_MAX_HEIGHT_RATIO = 0.070
SUBTITLE_MASK_OUTPUT_TOP_PADDING_RATIO = 0.06
SUBTITLE_MASK_OUTPUT_BOTTOM_PADDING_RATIO = 0.08
DELIVERY_SUBTITLE_MIN_GAP_SECONDS = 0.02
TTS_UNDERFILLED_GROUP_WINDOW_SECONDS = 0.95
TTS_UNDERFILLED_GROUP_EXCESS_SECONDS = 0.16
SUBTITLE_BURN_FONT_NAME = "Microsoft YaHei"
SUBTITLE_BURN_MIN_FONT_SIZE = 36
SUBTITLE_BURN_MAX_FONT_SIZE = 78
SUBTITLE_MOSAIC_BLOCK_SIZE = 18
API_KEY_ENV = "DRAMA_CLONE_AI_API_KEY"
TIME_RE = re.compile(
    r"(?P<start>\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(?P<end>\d{2}:\d{2}:\d{2},\d{3})"
)
ASS_TIME_RE = re.compile(r"(?P<h>\d+):(?P<m>\d{2}):(?P<s>\d{2})[.](?P<cs>\d{2})")
CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def hidden_subprocess_kwargs() -> Dict[str, object]:
    if os.name != "nt":
        return {}

    kwargs: Dict[str, object] = {
        "creationflags": int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
    }
    if hasattr(subprocess, "STARTUPINFO"):
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= int(getattr(subprocess, "STARTF_USESHOWWINDOW", 0))
        startupinfo.wShowWindow = int(getattr(subprocess, "SW_HIDE", 0))
        kwargs["startupinfo"] = startupinfo
    return kwargs


def run_subprocess_hidden(*popenargs, **kwargs):
    merged_kwargs = dict(hidden_subprocess_kwargs())
    if "creationflags" in kwargs:
        merged_kwargs["creationflags"] = int(merged_kwargs.get("creationflags", 0)) | int(kwargs.pop("creationflags") or 0)
    merged_kwargs.update(kwargs)
    return subprocess.run(*popenargs, **merged_kwargs)
FRAGMENT_TAIL_RE = re.compile(
    r"(?:[\u7684\u5730\u5f97\u4e86\u7740\u8fc7\u628a\u5c06\u7ed9\u8ddf\u5411\u5728\u4e8e\u548c\u4e0e\u5e76\u53ca\u800c\u5374\u5c31\u53c8\u8fd8\u4ece\u5bf9\u88ab\u8ba9\u66ff\u6bd4\u5f80\u671d\u62ff\u5230\u4e3a\u540c\u5417\u5462\u554a\u5427\u5440\u5566])+$"
)
SPEECH_INTRO_TAIL_RE = re.compile(
    r"(?:问(?:她|他|道)?|告诉(?:她|他|众人)?|说道|答道|回道|喊道|怒道|直言|表示|承诺|解释|安慰(?:她|他)?|提醒(?:她|他)?|忙问|反问|脱口而出)$"
)
READABLE_BREAK_RE = re.compile(
    r"(此时|随后|接着|然后|紧接着|下一秒|谁知|没想到|结果|原来|可偏偏|可谁知|可|却|而|于是|所以|便|直到|听完|说完|话音刚落|转眼间|这时|很快)"
)
INCOMPLETE_TAIL_RE = re.compile(
    r"(?:发现|看到|听到|得知|知道|意识到|想起|决定|告诉|问道|答道|回道|说道|承诺|表示|不|随后|这时|此时|很快|下一秒|紧接着|谁知|没想到|原来|结果|于是|所以|就这么)$"
)
TERMINAL_SENTENCE_PUNCT_RE = re.compile(r"[。！？!?]$")
MINOR_SENTENCE_PUNCT_RE = re.compile(r"[，、；：,;:]$")
SOFT_CONTINUATION_RE = re.compile(
    r"^(?:也|又|还|就|才|却|并|并且|而且|再|仍|仍旧|更|还在|还会|便|乃|把|将|给|跟|向|同样|甚至|于是|就是)"
)
STRONG_TRANSITION_RE = re.compile(
    r"^(?:这时|此时|随后|紧接着|下一秒|很快|转眼间|话音刚落|说完|听完|谁知|没想到|原来|结果)"
)


DANGLING_TTS_TAIL_RE = re.compile(
    r"(?:\u7684|\u5730|\u5f97|\u5411|\u5bf9|\u8ddf|\u7ed9|\u66ff|\u5e2e|\u628a|\u88ab|\u5c06|\u4e0e|\u548c|\u5e76|\u800c|\u53ca|\u5728|\u4ece|\u5f80|\u671d|\u4e8e|\u6bd4|\u4e3a|\u8ba9|\u4ee4|\u4f7f|\u7531)$"
)
SHORT_NOUN_TTS_TAIL_RE = re.compile(
    r"(?:\u5bf9\u65b9|\u81ea\u5df1|\u7537\u4eba|\u5973\u4eba|\u5c0f\u4f19|\u5973\u5b69|\u7537\u5b69|\u5b69\u5b50|\u8001\u4eba|\u6bcd\u4eb2|\u7236\u4eb2|\u7237\u7237|\u5976\u5976|\u533b\u751f|\u62a4\u58eb|\u8001\u677f|\u53f8\u673a|\u52a9\u7406|\u79d8\u4e66|\u670b\u53cb|\u95fa\u871c|\u6d88\u606f|\u7535\u8bdd|\u624b\u673a|\u5730\u5740|\u540d\u5b57|\u8eab\u4efd|\u4e1c\u897f|\u793c\u7269|\u6587\u4ef6|\u7167\u7247)$"
)
SENTENCE_END_HINT_RE = re.compile(
    r"(?:\u4e86|\u5566|\u5462|\u554a|\u5440|\u5427|\u5417|\u561b|\u8fc7|\u7740|\u6210\u4e86|\u4f4f\u4e86|\u5f00\u4e86|\u5230\u4e86|\u8d77\u6765|\u4e0b\u53bb|\u56de\u6765|\u56de\u53bb|\u51fa\u6765|\u8fdb\u53bb|\u7ed3\u675f|\u6210\u529f|\u5931\u8d25|\u79bb\u5f00|\u56de\u5bb6)$"
)
NARRATION_SUBJECT_RE = re.compile(
    r"(?:\u5c0f\u4f19|\u7537\u4eba|\u5973\u4eba|\u59bb\u5b50|\u8001\u5a46|\u4e08\u592b|\u513f\u5b50|\u5b69\u5b50|\u5973\u5b69|\u7537\u5b69|\u7236\u6bcd|\u5bf9\u65b9|\u4e24\u4eba|\u4ed6\u4eec|\u5979\u4eec|\u4f17\u4eba|\u7ecf\u7eaa\u4eba|\u52a9\u7406|\u7ee7\u627f\u4eba)"
)
NARRATION_CONNECTOR_RE = re.compile(
    r"(?:\u968f\u5373|\u968f\u540e|\u7d27\u63a5\u7740|\u63a5\u7740|\u8fd9\u65f6|\u6b64\u65f6|\u4e0b\u4e00\u79d2|\u4e8e\u662f|\u7136\u800c|\u7ed3\u679c|\u539f\u6765|\u8c01\u77e5|\u54ea\u77e5|\u6b8a\u4e0d\u77e5|\u6ca1\u60f3\u5230|\u5c82\u6599|\u4e3a\u6b64|\u5f53\u521d|\u4e94\u5e74\u6765|\u901a\u8fc7|\u7ec8\u4e8e|\u672c\u60f3|\u751a\u81f3|\u76f4\u63a5|\u7acb\u523b|\u8f6c\u8eab|\u5c31|\u5374|\u53c8|\u624d)"
)
NARRATION_ACTION_RE = re.compile(
    r"(?:\u53d1\u73b0|\u8ba4\u4e3a|\u770b\u5230|\u770b\u89c1|\u542c\u5230|\u62ff\u8d77|\u6253\u5f00|\u7b54\u5e94|\u58f0\u79f0|\u5632\u8bbd|\u7ef4\u62a4|\u8d76\u8d70|\u9690\u7792|\u642c\u4e86|\u6253\u62fc|\u5475\u62a4|\u6367\u6210|\u8c08\u4e0b|\u544a\u77e5|\u8bc1\u660e|\u649e\u89c1|\u5a01\u80c1|\u51b3\u5b9a|\u7b56\u5212|\u6536\u5230|\u53d8\u6210|\u6362\u6210|\u62ff\u4e0b|\u627e\u5230|\u7b49\u5230|\u5e2e|\u8ba9|\u7ed9)"
)
IMPERATIVE_DIALOGUE_RE = re.compile(
    r"^(?:\u7acb\u523b|\u9a6c\u4e0a|\u8d76\u7d27|\u5feb\u70b9|\u7ed9\u6211|\u8ba9\u6211|\u51fa\u53bb|\u6eda|\u95ed\u5634|\u4f4f\u624b|\u7b7e\u5b57|\u89e3\u9664|\u79bb\u5f00)"
)
OCR_CONSISTENCY_PROTECTED_CHARS = frozenset(
    (
        "\u6211\u4f60\u60a8\u4ed6\u5979\u5b83\u8fd9\u90a3\u54ea\u8c01\u5565"
        "\u4e0d\u6ca1\u65e0\u6709\u662f\u4e86\u7740\u8fc7"
        "\u6765\u53bb\u4e0a\u4e0b\u8fdb\u51fa\u56de"
        "\u5927\u5c0f\u7537\u5973\u8001\u5c0f"
        "\u524d\u540e\u5de6\u53f3\u91cc\u5916"
        "\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341\u767e\u5343\u4e07\u4e24"
    )
)


def _find_media_binary(command_name: str, fallback_paths: Sequence[Path], env_keys: Sequence[str]) -> Path:
    candidates: List[Path] = []

    for env_key in env_keys:
        raw_value = os.environ.get(env_key, "").strip()
        if raw_value:
            candidates.append(Path(raw_value).expanduser())

    if os.name == "nt":
        local_appdata = os.environ.get("LOCALAPPDATA", "").strip()
        if local_appdata:
            winget_links = Path(local_appdata) / "Microsoft" / "WinGet" / "Links" / f"{command_name}.exe"
            candidates.append(winget_links)

            winget_packages = Path(local_appdata) / "Microsoft" / "WinGet" / "Packages"
            if winget_packages.is_dir():
                for package_pattern in ("Gyan.FFmpeg*", "BtbN.FFmpeg*", "yt-dlp.FFmpeg*"):
                    for package_dir in sorted(winget_packages.glob(package_pattern)):
                        candidates.extend(sorted(package_dir.rglob(f"{command_name}.exe")))

    resolved = shutil.which(command_name)
    if resolved:
        candidates.append(Path(resolved))

    candidates.extend(fallback_paths)

    seen: set[Path] = set()
    for candidate in candidates:
        normalized = candidate.expanduser()
        if normalized in seen:
            continue
        seen.add(normalized)
        if normalized.exists():
            return normalized.resolve()

    return Path(command_name)


def _find_ffmpeg() -> Path:
    return _find_media_binary(
        "ffmpeg",
        [
            Path(r"D:\NarratoAI_v0.7\lib\ffmpeg\ffmpeg-7.0-essentials_build\ffmpeg.exe"),
            Path(r"D:\FFmpeg\bin\ffmpeg.exe"),
            Path(r"C:\FFmpeg\bin\ffmpeg.exe"),
        ],
        ("SERVER_AUTO_CLIP_FFMPEG", "FFMPEG_PATH"),
    )


def _find_ffprobe() -> Path:
    return _find_media_binary(
        "ffprobe",
        [
            Path(r"D:\NarratoAI_v0.7\lib\ffmpeg\ffprobe.exe"),
            Path(r"D:\NarratoAI_v0.7\lib\ffmpeg\ffmpeg-7.0-essentials_build\ffprobe.exe"),
            Path(r"D:\FFmpeg\bin\ffprobe.exe"),
            Path(r"C:\FFmpeg\bin\ffprobe.exe"),
        ],
        ("SERVER_AUTO_CLIP_FFPROBE", "FFPROBE_PATH"),
    )


DEFAULT_FFMPEG = _find_ffmpeg()
DEFAULT_FFPROBE = _find_ffprobe()
CONFIG_FILE = Path(__file__).parent / "config.json"
TTS_CACHE_DIR = Path(__file__).parent / "tts_cache"
TTS_CACHE_VERSION = "20260409_v1"
RESAMPLE_LANCZOS = getattr(getattr(Image, "Resampling", Image), "LANCZOS", None)
RESAMPLE_NEAREST = getattr(getattr(Image, "Resampling", Image), "NEAREST", None)


@dataclass
class Workspace:
    id: str
    name: str
    created_at: str
    reference_video: str = ""
    reference_subtitle: str = ""
    source_dir: str = ""
    output_dir: str = ""
    last_modified: str = ""
    render_count: int = 0


@dataclass(frozen=True)
class SubtitleEntry:
    index: int
    start: float
    end: float
    text: str
    entry_type: str = "narration"


@dataclass(frozen=True)
class FrameSample:
    video_path: str
    video_name: str
    video_order: int
    local_index: int
    global_index: int
    timestamp: float
    signature: Tuple[int, ...]
    flipped_signature: Tuple[int, ...] = ()
    frame_path: str = ""


@dataclass(frozen=True)
class ReferenceFrame:
    index: int
    timestamp: float
    signature: Tuple[int, ...]
    frame_path: str = ""


@dataclass(frozen=True)
class SegmentJob:
    source_video: str
    start: float
    duration: float
    hflip: bool = False


@dataclass(frozen=True)
class ProcessedSubtitleBundle:
    all_entries: List[SubtitleEntry]
    narration_entries: List[SubtitleEntry]
    counts: Dict[str, int]


@dataclass(frozen=True)
class CloneSettings:
    reference_video: Path
    source_dir: Path
    output_dir: Path
    subtitle_entries: List[SubtitleEntry]
    output_stem: str = "output"
    ai_api_key: str = ""
    ai_model: str = "qwen-plus"
    ai_api_url: str = ""
    ai_fallback_models: List[Dict[str, str]] = field(default_factory=list)
    tts_voice: str = DEFAULT_TTS_VOICE
    tts_rate: str = DEFAULT_TTS_RATE
    enable_backup_tts: bool = False
    azure_tts_key: str = ""
    azure_tts_region: str = ""
    azure_tts_voice: str = ""
    match_threshold: float = 0.70
    frame_interval: float = FRAME_INTERVAL
    keep_temp: bool = False


@dataclass(frozen=True)
class CloneResult:
    video_path: Path
    subtitle_path: Optional[Path]
    audio_path: Optional[Path]
    clean_video_path: Path
    reconstructed_duration: float
    reference_duration: float
    frame_matches: int
    reference_frames: int
    confident_match_rate: float
    low_similarity_count: int


@dataclass(frozen=True)
class TTSAttemptResult:
    success: bool
    used_voice: str = ""
    error_text: str = ""
    provider: str = ""


@dataclass(frozen=True)
class VideoMaskRegion:
    x: int
    y: int
    width: int
    height: int
    confidence: float = 0.0
    source: str = "auto"


def time_str_to_seconds(ts: str) -> float:
    parts = ts.strip().replace(",", ".").split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid subtitle time: {ts}")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])


def seconds_to_time_str(sec: float) -> str:
    sec = max(0.0, sec)
    total_ms = int(round(sec * 1000))
    total_s, ms = divmod(total_ms, 1000)
    hours, rem = divmod(total_s, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{ms:03d}"


def ass_time_to_seconds(raw: str) -> float:
    match = ASS_TIME_RE.fullmatch(raw.strip())
    if not match:
        raise ValueError(f"Invalid ASS time: {raw}")
    return (
        int(match.group("h")) * 3600
        + int(match.group("m")) * 60
        + int(match.group("s"))
        + int(match.group("cs")) / 100.0
    )


def load_text_file(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gbk", "gb2312", "gb18030", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="ignore")


def natural_path_key(path: Path) -> Tuple[Tuple[int, object], ...]:
    parts = re.split(r"(\d+)", path.stem.lower())
    key: List[Tuple[int, object]] = []
    for part in parts:
        if not part:
            continue
        if part.isdigit():
            key.append((0, int(part)))
        else:
            key.append((1, part))
    key.append((1, path.suffix.lower()))
    return tuple(key)


def strip_ass_text(raw: str) -> str:
    text = raw.replace("\\N", "\n").replace("\\n", "\n").replace("\\h", " ")
    text = re.sub(r"\{[^}]*\}", "", text)
    return text.strip()


def parse_srt(content: str) -> List[SubtitleEntry]:
    entries: List[SubtitleEntry] = []
    chunks = re.split(r"\r?\n\r?\n", content.strip())
    for chunk in chunks:
        lines = [line.strip() for line in chunk.splitlines() if line.strip()]
        if len(lines) < 3:
            continue
        try:
            index = int(lines[0])
        except ValueError:
            continue
        match = TIME_RE.search(lines[1])
        if not match:
            continue
        body = re.sub(r"\{[^}]*\}", "", "\n".join(lines[2:])).strip()
        entries.append(
            SubtitleEntry(
                index=index,
                start=time_str_to_seconds(match.group("start")),
                end=time_str_to_seconds(match.group("end")),
                text=body,
            )
        )
    return entries


def parse_ass(content: str) -> List[SubtitleEntry]:
    entries: List[SubtitleEntry] = []
    fields: List[str] = []
    counter = 1
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lower = line.lower()
        if lower.startswith("format:"):
            fields = [part.strip().lower() for part in line.split(":", 1)[1].split(",")]
            continue
        if not lower.startswith("dialogue:"):
            continue
        payload = line.split(":", 1)[1].lstrip()
        if fields:
            parts = payload.split(",", len(fields) - 1)
            if len(parts) != len(fields):
                continue
            mapping = {field: value for field, value in zip(fields, parts)}
            start_raw = mapping.get("start")
            end_raw = mapping.get("end")
            text_raw = mapping.get("text", "")
        else:
            parts = payload.split(",", 9)
            if len(parts) < 10:
                continue
            start_raw = parts[1]
            end_raw = parts[2]
            text_raw = parts[9]
        try:
            entries.append(
                SubtitleEntry(
                    index=counter,
                    start=ass_time_to_seconds(start_raw),
                    end=ass_time_to_seconds(end_raw),
                    text=strip_ass_text(text_raw),
                )
            )
        except ValueError:
            continue
        counter += 1
    return entries


def parse_subtitle_content(content: str, suffix: str = ".srt") -> List[SubtitleEntry]:
    suffix = suffix.lower()
    if suffix in {".ass", ".ssa"}:
        return parse_ass(content)
    entries = parse_srt(content)
    if entries:
        return entries
    if "dialogue:" in content.lower():
        return parse_ass(content)
    return []


def entries_to_srt(entries: Sequence[SubtitleEntry]) -> str:
    blocks = []
    for idx, entry in enumerate(entries, start=1):
        text = normalize_subtitle_text(entry.text)
        blocks.append(
            "\n".join(
                [
                    str(entry.index if entry.index > 0 else idx),
                    f"{seconds_to_time_str(entry.start)} --> {seconds_to_time_str(entry.end)}",
                    text,
                ]
            )
        )
    return "\n\n".join(blocks) + ("\n" if blocks else "")


def write_srt(path: Path, entries: Sequence[SubtitleEntry]) -> None:
    path.write_text(entries_to_srt(entries), encoding="utf-8-sig")


def repair_contextual_ocr_phrases(entries: Sequence[SubtitleEntry]) -> Tuple[List[SubtitleEntry], int]:
    if not entries:
        return [], 0

    repaired: List[SubtitleEntry] = []
    fix_count = 0
    radius = 3
    for idx, entry in enumerate(entries):
        text = normalize_subtitle_text(entry.text)
        if not text:
            repaired.append(entry)
            continue

        window_text = "".join(
            normalize_subtitle_text(item.text)
            for item in entries[max(0, idx - radius) : min(len(entries), idx + radius + 1)]
        )
        previous_text = normalize_subtitle_text(entries[idx - 1].text) if idx > 0 else ""
        next_text = normalize_subtitle_text(entries[idx + 1].text) if idx + 1 < len(entries) else ""
        candidate = text

        if "出了现" in candidate:
            if any(marker in window_text for marker in ("妻子", "闺蜜", "情侣睡衣", "不堪", "撞见", "照片", "小山")):
                candidate = candidate.replace("出了现", "出了轨")
            else:
                candidate = candidate.replace("出了现", "出现")

        if "的之下" in candidate:
            if "小山" in window_text or "孩子" in window_text:
                candidate = candidate.replace("的之下", "的撺掇之下")

        if candidate == "之下" and ("小山" in previous_text or "小山" in window_text):
            candidate = "的撺掇之下"

        if candidate != text:
            fix_count += 1
            repaired.append(clone_subtitle_entry(entry, text=candidate))
            continue

        repaired.append(entry)

    return repaired, fix_count


def _eligible_full_text_ocr_text(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    if not normalized or watermark_like_text(normalized):
        return False
    units = subtitle_display_units(normalized)
    if units < 4 or units > 20:
        return False
    if re.search(r"[A-Za-z]{2,}", normalized):
        return False
    if re.search(r"\d{2,}", normalized):
        return False
    cjk_count = sum(1 for ch in normalized if CJK_RE.fullmatch(ch))
    if cjk_count < 4:
        return False
    return cjk_count >= max(4, units - 1)


def _single_cjk_variant(
    source: str,
    target: str,
) -> Optional[Tuple[int, str, str]]:
    if len(source) != len(target):
        return None

    diff_index: Optional[int] = None
    source_char = ""
    target_char = ""
    for index, (source_piece, target_piece) in enumerate(zip(source, target)):
        if source_piece == target_piece:
            continue
        if diff_index is not None:
            return None
        if not CJK_RE.fullmatch(source_piece) or not CJK_RE.fullmatch(target_piece):
            return None
        diff_index = index
        source_char = source_piece
        target_char = target_piece

    if diff_index is None:
        return None
    return diff_index, source_char, target_char


def _safe_full_text_ocr_variant(source: str, target: str) -> bool:
    variant = _single_cjk_variant(source, target)
    if variant is None:
        return False

    _, source_char, target_char = variant
    if source_char in OCR_CONSISTENCY_PROTECTED_CHARS:
        return False
    if target_char in OCR_CONSISTENCY_PROTECTED_CHARS:
        return False
    if dialogue_like_text(source) != dialogue_like_text(target):
        return False
    if bool(TERMINAL_SENTENCE_PUNCT_RE.search(source)) != bool(TERMINAL_SENTENCE_PUNCT_RE.search(target)):
        return False
    return True


def _has_full_text_context_match(
    entries: Sequence[SubtitleEntry],
    source_position: int,
    target_positions: Sequence[int],
) -> bool:
    source_prev = normalize_subtitle_text(entries[source_position - 1].text) if source_position > 0 else ""
    source_next = (
        normalize_subtitle_text(entries[source_position + 1].text)
        if source_position + 1 < len(entries)
        else ""
    )
    for target_position in target_positions:
        if target_position == source_position:
            continue
        target_prev = normalize_subtitle_text(entries[target_position - 1].text) if target_position > 0 else ""
        target_next = (
            normalize_subtitle_text(entries[target_position + 1].text)
            if target_position + 1 < len(entries)
            else ""
        )
        if source_prev and source_prev == target_prev:
            return True
        if source_next and source_next == target_next:
            return True
    return False


def repair_full_text_ocr_consistency(entries: Sequence[SubtitleEntry]) -> Tuple[List[SubtitleEntry], int]:
    if len(entries) < 3:
        return list(entries), 0

    normalized_entries = [clone_subtitle_entry(entry, text=normalize_subtitle_text(entry.text)) for entry in entries]
    text_counts: Dict[str, int] = {}
    text_positions: Dict[str, List[int]] = {}
    signature_buckets: Dict[Tuple[int, str], Dict[str, int]] = {}

    for position, entry in enumerate(normalized_entries):
        text = entry.text
        if not _eligible_full_text_ocr_text(text):
            continue
        text_counts[text] = text_counts.get(text, 0) + 1
        text_positions.setdefault(text, []).append(position)

    for text, count in text_counts.items():
        for index, char in enumerate(text):
            if not CJK_RE.fullmatch(char):
                continue
            signature = (len(text), text[:index] + text[index + 1 :])
            bucket = signature_buckets.setdefault(signature, {})
            bucket[text] = count

    replacement_map: Dict[str, str] = {}
    replacement_strength: Dict[str, int] = {}
    for variants in signature_buckets.values():
        if len(variants) < 2:
            continue
        ordered = sorted(
            variants.items(),
            key=lambda item: (item[1], subtitle_display_units(item[0])),
            reverse=True,
        )
        target, target_count = ordered[0]
        if target_count < 2:
            continue

        for source, source_count in ordered[1:]:
            if source_count != 1:
                continue
            if target_count < source_count + 2:
                continue
            if not _safe_full_text_ocr_variant(source, target):
                continue

            source_positions = text_positions.get(source, [])
            target_positions = text_positions.get(target, [])
            if len(source_positions) != 1 or not target_positions:
                continue

            context_match = _has_full_text_context_match(
                normalized_entries,
                source_positions[0],
                target_positions,
            )
            if not context_match:
                if target_count < 3:
                    continue
                if subtitle_display_units(source) < 8:
                    continue

            existing_target = replacement_map.get(source)
            if existing_target is not None and replacement_strength.get(source, 0) >= target_count:
                continue
            replacement_map[source] = target
            replacement_strength[source] = target_count

    if not replacement_map:
        return normalized_entries, 0

    repaired: List[SubtitleEntry] = []
    fix_count = 0
    for entry in normalized_entries:
        replacement = replacement_map.get(entry.text)
        if replacement and replacement != entry.text:
            repaired.append(clone_subtitle_entry(entry, text=replacement))
            fix_count += 1
            continue
        repaired.append(entry)
    return repaired, fix_count


def lightly_repair_subtitle_timeline(
    entries: Sequence[SubtitleEntry],
    max_shift: float = 1.0,
) -> Tuple[List[SubtitleEntry], int]:
    if not entries:
        return [], 0

    durations = sorted(
        entry.end - entry.start
        for entry in entries
        if entry.end - entry.start >= 0.25
    )
    typical_duration = durations[len(durations) // 2] if durations else 1.6
    min_gap = 0.02
    min_duration = max(0.35, min(typical_duration, 2.4))
    repaired: List[SubtitleEntry] = []
    fixed = 0

    for index, entry in enumerate(entries):
        start = max(0.0, float(entry.start))
        end = max(start + 0.01, float(entry.end))
        source_start = start
        source_end = end

        if end <= start + 0.08:
            end = start + min_duration

        previous_end = repaired[-1].end if repaired else None
        if previous_end is not None:
            required_start = previous_end + min_gap
            shift = required_start - start
            if 0.08 < shift <= max_shift:
                start = required_start
                end = max(end + shift, start + min_duration)

        next_start = None
        if index + 1 < len(entries):
            next_start = max(0.0, float(entries[index + 1].start))

        if next_start is not None:
            allowed_end = next_start - min_gap
            if end > allowed_end and end - allowed_end <= max_shift and allowed_end >= start + 0.18:
                end = allowed_end

        if end <= start + 0.08:
            if next_start is not None and next_start - min_gap >= start + 0.18:
                end = min(start + min_duration, next_start - min_gap)
            else:
                end = start + max(0.18, min_duration * 0.65)

        if abs(start - source_start) > 1e-3 or abs(end - source_end) > 1e-3:
            fixed += 1

        repaired.append(
            SubtitleEntry(
                index=entry.index,
                start=start,
                end=end,
                text=entry.text,
                entry_type=entry.entry_type,
            )
        )

    return repaired, fixed


def preserve_reference_timeline_entries(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    preserved: List[SubtitleEntry] = []
    for entry in entries:
        preserved.append(
            SubtitleEntry(
                index=entry.index,
                start=float(entry.start),
                end=float(entry.end),
                text=normalize_subtitle_text(entry.text),
                entry_type=entry.entry_type,
            )
        )
    return preserved


def normalize_subtitle_text(raw: str) -> str:
    text = (raw or "").replace("\r", "\n")
    text = re.sub(r"\s*\n+\s*", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"([\u4e00-\u9fff])\s+([\u4e00-\u9fff])", r"\1\2", text)
    return text


def subtitle_display_units(text: str) -> int:
    normalized = normalize_subtitle_text(text)
    return sum(1 for ch in normalized if not ch.isspace())


def subtitle_char_budget(duration: float) -> int:
    dynamic_budget = int(round(max(1.0, duration + 0.15) * TARGET_SUBTITLE_CPS))
    return max(MIN_SUBTITLE_CHARS, min(MAX_SUBTITLE_CHARS, dynamic_budget))


def trim_text_to_units(text: str, max_units: int) -> str:
    if max_units <= 0:
        return ""
    normalized = normalize_subtitle_text(text)
    pieces: List[str] = []
    units = 0
    last_was_space = False
    for ch in normalized:
        if ch.isspace():
            if pieces and not last_was_space:
                pieces.append(" ")
                last_was_space = True
            continue
        if units >= max_units:
            break
        pieces.append(ch)
        units += 1
        last_was_space = False
    return "".join(pieces).strip(" ，,。！？!?；;：:、…")


def compact_subtitle_text(text: str, duration: float) -> str:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return ""

    budget = subtitle_char_budget(duration)
    if subtitle_display_units(normalized) <= budget:
        return normalized

    clauses = [
        normalize_subtitle_text(part)
        for part in re.split(r"[，,。！？!?；;：:、…]+", normalized)
        if normalize_subtitle_text(part)
    ]

    compact = ""
    if clauses:
        selected: List[str] = []
        for clause in clauses:
            candidate = "".join(selected) + clause
            if subtitle_display_units(candidate) > budget:
                if not selected:
                    compact = trim_text_to_units(clause, budget)
                break
            selected.append(clause)
            compact = "".join(selected)

        if not compact:
            compact = max(
                clauses,
                key=lambda clause: min(subtitle_display_units(clause), budget),
            )
            compact = trim_text_to_units(compact, budget)

    if not compact:
        compact = trim_text_to_units(normalized, budget)

    result = compact or trim_text_to_units(normalized, budget)
    result = result.strip("\"'`")
    if subtitle_display_units(result) > 3:
        stripped = FRAGMENT_TAIL_RE.sub("", result).strip()
        if stripped:
            result = stripped
    return result


def subtitle_speech_units(text: str) -> int:
    normalized = normalize_subtitle_text(text)
    count = 0
    for ch in normalized:
        if ch.isspace():
            continue
        if ch in "，,。！？!?；;：:、…":
            continue
        count += 1
    return max(1, count)


def trim_text_to_speech_units(text: str, max_units: int) -> str:
    if max_units <= 0:
        return ""
    normalized = normalize_subtitle_text(text)
    pieces: List[str] = []
    units = 0
    for ch in normalized:
        if ch.isspace():
            if pieces and pieces[-1] != " ":
                pieces.append(" ")
            continue
        pieces.append(ch)
        if ch in "，。！？；：、,.!?;:":
            continue
        units += 1
        if units >= max_units:
            break
    result = "".join(pieces).strip()
    result = re.sub(r"[，。！？；：、,.!?;:]+$", "", result).strip()
    return result


def compact_narration_to_speech_units(text: str, max_units: int) -> str:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return ""
    if subtitle_speech_units(normalized) <= max_units:
        return normalized

    clauses = [
        normalize_subtitle_text(part)
        for part in re.split(r"[，。！？；：、,.!?;:]+", normalized)
        if normalize_subtitle_text(part)
    ]

    compact = ""
    if clauses:
        selected: List[str] = []
        for clause in clauses:
            candidate = "".join(selected) + clause
            if subtitle_speech_units(candidate) > max_units:
                if not selected:
                    compact = trim_text_to_speech_units(clause, max_units)
                break
            selected.append(clause)
            compact = "".join(selected)

        if not compact:
            compact = max(
                clauses,
                key=lambda clause: min(subtitle_speech_units(clause), max_units),
            )
            compact = trim_text_to_speech_units(compact, max_units)

    if not compact:
        compact = trim_text_to_speech_units(normalized, max_units)

    result = compact or trim_text_to_speech_units(normalized, max_units)
    if subtitle_speech_units(result) > 3:
        stripped = FRAGMENT_TAIL_RE.sub("", result).strip()
        if stripped:
            result = stripped
    return normalize_subtitle_text(result)


def narration_rewrite_speech_budgets(entry: SubtitleEntry) -> Tuple[int, int]:
    duration = max(0.1, float(entry.end) - float(entry.start))
    source_units = max(1, subtitle_speech_units(entry.text))
    duration_units = max(4, int(round(max(0.85, duration + 0.05) * TARGET_TTS_CPS)))
    preferred_units = max(4, min(source_units, duration_units))
    slack_units = 1 if duration <= 2.6 else 2
    hard_units = max(preferred_units, min(source_units + slack_units, duration_units + slack_units))
    return preferred_units, hard_units


def fit_rewrite_candidate_to_timing(
    source_entry: SubtitleEntry,
    candidate: str,
    *,
    display_budget: int,
    speech_budget: int,
) -> str:
    def shortening_creates_dangling_fragment(original_text: str, shortened_text: str) -> bool:
        original_units = subtitle_speech_units(original_text)
        shortened_units = subtitle_speech_units(shortened_text)
        if shortened_units <= 0 or original_units <= 0:
            return True
        if shortened_units >= original_units:
            return False
        if looks_like_explicit_sentence_end(shortened_text):
            return False
        if ends_with_minor_sentence_pause(shortened_text):
            return False
        if probably_incomplete_text(shortened_text):
            return True
        if looks_like_dangling_tts_tail(shortened_text):
            return True
        return shortened_units + 1 < original_units

    normalized = normalize_spoken_narration_text(candidate)
    if not normalized:
        return ""
    source_text = normalize_spoken_narration_text(source_entry.text)
    duration = max(0.1, float(source_entry.end) - float(source_entry.start))
    fitted = normalized
    complete_fallback = (
        normalized
        if subtitle_display_units(normalized) <= display_budget and not probably_incomplete_text(normalized)
        else ""
    )
    for _ in range(2):
        if subtitle_display_units(fitted) > display_budget:
            compacted = compact_subtitle_text(fitted, duration)
            if compacted and shortening_creates_dangling_fragment(fitted, compacted):
                if complete_fallback:
                    fitted = complete_fallback
                    break
                return ""
            fitted = compacted
        if not fitted:
            return ""
        if subtitle_speech_units(fitted) > speech_budget:
            compacted = compact_narration_to_speech_units(fitted, speech_budget)
            if compacted and shortening_creates_dangling_fragment(fitted, compacted):
                if complete_fallback:
                    fitted = complete_fallback
                    break
                return ""
            if compacted and not probably_incomplete_text(compacted):
                fitted = compacted
                continue
            if complete_fallback:
                fitted = complete_fallback
                break
            fitted = compacted
    if not fitted:
        return ""
    if (
        source_text
        and subtitle_speech_units(source_text) <= 12
        and subtitle_speech_units(fitted) + 1 < subtitle_speech_units(source_text)
        and not looks_like_explicit_sentence_end(fitted)
        and not ends_with_minor_sentence_pause(fitted)
    ):
        if subtitle_display_units(source_text) <= display_budget:
            fitted = source_text
        else:
            return ""
    if (
        source_text
        and subtitle_speech_units(source_text) <= 8
        and not looks_like_explicit_sentence_end(source_text)
        and not looks_like_explicit_sentence_end(fitted)
        and not ends_with_minor_sentence_pause(fitted)
        and rewrite_similarity(source_text, fitted) < 0.85
    ):
        if subtitle_display_units(source_text) <= display_budget:
            fitted = source_text
        else:
            return ""
    if subtitle_display_units(fitted) > display_budget:
        return ""
    speech_overflow = subtitle_speech_units(fitted) - speech_budget
    if speech_overflow > 0:
        if probably_incomplete_text(fitted):
            return ""
        if speech_overflow > NARRATION_REWRITE_SOFT_OVERFLOW_UNITS:
            return ""
    return normalize_subtitle_text(fitted)


def percentile_value(values: Sequence[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * ratio))))
    return float(ordered[index])


def analyze_reference_subtitle_profile(entries: Sequence[SubtitleEntry]) -> Dict[str, float]:
    cps_values: List[float] = []
    gap_values: List[float] = []
    previous_end: Optional[float] = None

    for entry in entries:
        duration = max(0.001, entry.end - entry.start)
        units = subtitle_speech_units(entry.text)
        if duration >= 0.75 and units >= 6:
            cps_values.append(units / duration)
        if previous_end is not None:
            gap = max(0.0, entry.start - previous_end)
            if gap <= SHORT_GAP_MERGE_SECONDS:
                gap_values.append(gap)
        previous_end = entry.end

    if not cps_values:
        cps_values = [
            subtitle_speech_units(entry.text) / max(0.001, entry.end - entry.start)
            for entry in entries
            if entry.end > entry.start
        ]

    median_cps = percentile_value(cps_values, 0.5)
    p75_cps = percentile_value(cps_values, 0.75)
    avg_gap = sum(gap_values) / len(gap_values) if gap_values else 0.12
    return {
        "median_cps": median_cps,
        "p75_cps": p75_cps,
        "avg_gap": avg_gap,
    }


def max_rate_text(left: str, right: str) -> str:
    return format_rate_percent(max(parse_rate_percent(left), parse_rate_percent(right)))


def min_rate_text(left: str, right: str) -> str:
    return format_rate_percent(min(parse_rate_percent(left), parse_rate_percent(right)))


def suggest_reference_tts_rate(entries: Sequence[SubtitleEntry]) -> str:
    profile = analyze_reference_subtitle_profile(entries)
    median_cps = profile["median_cps"]
    p75_cps = profile["p75_cps"]

    if median_cps >= 6.5 or p75_cps >= 7.0:
        return "+6%"
    if median_cps >= 6.0:
        return "+4%"
    if median_cps >= 5.5:
        return "+2%"
    return "+0%"


def choose_reference_tts_voice(selected_voice: str, entries: Sequence[SubtitleEntry]) -> str:
    normalized = (selected_voice or "").strip()
    if not normalized:
        return DEFAULT_TTS_VOICE
    return normalized


def normalize_reference_gap(avg_gap: float) -> float:
    baseline = avg_gap if avg_gap > 0 else 0.12
    return clamp(baseline, MIN_REFERENCE_GAP_SECONDS, MAX_REFERENCE_GAP_SECONDS)


def planned_tts_window_end(
    entry: SubtitleEntry,
    next_block_start: Optional[float],
    total_duration: float,
    reference_gap: float,
) -> float:
    start = clamp(entry.start, 0.0, total_duration)
    end = clamp(entry.end, 0.0, total_duration)
    target_end = end
    desired_gap = normalize_reference_gap(reference_gap)
    if next_block_start is not None:
        next_start = clamp(next_block_start, 0.0, total_duration)
        available_gap = max(0.0, next_start - end)
        if available_gap > desired_gap:
            target_end = max(end, next_start - desired_gap)
    return clamp(target_end, start + 0.05, total_duration)


def parse_rate_percent(raw: str) -> float:
    match = re.fullmatch(r"\s*([+-]?\d+(?:\.\d+)?)\s*%?\s*", raw or "")
    if not match:
        return 0.0
    return float(match.group(1))


def format_rate_percent(value: float) -> str:
    rounded = int(round(value))
    return f"{rounded:+d}%"


def tts_rate_factor(raw: str) -> float:
    return clamp(1.0 + parse_rate_percent(raw) / 100.0, 0.80, MAX_TTS_SYNTH_RATE_FACTOR)


def factor_to_rate_text(factor: float) -> str:
    return format_rate_percent((clamp(factor, 0.80, MAX_TTS_SYNTH_RATE_FACTOR) - 1.0) * 100.0)


def scale_rate_text(base_rate: str, multiplier: float) -> str:
    return factor_to_rate_text(tts_rate_factor(base_rate) * max(multiplier, 0.01))


def adaptive_tts_rate(text: str, target_duration: float, base_rate: str) -> str:
    base_factor = clamp(tts_rate_factor(base_rate), MIN_TTS_SPEED_FACTOR, MAX_TTS_SYNTH_RATE_FACTOR)
    if target_duration <= 0.05:
        return factor_to_rate_text(base_factor)

    estimated_duration = subtitle_speech_units(text) / TARGET_TTS_CPS
    desired_factor = estimated_duration / max(target_duration, 0.08)
    adjustment = (desired_factor - 1.0) * 0.35
    final_factor = clamp(base_factor + adjustment, MIN_TTS_SPEED_FACTOR, MAX_TTS_SYNTH_RATE_FACTOR)
    return factor_to_rate_text(final_factor)


def derive_uniform_tts_rate(group_states: Sequence[Dict[str, object]], base_rate: str) -> str:
    base_factor = clamp(tts_rate_factor(base_rate), MIN_TTS_SPEED_FACTOR, MAX_TTS_SYNTH_RATE_FACTOR)
    weighted_items: List[Tuple[float, float]] = []
    for state in group_states:
        text = normalize_subtitle_text(str(state.get("text", "") or ""))
        raw_duration = max(0.0, tts_group_schedulable_duration(state))
        target_duration = max(0.0, float(state.get("target_duration", 0.0) or 0.0))
        if not text or raw_duration <= 0.0 or target_duration <= 0.0:
            continue
        weight = float(max(1, subtitle_speech_units(text)))
        required_multiplier = clamp(
            raw_duration / max(0.05, target_duration),
            0.92,
            MAX_TTS_SYNTH_RATE_FACTOR / max(base_factor, 0.01),
        )
        weighted_items.append((required_multiplier, weight))

    if not weighted_items:
        return factor_to_rate_text(base_factor)

    ordered = sorted(weighted_items, key=lambda item: item[0])
    total_weight = sum(weight for _, weight in ordered)
    weighted_avg = sum(value * weight for value, weight in ordered) / max(1.0, total_weight)

    running_weight = 0.0
    weighted_p75 = ordered[-1][0]
    weighted_p90 = ordered[-1][0]
    for value, weight in ordered:
        running_weight += weight
        if running_weight >= total_weight * 0.75:
            weighted_p75 = value
            break
    running_weight = 0.0
    for value, weight in ordered:
        running_weight += weight
        if running_weight >= total_weight * 0.90:
            weighted_p90 = value
            break

    min_uniform_factor = max(MIN_TTS_SPEED_FACTOR, base_factor - 0.06)
    max_uniform_factor = min(MAX_TTS_SYNTH_RATE_FACTOR, max(base_factor, 1.12))
    multiplier = clamp(
        weighted_avg * 0.40 + weighted_p75 * 0.35 + weighted_p90 * 0.25,
        min_uniform_factor / max(base_factor, 0.01),
        max_uniform_factor / max(base_factor, 0.01),
    )
    return factor_to_rate_text(base_factor * multiplier)


def estimate_tts_render_duration(text: str, base_rate: str) -> float:
    units = max(1, subtitle_speech_units(text))
    rate_factor = tts_rate_factor(base_rate)
    estimated = (0.42 + units / ESTIMATED_TTS_CPS) / rate_factor
    return max(0.20, estimated)


def schedule_tts_groups(
    group_states: Sequence[Dict[str, object]],
    total_duration: float,
) -> Tuple[float, float, float, float]:
    active_groups = [
        state
        for state in group_states
        if float(state.get("raw_duration", 0.0) or 0.0) > 0.0
    ]
    if not active_groups:
        return 1.0, 0.0, 0.0, 0.0

    max_speed = max(1.0, MAX_TTS_SPEED_FACTOR)

    def simulate(speed_factor: float) -> float:
        cursor = 0.0
        for state in active_groups:
            start_hint = clamp(float(state.get("start_hint", 0.0) or 0.0), 0.0, total_duration)
            duration = max(0.05, float(state.get("raw_duration", 0.0) or 0.0) / max(speed_factor, 0.01))
            start = max(start_hint, cursor)
            cursor = start + duration
        return cursor

    pacing_factor = 1.0
    end_at_normal = simulate(1.0)
    if end_at_normal > total_duration:
        end_at_max = simulate(max_speed)
        if end_at_max <= total_duration:
            low = 1.0
            high = max_speed
            for _ in range(32):
                mid = (low + high) / 2.0
                if simulate(mid) > total_duration:
                    low = mid
                else:
                    high = mid
            pacing_factor = high
        else:
            pacing_factor = max_speed

    cursor = 0.0
    total_raw = 0.0
    total_scheduled = 0.0
    for state in active_groups:
        start_hint = clamp(float(state.get("start_hint", 0.0) or 0.0), 0.0, total_duration)
        raw_duration = max(0.05, float(state.get("raw_duration", 0.0) or 0.0))
        scheduled_duration = max(0.05, raw_duration / max(pacing_factor, 0.01))
        scheduled_start = max(start_hint, cursor)
        scheduled_end = scheduled_start + scheduled_duration
        state["scheduled_start"] = scheduled_start
        state["scheduled_duration"] = scheduled_duration
        state["scheduled_end"] = scheduled_end
        cursor = scheduled_end
        total_raw += raw_duration
        total_scheduled += scheduled_duration

    overflow = max(0.0, cursor - total_duration)
    return pacing_factor, overflow, total_raw, total_scheduled


def extract_json_object(payload: str) -> Optional[object]:
    if not payload:
        return None

    text = payload.strip()
    fence_match = re.search(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", text, flags=re.S | re.I)
    if fence_match:
        text = fence_match.group(1).strip()

    try:
        return json.loads(text)
    except Exception:
        pass

    decoder = json.JSONDecoder()
    for idx, ch in enumerate(text):
        if ch not in "{[":
            continue
        try:
            parsed, _ = decoder.raw_decode(text[idx:])
            return parsed
        except Exception:
            continue
    return None


def extract_chat_message_text(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        return ""
    message = first_choice.get("message")
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""

    parts: List[str] = []
    for item in content:
        text = ""
        if isinstance(item, str):
            text = item
        elif isinstance(item, dict):
            raw_text = item.get("text")
            if isinstance(raw_text, str):
                text = raw_text
            else:
                raw_content = item.get("content")
                if isinstance(raw_content, str):
                    text = raw_content
        if text and text.strip():
            parts.append(text.strip())
    return "\n".join(parts).strip()


def summarize_for_log(payload: object, limit: int = 220) -> str:
    if isinstance(payload, str):
        text = payload
    else:
        try:
            text = json.dumps(payload, ensure_ascii=False)
        except Exception:
            text = repr(payload)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def is_retryable_ai_request_exception(exc: BaseException) -> bool:
    retryable_types = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ProxyError,
        requests.exceptions.ChunkedEncodingError,
    )
    return isinstance(exc, retryable_types)


def is_retryable_ai_status_code(status_code: int) -> bool:
    return int(status_code or 0) in {408, 409, 425, 429, 500, 502, 503, 504}


def ai_issue_requires_failover(detail: str) -> bool:
    normalized = (detail or "").strip().lower()
    if not normalized:
        return False
    if any(
        token in normalized
        for token in (
            "insufficient_quota",
            "quota",
            "balance",
            "credit",
            "billing",
            "余额",
            "欠费",
            "model_not_found",
            "no such model",
            "unknown model",
            "invalid model",
            "context_length_exceeded",
            "context length",
            "maximum context length",
            "status 401",
            "status 402",
            "status 403",
            "status 404",
            "status 429",
        )
    ):
        return True
    return False


def ai_issue_supports_smaller_chunk(detail: str) -> bool:
    normalized = (detail or "").strip().lower()
    if not normalized:
        return False
    if any(
        token in normalized
        for token in (
            "proxyerror",
            "unable to connect to proxy",
            "remote end closed connection",
            "auth failed",
            "status 401",
            "status 403",
            "status 404",
        )
    ):
        return False
    return any(
        token in normalized
        for token in (
            "readtimeout",
            "read timed out",
            "connecttimeout",
            "timed out",
            "empty response",
            "parse miss",
            "invalid json response",
            "status 413",
        )
    )


def request_ai_json_object(
    *,
    api_url: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    label: str,
    log_func: Optional[Callable[[str], None]] = None,
    issue_recorder: Optional[Callable[[str], None]] = None,
    timeout: int = 240,
    max_tokens: int = 8192,
    max_attempts: int = 1,
    retry_delay: float = 1.2,
) -> Optional[object]:
    def record(detail: str) -> None:
        message = detail.strip()
        if not message:
            return
        if issue_recorder:
            try:
                issue_recorder(message, log_func=log_func)
            except TypeError:
                issue_recorder(message)
            return
        if log_func:
            log_func(f"  {message}")

    attempts = max(1, int(max_attempts or 1))
    for attempt_index in range(attempts):
        attempt = attempt_index + 1
        try:
            response = requests.post(
                api_url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                },
                timeout=timeout,
            )
        except Exception as exc:
            if attempt < attempts and is_retryable_ai_request_exception(exc):
                record(
                    f"{label} transient request issue ({attempt}/{attempts}): "
                    f"{type(exc).__name__}: {summarize_for_log(str(exc), limit=260)}; retrying"
                )
                time.sleep(max(0.2, retry_delay) * attempt)
                continue
            record(f"{label} request failed: {type(exc).__name__}: {exc}")
            return None

        try:
            response.raise_for_status()
        except Exception:
            status_code = int(getattr(response, "status_code", 0) or 0)
            if attempt < attempts and is_retryable_ai_status_code(status_code):
                record(
                    f"{label} transient HTTP error ({attempt}/{attempts}): "
                    f"status {status_code}; retrying"
                )
                time.sleep(max(0.2, retry_delay) * attempt)
                continue
            record(
                f"{label} HTTP error: status {response.status_code}, body {summarize_for_log(response.text) or '<empty>'}"
            )
            return None

        try:
            response_payload = response.json()
        except Exception as exc:
            if attempt < attempts:
                record(
                    f"{label} transient invalid JSON response ({attempt}/{attempts}): "
                    f"{type(exc).__name__}: {summarize_for_log(response.text) or '<empty>'}; retrying"
                )
                time.sleep(max(0.2, retry_delay) * attempt)
                continue
            record(
                f"{label} invalid JSON response: {type(exc).__name__}: {exc}; body {summarize_for_log(response.text) or '<empty>'}"
            )
            return None

        message_text = extract_chat_message_text(response_payload)
        if not message_text:
            if attempt < attempts:
                record(
                    f"{label} transient empty response ({attempt}/{attempts}): "
                    f"{summarize_for_log(response_payload) or '<empty>'}; retrying"
                )
                time.sleep(max(0.2, retry_delay) * attempt)
                continue
            record(f"{label} empty response: {summarize_for_log(response_payload) or '<empty>'}")
            return None

        parsed = extract_json_object(message_text)
        if parsed is None:
            if attempt < attempts:
                record(
                    f"{label} transient parse miss ({attempt}/{attempts}): "
                    f"{summarize_for_log(message_text) or '<empty>'}; retrying"
                )
                time.sleep(max(0.2, retry_delay) * attempt)
                continue
            record(f"{label} parse miss: {summarize_for_log(message_text) or '<empty>'}")
            return None
        return parsed
    return None


def cleanup_rewrite_text(text: str) -> str:
    normalized = normalize_subtitle_text(text)
    normalized = normalized.strip()
    if normalized.count("“") != normalized.count("”"):
        normalized = normalized.replace("“", "").replace("”", "")
    normalized = normalized.strip("\"'`")
    normalized = re.sub(r"^[，。！？；：、…,.!?;:]+", "", normalized)
    normalized = re.sub(r"[，。！？；：、…,.!?;:]+$", "", normalized)
    return normalize_subtitle_text(normalized)


def normalize_spoken_narration_text(text: str) -> str:
    normalized = cleanup_rewrite_text(text)
    if not normalized:
        return ""
    phrase_replacements = [
        ("就这么", "于是"),
        ("命不久矣的大少爷", "快不行的大少爷"),
        ("命不久矣", "快不行了"),
        ("行将离世公子", "快不行的男人"),
        ("行将离世女子", "快不行的女人"),
        ("行将离世", "快不行了"),
        ("异于常人", "跟别人不一样"),
        ("怎料", "没想到"),
        ("岂料", "没想到"),
        ("未料", "没想到"),
        ("孰料", "没想到"),
        ("公子", "男人"),
        ("女子", "女人"),
        ("家中", "家里"),
        ("小丫头", "小女孩"),
        ("小姑娘", "小女孩"),
        ("丫头", "女孩"),
    ]
    for source, target in phrase_replacements:
        normalized = normalized.replace(source, target)
    normalized = normalized.replace("乃是", "就是")
    normalized = normalized.replace("便是", "就是")
    normalized = re.sub(r"乃(?!至)", "就是", normalized)
    normalized = re.sub(r"(^|[，。！？!?])乃(?=[^，。！？!?])", r"\1就是", normalized)
    normalized = normalized.replace("咱家里", "咱家的")
    normalized = normalized.replace("他家里", "他家")
    normalized = normalized.replace("她家里", "她家")
    return normalize_subtitle_text(normalized)


def ends_with_terminal_sentence_pause(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    return bool(normalized) and bool(TERMINAL_SENTENCE_PUNCT_RE.search(normalized))


def ends_with_minor_sentence_pause(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    return bool(normalized) and bool(MINOR_SENTENCE_PUNCT_RE.search(normalized))


def starts_with_soft_continuation(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    return bool(normalized) and bool(SOFT_CONTINUATION_RE.search(normalized))


def starts_with_structural_continuation(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    return bool(normalized) and bool(re.search(r"^(?:的|地|得|之|并|并且|而且)", normalized))


def starts_with_strong_transition(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    return bool(normalized) and bool(STRONG_TRANSITION_RE.search(normalized))


def looks_like_dangling_tts_tail(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return False
    if DANGLING_TTS_TAIL_RE.search(normalized):
        return True
    if subtitle_speech_units(normalized) <= 14 and SHORT_NOUN_TTS_TAIL_RE.search(normalized):
        return True
    if subtitle_speech_units(normalized) <= 10 and re.search(
        r"(?:\u8fd9\u4e2a|\u90a3\u4e2a|\u4e00\u4e2a|\u4e00\u4f4d|\u4e00\u540d|\u4e00\u6761|\u4e00\u4efd|\u4e00\u5c01)$",
        normalized,
    ):
        return True
    return False


def looks_like_explicit_sentence_end(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return False
    if ends_with_terminal_sentence_pause(normalized):
        return True
    if ends_with_minor_sentence_pause(normalized):
        return False
    if probably_incomplete_text(normalized):
        return False
    if speech_intro_score(normalized) >= 2:
        return False
    if looks_like_dangling_tts_tail(normalized):
        return False
    units = max(1, subtitle_speech_units(normalized))
    if units >= 18:
        return True
    if units >= 11 and SENTENCE_END_HINT_RE.search(normalized):
        return True
    return False


def rewrite_similarity(source: str, candidate: str) -> float:
    source_text = re.sub(r"\s+", "", normalize_spoken_narration_text(source))
    candidate_text = re.sub(r"\s+", "", normalize_spoken_narration_text(candidate))
    if not source_text or not candidate_text:
        return 0.0
    return difflib.SequenceMatcher(None, source_text, candidate_text).ratio()


def rewrite_needs_more_variation(source: str, candidate: str) -> bool:
    source_text = re.sub(r"\s+", "", normalize_spoken_narration_text(source))
    candidate_text = re.sub(r"\s+", "", normalize_spoken_narration_text(candidate))
    if not source_text or not candidate_text:
        return False
    if source_text == candidate_text:
        return True
    source_core = re.sub(r"[，。！？；：、,.!?;:\"'“”‘’`]", "", source_text)
    candidate_core = re.sub(r"[，。！？；：、,.!?;:\"'“”‘’`]", "", candidate_text)
    if source_core == candidate_core:
        return True
    similarity = difflib.SequenceMatcher(None, source_text, candidate_text).ratio()
    units = subtitle_display_units(source_text)
    if units <= 6:
        return similarity >= 0.995
    if units <= 10:
        return similarity >= 0.99
    return similarity >= 0.985


def diversify_narration_locally(source: str, budget: int) -> str:
    normalized = cleanup_rewrite_text(source)
    if not normalized:
        return ""

    candidates: List[str] = []
    replacement_rules = [
        ("没想到", "谁知道"),
        ("可这时", "偏偏这时"),
        ("紧接着", "下一秒"),
        ("随后", "接着"),
        ("随即", "立马"),
        ("于是男人", "男人转头"),
        ("于是", "这才"),
        ("介绍给了", "带去见了"),
        ("看到", "看见"),
    ]
    for old, new in replacement_rules:
        if old not in normalized:
            continue
        candidate = normalize_spoken_narration_text(normalized.replace(old, new, 1))
        if not candidate or candidate == normalized:
            continue
        if subtitle_display_units(candidate) > budget:
            continue
        candidates.append(candidate)

    if normalized.startswith("男人"):
        candidate = normalize_spoken_narration_text("这男人" + normalized[2:])
        if candidate != normalized and subtitle_display_units(candidate) <= budget:
            candidates.append(candidate)
    if normalized.startswith("女人"):
        candidate = normalize_spoken_narration_text("这女人" + normalized[2:])
        if candidate != normalized and subtitle_display_units(candidate) <= budget:
            candidates.append(candidate)
    if normalized.startswith("小女孩"):
        candidate = normalize_spoken_narration_text("这小女孩" + normalized[3:])
        if candidate != normalized and subtitle_display_units(candidate) <= budget:
            candidates.append(candidate)
    if normalized.startswith("小男孩"):
        candidate = normalize_spoken_narration_text("这小男孩" + normalized[3:])
        if candidate != normalized and subtitle_display_units(candidate) <= budget:
            candidates.append(candidate)
    if normalized.startswith("两人"):
        candidate = normalize_spoken_narration_text("这两人" + normalized[2:])
        if candidate != normalized and subtitle_display_units(candidate) <= budget:
            candidates.append(candidate)
    if normalized.startswith("可") and not normalized.startswith("可这时"):
        candidate = normalize_spoken_narration_text("偏偏" + normalized[1:])
        if candidate != normalized and subtitle_display_units(candidate) <= budget:
            candidates.append(candidate)

    best_candidate = ""
    best_similarity = 1.0
    for candidate in candidates:
        similarity = rewrite_similarity(source, candidate)
        if similarity < best_similarity:
            best_candidate = candidate
            best_similarity = similarity
    return best_candidate


def build_local_rewrite_map(entries: Sequence[SubtitleEntry]) -> Dict[int, str]:
    rewrite_map: Dict[int, str] = {}
    for entry in entries:
        budget = subtitle_char_budget(max(0.1, entry.end - entry.start))
        _, speech_budget = narration_rewrite_speech_budgets(entry)
        candidate = diversify_narration_locally(entry.text, budget)
        if not candidate:
            continue
        candidate = prefer_complete_narration_text(entry.text, candidate)
        candidate = fit_rewrite_candidate_to_timing(
            entry,
            candidate,
            display_budget=budget,
            speech_budget=speech_budget,
        )
        if not candidate or rewrite_needs_more_variation(entry.text, candidate):
            continue
        rewrite_map[entry.index] = candidate
    return rewrite_map


def request_rewrite_batch(
    ai_generator: "AINarrationGenerator",
    previous_context: Sequence[Dict[str, object]],
    payload_entries: Sequence[Dict[str, object]],
    next_context: Sequence[Dict[str, object]],
    *,
    force_variation: bool = False,
    log_func: Optional[Callable[[str], None]] = None,
) -> List[Dict[str, object]]:
    if not payload_entries or not ai_generator.api_key:
        return []

    if force_variation:
        system_prompt = (
            "You lightly rewrite Chinese short-drama narration lines. "
            "Return JSON only. "
            "Every rewrite must preserve the same story beat, logic, and cause-effect order. "
            "Keep the same sentence boundary for each item and do not merge adjacent lines. "
            "Make a mild natural paraphrase in spoken short-drama Chinese. "
            "It does not need to be aggressively different from the source, but it must not be identical or punctuation-only. "
            "Use short, colloquial everyday Chinese. Prefer plain wording such as 男人/女人/小女孩/家里/就是. "
            "Avoid literary or costume-drama wording like 公子/女子/乃/家中/其/便 unless it is a fixed proper noun that cannot be replaced. "
            "If the source line contains an obvious OCR or ASR mistake, broken collocation, or missing keyword that can be resolved from nearby context, silently fix it before rewriting. "
            "Every line must be complete, speakable, and smooth on its own, never cut in the middle. "
            "If the source line is slightly fragmented, rewrite it into a complete standalone line with the smallest possible change, without stealing later dialogue or merging adjacent lines. "
            "Do not end a line on a transition or lead-in such as 随后/这时/下一秒/原来/结果. "
            "If one line is only a lead-in like '告诉他/问她/说道/答道', do not absorb the following dialogue content into this line. "
            "Keep the spoken length close to the source and reference timing. "
            "preferred_speech_units is the target spoken length, and max_speech_units is the hard upper limit. "
            "If timing is tight, tighten the wording instead of adding explanation or filler. "
            "Stay within max_chars. "
            "Do not add timestamps, numbering, explanations, markdown, or code fences. "
            "JSON format: {\"entries\":[{\"index\":1,\"rewrite\":\"...\"}]}"
        )
        user_prompt = (
            "The previous rewrite stayed too close to the source. Rewrite each item below with a slightly clearer paraphrase while preserving the same meaning, sentence boundary, index, max_chars, and max_speech_units. "
            "Return JSON only.\n\n"
            f"{json.dumps({'previous_context': list(previous_context), 'entries': list(payload_entries), 'next_context': list(next_context)}, ensure_ascii=False, indent=2)}"
        )
        temperature = 0.45
    else:
        system_prompt = (
            "You lightly rewrite Chinese short-drama narration lines. "
            "Return JSON only. "
            "Each rewrite must keep the same story beat, keep adjacent lines logically connected, and stay within max_chars. "
            "Keep the same sentence boundary for each item and do not merge or split lines. "
            "Use short, colloquial everyday Chinese for short-drama commentary. "
            "Prefer plain wording such as 男人/女人/小女孩/家里/就是, and avoid literary or costume-drama wording like 公子/女子/乃/家中/其/便 unless it is a fixed proper noun. "
            "If the source line contains an obvious OCR or ASR mistake, broken collocation, or missing keyword that can be resolved from nearby context, silently fix it before rewriting. "
            "Every line must be complete, speakable, and smooth on its own, never cut in the middle. "
            "If the source line is slightly fragmented, rewrite it into a complete standalone line with the smallest possible change, without stealing later dialogue or merging adjacent lines. "
            "Do not end a line on a transition or lead-in such as 随后/这时/下一秒/原来/结果. "
            "Keep who is speaking, who is being talked about, and the cause-effect order consistent with the source. "
            "If one line is only a lead-in like '告诉他/问她/说道/答道', do not absorb the following dialogue content into this line. "
            "Keep the spoken length close to the source and reference timing. "
            "preferred_speech_units is the target spoken length, and max_speech_units is the hard upper limit. "
            "If timing is tight, prefer shorter wording over explanatory polish, and do not add filler transitions. "
            "Prefer concise spoken-Chinese commentary, but do not force a big rewrite when a light rewrite is enough. "
            "Do not add timestamps, numbering, explanations, markdown, or code fences. "
            "JSON format: {\"entries\":[{\"index\":1,\"rewrite\":\"...\"}]}"
        )
        user_prompt = (
            "Rewrite each item below. Keep the same index. "
            "Use nearby lines as context so the sequence stays coherent. "
            "The rewrite should preserve meaning, stay smooth and colloquial, and keep the original sentence boundary. "
            "A light natural rewrite is enough; do not over-chase de-duplication. Stay close to preferred_speech_units and never exceed max_speech_units. "
            "Avoid vague jumps between adjacent lines.\n\n"
            f"{json.dumps({'previous_context': list(previous_context), 'entries': list(payload_entries), 'next_context': list(next_context)}, ensure_ascii=False, indent=2)}"
        )
        temperature = 0.3

    try:
        response = requests.post(
            ai_generator.api_url,
            headers={
                "Authorization": f"Bearer {ai_generator.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": ai_generator.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": temperature,
                "max_tokens": 8192,
            },
            timeout=240,
        )
    except Exception as exc:
        label = "AI rewrite retry request failed" if force_variation else "AI rewrite request failed"
        ai_generator.note_rewrite_issue(
            f"{label}: {type(exc).__name__}: {exc}",
            log_func=log_func,
        )
        return []

    try:
        response.raise_for_status()
    except Exception as exc:
        label = "AI rewrite retry HTTP error" if force_variation else "AI rewrite HTTP error"
        body_preview = summarize_for_log(response.text)
        ai_generator.note_rewrite_issue(
            f"{label}: status {response.status_code}, body {body_preview or '<empty>'}",
            log_func=log_func,
        )
        return []

    try:
        response_payload = response.json()
    except Exception as exc:
        label = "AI rewrite retry invalid JSON response" if force_variation else "AI rewrite invalid JSON response"
        ai_generator.note_rewrite_issue(
            f"{label}: {type(exc).__name__}: {exc}; body {summarize_for_log(response.text) or '<empty>'}",
            log_func=log_func,
        )
        return []

    payload = extract_chat_message_text(response_payload)
    if not payload:
        label = "AI rewrite retry empty response" if force_variation else "AI rewrite empty response"
        ai_generator.note_rewrite_issue(
            f"{label}: {summarize_for_log(response_payload) or '<empty>'}",
            log_func=log_func,
        )
        return []

    parsed = extract_json_object(payload)
    if isinstance(parsed, dict) and isinstance(parsed.get("entries"), list):
        return list(parsed["entries"])

    label = "AI rewrite retry parse miss" if force_variation else "AI rewrite parse miss"
    ai_generator.note_rewrite_issue(
        f"{label}: {summarize_for_log(payload) or '<empty>'}",
        log_func=log_func,
    )
    return []


def clone_subtitle_entry(
    entry: SubtitleEntry,
    *,
    index: Optional[int] = None,
    start: Optional[float] = None,
    end: Optional[float] = None,
    text: Optional[str] = None,
    entry_type: Optional[str] = None,
) -> SubtitleEntry:
    return SubtitleEntry(
        index=entry.index if index is None else index,
        start=entry.start if start is None else start,
        end=entry.end if end is None else end,
        text=entry.text if text is None else text,
        entry_type=entry.entry_type if entry_type is None else entry_type,
    )


def speech_intro_score(text: str) -> int:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return 0

    score = 0
    if SPEECH_INTRO_TAIL_RE.search(normalized):
        score += 2
    if re.search(r"(他说|她说|男人说|女人说|女孩说|男孩说|对他说|对她说|冲她说|冲他说)", normalized):
        score += 1
    return score


def probably_incomplete_text(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return True
    if speech_intro_score(normalized) >= 2:
        return False
    if FRAGMENT_TAIL_RE.search(normalized):
        return True
    if INCOMPLETE_TAIL_RE.search(normalized) and subtitle_display_units(normalized) >= 6:
        return True
    return False


def prefer_complete_narration_text(source_text: str, candidate_text: str) -> str:
    source = normalize_spoken_narration_text(source_text)
    candidate = normalize_spoken_narration_text(candidate_text)
    if not candidate:
        return source
    source_incomplete = probably_incomplete_text(source)
    candidate_incomplete = probably_incomplete_text(candidate)
    source_units = subtitle_speech_units(source)
    candidate_units = subtitle_speech_units(candidate)
    if source and source_units <= 12 and candidate_units < source_units:
        return source
    if candidate_incomplete and not source_incomplete:
        return source
    if source_incomplete and candidate_incomplete:
        if candidate_units + 1 < source_units:
            return source
    return candidate


def break_positions(text: str) -> List[int]:
    normalized = normalize_subtitle_text(text)
    positions = set()
    for match in re.finditer(r"[，,。！？!?；;：:、…]+", normalized):
        positions.add(match.end())
    for match in READABLE_BREAK_RE.finditer(normalized):
        if 2 < match.start() < len(normalized) - 2:
            positions.add(match.start())
    return sorted(positions)


def split_text_at_budget(text: str, max_units: int, min_units: int = 4) -> List[str]:
    normalized = normalize_subtitle_text(text)
    if not normalized or subtitle_display_units(normalized) <= max_units:
        return [normalized] if normalized else []

    positions = break_positions(normalized)
    best_pos: Optional[int] = None
    best_distance: Optional[int] = None
    target_units = min(max_units, max(min_units, subtitle_display_units(normalized) // 2))
    for pos in positions:
        left = normalize_subtitle_text(normalized[:pos])
        right = normalize_subtitle_text(normalized[pos:])
        left_units = subtitle_display_units(left)
        right_units = subtitle_display_units(right)
        if left_units < min_units or right_units < min_units:
            continue
        if left_units > max_units:
            continue
        distance = abs(left_units - target_units)
        if best_distance is None or distance < best_distance:
            best_pos = pos
            best_distance = distance

    if best_pos is None or best_pos <= 0 or best_pos >= len(normalized):
        return [normalized]

    left_text = normalize_subtitle_text(normalized[:best_pos])
    right_text = normalize_subtitle_text(normalized[best_pos:])
    if not left_text or not right_text:
        return [trim_text_to_units(normalized, max_units)]

    pieces: List[str] = []
    for chunk in (left_text, right_text):
        if subtitle_display_units(chunk) > max_units:
            pieces.extend(split_text_at_budget(chunk, max_units, min_units=min_units))
        else:
            pieces.append(chunk)
    return [piece for piece in pieces if piece]


def split_entry_for_readability(entry: SubtitleEntry) -> List[SubtitleEntry]:
    duration = max(0.1, entry.end - entry.start)
    normalized = normalize_subtitle_text(entry.text)
    if not normalized:
        return []

    budget = subtitle_char_budget(duration)
    units = subtitle_display_units(normalized)
    incomplete = probably_incomplete_text(normalized)
    if (
        entry.entry_type == "narration"
        and not incomplete
        and units <= min(MAX_SUBTITLE_CHARS, budget + 2)
    ):
        return [clone_subtitle_entry(entry, text=normalized)]
    if entry.entry_type == "narration" and not incomplete and not break_positions(normalized):
        return [clone_subtitle_entry(entry, text=normalized)]
    should_try_split = (
        entry.entry_type == "narration"
        and (
            (
                units > MAX_SUBTITLE_CHARS
                and bool(break_positions(normalized))
            )
            or (
                units >= max(14, budget - 1)
                and duration >= 3.0
                and bool(break_positions(normalized))
            )
            or incomplete
        )
    )
    if not should_try_split:
        return [clone_subtitle_entry(entry, text=normalized)]

    split_budget = max(MIN_SUBTITLE_CHARS, min(MAX_SUBTITLE_CHARS, budget))
    if incomplete and bool(break_positions(normalized)):
        split_budget = min(split_budget, 16)
    parts = split_text_at_budget(normalized, max_units=split_budget)
    if len(parts) <= 1:
        return [clone_subtitle_entry(entry, text=normalized)]
    if any(probably_incomplete_text(part) for part in parts):
        return [clone_subtitle_entry(entry, text=normalized)]

    total_units = sum(max(1, subtitle_speech_units(part)) for part in parts)
    cursor = entry.start
    split_entries: List[SubtitleEntry] = []
    for idx, part in enumerate(parts):
        if idx == len(parts) - 1:
            part_end = entry.end
        else:
            part_units = max(1, subtitle_speech_units(part))
            part_span = duration * part_units / max(1, total_units)
            part_end = min(entry.end, max(cursor + 0.20, cursor + part_span))
        split_entries.append(
            SubtitleEntry(
                index=entry.index,
                start=cursor,
                end=part_end,
                text=part,
                entry_type=entry.entry_type,
            )
        )
        cursor = part_end
    if split_entries:
        split_entries[-1] = clone_subtitle_entry(split_entries[-1], end=entry.end)
    return split_entries


def expand_entries_for_readability(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    expanded: List[SubtitleEntry] = []
    for entry in entries:
        pieces = split_entry_for_readability(entry)
        if pieces:
            expanded.extend(pieces)
    return [
        SubtitleEntry(
            index=index,
            start=item.start,
            end=item.end,
            text=item.text,
            entry_type=item.entry_type,
        )
        for index, item in enumerate(expanded, start=1)
    ]


def merge_rendered_entries(
    timeline_entries: Sequence[SubtitleEntry],
    rendered_entries: Sequence[SubtitleEntry],
) -> List[SubtitleEntry]:
    rendered_map = {entry.index: entry for entry in rendered_entries}
    merged: List[SubtitleEntry] = []
    for entry in timeline_entries:
        merged.append(rendered_map.get(entry.index, entry))
    return merged


def normalize_delivery_subtitle_timeline(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    if not entries:
        return []

    normalized_entries: List[SubtitleEntry] = []
    for entry in entries:
        start = max(0.0, float(entry.start))
        end = max(start + 0.05, float(entry.end))
        normalized_entries.append(clone_subtitle_entry(entry, start=start, end=end))

    for index, entry in enumerate(normalized_entries):
        if index + 1 >= len(normalized_entries):
            continue
        next_entry = normalized_entries[index + 1]
        next_start = max(0.0, float(next_entry.start))
        end = float(entry.end)
        if end > next_start - DELIVERY_SUBTITLE_MIN_GAP_SECONDS:
            clipped_end = max(float(entry.start) + 0.01, next_start - DELIVERY_SUBTITLE_MIN_GAP_SECONDS)
            normalized_entries[index] = clone_subtitle_entry(entry, end=min(end, clipped_end))
    return normalized_entries


def build_delivery_subtitle_entries(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    delivered: List[SubtitleEntry] = []
    visible_entries = normalize_delivery_subtitle_timeline(
        [
            entry
            for entry in entries
            if entry.entry_type != "watermark" and normalize_subtitle_text(entry.text)
        ]
    )
    visible_entries = [
        entry
        for entry in visible_entries
        if entry.end > entry.start + 0.01
    ]
    for index, entry in enumerate(visible_entries, start=1):
        delivered.append(
            SubtitleEntry(
                index=index,
                start=entry.start,
                end=entry.end,
                text=normalize_subtitle_text(entry.text),
                entry_type=entry.entry_type,
            )
        )
    return delivered


def build_subtitle_burn_layout(
    video_width: int,
    video_height: int,
    subtitle_region: Optional[VideoMaskRegion] = None,
) -> Dict[str, object]:
    default_font_size = int(
        round(clamp(video_height * 0.041, SUBTITLE_BURN_MIN_FONT_SIZE, SUBTITLE_BURN_MAX_FONT_SIZE))
    )
    if subtitle_region is not None:
        region_x = max(0, min(video_width - 1, int(subtitle_region.x)))
        region_y = max(0, min(video_height - 1, int(subtitle_region.y)))
        region_width = max(1, min(video_width - region_x, int(subtitle_region.width)))
        region_height = max(1, min(video_height - region_y, int(subtitle_region.height)))
        inner_pad_x = max(10, int(round(region_width * 0.04)))
        inner_pad_y = max(8, int(round(region_height * 0.12)))
        min_visible_width = max(220, int(round(video_width * 0.26)))
        visible_width = max(min_visible_width, region_width - inner_pad_x * 2)
        visible_width = min(video_width, visible_width)
        margin_l = max(0, int(round((video_width - visible_width) / 2.0)))
        margin_r = margin_l
        bottom_gap = max(0, video_height - (region_y + region_height))
        margin_v = max(6, bottom_gap + inner_pad_y)
        font_size = default_font_size
    else:
        margin_l = max(36, int(round(video_width * 0.08)))
        margin_r = margin_l
        margin_v = max(30, int(round(video_height * 0.08)))
        font_size = default_font_size

    outline = clamp(font_size * 0.055, 1.6, 3.2)
    return {
        "font_name": SUBTITLE_BURN_FONT_NAME,
        "font_size": font_size,
        "margin_l": margin_l,
        "margin_r": margin_r,
        "margin_v": margin_v,
        "outline": outline,
    }


def build_subtitle_burn_style(
    video_width: int,
    video_height: int,
    subtitle_region: Optional[VideoMaskRegion] = None,
) -> str:
    layout = build_subtitle_burn_layout(video_width, video_height, subtitle_region)
    style_items = [
        f"FontName={layout['font_name']}",
        f"FontSize={layout['font_size']}",
        "Alignment=2",
        f"MarginL={layout['margin_l']}",
        f"MarginR={layout['margin_r']}",
        f"MarginV={layout['margin_v']}",
        "PrimaryColour=&H00FFFFFF",
        "SecondaryColour=&H00FFFFFF",
        "OutlineColour=&H00303030",
        "BackColour=&H00000000",
        "BorderStyle=1",
        f"Outline={float(layout['outline']):.2f}",
        "Shadow=0",
        "Bold=1",
        "WrapStyle=2",
    ]
    return ",".join(style_items)


def seconds_to_ass_time_text(sec: float) -> str:
    total_cs = int(round(max(0.0, sec) * 100))
    total_s, cs = divmod(total_cs, 100)
    hours, rem = divmod(total_s, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{seconds:02d}.{cs:02d}"


def escape_ass_dialogue_text(text: str) -> str:
    normalized = normalize_subtitle_text(text)
    normalized = normalized.replace("\\", r"\\")
    normalized = normalized.replace("{", "(").replace("}", ")")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    return normalized.replace("\n", r"\N")


def entries_to_ass(
    entries: Sequence[SubtitleEntry],
    video_width: int,
    video_height: int,
    subtitle_region: Optional[VideoMaskRegion] = None,
) -> str:
    layout = build_subtitle_burn_layout(video_width, video_height, subtitle_region)
    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {video_width}",
        f"PlayResY: {video_height}",
        "WrapStyle: 2",
        "ScaledBorderAndShadow: yes",
        "",
        "[V4+ Styles]",
        "Format: Name,Fontname,Fontsize,PrimaryColour,SecondaryColour,OutlineColour,BackColour,"
        "Bold,Italic,Underline,StrikeOut,ScaleX,ScaleY,Spacing,Angle,BorderStyle,Outline,Shadow,"
        "Alignment,MarginL,MarginR,MarginV,Encoding",
        (
            "Style: Default,"
            f"{layout['font_name']},"
            f"{layout['font_size']},"
            "&H00FFFFFF,&H00FFFFFF,&H00303030,&H00000000,"
            "-1,0,0,0,100,100,0,0,1,"
            f"{float(layout['outline']):.2f},"
            "0,2,"
            f"{layout['margin_l']},"
            f"{layout['margin_r']},"
            f"{layout['margin_v']},"
            "1"
        ),
        "",
        "[Events]",
        "Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text",
    ]
    for entry in entries:
        if entry.end <= entry.start + 0.01:
            continue
        text = escape_ass_dialogue_text(entry.text)
        if not text:
            continue
        lines.append(
            "Dialogue: 0,"
            f"{seconds_to_ass_time_text(entry.start)},"
            f"{seconds_to_ass_time_text(entry.end)},"
            f"Default,,0,0,0,,{text}"
        )
    return "\n".join(lines) + "\n"


def watermark_like_text(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    lowered = normalized.lower()
    if not normalized:
        return False

    keywords = (
        "抖音",
        "douyin",
        "快手",
        "小红书",
        "b站",
        "bilibili",
        "点击头像",
        "点我头像",
        "右下角",
        "左下角",
        "关注",
        "点赞",
        "收藏",
        "转发",
        "评论区",
        "主页",
        "进直播间",
        "直播间",
        "搜剧名",
        "完整版",
        "上集",
        "下集",
    )
    if any(keyword in normalized or keyword in lowered for keyword in keywords):
        return True
    if re.search(r"第\s*\d+\s*[集话季]", normalized):
        return True
    return False


def narration_context_signal_score(text: str) -> int:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return 0

    score = 0
    if NARRATION_SUBJECT_RE.search(normalized):
        score += 1
    if NARRATION_CONNECTOR_RE.search(normalized) or starts_with_strong_transition(normalized):
        score += 1
    if NARRATION_ACTION_RE.search(normalized):
        score += 1
    if probably_incomplete_text(normalized):
        score += 1
    return score


def dialogue_score(text: str) -> int:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return 0

    score = 0
    if any(ch in normalized for ch in ('"', "'", "“", "”", "「", "」", "『", "』")):
        score += 3
    if re.search(r"(吗|呢|吧|啊|呀|哎|诶|欸|喂)[。！？!?]?$", normalized):
        score += 2
    if re.search(
        r"(?:^|[，。！？!?])(?:你|您|我|我们|你们)(?:竟|居然|怎么|为何|凭什么|还|也|都|真|是|要|会|敢|能|认识|知道|找|跟)",
        normalized,
    ):
        score += 2
    if sum(normalized.count(token) for token in ("我", "你", "我们", "你们", "爸爸", "妈妈", "奶奶", "爷爷")) >= 2:
        score += 1
    elif subtitle_display_units(normalized) <= 12 and re.search(r"(?:我|你|您)", normalized):
        score += 1
    if normalized.startswith(("别", "快", "来", "走", "听", "看", "妈", "爸", "奶奶", "爷爷", "叔叔", "阿姨")):
        score += 1
    if subtitle_display_units(normalized) <= 14 and IMPERATIVE_DIALOGUE_RE.search(normalized):
        score += 2
    return score


def narration_score(text: str) -> int:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return 0

    score = 0
    if re.search(
        r"(男人|女人|小姑娘|小丫头|老太太|老爷子|众人|这时|随后|下一秒|谁知|只见|眼看|原来|结果|听见|看到|见她|紧接着|此时|竟然|直接|立马)",
        normalized,
    ):
        score += 2
    if re.search(r"(他|她|他们|她们)[^我你]{0,6}(说|看|听|发现|觉得|带|抱|问|转身|决定)", normalized):
        score += 1
    if re.search(r"(可怜|心疼|震惊|没想到|顿时|当场|立刻|果然)", normalized):
        score += 1
    context_score = narration_context_signal_score(normalized)
    if context_score >= 3:
        score += 2
    elif context_score >= 1:
        score += 1
    return score


def heuristic_entry_type(text: str) -> Optional[str]:
    if watermark_like_text(text):
        return "watermark"

    dialogue = dialogue_score(text)
    narration = narration_score(text)
    if dialogue >= narration + 2:
        return "dialogue"
    if narration >= dialogue + 1:
        return "narration"
    return None


def original_subtitle_score(text: str) -> int:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return 0

    score = 0
    if re.search(r"(?:\u4e09\u5e74\u540e|\u4e94\u5e74\u540e|\u5341\u5206\u949f\u540e|\u6b64\u540c\u65f6|\u53e6\u4e00\u8fb9|\u540c\u4e00\u65f6\u95f4)", normalized):
        score += 2
    if (
        re.search(
            r"(?:\u67d0\u67d0\u533b\u9662|\u67d0\u67d0\u522b\u5885|\u95e8\u53e3|\u697c\u4e0b|\u697c\u4e0a|\u5ba2\u5385|\u75c5\u623f|\u52de\u623f|\u6c11\u653f\u5c40|\u9152\u5e97|\u5b66\u6821)",
            normalized,
        )
        and subtitle_display_units(normalized) <= 8
        and not re.search(
            r"(?:\u6765\u5230|\u8d70\u5230|\u56de\u5230|\u8fdb\u5165|\u51fa\u73b0|\u5728|\u53bb|\u770b\u5230|\u649e\u89c1|\u8d76\u5230|\u8d70\u8fdb)",
            normalized,
        )
    ):
        score += 2
    if re.search(r"(?:\u7b2c\s*\d+\s*[\u5929\u5e74\u6708\u96c6\u8bdd]|\d+\s*:\s*\d+)", normalized):
        score += 2
    if re.search(r"(?:\u65e9\u4e0a|\u4e0a\u5348|\u4e2d\u5348|\u4e0b\u5348|\u665a\u4e0a|\u51cc\u6668|\u50cd\u665a|\u6df1\u591c)\s*[一二三四五六七八九十两\d]+\s*[\u70b9\u65f6]", normalized):
        score += 2
    if (
        subtitle_display_units(normalized) <= 4
        and not CJK_RE.search(normalized)
        and dialogue_score(normalized) == 0
        and narration_context_signal_score(normalized) == 0
    ):
        score += 1
    return score


def dialogue_like_text(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return False
    if dialogue_score(normalized) >= 2:
        return True
    if re.search(
        r"(?:^|[，。！？!?])(?:你|您|我|我们|你们)(?:竟|居然|怎么|为何|凭什么|还|也|都|真|是|要|会|敢|能|认识|知道|找|跟)",
        normalized,
    ):
        return True
    if re.search(r"(?:\u6211|\u4f60|\u60a8|\u6211\u4eec|\u4f60\u4eec|\u7238\u7238|\u5988\u5988|\u5976\u5976|\u7237\u7237)", normalized):
        return True
    return False


def strong_narration_text(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    return narration_score(normalized) >= 2


def narration_fragment_candidate(text: str) -> bool:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return False
    if watermark_like_text(normalized) or dialogue_like_text(normalized):
        return False
    if original_subtitle_score(normalized) >= 2:
        return False

    units = subtitle_display_units(normalized)
    signal_score = narration_context_signal_score(normalized)
    if signal_score >= 2:
        return True
    if units <= 10 and (
        probably_incomplete_text(normalized)
        or NARRATION_SUBJECT_RE.search(normalized)
        or NARRATION_CONNECTOR_RE.search(normalized)
    ):
        return True
    return False


def whole_text_classification_scores(
    entry: SubtitleEntry,
    prev_entry: Optional[SubtitleEntry],
    next_entry: Optional[SubtitleEntry],
    *,
    trust_existing_type: bool = False,
) -> Dict[str, float]:
    text = normalize_subtitle_text(entry.text)
    scores = {
        "narration": 0.35,
        "dialogue": 0.10,
        "original_subtitle": -0.10,
        "watermark": -8.0,
    }
    if not text:
        scores["original_subtitle"] += 1.0
        return scores

    if watermark_like_text(text):
        scores["watermark"] = 18.0
        scores["narration"] -= 5.0
        scores["dialogue"] -= 5.0
        scores["original_subtitle"] -= 5.0
        return scores

    dialogue = dialogue_score(text)
    narration = narration_score(text)
    original = original_subtitle_score(text)
    speech_intro = speech_intro_score(text)
    units = subtitle_display_units(text)
    is_dialogue_like = dialogue_like_text(text)
    is_strong_narration = strong_narration_text(text)
    narration_signal = narration_context_signal_score(text)
    is_narration_fragment = narration_fragment_candidate(text)

    scores["dialogue"] += dialogue * 2.4
    scores["narration"] += narration * 2.2
    scores["original_subtitle"] += original * 2.6

    if is_dialogue_like:
        scores["dialogue"] += 1.8
        scores["narration"] -= 1.3
        scores["original_subtitle"] -= 1.1
    if is_strong_narration:
        scores["narration"] += 1.8
        scores["dialogue"] -= 1.1
        scores["original_subtitle"] -= 0.8
    if is_narration_fragment and not is_dialogue_like:
        scores["narration"] += 1.3 + min(1.0, 0.35 * max(0, narration_signal - 1))
        scores["original_subtitle"] -= 0.9
    if starts_with_strong_transition(text):
        scores["narration"] += 0.8
    if speech_intro >= 2:
        scores["narration"] += 0.9
        scores["dialogue"] -= 0.4
    if original >= 2 and not is_dialogue_like:
        scores["original_subtitle"] += 1.6
    if units <= 4 and dialogue == 0 and narration == 0 and original > 0 and narration_signal == 0:
        scores["original_subtitle"] += 0.35
    if original > 0:
        scores["narration"] -= original * 0.9
        if not is_dialogue_like:
            scores["dialogue"] -= original * 1.2

    prev_text = normalize_subtitle_text(prev_entry.text) if prev_entry else ""
    next_text = normalize_subtitle_text(next_entry.text) if next_entry else ""
    prev_dialogue = bool(prev_text) and dialogue_like_text(prev_text)
    next_dialogue = bool(next_text) and dialogue_like_text(next_text)
    prev_narration = bool(prev_text) and strong_narration_text(prev_text)
    next_narration = bool(next_text) and strong_narration_text(next_text)
    prev_narrationish = prev_narration or (bool(prev_text) and narration_fragment_candidate(prev_text))
    next_narrationish = next_narration or (bool(next_text) and narration_fragment_candidate(next_text))
    prev_original = bool(prev_text) and original_subtitle_score(prev_text) >= 2
    next_original = bool(next_text) and original_subtitle_score(next_text) >= 2

    if prev_entry and speech_intro_score(prev_entry.text) >= 2 and original <= 1:
        scores["dialogue"] += 1.6
        scores["narration"] -= 0.4
    if prev_dialogue and next_dialogue and not is_strong_narration:
        scores["dialogue"] += 1.2
    if prev_narration and next_narration and not is_dialogue_like:
        scores["narration"] += 1.0
    if prev_narrationish and next_narrationish and not is_dialogue_like and original < 2:
        scores["narration"] += 1.4
        scores["original_subtitle"] -= 1.1
    elif (prev_narrationish or next_narrationish) and is_narration_fragment and not is_dialogue_like and original < 2:
        scores["narration"] += 0.8
        scores["original_subtitle"] -= 0.6
    if prev_original and next_original and original >= 1 and not is_dialogue_like and not is_narration_fragment:
        scores["original_subtitle"] += 1.2

    if trust_existing_type and entry.entry_type in scores:
        scores[entry.entry_type] += 1.5

    return scores


def whole_text_transition_score(
    previous_type: str,
    current_type: str,
    previous_entry: Optional[SubtitleEntry],
    current_entry: SubtitleEntry,
) -> float:
    score = 0.85 if previous_type == current_type else -0.55
    current_text = normalize_subtitle_text(current_entry.text)
    if previous_type == current_type:
        if current_type == "dialogue":
            score += 0.35
        elif current_type == "original_subtitle":
            score += 0.45
        elif current_type == "narration":
            score += 0.20
    if previous_entry is not None and speech_intro_score(previous_entry.text) >= 2 and current_type == "dialogue":
        score += 1.8
    if starts_with_strong_transition(current_text) and current_type == "narration":
        score += 0.8
    if previous_type == "dialogue" and current_type == "dialogue" and dialogue_like_text(current_text):
        score += 0.6
    if previous_type == "original_subtitle" and current_type == "original_subtitle" and original_subtitle_score(current_text) >= 1:
        score += 0.6
    if previous_type == "dialogue" and current_type == "original_subtitle":
        score -= 0.5
    if previous_type == "original_subtitle" and current_type == "dialogue":
        score -= 0.5
    return score


def smooth_isolated_classification_runs(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    if len(entries) < 3:
        return list(entries)

    smoothed = list(entries)
    for index in range(1, len(entries) - 1):
        prev_type = entries[index - 1].entry_type
        current = entries[index]
        next_type = entries[index + 1].entry_type
        if prev_type != next_type or current.entry_type == prev_type:
            continue

        text = normalize_subtitle_text(current.text)
        replacement: Optional[str] = None
        if prev_type == "dialogue" and not strong_narration_text(text) and original_subtitle_score(text) <= 1:
            replacement = "dialogue"
        elif prev_type == "original_subtitle" and original_subtitle_score(text) >= 1 and not dialogue_like_text(text):
            replacement = "original_subtitle"
        elif prev_type == "narration" and not dialogue_like_text(text):
            replacement = "narration"

        if replacement:
            smoothed[index] = clone_subtitle_entry(current, entry_type=replacement)
    return smoothed


def classify_entries_with_whole_text_context(
    entries: Sequence[SubtitleEntry],
    *,
    trust_existing_type: bool = False,
) -> List[SubtitleEntry]:
    if not entries:
        return []

    states = ("narration", "dialogue", "original_subtitle", "watermark")
    emissions: List[Dict[str, float]] = []
    dp: List[Dict[str, float]] = []
    backtrack: List[Dict[str, Optional[str]]] = []

    for index, entry in enumerate(entries):
        prev_entry = entries[index - 1] if index > 0 else None
        next_entry = entries[index + 1] if index + 1 < len(entries) else None
        emission = whole_text_classification_scores(
            entry,
            prev_entry,
            next_entry,
            trust_existing_type=trust_existing_type,
        )
        emissions.append(emission)

        current_scores: Dict[str, float] = {}
        current_backtrack: Dict[str, Optional[str]] = {}
        for state in states:
            emission_score = emission.get(state, -9.0)
            if index == 0:
                current_scores[state] = emission_score
                current_backtrack[state] = None
                continue

            best_score: Optional[float] = None
            best_previous: Optional[str] = None
            previous_entry = entries[index - 1]
            for previous_state in states:
                candidate = dp[index - 1][previous_state] + whole_text_transition_score(
                    previous_state,
                    state,
                    previous_entry,
                    entry,
                )
                if best_score is None or candidate > best_score:
                    best_score = candidate
                    best_previous = previous_state

            current_scores[state] = emission_score + (best_score if best_score is not None else 0.0)
            current_backtrack[state] = best_previous

        dp.append(current_scores)
        backtrack.append(current_backtrack)

    best_state = max(states, key=lambda state: dp[-1][state])
    assigned_states = [best_state]
    for index in range(len(entries) - 1, 0, -1):
        previous_state = backtrack[index].get(assigned_states[-1])
        assigned_states.append(previous_state or states[0])
    assigned_states.reverse()

    classified = [
        clone_subtitle_entry(entry, entry_type=entry_type)
        for entry, entry_type in zip(entries, assigned_states)
    ]
    return smooth_isolated_classification_runs(classified)


def strengthen_classification(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    if not entries:
        return []

    strengthened: List[SubtitleEntry] = []
    for index, entry in enumerate(entries):
        prev_entry = entries[index - 1] if index > 0 else None
        next_entry = entries[index + 1] if index + 1 < len(entries) else None
        new_type = entry.entry_type
        text = entry.text

        if new_type == "narration" and dialogue_like_text(text) and not strong_narration_text(text):
            new_type = "dialogue"

        if (
            new_type in {"narration", "original_subtitle"}
            and prev_entry is not None
            and speech_intro_score(prev_entry.text) >= 2
            and not strong_narration_text(text)
            and original_subtitle_score(text) <= 1
        ):
            new_type = "dialogue"

        if new_type == "narration" and original_subtitle_score(text) >= 2 and not dialogue_like_text(text):
            new_type = "original_subtitle"

        if new_type == "original_subtitle" and dialogue_like_text(text) and original_subtitle_score(text) <= 1:
            new_type = "dialogue"

        if new_type == "original_subtitle" and strong_narration_text(text) and original_subtitle_score(text) == 0:
            new_type = "narration"

        if (
            new_type == "narration"
            and prev_entry is not None
            and next_entry is not None
            and prev_entry.entry_type == "dialogue"
            and next_entry.entry_type == "dialogue"
            and not strong_narration_text(text)
        ):
            new_type = "dialogue"

        if (
            new_type == "dialogue"
            and prev_entry is not None
            and next_entry is not None
            and prev_entry.entry_type == "narration"
            and next_entry.entry_type == "narration"
            and strong_narration_text(text)
        ):
            new_type = "narration"

        if (
            new_type == "narration"
            and prev_entry is not None
            and prev_entry.entry_type == "dialogue"
            and not strong_narration_text(text)
            and original_subtitle_score(text) == 0
            and subtitle_display_units(text) <= 16
        ):
            new_type = "dialogue"

        strengthened.append(clone_subtitle_entry(entry, entry_type=new_type))

    stabilized: List[SubtitleEntry] = []
    for index, entry in enumerate(strengthened):
        prev_type = strengthened[index - 1].entry_type if index > 0 else ""
        next_type = strengthened[index + 1].entry_type if index + 1 < len(strengthened) else ""
        new_type = entry.entry_type

        if (
            new_type == "narration"
            and prev_type == "dialogue"
            and next_type == "dialogue"
            and not strong_narration_text(entry.text)
        ):
            new_type = "dialogue"
        elif (
            new_type == "narration"
            and prev_type == "original_subtitle"
            and next_type == "original_subtitle"
            and original_subtitle_score(entry.text) >= 1
            and not strong_narration_text(entry.text)
        ):
            new_type = "original_subtitle"

        stabilized.append(clone_subtitle_entry(entry, entry_type=new_type))

    return stabilized


def subtitle_entry_gap(left: Optional[SubtitleEntry], right: Optional[SubtitleEntry]) -> float:
    if left is None or right is None:
        return 999.0
    return max(0.0, right.start - left.end)


def entry_is_narration_context(entry: Optional[SubtitleEntry]) -> bool:
    if entry is None:
        return False
    if entry.entry_type == "narration":
        return True
    return strong_narration_text(entry.text) or narration_fragment_candidate(entry.text)


def should_recover_original_run_as_narration(
    entries: Sequence[SubtitleEntry],
    start: int,
    end: int,
) -> bool:
    run = list(entries[start:end])
    if not run:
        return False

    texts = [normalize_subtitle_text(entry.text) for entry in run]
    if not any(texts):
        return False
    if any(watermark_like_text(text) for text in texts):
        return False
    if any(original_subtitle_score(text) >= 2 for text in texts):
        return False
    if any(dialogue_score(text) >= 2 for text in texts):
        return False
    if any(dialogue_like_text(text) and narration_context_signal_score(text) == 0 for text in texts):
        return False

    left = entries[start - 1] if start > 0 else None
    right = entries[end] if end < len(entries) else None
    max_context_gap = SHORT_GAP_MERGE_SECONDS + 1.0
    left_narration = entry_is_narration_context(left) and subtitle_entry_gap(left, run[0]) <= max_context_gap
    right_narration = entry_is_narration_context(right) and subtitle_entry_gap(run[-1], right) <= max_context_gap

    joined_text = normalize_subtitle_text("".join(texts))
    signal_score = narration_context_signal_score(joined_text)
    fragment_count = sum(1 for text in texts if narration_fragment_candidate(text) or probably_incomplete_text(text))
    neutral_short_count = sum(
        1
        for text in texts
        if subtitle_display_units(text) <= 10 and original_subtitle_score(text) < 2 and not dialogue_like_text(text)
    )
    run_duration = max(0.0, run[-1].end - run[0].start)

    if left_narration and right_narration:
        return (
            signal_score >= 1
            or fragment_count >= max(1, len(run) // 2)
            or (neutral_short_count == len(run) and run_duration <= 8.0)
        )
    if right_narration and start == 0:
        return signal_score >= 1 or fragment_count >= 1 or run_duration <= 4.0
    if left_narration or right_narration:
        return signal_score >= 2 or (signal_score >= 1 and fragment_count >= 1)
    return False


def recover_narration_fragment_runs(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    if not entries:
        return []

    recovered = list(entries)
    index = 0
    while index < len(recovered):
        if recovered[index].entry_type != "original_subtitle":
            index += 1
            continue

        start = index
        while index < len(recovered) and recovered[index].entry_type == "original_subtitle":
            index += 1
        end = index

        if should_recover_original_run_as_narration(recovered, start, end):
            for run_index in range(start, end):
                recovered[run_index] = clone_subtitle_entry(recovered[run_index], entry_type="narration")

    return recovered


def classify_entries_locally(entries: Sequence[SubtitleEntry]) -> Dict[str, List[Dict[str, str]]]:
    if not entries:
        return {"entries": []}

    staged_entries: List[SubtitleEntry] = []
    for entry in entries:
        corrected = normalize_subtitle_text(entry.text)
        entry_type = heuristic_entry_type(corrected) or "narration"
        staged_entries.append(
            SubtitleEntry(
                index=entry.index,
                start=entry.start,
                end=entry.end,
                text=corrected,
                entry_type=entry_type,
            )
        )

    staged_entries = refine_classified_entries(staged_entries)
    staged_entries = strengthen_classification(staged_entries)
    staged_entries = classify_entries_with_whole_text_context(
        staged_entries,
        trust_existing_type=False,
    )
    staged_entries = refine_classified_entries(staged_entries)
    staged_entries = strengthen_classification(staged_entries)
    staged_entries = recover_narration_fragment_runs(staged_entries)
    return {
        "entries": [
            {
                "index": entry.index,
                "type": entry.entry_type,
                "original": entry.text,
                "corrected": entry.text,
            }
            for entry in staged_entries
        ]
    }


def refine_classified_entries(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    if not entries:
        return []

    adjusted: List[SubtitleEntry] = []
    for entry in entries:
        if entry.entry_type == "original_subtitle":
            adjusted.append(entry)
            continue
        forced_type = heuristic_entry_type(entry.text)
        if forced_type and forced_type != entry.entry_type:
            adjusted.append(clone_subtitle_entry(entry, entry_type=forced_type))
        else:
            adjusted.append(entry)

    refined: List[SubtitleEntry] = []
    for idx, entry in enumerate(adjusted):
        prev_type = adjusted[idx - 1].entry_type if idx > 0 else ""
        next_type = adjusted[idx + 1].entry_type if idx + 1 < len(adjusted) else ""
        units = subtitle_display_units(entry.text)
        forced_type = heuristic_entry_type(entry.text)
        entry_type = entry.entry_type

        if (
            entry_type == "dialogue"
            and prev_type == "narration"
            and next_type == "narration"
            and units <= 10
            and forced_type != "dialogue"
        ):
            entry_type = "narration"
        elif (
            entry_type == "narration"
            and prev_type == "dialogue"
            and next_type == "dialogue"
            and forced_type == "dialogue"
        ):
            entry_type = "dialogue"

        refined.append(clone_subtitle_entry(entry, entry_type=entry_type))

    return refined


def looks_like_narration_fragment(entry: SubtitleEntry) -> bool:
    text = normalize_subtitle_text(entry.text)
    if not text:
        return False
    duration = max(0.0, entry.end - entry.start)
    units = subtitle_display_units(text)
    if duration <= SHORT_GAP_FRAGMENT_SECONDS:
        return True
    if units <= 8:
        return True
    if FRAGMENT_TAIL_RE.search(text):
        return True
    if re.search(r"^(见|看|听|让|把|将|被|就|又|还|可|便|而|却|只见|谁知|结果|原来)", text):
        return True
    return False


def join_narration_text(left: str, right: str, gap: float) -> str:
    left_text = normalize_subtitle_text(left)
    right_text = normalize_subtitle_text(right)
    if not left_text:
        return right_text
    if not right_text:
        return left_text

    if re.search(r"[，。！？!?；;：:、…]$", left_text) or re.search(r"^[，。！？!?；;：:、…]", right_text):
        merged = left_text + right_text
    else:
        separator = "，" if gap >= 0.25 and CJK_RE.search(left_text + right_text) else ""
        merged = left_text + separator + right_text
    return normalize_subtitle_text(merged)


def should_merge_narration_entries(current: SubtitleEntry, upcoming: SubtitleEntry) -> bool:
    if current.entry_type != "narration" or upcoming.entry_type != "narration":
        return False
    if dialogue_score(current.text) >= 3 or dialogue_score(upcoming.text) >= 3:
        return False
    if speech_intro_score(current.text) >= 2 or speech_intro_score(upcoming.text) >= 2:
        return False

    gap = upcoming.start - current.end
    if gap < 0 or gap > SHORT_GAP_MERGE_SECONDS:
        return False
    if upcoming.end - current.start > MAX_MERGED_NARRATION_DURATION:
        return False

    merged_text = join_narration_text(current.text, upcoming.text, gap)
    current_is_fragment = looks_like_narration_fragment(current) or probably_incomplete_text(current.text)
    if subtitle_display_units(merged_text) > MAX_SUBTITLE_CHARS and not current_is_fragment:
        return False
    if subtitle_display_units(merged_text) > MAX_SUBTITLE_CHARS + 12:
        return False

    if gap <= 0.25:
        return True
    return current_is_fragment


def merge_short_gap_entries(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    if not entries:
        return []

    merged: List[SubtitleEntry] = []
    current = entries[0]
    for upcoming in entries[1:]:
        if should_merge_narration_entries(current, upcoming):
            merged_text = join_narration_text(current.text, upcoming.text, upcoming.start - current.end)
            current = SubtitleEntry(
                index=current.index,
                start=current.start,
                end=upcoming.end,
                text=normalize_subtitle_text(merged_text),
                entry_type="narration",
            )
            continue

        merged.append(current)
        current = upcoming

    merged.append(current)
    return [
        SubtitleEntry(
            index=index,
            start=entry.start,
            end=entry.end,
            text=entry.text,
            entry_type=entry.entry_type,
        )
        for index, entry in enumerate(merged, start=1)
    ]


def repair_incomplete_narration_entries(entries: Sequence[SubtitleEntry]) -> List[SubtitleEntry]:
    if not entries:
        return []

    repaired: List[SubtitleEntry] = []
    index = 0
    while index < len(entries):
        current = entries[index]
        if (
            current.entry_type == "narration"
            and probably_incomplete_text(current.text)
            and index + 1 < len(entries)
        ):
            upcoming = entries[index + 1]
            gap = upcoming.start - current.end
            if (
                upcoming.entry_type == "narration"
                and 0.0 <= gap <= SHORT_GAP_MERGE_SECONDS
                and dialogue_score(current.text) < 3
                and dialogue_score(upcoming.text) < 3
            ):
                repaired.append(
                    SubtitleEntry(
                        index=current.index,
                        start=current.start,
                        end=upcoming.end,
                        text=join_narration_text(current.text, upcoming.text, gap),
                        entry_type="narration",
                    )
                )
                index += 2
                continue
        repaired.append(current)
        index += 1

    return [
        SubtitleEntry(
            index=new_index,
            start=entry.start,
            end=entry.end,
            text=normalize_subtitle_text(entry.text),
            entry_type=entry.entry_type,
        )
        for new_index, entry in enumerate(repaired, start=1)
    ]


def should_merge_tts_narration_group(
    current_group: Sequence[SubtitleEntry],
    upcoming: SubtitleEntry,
    reference_gap: float,
    forced_join: Optional[bool] = None,
) -> bool:
    if not current_group:
        return False
    current = current_group[-1]
    if current.entry_type != "narration" or upcoming.entry_type != "narration":
        return False
    if dialogue_score(current.text) >= 3 or dialogue_score(upcoming.text) >= 3:
        return False

    next_group_size = len(current_group) + 1
    if next_group_size > MAX_TTS_GROUP_ENTRIES:
        return False

    gap = upcoming.start - current.end
    merge_gap_limit = min(0.42, max(0.10, reference_gap * 2.0 + 0.06))
    if forced_join is True:
        merge_gap_limit = min(0.58, max(merge_gap_limit, reference_gap * 2.8 + 0.08))
    if gap < 0 or gap > merge_gap_limit:
        return False
    if upcoming.end - current_group[0].start > MAX_TTS_GROUP_DURATION:
        return False

    combined_units = sum(max(1, subtitle_speech_units(entry.text)) for entry in current_group)
    combined_units += max(1, subtitle_speech_units(upcoming.text))
    if combined_units > MAX_TTS_GROUP_UNITS:
        return False

    if forced_join is False:
        return False

    current_text = normalize_subtitle_text(current.text)
    upcoming_text = normalize_subtitle_text(upcoming.text)
    current_units = max(1, subtitle_speech_units(current_text))
    upcoming_units = max(1, subtitle_speech_units(upcoming_text))
    if ends_with_terminal_sentence_pause(current_text):
        return False
    if starts_with_strong_transition(upcoming_text):
        return False
    continuation_start = starts_with_soft_continuation(upcoming_text) or starts_with_structural_continuation(
        upcoming_text
    )
    current_incomplete = probably_incomplete_text(current_text)
    comma_bridge = ends_with_minor_sentence_pause(current_text)
    very_short_split = (
        gap <= max(0.06, min(0.12, reference_gap * 0.75 + 0.02))
        and current_units <= 8
        and upcoming_units <= 14
    )
    if forced_join is True:
        return True
    if continuation_start:
        return True
    if current_incomplete and gap <= merge_gap_limit:
        return True
    if comma_bridge and gap <= merge_gap_limit:
        return True
    if very_short_split and not ends_with_minor_sentence_pause(upcoming_text):
        return True
    return False


def plan_tts_sentence_links_locally(
    entries: Sequence[SubtitleEntry],
    reference_gap: float,
) -> Dict[int, bool]:
    if len(entries) < 2:
        return {}

    join_map: Dict[int, bool] = {}
    sentence_buffer = normalize_subtitle_text(entries[0].text)
    for idx in range(len(entries) - 1):
        current = entries[idx]
        upcoming = entries[idx + 1]
        upcoming_text = normalize_subtitle_text(upcoming.text)
        if current.entry_type != "narration" or upcoming.entry_type != "narration":
            sentence_buffer = upcoming_text
            continue
        if dialogue_score(current.text) >= 3 or dialogue_score(upcoming.text) >= 3:
            sentence_buffer = upcoming_text
            continue

        gap = upcoming.start - current.end
        local_join_limit = min(0.42, max(0.12, reference_gap * 2.2 + 0.06))
        if gap < 0 or gap > local_join_limit:
            sentence_buffer = upcoming_text
            continue

        current_text = normalize_subtitle_text(current.text)
        current_sentence = normalize_subtitle_text(sentence_buffer or current_text)
        if ends_with_terminal_sentence_pause(current_text) or ends_with_terminal_sentence_pause(current_sentence):
            sentence_buffer = upcoming_text
            continue
        if starts_with_strong_transition(upcoming_text) and looks_like_explicit_sentence_end(current_sentence):
            sentence_buffer = upcoming_text
            continue

        current_units = max(1, subtitle_speech_units(current_text))
        upcoming_units = max(1, subtitle_speech_units(upcoming_text))
        sentence_units = max(1, subtitle_speech_units(current_sentence))
        continuation_start = starts_with_soft_continuation(upcoming_text) or starts_with_structural_continuation(
            upcoming_text
        )
        current_incomplete = probably_incomplete_text(current_text)
        sentence_incomplete = probably_incomplete_text(current_sentence)
        comma_bridge = ends_with_minor_sentence_pause(current_text)
        speech_intro_bridge = speech_intro_score(current_text) >= 2
        dangling_tail = looks_like_dangling_tts_tail(current_text) or (
            sentence_units <= 20 and looks_like_dangling_tts_tail(current_sentence)
        )
        explicit_end = looks_like_explicit_sentence_end(current_sentence)
        tiny_gap = gap <= max(0.08, min(0.15, reference_gap * 0.90 + 0.03))
        short_line = current_units <= 12

        join_score = 0
        if current_incomplete or sentence_incomplete:
            join_score += 3
        if continuation_start:
            join_score += 2
        if comma_bridge:
            join_score += 2
        if speech_intro_bridge:
            join_score += 2
        if dangling_tail:
            join_score += 3
        if short_line:
            join_score += 1
        if sentence_units <= 20:
            join_score += 1
        if tiny_gap:
            join_score += 1
        if explicit_end:
            join_score -= 3
        if starts_with_strong_transition(upcoming_text):
            join_score -= 3
        if gap > max(0.18, reference_gap * 1.4 + 0.03):
            join_score -= 1
        if current_units >= 16 and upcoming_units >= 10 and not continuation_start and not dangling_tail:
            join_score -= 1

        should_join = join_score >= 2
        if (
            not should_join
            and tiny_gap
            and not explicit_end
            and not starts_with_strong_transition(upcoming_text)
            and (short_line or dangling_tail or sentence_units <= 14)
        ):
            should_join = True

        if should_join:
            join_map[current.index] = True
            sentence_buffer = join_narration_text(current_sentence, upcoming_text, gap)
        else:
            sentence_buffer = upcoming_text

    return join_map


def group_narration_entries_for_tts(
    entries: Sequence[SubtitleEntry],
    reference_gap: float,
    join_map: Optional[Dict[int, bool]] = None,
) -> List[List[SubtitleEntry]]:
    if not entries:
        return []

    groups: List[List[SubtitleEntry]] = []
    current_group: List[SubtitleEntry] = [entries[0]]
    for position, upcoming in enumerate(entries[1:], start=1):
        current_index = current_group[-1].index
        forced_join = join_map.get(current_index) if join_map else None
        merge_current = should_merge_tts_narration_group(
            current_group,
            upcoming,
            reference_gap,
            forced_join=forced_join,
        )
        if (
            merge_current
            and forced_join is not True
            and len(current_group) + 1 >= MAX_TTS_GROUP_ENTRIES
            and position + 1 < len(entries)
        ):
            following = entries[position + 1]
            following_join = join_map.get(upcoming.index) if join_map else None
            upcoming_text = normalize_subtitle_text(upcoming.text)
            following_text = normalize_subtitle_text(following.text)
            if (
                should_merge_tts_narration_group(
                    [upcoming],
                    following,
                    reference_gap,
                    forced_join=following_join,
                )
                and not looks_like_explicit_sentence_end(upcoming_text)
                and (
                    starts_with_soft_continuation(following_text)
                    or starts_with_structural_continuation(following_text)
                    or probably_incomplete_text(upcoming_text)
                    or looks_like_dangling_tts_tail(upcoming_text)
                )
            ):
                merge_current = False
        if merge_current:
            current_group.append(upcoming)
            continue
        groups.append(current_group)
        current_group = [upcoming]
    groups.append(current_group)
    return groups


def join_narration_group_text(entries: Sequence[SubtitleEntry]) -> str:
    merged = ""
    previous: Optional[SubtitleEntry] = None
    for entry in entries:
        if previous is None:
            merged = normalize_subtitle_text(entry.text)
        else:
            merged = join_narration_text(merged, entry.text, entry.start - previous.end)
        previous = entry
    return normalize_spoken_narration_text(merged)


def tts_group_split_overflow_limit(window_duration: float) -> float:
    soft_window_duration = window_duration + MAX_TTS_TIMELINE_OVERFLOW_SECONDS
    return max(
        0.05,
        max(
            window_duration * TTS_GROUP_OVERFLOW_SPLIT_RATIO + TTS_GROUP_OVERFLOW_SPLIT_MARGIN_SECONDS,
            soft_window_duration + TTS_GROUP_OVERFLOW_SPLIT_MARGIN_SECONDS,
        ),
    )


def tts_group_soft_window_end(start: float, end: float, total_duration: float) -> float:
    strict_start = max(0.0, float(start))
    strict_end = max(strict_start + 0.05, float(end))
    return clamp(
        strict_end + MAX_TTS_TIMELINE_OVERFLOW_SECONDS,
        strict_start + 0.05,
        total_duration,
    )


def build_prepared_tts_groups(
    tts_groups: Sequence[Sequence[SubtitleEntry]],
    raw_dir: Path,
    total_duration: float,
) -> List[Dict[str, object]]:
    prepared_groups: List[Dict[str, object]] = []
    for idx, group_entries in enumerate(tts_groups, start=1):
        group_list = list(group_entries)
        if not group_list:
            continue
        first_entry = group_list[0]
        last_entry = group_list[-1]
        group_text = join_narration_group_text(group_list)
        if not group_text:
            continue

        strict_start = max(0.0, float(first_entry.start))
        strict_end = max(strict_start + 0.05, float(last_entry.end))
        soft_end = tts_group_soft_window_end(strict_start, strict_end, total_duration)
        latest_start = clamp(
            strict_start + MAX_TTS_TIMELINE_OVERFLOW_SECONDS,
            strict_start,
            total_duration,
        )
        prepared_groups.append(
            {
                "order": idx,
                "entries": group_list,
                "label": (
                    f"{first_entry.index}"
                    if len(group_list) == 1
                    else f"{first_entry.index}-{last_entry.index}"
                ),
                "strict_start": strict_start,
                "strict_end": strict_end,
                "window_start": strict_start,
                "window_end": soft_end,
                "latest_start": latest_start,
                "text": group_text,
                "raw_path": raw_dir / f"{idx:03d}.mp3",
                "prepared_path": raw_dir / f"{idx:03d}_prepared.wav",
                "target_duration": max(0.05, soft_end - strict_start),
            }
        )
    return prepared_groups


def prepared_tts_group_signature(prepared_groups: Sequence[Dict[str, object]]) -> Tuple[Tuple[int, ...], ...]:
    signature: List[Tuple[int, ...]] = []
    for group_state in prepared_groups:
        group_entries = list(group_state.get("entries", []))
        entry_indexes = tuple(int(entry.index) for entry in group_entries if isinstance(entry, SubtitleEntry))
        if entry_indexes:
            signature.append(entry_indexes)
    return tuple(signature)


def choose_tts_group_split_index(entries: Sequence[SubtitleEntry]) -> Optional[int]:
    if len(entries) <= 1:
        return None

    total_units = sum(max(1, subtitle_speech_units(entry.text)) for entry in entries)
    best_index: Optional[int] = None
    best_score: Optional[float] = None
    for idx in range(1, len(entries)):
        left_entries = list(entries[:idx])
        right_entries = list(entries[idx:])
        left_text = join_narration_group_text(left_entries)
        right_text = join_narration_group_text(right_entries)
        if not left_text or not right_text:
            continue

        left_units = sum(max(1, subtitle_speech_units(entry.text)) for entry in left_entries)
        right_units = max(1, total_units - left_units)
        gap = max(0.0, float(entries[idx].start) - float(entries[idx - 1].end))
        balance_penalty = abs(left_units - right_units) / max(1.0, float(total_units))

        score = gap * 7.5 - balance_penalty * 3.2
        if looks_like_explicit_sentence_end(left_text):
            score += 3.0
        elif ends_with_minor_sentence_pause(left_text):
            score += 1.2
        if starts_with_strong_transition(right_text):
            score += 2.0
        if starts_with_soft_continuation(right_text) or starts_with_structural_continuation(right_text):
            score -= 2.4
        if probably_incomplete_text(left_text):
            score -= 2.2
        if looks_like_dangling_tts_tail(left_text):
            score -= 2.8
        if idx == 1 or idx == len(entries) - 1:
            score -= 0.3

        if best_score is None or score > best_score:
            best_score = score
            best_index = idx
    return best_index


def split_overflowing_tts_group_entries(
    entries: Sequence[SubtitleEntry],
    base_rate: str,
    *,
    depth: int = 0,
) -> List[List[SubtitleEntry]]:
    group_entries = list(entries)
    if len(group_entries) <= 1 or depth >= MAX_TTS_SEGMENT_SPLIT_DEPTH + 1:
        return [group_entries] if group_entries else []

    group_text = join_narration_group_text(group_entries)
    if not group_text:
        return [group_entries]

    window_duration = max(0.05, float(group_entries[-1].end) - float(group_entries[0].start))
    estimated_duration = estimate_tts_render_duration(group_text, base_rate)
    if estimated_duration <= tts_group_split_overflow_limit(window_duration):
        return [group_entries]

    split_index = choose_tts_group_split_index(group_entries)
    if split_index is None:
        split_index = len(group_entries) // 2
    if split_index <= 0 or split_index >= len(group_entries):
        return [group_entries]

    left_parts = split_overflowing_tts_group_entries(group_entries[:split_index], base_rate, depth=depth + 1)
    right_parts = split_overflowing_tts_group_entries(group_entries[split_index:], base_rate, depth=depth + 1)
    return left_parts + right_parts


def refine_tts_groups_for_timing(
    groups: Sequence[Sequence[SubtitleEntry]],
    base_rate: str,
) -> Tuple[List[List[SubtitleEntry]], int]:
    refined_groups: List[List[SubtitleEntry]] = []
    added_groups = 0
    for group_entries in groups:
        split_groups = split_overflowing_tts_group_entries(group_entries, base_rate)
        if len(split_groups) > 1:
            added_groups += len(split_groups) - 1
        refined_groups.extend(split_groups)
    return refined_groups, added_groups


def split_rendered_tts_groups_for_timing(
    prepared_groups: Sequence[Dict[str, object]],
    raw_dir: Path,
    total_duration: float,
    render_rate: str,
) -> Tuple[List[Dict[str, object]], int]:
    rebuilt_groups: List[List[SubtitleEntry]] = []
    split_count = 0
    for group_state in prepared_groups:
        group_entries = list(group_state.get("entries", []))
        raw_duration = max(0.05, tts_group_schedulable_duration(group_state))
        strict_start = max(0.0, float(group_state.get("strict_start", 0.0) or 0.0))
        strict_end = max(
            strict_start + 0.05,
            float(group_state.get("strict_end", strict_start + 0.05) or (strict_start + 0.05)),
        )
        soft_duration = max(0.05, tts_group_soft_window_end(strict_start, strict_end, total_duration) - strict_start)
        min_render_duration = raw_duration / max(MAX_TTS_SPEED_FACTOR, 1.0)
        if len(group_entries) > 1 and min_render_duration > soft_duration + 0.03:
            split_index = choose_tts_group_split_index(group_entries)
            if split_index is not None and 0 < split_index < len(group_entries):
                rebuilt_groups.append(group_entries[:split_index])
                rebuilt_groups.append(group_entries[split_index:])
                split_count += 1
                continue
        rebuilt_groups.append(group_entries)

    if split_count <= 0:
        return list(prepared_groups), 0
    return build_prepared_tts_groups(rebuilt_groups, raw_dir, total_duration), split_count


def should_merge_underfilled_tts_groups(
    current_state: Dict[str, object],
    next_state: Dict[str, object],
    render_rate: str,
    total_duration: float,
) -> bool:
    current_entries = list(current_state.get("entries", []))
    next_entries = list(next_state.get("entries", []))
    if not current_entries or not next_entries:
        return False

    current_last = current_entries[-1]
    next_first = next_entries[0]
    if current_last.entry_type != "narration" or next_first.entry_type != "narration":
        return False

    combined_entries = current_entries + next_entries
    if len(combined_entries) > MAX_TTS_GROUP_ENTRIES:
        return False

    reference_gap = max(0.06, float(next_first.start) - float(current_last.end))
    if not should_merge_tts_narration_group(current_entries, next_first, reference_gap, forced_join=True):
        return False

    current_text = normalize_subtitle_text(str(current_state.get("text", "") or ""))
    next_text = normalize_subtitle_text(str(next_state.get("text", "") or ""))
    combined_text = join_narration_group_text(combined_entries)
    if not current_text or not next_text or not combined_text:
        return False

    current_start = max(0.0, float(current_state.get("strict_start", 0.0) or 0.0))
    current_end = max(current_start + 0.05, float(current_state.get("strict_end", current_start + 0.05) or (current_start + 0.05)))
    next_start = max(0.0, float(next_state.get("strict_start", 0.0) or 0.0))
    next_end = max(next_start + 0.05, float(next_state.get("strict_end", next_start + 0.05) or (next_start + 0.05)))

    current_soft = max(0.05, tts_group_soft_window_end(current_start, current_end, total_duration) - current_start)
    next_soft = max(0.05, tts_group_soft_window_end(next_start, next_end, total_duration) - next_start)
    combined_soft = max(0.05, tts_group_soft_window_end(current_start, next_end, total_duration) - current_start)

    current_raw = max(0.05, tts_group_schedulable_duration(current_state))
    next_raw = max(0.05, tts_group_schedulable_duration(next_state))
    current_min = current_raw / max(MAX_TTS_SPEED_FACTOR, 1.0)
    next_min = next_raw / max(MAX_TTS_SPEED_FACTOR, 1.0)
    combined_estimated = estimate_tts_render_duration(combined_text, render_rate)
    combined_min = combined_estimated / max(MAX_TTS_SPEED_FACTOR, 1.0)

    separate_excess = max(0.0, current_min - current_soft) + max(0.0, next_min - next_soft)
    combined_excess = max(0.0, combined_min - combined_soft)
    tiny_window = current_soft <= TTS_UNDERFILLED_GROUP_WINDOW_SECONDS or next_soft <= TTS_UNDERFILLED_GROUP_WINDOW_SECONDS
    overflowing = (
        max(0.0, current_min - current_soft) >= TTS_UNDERFILLED_GROUP_EXCESS_SECONDS
        or max(0.0, next_min - next_soft) >= TTS_UNDERFILLED_GROUP_EXCESS_SECONDS
    )
    if not tiny_window and not overflowing:
        return False
    if combined_min > combined_soft + 0.03:
        return False
    if combined_excess + 0.02 < separate_excess:
        return True
    return combined_min <= combined_soft + 0.08 and combined_excess <= separate_excess


def merge_underfilled_tts_groups_for_timing(
    prepared_groups: Sequence[Dict[str, object]],
    raw_dir: Path,
    total_duration: float,
    render_rate: str,
) -> Tuple[List[Dict[str, object]], int]:
    merged_groups: List[List[SubtitleEntry]] = []
    merge_count = 0
    index = 0
    states = list(prepared_groups)
    while index < len(states):
        current_state = states[index]
        if index + 1 < len(states):
            next_state = states[index + 1]
            if should_merge_underfilled_tts_groups(current_state, next_state, render_rate, total_duration):
                merged_groups.append(list(current_state.get("entries", [])) + list(next_state.get("entries", [])))
                merge_count += 1
                index += 2
                continue
        merged_groups.append(list(current_state.get("entries", [])))
        index += 1

    if merge_count <= 0:
        return list(prepared_groups), 0
    return build_prepared_tts_groups(merged_groups, raw_dir, total_duration), merge_count


def schedule_prepared_tts_groups(
    prepared_groups: Sequence[Dict[str, object]],
    total_duration: float,
) -> Dict[str, float]:
    cursor = 0.0
    hard_trim_count = 0
    max_start_drift = 0.0
    max_end_drift = 0.0
    for index, group_state in enumerate(prepared_groups):
        strict_start = max(0.0, float(group_state.get("strict_start", 0.0) or 0.0))
        strict_end = max(
            strict_start + 0.05,
            float(group_state.get("strict_end", strict_start + 0.05) or (strict_start + 0.05)),
        )
        latest_start = clamp(
            float(group_state.get("latest_start", strict_start + MAX_TTS_TIMELINE_OVERFLOW_SECONDS)
            or (strict_start + MAX_TTS_TIMELINE_OVERFLOW_SECONDS)),
            strict_start,
            total_duration,
        )
        latest_end = clamp(
            float(group_state.get("window_end", strict_end + MAX_TTS_TIMELINE_OVERFLOW_SECONDS)
            or (strict_end + MAX_TTS_TIMELINE_OVERFLOW_SECONDS)),
            strict_start + 0.05,
            total_duration,
        )
        next_latest_start = total_duration
        if index + 1 < len(prepared_groups):
            next_state = prepared_groups[index + 1]
            next_strict_start = max(0.0, float(next_state.get("strict_start", 0.0) or 0.0))
            next_latest_start = clamp(
                float(next_state.get("latest_start", next_strict_start + MAX_TTS_TIMELINE_OVERFLOW_SECONDS)
                or (next_strict_start + MAX_TTS_TIMELINE_OVERFLOW_SECONDS)),
                next_strict_start,
                total_duration,
            )

        scheduled_start = max(strict_start, cursor)
        if scheduled_start > latest_start:
            scheduled_start = latest_start
        scheduled_end = min(latest_end, next_latest_start) if index + 1 < len(prepared_groups) else latest_end
        scheduled_end = max(scheduled_start + 0.05, scheduled_end)

        raw_duration = max(0.05, tts_group_schedulable_duration(group_state))
        available_duration = max(0.05, scheduled_end - scheduled_start)
        target_duration = min(raw_duration, available_duration)
        min_render_duration = raw_duration / max(MAX_TTS_SPEED_FACTOR, 1.0)
        hard_trim = min_render_duration > available_duration + 0.03

        group_state["scheduled_start"] = scheduled_start
        group_state["scheduled_end"] = scheduled_end
        group_state["target_duration"] = max(0.05, target_duration)
        group_state["min_duration"] = min_render_duration
        group_state["hard_trim"] = hard_trim
        group_state["start_drift"] = max(0.0, scheduled_start - strict_start)
        group_state["planned_end_drift"] = max(0.0, scheduled_start + target_duration - strict_end)

        max_start_drift = max(max_start_drift, float(group_state["start_drift"]))
        max_end_drift = max(max_end_drift, float(group_state["planned_end_drift"]))
        if hard_trim:
            hard_trim_count += 1
        cursor = scheduled_start + target_duration

    return {
        "hard_trim_count": float(hard_trim_count),
        "max_start_drift": max_start_drift,
        "max_end_drift": max_end_drift,
    }


def distribute_group_rendered_entries(
    entries: Sequence[SubtitleEntry],
    group_start: float,
    group_end: float,
) -> List[SubtitleEntry]:
    if not entries:
        return []

    if len(entries) == 1:
        entry = entries[0]
        return [
            SubtitleEntry(
                index=entry.index,
                start=group_start,
                end=group_end,
                text=entry.text,
                entry_type=entry.entry_type,
            )
        ]

    total_duration = max(0.05, group_end - group_start)
    weights = [max(1, subtitle_speech_units(entry.text)) for entry in entries]
    total_weight = sum(weights)
    cursor = group_start
    rendered: List[SubtitleEntry] = []
    for idx, entry in enumerate(entries):
        if idx == len(entries) - 1:
            part_end = group_end
        else:
            remaining_entries = len(entries) - idx - 1
            min_remaining = 0.08 * remaining_entries
            part_span = total_duration * weights[idx] / max(1, total_weight)
            part_end = min(group_end - min_remaining, max(cursor + 0.12, cursor + part_span))
        rendered.append(
            SubtitleEntry(
                index=entry.index,
                start=cursor,
                end=part_end,
                text=entry.text,
                entry_type=entry.entry_type,
            )
        )
        cursor = part_end
    return rendered


def choose_display_split_position(
    text: str,
    max_units: int,
    target_units: int,
    remaining_slots: int,
    extra_positions: Optional[Sequence[int]] = None,
) -> Optional[int]:
    normalized = normalize_subtitle_text(text)
    positions = set(break_positions(normalized))
    if extra_positions:
        positions.update(pos for pos in extra_positions if 0 < pos < len(normalized))
    positions = sorted(positions)
    if not positions:
        return None

    min_remaining_units = max(1, remaining_slots * MIN_SUBTITLE_CHARS)
    best_pos: Optional[int] = None
    best_score: Optional[float] = None
    for pos in positions:
        left = normalize_subtitle_text(normalized[:pos])
        right = normalize_subtitle_text(normalized[pos:])
        left_units = subtitle_display_units(left)
        right_units = subtitle_display_units(right)
        if left_units < MIN_SUBTITLE_CHARS or left_units > max_units:
            continue
        if right_units < min_remaining_units:
            continue

        score = abs(left_units - target_units)
        if probably_incomplete_text(left):
            score += 6.0
        if starts_with_soft_continuation(right):
            score += 4.0
        if starts_with_strong_transition(right):
            score -= 1.0
        if ends_with_terminal_sentence_pause(left):
            score -= 0.5
        if extra_positions and pos in extra_positions:
            score -= 0.75

        if best_score is None or score < best_score:
            best_pos = pos
            best_score = score
    return best_pos


def rebalance_group_display_texts(entries: Sequence[SubtitleEntry], group_text: str) -> List[str]:
    normalized = normalize_subtitle_text(group_text)
    if len(entries) <= 1 or not normalized:
        return [normalize_subtitle_text(entry.text) for entry in entries]

    boundary_positions: List[int] = []
    assembled = ""
    previous_entry: Optional[SubtitleEntry] = None
    for entry in entries[:-1]:
        entry_text = normalize_subtitle_text(entry.text)
        if previous_entry is None:
            assembled = entry_text
        else:
            assembled = join_narration_text(assembled, entry_text, entry.start - previous_entry.end)
        boundary_positions.append(len(assembled))
        previous_entry = entry

    chunks: List[str] = []
    remaining = normalized
    for idx, entry in enumerate(entries[:-1]):
        remaining_slots = len(entries) - idx - 1
        remaining_units = subtitle_display_units(remaining)
        if remaining_units <= 0:
            return [normalize_subtitle_text(item.text) for item in entries]

        duration = max(0.1, entry.end - entry.start)
        max_units = max(MIN_SUBTITLE_CHARS, min(MAX_SUBTITLE_CHARS + 2, subtitle_char_budget(duration) + 2))
        min_required = remaining_slots * MIN_SUBTITLE_CHARS
        if remaining_units <= max_units + min_required:
            return [normalize_subtitle_text(item.text) for item in entries]

        target_units = min(max_units, max(MIN_SUBTITLE_CHARS, int(round(remaining_units / (remaining_slots + 1)))))
        split_pos = choose_display_split_position(
            remaining,
            max_units,
            target_units,
            remaining_slots,
            extra_positions=boundary_positions,
        )
        if split_pos is None:
            return [normalize_subtitle_text(item.text) for item in entries]

        left = normalize_subtitle_text(remaining[:split_pos])
        right = normalize_subtitle_text(remaining[split_pos:])
        if not left or not right:
            return [normalize_subtitle_text(item.text) for item in entries]
        chunks.append(left)
        remaining = right
        boundary_positions = [pos - split_pos for pos in boundary_positions if pos > split_pos]

    chunks.append(remaining)
    if len(chunks) != len(entries) or any(not chunk for chunk in chunks):
        return [normalize_subtitle_text(item.text) for item in entries]
    if any(probably_incomplete_text(chunk) for chunk in chunks):
        return [normalize_subtitle_text(item.text) for item in entries]
    return chunks


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def safe_remove_dir(path: Path, max_retries: int = 5, delay: float = 0.5) -> None:
    if not path.exists():
        return

    def _onerror(func, target, _exc_info):
        try:
            os.chmod(target, stat.S_IWRITE | stat.S_IREAD)
            func(target)
        except OSError:
            pass

    for _ in range(max_retries):
        try:
            shutil.rmtree(str(path), onerror=_onerror)
            if not path.exists():
                return
        except OSError:
            time.sleep(delay)


def safe_unlink_file(path: Path, max_retries: int = 6, delay: float = 0.35) -> bool:
    if not path.exists():
        return True

    for _ in range(max_retries):
        try:
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
        except OSError:
            pass
        try:
            path.unlink()
            return True
        except FileNotFoundError:
            return True
        except OSError:
            time.sleep(delay)

    return not path.exists()


def next_available_path(preferred_path: Path, max_attempts: int = 99) -> Path:
    if not preferred_path.exists():
        return preferred_path

    stem = preferred_path.stem
    suffix = preferred_path.suffix
    for index in range(1, max_attempts + 1):
        candidate = preferred_path.with_name(f"{stem}_{index:02d}{suffix}")
        if not candidate.exists():
            return candidate

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return preferred_path.with_name(f"{stem}_{timestamp}{suffix}")


def move_output_file(
    src_path: Path,
    preferred_path: Path,
    log_func: Optional[Callable[[str], None]] = None,
    artifact_label: str = "output",
) -> Path:
    if src_path == preferred_path:
        return src_path

    candidates: List[Path] = [preferred_path]
    last_error: Optional[BaseException] = None

    while candidates:
        candidate = candidates.pop(0)
        if candidate.exists() and not safe_unlink_file(candidate):
            last_error = PermissionError(f"locked target: {candidate}")
            if candidate == preferred_path:
                fallback_path = next_available_path(preferred_path)
                if log_func:
                    log_func(f"  Target file is busy, using fallback {artifact_label} name: {fallback_path.name}")
                candidates.append(fallback_path)
            continue

        try:
            src_path.replace(candidate)
            return candidate
        except OSError as exc:
            last_error = exc
            if candidate == preferred_path:
                fallback_path = next_available_path(preferred_path)
                if fallback_path != candidate:
                    if log_func:
                        log_func(
                            f"  Could not replace {candidate.name}, using fallback {artifact_label} name: {fallback_path.name}"
                        )
                    candidates.append(fallback_path)

    raise RuntimeError(f"could not move {artifact_label} file to {preferred_path}") from last_error


def datetime_now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def sanitize_stem(raw: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", raw).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned or "output"


def fps_to_float(raw: str) -> float:
    if "/" in raw:
        numerator, denominator = raw.split("/", 1)
        if denominator == "0":
            return 25.0
        return float(numerator) / float(denominator)
    return float(raw or 25.0)


def crop_box_ratio(
    width: int,
    height: int,
    left: float,
    top: float,
    right: float,
    bottom: float,
) -> Tuple[int, int, int, int]:
    return (
        max(0, min(width - 1, int(width * left))),
        max(0, min(height - 1, int(height * top))),
        max(1, min(width, int(width * right))),
        max(1, min(height, int(height * bottom))),
    )


def crop_image_ratio(image, left: float, top: float, right: float, bottom: float):
    width, height = image.size
    return image.crop(crop_box_ratio(width, height, left, top, right, bottom))


def smooth_numeric_profile(values, window: int):
    if not NUMPY_AVAILABLE:
        return values
    normalized_window = max(1, int(window))
    if normalized_window % 2 == 0:
        normalized_window += 1
    if normalized_window <= 1 or len(values) <= 2:
        return values
    kernel = np.ones(normalized_window, dtype=np.float32) / float(normalized_window)
    return np.convolve(values, kernel, mode="same")


def build_video_sample_timestamps(duration: float) -> List[float]:
    if duration <= 0.05:
        return [0.0]
    sample_count = min(
        SUBTITLE_MASK_MAX_SAMPLES,
        max(SUBTITLE_MASK_MIN_SAMPLES, int(math.ceil(duration / SUBTITLE_MASK_SAMPLE_INTERVAL))),
    )
    start = min(0.35, max(0.0, duration * 0.04))
    end = max(start, duration - min(0.35, max(0.05, duration * 0.04)))
    latest = max(0.0, duration - 0.02)
    if end <= start + 0.04:
        return [clamp(duration * 0.5, 0.0, latest)]

    timestamps: List[float] = []
    seen: set[float] = set()
    for index in range(sample_count):
        ratio = 0.5 if sample_count == 1 else index / max(1, sample_count - 1)
        timestamp = clamp(start + (end - start) * ratio, 0.0, latest)
        key = round(timestamp, 3)
        if key in seen:
            continue
        timestamps.append(timestamp)
        seen.add(key)
    return timestamps or [0.0]


def extract_video_sample_frames(
    video_path: Path,
    sample_dir: Path,
    video_processor: "VideoProcessor",
) -> List[Path]:
    sample_dir.mkdir(parents=True, exist_ok=True)
    timestamps = build_video_sample_timestamps(video_processor.probe_duration(video_path))
    sample_paths: List[Path] = []
    for index, timestamp in enumerate(timestamps, start=1):
        sample_path = sample_dir / f"sample_{index:03d}.jpg"
        result = run_subprocess_hidden(
            [
                str(video_processor.ffmpeg),
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-y",
                "-ss",
                f"{timestamp:.3f}",
                "-i",
                str(video_path),
                "-frames:v",
                "1",
                "-q:v",
                "2",
                str(sample_path),
            ],
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode == 0 and sample_path.exists() and sample_path.stat().st_size > 0:
            sample_paths.append(sample_path)
    return sample_paths


def build_subtitle_detection_mask(image):
    if not NUMPY_AVAILABLE or not PIL_AVAILABLE or image is None:
        return None
    rgb = ImageOps.autocontrast(image.convert("RGB"))
    if rgb.width < 32 or rgb.height < 20:
        return None
    if rgb.width > 480:
        target_width = 480
        target_height = max(32, int(round(rgb.height * target_width / max(1, rgb.width))))
        rgb = rgb.resize((target_width, target_height), RESAMPLE_LANCZOS)

    gray = ImageOps.autocontrast(ImageOps.grayscale(rgb))
    blur_radius = max(1.0, min(rgb.width, rgb.height) / 180.0)
    blurred = gray.filter(ImageFilter.GaussianBlur(radius=blur_radius))
    gray_arr = np.asarray(gray, dtype=np.float32) / 255.0
    blur_arr = np.asarray(blurred, dtype=np.float32) / 255.0

    local_contrast = np.abs(gray_arr - blur_arr)
    edge_x = np.zeros_like(gray_arr)
    edge_y = np.zeros_like(gray_arr)
    edge_x[:, 1:] = np.abs(np.diff(gray_arr, axis=1))
    edge_y[1:, :] = np.abs(np.diff(gray_arr, axis=0))
    edge = np.maximum(np.maximum(edge_x, edge_y), local_contrast * 0.8)

    bright_threshold = max(0.62, float(np.quantile(gray_arr, 0.84)))
    contrast_threshold = max(0.045, float(np.quantile(local_contrast, 0.78)))
    edge_threshold = max(0.060, float(np.quantile(edge, 0.82)))
    bright_mask = gray_arr >= bright_threshold
    text_mask = ((local_contrast >= contrast_threshold) & (edge >= edge_threshold * 0.92)) | (
        bright_mask & (edge >= edge_threshold)
    )
    return np.asarray(text_mask, dtype=bool)


def load_subtitle_detection_mask(path_text: str):
    if not NUMPY_AVAILABLE or not PIL_AVAILABLE or not path_text:
        return None
    with Image.open(path_text) as image:
        crop = crop_image_ratio(
            image,
            SUBTITLE_MASK_LEFT_RATIO,
            SUBTITLE_MASK_TOP_RATIO,
            SUBTITLE_MASK_RIGHT_RATIO,
            SUBTITLE_MASK_BOTTOM_RATIO,
        )
        return build_subtitle_detection_mask(crop)


def detect_subtitle_component_box(
    mask,
    *,
    min_width_ratio: float = SUBTITLE_MASK_COMPONENT_MIN_WIDTH_RATIO,
    max_height_ratio: float = SUBTITLE_MASK_COMPONENT_MAX_HEIGHT_RATIO,
    preferred_center_y: float = 0.28,
) -> Optional[Tuple[int, int, int, int, float]]:
    if not CV2_AVAILABLE or not NUMPY_AVAILABLE or mask is None or not getattr(mask, "size", 0):
        return None

    mask_height, mask_width = mask.shape
    if mask_height < 20 or mask_width < 32:
        return None

    merged = (mask.astype(np.uint8) * 255)
    kernel_width = max(11, int(round(mask_width * 0.060)))
    kernel_height = max(3, int(round(mask_height * 0.020)))
    close_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_width, kernel_height))
    merged = cv2.morphologyEx(merged, cv2.MORPH_CLOSE, close_kernel)
    merged = cv2.morphologyEx(merged, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)))

    contours, _ = cv2.findContours(merged, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return None

    min_width = max(12, int(round(mask_width * min_width_ratio)))
    min_height = max(8, int(round(mask_height * 0.035)))
    max_height = max(min_height + 4, int(round(mask_height * max_height_ratio)))
    best_box: Optional[Tuple[int, int, int, int, float]] = None
    best_score = -1.0

    for contour in contours:
        x, y, width, height = cv2.boundingRect(contour)
        if width < min_width or height < min_height or height > max_height:
            continue
        if y >= int(mask_height * 0.80):
            continue
        aspect_ratio = width / max(1.0, float(height))
        if aspect_ratio < 2.0:
            continue

        box_mask = mask[y : y + height, x : x + width]
        density = float(box_mask.mean()) if box_mask.size else 0.0
        if density < 0.06:
            continue

        center_x_ratio = (x + width * 0.5) / max(1.0, mask_width)
        center_y_ratio = (y + height * 0.5) / max(1.0, mask_height)
        width_ratio = width / max(1.0, mask_width)
        vertical_score = max(0.0, 1.0 - abs(center_y_ratio - preferred_center_y) / 0.50)
        horizontal_score = max(0.0, 1.0 - abs(center_x_ratio - 0.5) / 0.68)
        aspect_score = min(1.0, aspect_ratio / 9.0)
        density_score = clamp((density - 0.06) / 0.18, 0.0, 1.0)
        score = (
            width_ratio * 0.46
            + aspect_score * 0.18
            + density_score * 0.18
            + vertical_score * 0.14
            + horizontal_score * 0.04
        )
        if score <= best_score:
            continue

        pad_x = max(3, int(round(width * 0.05)))
        pad_y = max(3, int(round(height * 0.18)))
        best_box = (
            max(0, x - pad_x),
            max(0, y - pad_y),
            min(mask_width, x + width + pad_x),
            min(mask_height, y + height + pad_y),
            score,
        )
        best_score = score

    return best_box


def detect_subtitle_box_in_image(image) -> Optional[Tuple[int, int, int, int, float]]:
    if not NUMPY_AVAILABLE or not PIL_AVAILABLE or image is None:
        return None
    mask = build_subtitle_detection_mask(image)
    component = detect_subtitle_component_box(
        mask,
        min_width_ratio=0.15,
        max_height_ratio=0.82,
        preferred_center_y=0.50,
    )
    if component is None:
        return None

    mask_x1, mask_y1, mask_x2, mask_y2, score = component
    scale_x = image.width / max(1, mask.shape[1])
    scale_y = image.height / max(1, mask.shape[0])
    return (
        max(0, int(round(mask_x1 * scale_x))),
        max(0, int(round(mask_y1 * scale_y))),
        min(image.width, int(round(mask_x2 * scale_x))),
        min(image.height, int(round(mask_y2 * scale_y))),
        score,
    )


def build_subtitle_text_mask_in_image(image):
    if not NUMPY_AVAILABLE or not PIL_AVAILABLE or image is None:
        return None
    mask = build_subtitle_detection_mask(image)
    if mask is None or not getattr(mask, "size", 0):
        return None
    mask_image = Image.fromarray((mask.astype(np.uint8) * 255), mode="L")
    if mask_image.size != image.size:
        mask_image = mask_image.resize(image.size, RESAMPLE_NEAREST)
    return np.asarray(mask_image, dtype=np.uint8) >= 128


def find_profile_segments(profile: Sequence[float], threshold: float) -> List[Tuple[int, int, float]]:
    segments: List[Tuple[int, int, float]] = []
    start: Optional[int] = None
    score = 0.0
    for index, value in enumerate(profile):
        if value >= threshold:
            if start is None:
                start = index
                score = 0.0
            score += float(value)
            continue
        if start is not None:
            segments.append((start, index, score))
            start = None
    if start is not None:
        segments.append((start, len(profile), score))
    return segments


def fallback_subtitle_mask_region(
    width: int,
    height: int,
    *,
    detected_top: Optional[int] = None,
    detected_bottom: Optional[int] = None,
    confidence: float = 0.0,
) -> VideoMaskRegion:
    x = max(0, int(round(width * SUBTITLE_MASK_DEFAULT_X_MARGIN_RATIO)))
    region_width = max(1, width - x * 2)
    if detected_top is None or detected_bottom is None or detected_bottom <= detected_top:
        y = max(0, int(round(height * SUBTITLE_MASK_DEFAULT_TOP_RATIO)))
        region_height = max(12, int(round(height * SUBTITLE_MASK_DEFAULT_HEIGHT_RATIO)))
    else:
        detected_height = max(12, detected_bottom - detected_top)
        y = max(0, detected_top - max(6, int(round(detected_height * 0.16))))
        region_height = max(
            12,
            min(height - y, detected_height + max(8, int(round(detected_height * 0.32)))),
        )
    if y + region_height > height:
        region_height = max(1, height - y)
    return VideoMaskRegion(
        x=x,
        y=y,
        width=region_width,
        height=region_height,
        confidence=confidence,
        source="fallback",
    )


def tighten_subtitle_output_region(
    region: VideoMaskRegion,
    video_width: int,
    video_height: int,
) -> VideoMaskRegion:
    x1 = 0
    x2 = video_width

    detected_y1 = max(0, min(video_height - 1, int(region.y)))
    detected_y2 = max(detected_y1 + 1, min(video_height, int(region.y + region.height)))
    detected_height = max(1, detected_y2 - detected_y1)
    y1 = detected_y1 - max(2, int(round(detected_height * SUBTITLE_MASK_OUTPUT_TOP_PADDING_RATIO)))
    y2 = detected_y2 + max(2, int(round(detected_height * SUBTITLE_MASK_OUTPUT_BOTTOM_PADDING_RATIO)))

    min_height = max(72, int(round(video_height * SUBTITLE_MASK_OUTPUT_MIN_HEIGHT_RATIO)))
    max_height = max(min_height + 8, int(round(video_height * SUBTITLE_MASK_OUTPUT_MAX_HEIGHT_RATIO)))
    current_height = max(1, y2 - y1)
    if current_height < min_height:
        missing = min_height - current_height
        y1 -= int(math.ceil(missing * 0.55))
        y2 += int(math.floor(missing * 0.45))
    elif current_height > max_height:
        excess = current_height - max_height
        trim_top = int(round(excess * 0.12))
        trim_bottom = excess - trim_top
        y1 += trim_top
        y2 -= trim_bottom

    if y1 < 0:
        y2 -= y1
        y1 = 0
    if y2 > video_height:
        y1 -= y2 - video_height
        y2 = video_height
    y1 = max(0, min(video_height - 1, y1))
    y2 = max(y1 + 1, min(video_height, y2))

    return VideoMaskRegion(
        x=x1,
        y=y1,
        width=max(1, x2 - x1),
        height=max(1, y2 - y1),
        confidence=region.confidence,
        source=f"{region.source}-band",
    )


def detect_subtitle_mask_region(
    video_path: Path,
    work_dir: Path,
    video_processor: "VideoProcessor",
    log_func: Optional[Callable[[str], None]] = None,
) -> Optional[VideoMaskRegion]:
    profile = video_processor.probe_video(video_path)
    width = int(profile["width"])
    height = int(profile["height"])
    if width < 160 or height < 160:
        return None
    if not PIL_AVAILABLE or not NUMPY_AVAILABLE:
        region = fallback_subtitle_mask_region(width, height, confidence=0.0)
        if log_func:
            log_func("  Subtitle mask detection fallback: local image analysis unavailable")
        return region

    sample_dir = work_dir / "subtitle_mask_samples"
    sample_paths = extract_video_sample_frames(video_path, sample_dir, video_processor)
    masks = [load_subtitle_detection_mask(str(path.resolve())) for path in sample_paths]
    masks = [mask for mask in masks if mask is not None and getattr(mask, "size", 0)]
    if len(masks) < 6:
        fallback = fallback_subtitle_mask_region(width, height, confidence=0.0)
        if log_func:
            log_func(
                "  Subtitle mask detection fallback: too few usable video samples, "
                f"using default band x={fallback.x}, y={fallback.y}, w={fallback.width}, h={fallback.height}"
            )
        return fallback

    crop_left, crop_top, crop_right, crop_bottom = crop_box_ratio(
        width,
        height,
        SUBTITLE_MASK_LEFT_RATIO,
        SUBTITLE_MASK_TOP_RATIO,
        SUBTITLE_MASK_RIGHT_RATIO,
        SUBTITLE_MASK_BOTTOM_RATIO,
    )
    crop_width = max(1, crop_right - crop_left)
    crop_height = max(1, crop_bottom - crop_top)
    mask_height, mask_width = masks[0].shape
    scale_x = crop_width / max(1, mask_width)
    scale_y = crop_height / max(1, mask_height)

    center_weights = 1.0 - 0.35 * np.abs(np.linspace(-1.0, 1.0, mask_width, dtype=np.float32))
    component_boxes: List[Tuple[int, int, int, int, float]] = []
    sample_row_profiles: List[np.ndarray] = []
    sample_band_tops: List[int] = []
    sample_band_bottoms: List[int] = []
    for mask in masks:
        component = detect_subtitle_component_box(mask)
        if component is not None:
            component_boxes.append(component)

        sample_profile = mask.astype(np.float32).dot(center_weights) / max(1.0, center_weights.sum())
        sample_row_profiles.append(sample_profile.astype(np.float32))
        if float(sample_profile.max()) <= 1e-6:
            continue

        local_peak = int(np.argmax(sample_profile))
        local_floor = max(0.010, float(np.quantile(sample_profile, 0.58)))
        local_threshold = max(0.040, local_floor * 1.45, float(sample_profile[local_peak]) * 0.42)
        local_top = local_peak
        local_bottom = local_peak + 1
        while local_top > 0 and sample_profile[local_top - 1] >= local_threshold:
            local_top -= 1
        while local_bottom < len(sample_profile) and sample_profile[local_bottom] >= local_threshold:
            local_bottom += 1

        support_threshold = max(0.026, local_threshold * 0.58, float(sample_profile[local_peak]) * 0.22)
        local_window = max(8, int(round(mask_height * 0.14)))
        support_rows = np.flatnonzero(sample_profile >= support_threshold)
        if support_rows.size:
            support_rows = support_rows[
                (support_rows >= max(0, local_peak - local_window))
                & (support_rows < min(mask_height, local_peak + local_window))
            ]
            if support_rows.size:
                local_top = min(local_top, int(support_rows[0]))
                local_bottom = max(local_bottom, int(support_rows[-1] + 1))

        if component is not None and component[4] >= 0.20:
            component_top = max(0, int(component[1]))
            component_bottom = min(mask_height, int(component[3]))
            local_height = max(1, local_bottom - local_top)
            max_extension = max(6, int(round(local_height * 0.65)))
            if component_top < local_top and local_top - component_top <= max_extension:
                local_top = component_top
            if component_bottom > local_bottom and component_bottom - local_bottom <= max_extension:
                local_bottom = component_bottom

        local_height = max(1, local_bottom - local_top)
        local_height_ratio = local_height / max(1.0, float(mask_height))
        if 0.015 <= local_height_ratio <= 0.20:
            sample_band_tops.append(local_top)
            sample_band_bottoms.append(local_bottom)

    row_profiles = np.vstack(sample_row_profiles)
    component_hint_top: Optional[int] = None
    component_hint_bottom: Optional[int] = None
    component_confidence = 0.0
    min_component_frames = min(len(masks), max(SUBTITLE_MASK_COMPONENT_MIN_FRAMES, int(round(len(masks) * 0.10))))
    if len(component_boxes) >= min_component_frames:
        component_scores = [box[4] for box in component_boxes]
        coverage = len(component_boxes) / max(1, len(masks))
        component_confidence = clamp(float(np.mean(component_scores)) * 0.78 + coverage * 0.22, 0.0, 1.0)
        component_hint_top = int(round(float(np.quantile([box[1] for box in component_boxes], 0.28))))
        component_hint_bottom = int(round(float(np.quantile([box[3] for box in component_boxes], 0.72))))
        if log_func:
            log_func(
                "  Subtitle mask component hint: "
                f"top={component_hint_top}, bottom={component_hint_bottom}, "
                f"confidence {component_confidence:.2f}, samples {len(component_boxes)}/{len(masks)}"
            )

    sample_hint_top: Optional[int] = None
    sample_hint_bottom: Optional[int] = None
    if len(sample_band_tops) >= min_component_frames:
        sample_hint_top = int(round(float(np.quantile(sample_band_tops, 0.18))))
        sample_hint_bottom = int(round(float(np.quantile(sample_band_bottoms, 0.82))))
        if log_func:
            log_func(
                "  Subtitle mask row envelope hint: "
                f"top={sample_hint_top}, bottom={sample_hint_bottom}, "
                f"samples {len(sample_band_tops)}/{len(masks)}"
            )

    row_floor = max(0.010, float(np.quantile(row_profiles, 0.35)))
    row_presence = np.mean(row_profiles >= max(0.022, row_floor * 1.7), axis=0)
    row_strength = np.quantile(row_profiles, 0.70, axis=0)
    if float(row_strength.max()) <= 1e-6:
        return None
    row_strength = row_strength / max(1e-6, float(row_strength.max()))
    row_profile = row_presence * 0.60 + row_strength * 0.40
    row_profile *= np.linspace(0.82, 1.42, len(row_profile), dtype=np.float32)
    row_profile = smooth_numeric_profile(row_profile, max(5, int(len(row_profile) * 0.05)))

    peak_row = int(np.argmax(row_profile))
    strong_threshold = max(
        0.16,
        float(np.quantile(row_profile, 0.84)) * 0.92,
        float(row_profile[peak_row]) * 0.70,
    )
    best_top = peak_row
    best_bottom = peak_row + 1
    while best_top > 0 and row_profile[best_top - 1] >= strong_threshold:
        best_top -= 1
    while best_bottom < len(row_profile) and row_profile[best_bottom] >= strong_threshold:
        best_bottom += 1

    support_threshold = max(
        0.050,
        float(np.quantile(row_profile, 0.70)) * 0.45,
        float(row_profile[peak_row]) * 0.24,
    )
    support_window = max(10, int(round(mask_height * 0.16)))
    while best_top > max(0, peak_row - support_window):
        probe = best_top - 1
        if row_profile[probe] >= support_threshold or (row_presence[probe] >= 0.05 and row_strength[probe] >= 0.14):
            best_top -= 1
            continue
        break
    while best_bottom < min(len(row_profile), peak_row + support_window):
        probe = best_bottom
        if row_profile[probe] >= support_threshold or (row_presence[probe] >= 0.05 and row_strength[probe] >= 0.14):
            best_bottom += 1
            continue
        break

    if sample_hint_top is not None and sample_hint_bottom is not None and sample_hint_bottom > sample_hint_top:
        best_top = min(best_top, sample_hint_top)
        best_bottom = max(best_bottom, sample_hint_bottom)

    if component_hint_top is not None and component_hint_bottom is not None and component_hint_bottom > component_hint_top:
        current_height = max(1, best_bottom - best_top)
        max_hint_extension = max(8, int(round(current_height * 0.55)))
        if component_hint_top < best_top and best_top - component_hint_top <= max_hint_extension:
            best_top = component_hint_top
        if component_hint_bottom > best_bottom and component_hint_bottom - best_bottom <= max_hint_extension:
            best_bottom = component_hint_bottom

    leak_threshold = max(
        0.032,
        float(np.quantile(row_profile, 0.62)) * 0.42,
        float(row_profile[peak_row]) * 0.16,
    )
    leak_scan = max(10, int(round(mask_height * 0.12)))
    above_start = max(0, best_top - leak_scan)
    above_candidates = np.flatnonzero(
        (row_profile[above_start:best_top] >= leak_threshold)
        & ((row_presence[above_start:best_top] >= 0.05) | (row_strength[above_start:best_top] >= 0.10))
    )
    if above_candidates.size:
        best_top = above_start + int(above_candidates[0])

    below_end = min(len(row_profile), best_bottom + leak_scan)
    below_candidates = np.flatnonzero(
        (row_profile[best_bottom:below_end] >= leak_threshold)
        & ((row_presence[best_bottom:below_end] >= 0.05) | (row_strength[best_bottom:below_end] >= 0.10))
    )
    if below_candidates.size:
        best_bottom = best_bottom + int(below_candidates[-1] + 1)

    band_height = max(1, best_bottom - best_top)
    best_top = max(0, best_top - max(2, int(round(band_height * 0.08))))
    best_bottom = min(len(row_profile), best_bottom + max(2, int(round(band_height * 0.10))))

    column_probe_top = max(0, best_top - max(1, int(round((best_bottom - best_top) * 0.06))))
    column_probe_bottom = min(
        mask_height,
        best_bottom + max(4, int(round((best_bottom - best_top) * 0.75))),
    )
    column_profiles = np.vstack([mask[column_probe_top:column_probe_bottom, :].mean(axis=0) for mask in masks])
    col_floor = max(0.010, float(np.quantile(column_profiles, 0.45)))
    col_presence = np.mean(column_profiles >= max(0.018, col_floor * 1.6), axis=0)
    col_strength = np.quantile(column_profiles, 0.70, axis=0)
    if float(col_strength.max()) > 1e-6:
        col_strength = col_strength / max(1e-6, float(col_strength.max()))
    col_profile = col_presence * 0.45 + col_strength * 0.55
    col_profile *= 1.0 - 0.15 * np.abs(np.linspace(-1.0, 1.0, len(col_profile), dtype=np.float32))
    col_profile = smooth_numeric_profile(col_profile, max(5, int(len(col_profile) * 0.03)))

    col_threshold = max(0.08, float(col_profile.max()) * 0.34)
    segments = find_profile_segments(col_profile, col_threshold)
    min_segment_width = max(12, int(round(mask_width * 0.24)))
    center = mask_width / 2.0
    selected_segment: Optional[Tuple[int, int, float]] = None
    selected_score = -999.0
    for start, end, segment_score in segments:
        width_score = (end - start) / max(1.0, mask_width)
        if end - start < min_segment_width:
            continue
        distance_penalty = abs(((start + end) / 2.0) - center) / max(1.0, mask_width)
        score = segment_score / max(1.0, end - start) + width_score * 0.35 - distance_penalty * 0.25
        if score > selected_score:
            selected_segment = (start, end, segment_score)
            selected_score = score

    if selected_segment is None:
        mask_x1 = int(round(mask_width * 0.10))
        mask_x2 = int(round(mask_width * 0.90))
    else:
        mask_x1, mask_x2, _ = selected_segment
        if mask_x2 - mask_x1 < min_segment_width:
            expand = (min_segment_width - (mask_x2 - mask_x1)) // 2 + 1
            mask_x1 = max(0, mask_x1 - expand)
            mask_x2 = min(mask_width, mask_x2 + expand)

    x_margin = max(4, int(round((mask_x2 - mask_x1) * 0.05)))
    y_margin_top = max(2, int(round((best_bottom - best_top) * 0.08)))
    y_margin_bottom = max(2, int(round((best_bottom - best_top) * 0.10)))
    mask_x1 = max(0, mask_x1 - x_margin)
    mask_x2 = min(mask_width, mask_x2 + x_margin)
    mask_y1 = max(0, best_top - y_margin_top)
    mask_y2 = min(mask_height, best_bottom + y_margin_bottom)

    detected_x1 = crop_left + int(round(mask_x1 * scale_x))
    detected_x2 = crop_left + int(round(mask_x2 * scale_x))
    detected_y1 = crop_top + int(round(mask_y1 * scale_y))
    detected_y2 = crop_top + int(round(mask_y2 * scale_y))
    detected_x1 = max(0, min(width - 1, detected_x1))
    detected_x2 = max(detected_x1 + 1, min(width, detected_x2))
    detected_y1 = max(0, min(height - 1, detected_y1))
    detected_y2 = max(detected_y1 + 1, min(height, detected_y2))

    band_signal = float(row_profile[best_top:best_bottom].mean()) if best_bottom > best_top else 0.0
    presence_signal = float(row_presence[best_top:best_bottom].mean()) if best_bottom > best_top else 0.0
    column_signal = float(col_profile[mask_x1:mask_x2].mean()) if mask_x2 > mask_x1 else 0.0
    confidence = clamp(band_signal * 0.55 + presence_signal * 0.25 + column_signal * 0.20, 0.0, 1.0)

    if confidence < SUBTITLE_MASK_MIN_CONFIDENCE:
        peak_signal = max(confidence, float(row_profile.max()))
        if peak_signal < SUBTITLE_MASK_FALLBACK_SIGNAL:
            if log_func:
                log_func(f"  Subtitle mask detection skipped: low confidence {confidence:.2f}")
            return None
        fallback = fallback_subtitle_mask_region(
            width,
            height,
            detected_top=detected_y1,
            detected_bottom=detected_y2,
            confidence=peak_signal,
        )
        if log_func:
            log_func(
                "  Subtitle mask region fallback: "
                f"x={fallback.x}, y={fallback.y}, w={fallback.width}, h={fallback.height}, "
                f"confidence {fallback.confidence:.2f}, samples {len(masks)}"
            )
        return fallback

    region = VideoMaskRegion(
        x=detected_x1,
        y=detected_y1,
        width=max(1, detected_x2 - detected_x1),
        height=max(1, detected_y2 - detected_y1),
        confidence=confidence,
        source="auto",
    )
    if log_func:
        log_func(
            "  Subtitle mask region detected: "
            f"x={region.x}, y={region.y}, w={region.width}, h={region.height}, "
            f"confidence {region.confidence:.2f}, samples {len(masks)}"
        )
    return region


def _apply_static_subtitle_mask_blur(
    source_video: Path,
    output_path: Path,
    region: VideoMaskRegion,
    video_processor: "VideoProcessor",
) -> None:
    blur_width = max(96, region.width // 3)
    blur_height = max(18, region.height // 3)
    blur_luma_radius = max(6, min(18, region.height // 5))
    blur_chroma_radius = max(4, min(12, region.height // 7))
    filter_complex = (
        "[0:v]split=2[vbase][vmasksrc];"
        f"[vmasksrc]crop={region.width}:{region.height}:{region.x}:{region.y},"
        f"scale={blur_width}:{blur_height}:flags=bicubic,"
        f"scale={region.width}:{region.height}:flags=bicubic,"
        f"boxblur=luma_radius={blur_luma_radius}:luma_power=2:chroma_radius={blur_chroma_radius}:chroma_power=1,"
        "eq=saturation=0.86:contrast=0.92:brightness=0.012,"
        "drawbox=x=0:y=0:w=iw:h=ih:color=0xB7B0A9@0.24:t=fill[vmask];"
        f"[vbase][vmask]overlay={region.x}:{region.y}:format=auto[vout]"
    )
    result = run_subprocess_hidden(
        [
            str(video_processor.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-i",
            str(source_video),
            "-filter_complex",
            filter_complex,
            "-map",
            "[vout]",
            "-map",
            "0:a?",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            str(output_path),
        ],
        capture_output=True,
        timeout=600,
        check=False,
    )
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore")[:400] or "subtitle mask blur failed")


def build_feathered_subtitle_alpha(height: int, width: int, box: Tuple[int, int, int, int]):
    if not CV2_AVAILABLE or not NUMPY_AVAILABLE:
        return None
    x1, y1, x2, y2 = box
    alpha = np.zeros((height, width), dtype=np.float32)
    if x2 <= x1 or y2 <= y1:
        return alpha
    cv2.rectangle(
        alpha,
        (int(x1), int(y1)),
        (int(max(x1 + 1, x2 - 1)), int(max(y1 + 1, y2 - 1))),
        color=1.0,
        thickness=-1,
        lineType=cv2.LINE_AA,
    )
    feather = max(3.0, min(width, height, max(6.0, min((x2 - x1) * 0.12, (y2 - y1) * 0.34))))
    alpha = cv2.GaussianBlur(alpha, (0, 0), sigmaX=feather, sigmaY=max(2.0, feather * 0.72))
    return np.clip(alpha * 1.08, 0.0, 1.0)


def apply_dynamic_subtitle_mask(
    source_video: Path,
    output_path: Path,
    region: VideoMaskRegion,
    video_processor: "VideoProcessor",
    log_func: Optional[Callable[[str], None]] = None,
) -> bool:
    if not CV2_AVAILABLE or not NUMPY_AVAILABLE or not PIL_AVAILABLE:
        return False

    capture = cv2.VideoCapture(str(source_video))
    if not capture.isOpened():
        raise RuntimeError(f"failed to open video for subtitle masking: {source_video}")

    width = int(round(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or 0))
    height = int(round(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0))
    fps = float(capture.get(cv2.CAP_PROP_FPS) or 25.0)
    frame_count = int(round(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0))
    if width <= 0 or height <= 0:
        capture.release()
        raise RuntimeError("invalid video dimensions for subtitle masking")

    region_x1 = max(0, min(width - 1, region.x))
    region_y1 = max(0, min(height - 1, region.y))
    region_x2 = max(region_x1 + 1, min(width, region.x + region.width))
    region_y2 = max(region_y1 + 1, min(height, region.y + region.height))
    region_width = max(1, region_x2 - region_x1)
    region_height = max(1, region_y2 - region_y1)
    inset_x = max(3, int(round(region_width * 0.02)))
    inset_y = max(2, int(round(region_height * 0.10)))
    base_alpha = build_feathered_subtitle_alpha(
        region_height,
        region_width,
        (
            inset_x,
            inset_y,
            max(inset_x + 1, region_width - inset_x),
            max(inset_y + 1, region_height - inset_y),
        ),
    )
    base_sigma_x = max(1.8, min(5.2, region_width * 0.012))
    base_sigma_y = max(1.6, min(4.2, region_height * 0.18))

    with tempfile.TemporaryDirectory(prefix="subtitle_mask_", dir=str(output_path.parent)) as temp_dir_text:
        temp_dir = Path(temp_dir_text)
        temp_video_path = temp_dir / "masked_video.mp4"
        writer = cv2.VideoWriter(
            str(temp_video_path),
            cv2.VideoWriter_fourcc(*"mp4v"),
            fps,
            (width, height),
        )
        if not writer.isOpened():
            capture.release()
            raise RuntimeError("failed to create temporary subtitle-masked video")

        masked_frames = 0
        base_masked_frames = 0
        frame_index = 0
        last_subtitle_box: Optional[Tuple[int, int, int, int]] = None
        last_box_hold = 0
        try:
            while True:
                ok, frame = capture.read()
                if not ok:
                    break
                frame_index += 1
                crop = frame[region_y1:region_y2, region_x1:region_x2]
                if crop.size:
                    crop_float = crop.astype(np.float32)
                    if base_alpha is not None and float(base_alpha.max()) > 0.01:
                        base_softened = cv2.GaussianBlur(crop, (0, 0), sigmaX=base_sigma_x, sigmaY=base_sigma_y)
                        base_blend_alpha = (base_alpha * SUBTITLE_MASK_BASE_ALPHA)[:, :, None]
                        crop_float = crop_float * (1.0 - base_blend_alpha) + base_softened.astype(np.float32) * base_blend_alpha
                        crop = np.clip(crop_float, 0, 255).astype(np.uint8)
                        frame[region_y1:region_y2, region_x1:region_x2] = crop
                        base_masked_frames += 1
                    crop_rgb = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB)
                    crop_image = Image.fromarray(crop_rgb)
                    subtitle_box = detect_subtitle_box_in_image(crop_image)
                    active_box: Optional[Tuple[int, int, int, int]] = None
                    text_mask = build_subtitle_text_mask_in_image(crop_image)
                    if subtitle_box is not None and subtitle_box[4] >= 0.24:
                        active_box = (subtitle_box[0], subtitle_box[1], subtitle_box[2], subtitle_box[3])
                        last_subtitle_box = active_box
                        last_box_hold = SUBTITLE_MASK_BOX_HOLD_FRAMES
                    elif last_subtitle_box is not None and last_box_hold > 0:
                        active_box = last_subtitle_box
                        last_box_hold -= 1
                    else:
                        last_subtitle_box = None
                        last_box_hold = 0

                    if active_box is not None:
                        box_x1, box_y1, box_x2, box_y2 = active_box
                        alpha = build_feathered_subtitle_alpha(crop.shape[0], crop.shape[1], active_box)
                        if alpha is not None and float(alpha.max()) > 0.02 and text_mask is not None:
                            inpaint_mask = np.zeros((crop.shape[0], crop.shape[1]), dtype=np.uint8)
                            clipped_mask = text_mask[box_y1:box_y2, box_x1:box_x2]
                            if clipped_mask.size and bool(clipped_mask.any()):
                                inpaint_mask[box_y1:box_y2, box_x1:box_x2] = (clipped_mask.astype(np.uint8) * 255)
                            else:
                                inpaint_mask[box_y1:box_y2, box_x1:box_x2] = 255
                            if bool(inpaint_mask.any()):
                                kernel_width = max(5, int(round((box_x2 - box_x1) * 0.055)))
                                kernel_height = max(5, int(round((box_y2 - box_y1) * 0.32)))
                                dilate_kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (kernel_width, kernel_height))
                                inpaint_mask = cv2.dilate(inpaint_mask, dilate_kernel, iterations=1)
                                inpaint_radius = max(4, min(11, int(round((box_y2 - box_y1) * 0.24))))
                                inpainted = cv2.inpaint(crop, inpaint_mask, inpaint_radius, cv2.INPAINT_TELEA)
                                soften_sigma_x = max(2.5, min(7.0, (box_x2 - box_x1) * 0.035))
                                soften_sigma_y = max(2.0, min(5.0, (box_y2 - box_y1) * 0.10))
                                softened = cv2.GaussianBlur(inpainted, (0, 0), sigmaX=soften_sigma_x, sigmaY=soften_sigma_y)
                                alpha_map = alpha * SUBTITLE_MASK_DYNAMIC_ALPHA
                                alpha_map[inpaint_mask > 0] = 1.0
                                blend_alpha = alpha_map[:, :, None]
                                crop_float = crop.astype(np.float32)
                                blended_float = softened.astype(np.float32)
                                blended = crop_float * (1.0 - blend_alpha) + blended_float * blend_alpha
                                frame[region_y1:region_y2, region_x1:region_x2] = np.clip(blended, 0, 255).astype(np.uint8)
                                masked_frames += 1
                writer.write(frame)
                if log_func and frame_count > 0 and frame_index % max(1, int(fps * 20)) == 0:
                    log_func(f"  Subtitle mask dynamic pass: {frame_index}/{frame_count} frames")
        finally:
            capture.release()
            writer.release()

        if base_masked_frames <= 0 and masked_frames <= 0:
            return False

        result = run_subprocess_hidden(
            [
                str(video_processor.ffmpeg),
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-y",
                "-i",
                str(temp_video_path),
                "-i",
                str(source_video),
                "-map",
                "0:v:0",
                "-map",
                "1:a?",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "20",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-movflags",
                "+faststart",
                str(output_path),
            ],
            capture_output=True,
            timeout=1800,
            check=False,
        )
        if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
            raise RuntimeError(
                result.stderr.decode("utf-8", errors="ignore")[:400] or "dynamic subtitle mask mux failed"
            )
        if log_func:
            log_func(
                f"  Subtitle mask dynamic mode applied on {base_masked_frames} base frames, "
                f"{masked_frames} text frames"
            )
        return True


def apply_subtitle_mask_mosaic(
    source_video: Path,
    output_path: Path,
    region: VideoMaskRegion,
    video_processor: "VideoProcessor",
    log_func: Optional[Callable[[str], None]] = None,
) -> None:
    if log_func:
        log_func("  Subtitle mask mode: static full-region blur")
    _apply_static_subtitle_mask_blur(source_video, output_path, region, video_processor)


@lru_cache(maxsize=16384)
def load_structural_audit_frame(path_text: str, flip_horizontal: bool = False):
    if not NUMPY_AVAILABLE or not PIL_AVAILABLE or not path_text:
        return None
    with Image.open(path_text) as image:
        rgb = image.convert("RGB")
        if flip_horizontal:
            rgb = ImageOps.mirror(rgb)
        cropped = crop_image_ratio(rgb, 0.08, 0.05, 0.92, 0.68)
        gray = ImageOps.grayscale(cropped).resize((96, 128), RESAMPLE_LANCZOS)
        return np.asarray(gray, dtype=np.float32) / 255.0


def structural_ssim_value(left, right) -> float:
    if not NUMPY_AVAILABLE or left is None or right is None:
        return 0.0
    c1 = 0.01**2
    c2 = 0.03**2
    mean_left = float(left.mean())
    mean_right = float(right.mean())
    var_left = float(((left - mean_left) ** 2).mean())
    var_right = float(((right - mean_right) ** 2).mean())
    covariance = float(((left - mean_left) * (right - mean_right)).mean())
    numerator = (2 * mean_left * mean_right + c1) * (2 * covariance + c2)
    denominator = (mean_left * mean_left + mean_right * mean_right + c1) * (var_left + var_right + c2)
    if denominator <= 0:
        return 0.0
    return max(0.0, min(1.0, numerator / denominator))


def structural_frame_similarity_from_paths(left_path: str, right_path: str, flip_right: bool = False) -> float:
    if not NUMPY_AVAILABLE or not left_path or not right_path:
        return 0.0
    left = load_structural_audit_frame(left_path)
    right = load_structural_audit_frame(right_path, flip_horizontal=flip_right)
    if left is None or right is None:
        return 0.0

    y_splits = np.linspace(0, left.shape[0], 7, dtype=int)
    x_splits = np.linspace(0, left.shape[1], 5, dtype=int)
    scores: List[float] = []
    for y_index in range(len(y_splits) - 1):
        for x_index in range(len(x_splits) - 1):
            left_block = left[y_splits[y_index] : y_splits[y_index + 1], x_splits[x_index] : x_splits[x_index + 1]]
            right_block = right[y_splits[y_index] : y_splits[y_index + 1], x_splits[x_index] : x_splits[x_index + 1]]
            scores.append(structural_ssim_value(left_block, right_block))
    return sum(scores) / max(1, len(scores))


def sample_signature_similarity(
    hasher: "VisualHasher",
    reference_signature: Tuple[int, ...],
    sample: FrameSample,
    *,
    preferred_flip: Optional[bool] = None,
    flip_margin: float = 0.015,
) -> Tuple[float, bool, float, float]:
    normal = hasher.similarity(reference_signature, sample.signature)
    flipped = hasher.similarity(reference_signature, sample.flipped_signature) if sample.flipped_signature else normal
    if preferred_flip is True and sample.flipped_signature:
        return flipped, True, normal, flipped
    if preferred_flip is False or not sample.flipped_signature:
        return normal, False, normal, flipped
    if flipped > normal + flip_margin:
        return flipped, True, normal, flipped
    return normal, False, normal, flipped


def sample_refined_similarity(
    hasher: "VisualHasher",
    reference_frame: ReferenceFrame,
    sample: FrameSample,
    *,
    preferred_flip: Optional[bool] = None,
    flip_margin: float = 0.012,
) -> Tuple[float, bool, float, float]:
    if not reference_frame.frame_path or not sample.frame_path:
        return 0.0, False, 0.0, 0.0
    normal = hasher.refined_similarity_from_paths(reference_frame.frame_path, sample.frame_path)
    flipped = hasher.refined_similarity_from_paths(
        reference_frame.frame_path,
        sample.frame_path,
        flip_right=True,
    )
    if preferred_flip is True:
        return flipped, True, normal, flipped
    if preferred_flip is False:
        return normal, False, normal, flipped
    if flipped > normal + flip_margin:
        return flipped, True, normal, flipped
    return normal, False, normal, flipped


class VisualHasher:
    def __init__(self, hash_size: int = 16):
        if not PIL_AVAILABLE:
            raise RuntimeError("缺少 Pillow，无法进行帧匹配。")
        self.hash_size = hash_size
        self.signature_parts = 10
        self.refined_gray_size = (18, 32)
        self.refined_color_size = (8, 14)

    def _normalize_hash_image(self, image):
        return ImageOps.autocontrast(ImageOps.grayscale(image))

    def _dhash_horizontal(self, image) -> int:
        gray = self._normalize_hash_image(image)
        resized = gray.resize((self.hash_size + 1, self.hash_size), RESAMPLE_LANCZOS)
        pixels = resized.tobytes()
        value = 0
        for idx in range(self.hash_size * self.hash_size):
            if pixels[idx] > pixels[idx + self.hash_size]:
                value |= 1 << idx
        return value

    def _dhash_vertical(self, image) -> int:
        gray = self._normalize_hash_image(image)
        resized = gray.resize((self.hash_size, self.hash_size + 1), RESAMPLE_LANCZOS)
        pixels = resized.tobytes()
        value = 0
        row_span = self.hash_size
        for row in range(self.hash_size):
            base = row * row_span
            next_row = (row + 1) * row_span
            for col in range(self.hash_size):
                idx = base + col
                if pixels[idx] > pixels[next_row + col]:
                    value |= 1 << (row * self.hash_size + col)
        return value

    def _ahash(self, image) -> int:
        gray = self._normalize_hash_image(image)
        resized = gray.resize((self.hash_size, self.hash_size), RESAMPLE_LANCZOS)
        pixels = list(resized.getdata())
        mean_value = sum(pixels) / max(1, len(pixels))
        value = 0
        for idx, pixel in enumerate(pixels):
            if pixel >= mean_value:
                value |= 1 << idx
        return value

    def _crop_ratio(self, image, left: float, top: float, right: float, bottom: float):
        width, height = image.size
        box = (
            max(0, min(width - 1, int(width * left))),
            max(0, min(height - 1, int(height * top))),
            max(1, min(width, int(width * right))),
            max(1, min(height, int(height * bottom))),
        )
        return image.crop(box)

    def compute_signature(self, image) -> Tuple[int, ...]:
        width, height = image.size
        full = image
        focus = self._crop_ratio(image, 0.12, 0.12, 0.88, 0.78)
        upper = image.crop((0, int(height * 0.06), width, int(height * 0.58)))
        body = self._crop_ratio(image, 0.08, 0.18, 0.92, 0.74)
        return (
            self._dhash_horizontal(full),
            self._dhash_vertical(full),
            self._dhash_horizontal(focus),
            self._dhash_vertical(focus),
            self._dhash_horizontal(upper),
            self._dhash_vertical(upper),
            self._dhash_horizontal(body),
            self._dhash_vertical(body),
            self._ahash(focus),
            self._ahash(body),
        )

    def compute_signature_from_file(self, path: Path, flip_horizontal: bool = False) -> Tuple[int, ...]:
        with Image.open(path) as image:
            if flip_horizontal:
                image = ImageOps.mirror(image)
            return self.compute_signature(image)

    def _gray_bytes(self, image, size: Tuple[int, int]) -> bytes:
        gray = self._normalize_hash_image(image)
        return gray.resize(size, RESAMPLE_LANCZOS).tobytes()

    def _edge_bytes(self, image, size: Tuple[int, int]) -> bytes:
        gray = self._normalize_hash_image(image)
        edged = gray.filter(ImageFilter.FIND_EDGES)
        edged = ImageOps.autocontrast(edged)
        return edged.resize(size, RESAMPLE_LANCZOS).tobytes()

    def _color_bytes(self, image, size: Tuple[int, int]) -> bytes:
        rgb = ImageOps.autocontrast(image.convert("RGB"))
        return rgb.resize(size, RESAMPLE_LANCZOS).tobytes()

    def compute_refined_signature(self, image) -> Tuple[bytes, ...]:
        body = self._crop_ratio(image, 0.08, 0.14, 0.92, 0.78)
        focus = self._crop_ratio(image, 0.16, 0.18, 0.84, 0.72)
        upper = self._crop_ratio(image, 0.04, 0.06, 0.96, 0.56)
        return (
            self._gray_bytes(body, self.refined_gray_size),
            self._gray_bytes(focus, self.refined_gray_size),
            self._gray_bytes(upper, self.refined_gray_size),
            self._edge_bytes(focus, self.refined_gray_size),
            self._color_bytes(body, self.refined_color_size),
        )

    @lru_cache(maxsize=16384)
    def compute_refined_signature_from_file(
        self,
        path_text: str,
        flip_horizontal: bool = False,
    ) -> Tuple[bytes, ...]:
        with Image.open(path_text) as image:
            if flip_horizontal:
                image = ImageOps.mirror(image)
            return self.compute_refined_signature(image)

    def _byte_similarity(self, left: bytes, right: bytes) -> float:
        if not left or len(left) != len(right):
            return 0.0
        diff_total = 0
        for lhs, rhs in zip(left, right):
            diff_total += abs(lhs - rhs)
        return max(0.0, 1.0 - diff_total / (255.0 * len(left)))

    def refined_similarity_from_paths(
        self,
        left_path: str,
        right_path: str,
        flip_right: bool = False,
    ) -> float:
        if not left_path or not right_path:
            return 0.0
        left = self.compute_refined_signature_from_file(left_path)
        right = self.compute_refined_signature_from_file(right_path, flip_horizontal=flip_right)
        weights = (0.28, 0.24, 0.18, 0.18, 0.12)
        total = 0.0
        for weight, lhs, rhs in zip(weights, left, right):
            total += weight * self._byte_similarity(lhs, rhs)
        return total

    def similarity(
        self,
        left: Tuple[int, ...],
        right: Tuple[int, ...],
    ) -> float:
        weights = (0.10, 0.07, 0.18, 0.13, 0.12, 0.10, 0.12, 0.08, 0.06, 0.04)
        total = 0.0
        bits = self.hash_size * self.hash_size
        for weight, lhs, rhs in zip(weights, left, right):
            distance = bin(lhs ^ rhs).count("1")
            total += weight * (1.0 - distance / bits)
        return total


class VideoProcessor:
    def __init__(self, ffmpeg: Path = DEFAULT_FFMPEG, ffprobe: Path = DEFAULT_FFPROBE):
        self.ffmpeg = ffmpeg
        self.ffprobe = ffprobe
        self._audio_stream_presence_cache: Dict[str, bool] = {}

    def probe_duration(self, path: Path) -> float:
        try:
            result = run_subprocess_hidden(
                [
                    str(self.ffprobe),
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(path),
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                timeout=30,
                check=False,
            )
            return float(result.stdout.strip())
        except (ValueError, OSError):
            return 0.0

    def probe_audio(self, path: Path) -> Dict[str, str]:
        result = run_subprocess_hidden(
            [
                str(self.ffprobe),
                "-v",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=codec_name,sample_rate,channels",
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "ffprobe audio failed")
        payload = json.loads(result.stdout)
        stream = payload["streams"][0]
        return {
            "codec_name": str(stream.get("codec_name", "")),
            "sample_rate": str(stream.get("sample_rate", "")),
            "channels": str(stream.get("channels", "")),
        }

    def has_audio_stream(self, path: Path) -> bool:
        try:
            cache_key = str(path.resolve())
        except OSError:
            cache_key = str(path)
        if cache_key in self._audio_stream_presence_cache:
            return self._audio_stream_presence_cache[cache_key]
        try:
            audio_info = self.probe_audio(path)
            has_audio = bool(str(audio_info.get("codec_name", "") or "").strip())
        except Exception:
            has_audio = False
        self._audio_stream_presence_cache[cache_key] = has_audio
        return has_audio

    def probe_audio_volume(self, path: Path) -> Dict[str, float]:
        try:
            result = run_subprocess_hidden(
                [
                    str(self.ffmpeg),
                    "-hide_banner",
                    "-loglevel",
                    "info",
                    "-nostdin",
                    "-i",
                    str(path),
                    "-vn",
                    "-af",
                    "volumedetect",
                    "-f",
                    "null",
                    "-",
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                timeout=60,
                check=False,
            )
        except OSError:
            return {}

        output_text = "\n".join(part for part in [result.stdout, result.stderr] if part)
        stats: Dict[str, float] = {}
        for key in ("mean_volume", "max_volume"):
            match = re.search(rf"{key}:\s*(-?\d+(?:\.\d+)?)\s*dB", output_text)
            if match:
                stats[key] = float(match.group(1))
        return stats

    def probe_video(self, path: Path) -> Dict[str, str]:
        result = run_subprocess_hidden(
            [
                str(self.ffprobe),
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height,avg_frame_rate",
                "-show_entries",
                "format=duration",
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "ffprobe failed")
        payload = json.loads(result.stdout)
        stream = payload["streams"][0]
        return {
            "width": str(stream["width"]),
            "height": str(stream["height"]),
            "fps": stream["avg_frame_rate"],
            "duration": str(payload["format"]["duration"]),
        }

    def cut_segment(
        self,
        source: Path,
        output: Path,
        start: float,
        duration: float,
        width: int,
        height: int,
        fps: float,
        hflip: bool = False,
    ) -> None:
        if duration <= 0:
            raise ValueError("segment duration must be positive")
        vf_parts: List[str] = []
        source_has_audio = self.has_audio_stream(source)
        if hflip:
            vf_parts.append("hflip")
        vf_parts.extend(
            [
                f"scale={width}:{height}:force_original_aspect_ratio=decrease",
                f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black",
                f"fps={fps:.3f}",
            ]
        )
        vf = ",".join(vf_parts)
        command = [
            str(self.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-i",
            str(source),
        ]
        if not source_has_audio:
            command.extend(
                [
                    "-f",
                    "lavfi",
                    "-i",
                    "anullsrc=channel_layout=stereo:sample_rate=48000",
                ]
            )
        command.extend(
            [
                "-ss",
                f"{start:.3f}",
                "-t",
                f"{duration:.3f}",
                "-vf",
                vf,
                "-r",
                f"{fps:.3f}",
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?" if source_has_audio else "1:a:0",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "20",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "160k",
                "-ar",
                "48000",
                "-ac",
                "2",
                str(output),
            ]
        )
        result = run_subprocess_hidden(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip()[:400] or "ffmpeg cut failed")

    def concat_videos(self, concat_list: Path, output: Path) -> None:
        valid_lines: List[str] = []
        for line in concat_list.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.startswith("file '"):
                continue
            path_text = line[len("file '") : -1]
            if Path(path_text).exists():
                valid_lines.append(line)
        if not valid_lines:
            raise RuntimeError("没有可拼接的片段。")
        concat_list.write_text("\n".join(valid_lines) + "\n", encoding="utf-8")
        result = run_subprocess_hidden(
            [
                str(self.ffmpeg),
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_list),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "20",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-movflags",
                "+faststart",
                str(output),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=900,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip()[:400] or "ffmpeg concat failed")


class AINarrationGenerator:
    PRESETS = {
        "通义千问": {
            "api_url": "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
            "models": ["qwen-turbo", "qwen-plus", "qwen-max"],
        },
        "OpenAI": {
            "api_url": "https://api.openai.com/v1/chat/completions",
            "models": ["gpt-4o", "gpt-4o-mini"],
        },
        "DeepSeek": {
            "api_url": "https://api.deepseek.com/v1/chat/completions",
            "models": ["deepseek-chat"],
        },
        "智谱清言": {
            "api_url": "https://open.bigmodel.cn/api/paas/v4/chat/completions",
            "models": ["glm-4", "glm-4-flash"],
        },
        "Ollama 本地": {
            "api_url": "http://localhost:11434/v1/chat/completions",
            "models": ["llama3", "qwen2.5"],
        },
        "自定义": {"api_url": "", "models": []},
    }

    def __init__(
        self,
        api_key: str,
        model: str,
        api_url: str,
        fallback_models: Optional[Sequence[Dict[str, str]]] = None,
    ):
        primary = {
            "label": "主模型",
            "api_key": api_key or os.getenv(API_KEY_ENV, ""),
            "model": model,
            "api_url": api_url or self.PRESETS["通义千问"]["api_url"],
        }
        self._configs = [primary]
        for index, item in enumerate(fallback_models or [], start=1):
            if not isinstance(item, dict):
                continue
            fallback_model = str(item.get("ai_model") or item.get("model") or "").strip()
            fallback_api_url = str(item.get("ai_api_url") or item.get("api_url") or "").strip()
            fallback_api_key = str(item.get("ai_api_key") or item.get("api_key") or "").strip()
            if not fallback_model:
                continue
            self._configs.append(
                {
                    "label": str(item.get("label") or f"备用模型{index}"),
                    "api_key": fallback_api_key or primary["api_key"],
                    "model": fallback_model,
                    "api_url": fallback_api_url or primary["api_url"],
                }
            )
        self._active_config_index = 0
        self.api_key = ""
        self.model = ""
        self.api_url = ""
        self._apply_active_config()
        self.last_rewrite_issue = ""
        self.last_ai_issue = ""
        self._logged_rewrite_issues: set[str] = set()
        self._logged_ai_issues: set[str] = set()

    def _apply_active_config(self) -> None:
        current = self._configs[self._active_config_index]
        self.api_key = str(current.get("api_key") or "")
        self.model = str(current.get("model") or "")
        self.api_url = str(current.get("api_url") or self.PRESETS["通义千问"]["api_url"])

    def _switch_to_next_config(
        self,
        detail: str,
        *,
        log_func: Optional[Callable[[str], None]] = None,
    ) -> bool:
        if self._active_config_index >= len(self._configs) - 1:
            return False
        previous = self._configs[self._active_config_index]
        self._active_config_index += 1
        self._apply_active_config()
        current = self._configs[self._active_config_index]
        message = (
            f"AI 失败切换：{previous.get('label', previous.get('model', '当前模型'))} -> "
            f"{current.get('label', current.get('model', '备用模型'))} "
            f"({summarize_for_log(detail, limit=180)})"
        )
        self.note_ai_issue(message, log_func=log_func)
        return True

    def request_json_object(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        label: str,
        log_func: Optional[Callable[[str], None]] = None,
        issue_recorder: Optional[Callable[..., None]] = None,
        timeout: int = 240,
        max_tokens: int = 8192,
        max_attempts: int = 1,
        retry_delay: float = 1.2,
    ) -> Optional[object]:
        while True:
            last_issue = ""

            def recorder(detail: str, **kwargs: object) -> None:
                nonlocal last_issue
                last_issue = detail.strip()
                if issue_recorder:
                    issue_recorder(detail, **kwargs)
                else:
                    self.note_ai_issue(detail, log_func=log_func)

            parsed = request_ai_json_object(
                api_url=self.api_url,
                api_key=self.api_key,
                model=self.model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                label=label,
                log_func=log_func,
                issue_recorder=recorder,
                timeout=timeout,
                max_tokens=max_tokens,
                max_attempts=max_attempts,
                retry_delay=retry_delay,
            )
            if parsed is not None:
                return parsed
            if not ai_issue_requires_failover(last_issue):
                return None
            if not self._switch_to_next_config(last_issue, log_func=log_func):
                return None

    def request_text_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        label: str,
        log_func: Optional[Callable[[str], None]] = None,
        max_tokens: int = 8192,
        timeout: int = 240,
    ) -> Optional[str]:
        while True:
            last_issue = ""
            try:
                response = requests.post(
                    self.api_url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                    },
                    timeout=timeout,
                )
            except Exception as exc:
                last_issue = f"{label} request failed: {type(exc).__name__}: {exc}"
                self.note_rewrite_issue(last_issue, log_func=log_func)
            else:
                try:
                    response.raise_for_status()
                except Exception:
                    last_issue = (
                        f"{label} HTTP error: status {response.status_code}, "
                        f"body {summarize_for_log(response.text) or '<empty>'}"
                    )
                    self.note_rewrite_issue(last_issue, log_func=log_func)
                else:
                    try:
                        response_payload = response.json()
                        choice = response_payload["choices"][0]["message"]["content"]
                    except Exception as exc:
                        last_issue = f"{label} parse failed: {type(exc).__name__}: {exc}"
                        self.note_rewrite_issue(last_issue, log_func=log_func)
                    else:
                        content = str(choice or "").strip()
                        if content:
                            return content
                        last_issue = f"{label} returned empty content"
                        self.note_rewrite_issue(last_issue, log_func=log_func)

            if not ai_issue_requires_failover(last_issue):
                return None
            if not self._switch_to_next_config(last_issue, log_func=log_func):
                return None

    def note_ai_issue(
        self,
        detail: str,
        *,
        log_func: Optional[Callable[[str], None]] = None,
    ) -> None:
        message = detail.strip()
        if not message:
            return
        self.last_ai_issue = message
        if log_func and message not in self._logged_ai_issues:
            log_func(f"  {message}")
            self._logged_ai_issues.add(message)

    def note_rewrite_issue(
        self,
        detail: str,
        *,
        log_func: Optional[Callable[[str], None]] = None,
    ) -> None:
        message = detail.strip()
        if not message:
            return
        self.last_rewrite_issue = message
        self.last_ai_issue = message
        if log_func and message not in self._logged_rewrite_issues:
            log_func(f"  {message}")
            self._logged_rewrite_issues.add(message)
        if message not in self._logged_ai_issues:
            self._logged_ai_issues.add(message)

    def _fallback_classification(self, content: str) -> Dict[str, List[Dict[str, str]]]:
        entries = parse_subtitle_content(content)
        return classify_entries_locally(entries)

    def review_subtitle_ocr(
        self,
        entries: Sequence[SubtitleEntry],
        log_func: Optional[Callable[[str], None]] = None,
    ) -> Dict[int, str]:
        if not entries or not self.api_key:
            return {}
        corrected_map: Dict[int, str] = {}
        initial_chunk_size = 48
        min_chunk_size = 12
        context_radius = 3
        successful_windows = 0
        failed_windows = 0
        adaptive_splits = 0

        def review_focus_window(start_offset: int, end_offset: int) -> None:
            nonlocal successful_windows, failed_windows, adaptive_splits
            focus_chunk = list(entries[start_offset:end_offset])
            if not focus_chunk:
                return
            window_start = max(0, start_offset - context_radius)
            window_end = min(len(entries), end_offset + context_radius)
            review_chunk = list(entries[window_start:window_end])
            focus_indexes = {entry.index for entry in focus_chunk}
            payload_entries = [
                {
                    "index": entry.index,
                    "focus": entry.index in focus_indexes,
                    "start": round(entry.start, 3),
                    "end": round(entry.end, 3),
                    "text": normalize_subtitle_text(entry.text),
                }
                for entry in review_chunk
            ]
            system_prompt = (
                "You review Chinese short-drama subtitles before rewrite and TTS. "
                "Return JSON only. "
                "Use the surrounding lines as context and correct only high-confidence OCR, ASR, or subtitle-recognition mistakes. "
                "You may repair broken collocations, fixed phrases, idioms, kinship terms, titles, names, or missing characters when the whole-script context makes the intended wording clear. "
                "For example, if context clearly shows a broken phrase like 出了现 that should be 出了轨, or 的之下 that should be 的撺掇之下, fix it. "
                "Keep the same meaning, keep the same sentence boundary, and do not rewrite for style. "
                "If you are not confident, keep the original text unchanged. "
                "Return only entries that should actually be changed. "
                "JSON format: {\"entries\":[{\"index\":1,\"corrected\":\"...\"}]}"
            )
            user_prompt = (
                "Review this subtitle window from a full script. "
                "Only return changed entries whose focus=true. "
                "Return JSON only.\n\n"
                f"{json.dumps({'entries': payload_entries}, ensure_ascii=False, indent=2)}"
            )
            request_issue = ""

            def issue_callback(
                detail: str,
                *,
                log_func: Optional[Callable[[str], None]] = None,
            ) -> None:
                nonlocal request_issue
                request_issue = detail.strip()
                self.note_ai_issue(detail, log_func=log_func or log_func_outer)

            parsed = self.request_json_object(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.1,
                label="AI OCR review",
                log_func=log_func_outer,
                issue_recorder=issue_callback,
                timeout=90,
                max_tokens=4096,
                max_attempts=2,
                retry_delay=1.25,
            )
            if isinstance(parsed, dict):
                items = parsed.get("entries")
                if not isinstance(items, list):
                    request_issue = "AI OCR review returned no entries list"
                    self.note_ai_issue(request_issue, log_func=log_func_outer)
                else:
                    successful_windows += 1
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        try:
                            index = int(item.get("index", 0))
                        except (TypeError, ValueError):
                            continue
                        if index not in focus_indexes:
                            continue
                        corrected = cleanup_rewrite_text(str(item.get("corrected", "") or ""))
                        if corrected:
                            corrected_map[index] = corrected
                    return

            if len(focus_chunk) > min_chunk_size and ai_issue_supports_smaller_chunk(request_issue):
                split_size = len(focus_chunk) // 2
                split_index = start_offset + max(1, split_size)
                if split_index > start_offset and split_index < end_offset:
                    adaptive_splits += 1
                    if log_func_outer:
                        log_func_outer(
                            f"  AI OCR review adaptive retry: shrinking focus window {len(focus_chunk)} -> "
                            f"{split_index - start_offset}+{end_offset - split_index}"
                        )
                    review_focus_window(start_offset, split_index)
                    review_focus_window(split_index, end_offset)
                    return

            failed_windows += 1

        log_func_outer = log_func
        for offset in range(0, len(entries), initial_chunk_size):
            review_focus_window(offset, min(len(entries), offset + initial_chunk_size))

        if log_func:
            if corrected_map:
                log_func(
                    f"  AI OCR review applied: {len(corrected_map)} entries across {successful_windows} successful window(s)"
                )
                if adaptive_splits:
                    log_func(f"  AI OCR review adaptive splits: {adaptive_splits}")
            elif failed_windows > 0 or adaptive_splits > 0:
                log_func(
                    "  AI OCR review fallback: no accepted AI corrections; "
                    f"{failed_windows} window(s) failed, {adaptive_splits} adaptive split(s)"
                )
            elif successful_windows > 0:
                log_func("  AI OCR review completed: no high-confidence corrections suggested")
        return corrected_map

    def plan_tts_sentence_links(self, entries: Sequence[SubtitleEntry]) -> Dict[int, bool]:
        if not entries or not self.api_key:
            return {}
        join_map: Dict[int, bool] = {}
        chunk_size = 64
        context_radius = 3
        system_prompt = (
            "You review final Chinese short-drama narration lines for TTS sentence pauses. "
            "Return JSON only. "
            "For each focus entry, set join_next=true only when this line should flow directly into the next line "
            "as the same spoken sentence with no full-sentence stop after it. "
            "Only do this when the current line is clearly incomplete and must continue into the next line. "
            "Set join_next=false when there should be a clear sentence pause after this line, "
            "even if the next line is nearby in time. "
            "Be strict and do not merge separate sentences just because the gap is short. "
            "Never merge after a complete sentence, and never merge into a strong transition such as 随后/这时/下一秒/结果/原来. "
            "Keep all decisions based on the full script context. "
            "Return only focus=true entries. "
            "JSON format: {\"entries\":[{\"index\":1,\"join_next\":false}]}"
        )
        for offset in range(0, len(entries), chunk_size):
            focus_chunk = list(entries[offset : offset + chunk_size])
            if not focus_chunk:
                continue
            window_start = max(0, offset - context_radius)
            window_end = min(len(entries), offset + chunk_size + context_radius)
            review_chunk = list(entries[window_start:window_end])
            focus_indexes = {entry.index for entry in focus_chunk}
            payload_entries = [
                {
                    "index": entry.index,
                    "focus": entry.index in focus_indexes,
                    "start": round(entry.start, 3),
                    "end": round(entry.end, 3),
                    "text": normalize_subtitle_text(entry.text),
                }
                for entry in review_chunk
            ]
            user_prompt = (
                "Review this narration window from a full script and decide where TTS should continue into the next line. "
                "Return JSON only.\n\n"
                f"{json.dumps({'entries': payload_entries}, ensure_ascii=False, indent=2)}"
            )
            parsed = self.request_json_object(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.1,
                label="AI TTS pause review",
                log_func=log_func,
                issue_recorder=self.note_ai_issue,
            )
            if not isinstance(parsed, dict):
                continue
            items = parsed.get("entries")
            if not isinstance(items, list):
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue
                try:
                    index = int(item.get("index", 0))
                except (TypeError, ValueError):
                    continue
                if index not in focus_indexes:
                    continue
                join_next = item.get("join_next")
                if isinstance(join_next, bool):
                    join_map[index] = join_next
        return join_map

    def classify_srt(self, content: str) -> Dict[str, List[Dict[str, str]]]:
        if not content.strip() or not self.api_key:
            return self._fallback_classification(content)
        system_prompt = (
            "You classify Chinese short-drama subtitle lines and lightly fix obvious OCR mistakes. "
            "Return JSON only. "
            "Valid types are narration, dialogue, original_subtitle, watermark. "
            "narration means commentator voice, third-person explanation, emotional explanation, or transition narration intended for voice-over. "
            "dialogue means a character is directly speaking, asking, responding, calling someone, making a promise, or talking to another person. "
            "original_subtitle means original drama subtitle text, scene or location text, time cards, or source story text that belongs to the source video and should not be used as commentary voice-over. "
            "watermark means platform prompts, branding, episode tags, CTA, or non-story overlay text. "
            "Use nearby lines as context. "
            "If adjacent short lines form a third-person plot summary, keep them as narration instead of original_subtitle. "
            "Only use original_subtitle for true source-caption text such as scene cards, time/location text, or obvious in-drama subtitle carryover. "
            "Return JSON format: {\"entries\":[{\"index\":1,\"type\":\"narration\",\"original\":\"...\",\"corrected\":\"...\"}]}"
        )
        user_prompt = f"Classify the subtitle content below and return JSON only:\n\n{content}"
        parsed = self.request_json_object(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.2,
            label="AI subtitle classification",
            issue_recorder=self.note_ai_issue,
        )
        if isinstance(parsed, dict) and isinstance(parsed.get("entries"), list):
            return parsed
        return self._fallback_classification(content)
        """
        system_prompt = (
            "你是短剧字幕处理助手。请逐条纠正明显 OCR 错字，并把每条字幕分类为 "
            "narration、dialogue、watermark 之一。"
            "只返回 JSON，格式为 "
            '{"entries":[{"index":1,"type":"narration","original":"原文","corrected":"纠正文"}]}。'
        )
        try:
            response = requests.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {
                            "role": "user",
                            "content": f"处理下面的字幕内容，只输出 JSON:\n\n{content}",
                        },
                    ],
                    "temperature": 0.2,
                    "max_tokens": 8192,
                },
                timeout=240,
            )
            response.raise_for_status()
            payload = response.json()["choices"][0]["message"]["content"]
            parsed = extract_json_object(payload)
            if isinstance(parsed, dict) and isinstance(parsed.get("entries"), list):
                return parsed
        except Exception:
            pass
        return self._fallback_classification(content)
        """

    def rewrite_srt_full(self, content: str, log_func: Optional[Callable[[str], None]] = None) -> str:
        if not content.strip() or not self.api_key:
            return content
        system_prompt = (
            "你是短剧解说改写助手。请在不改变 SRT 编号和时间轴的前提下，只改写正文。"
            "用短剧解说常用的口语短句，表达自然、直接、好念。"
            "避免使用公子、女子、乃、家中、其、便这类书面或古风词，除非它是无法替换的专有称呼。"
            "每一条都要单独读得顺，不能把句子卡在“随后、这时、下一秒、原来、结果”这类承接词上。"
            "不要合并或拆分条目，输出完整 SRT。"
        )
        message_text = self.request_text_completion(
            system_prompt=system_prompt,
            user_prompt=content,
            temperature=0.6,
            label="AI full rewrite",
            log_func=log_func,
            max_tokens=8192,
            timeout=240,
        )
        if message_text:
            return message_text

        return content


def rewrite_narration_entries(
    ai_generator: AINarrationGenerator,
    entries: Sequence[SubtitleEntry],
    chunk_size: int = 16,
    log_func: Optional[Callable[[str], None]] = None,
) -> Dict[int, str]:
    if not entries or not ai_generator.api_key:
        return {}

    rewrite_map: Dict[int, str] = {}
    retry_requested = 0
    retry_recovered = 0
    local_diversified = 0
    empty_response_chunks = 0
    for offset in range(0, len(entries), chunk_size):
        chunk = list(entries[offset : offset + chunk_size])
        chunk_by_index = {entry.index: entry for entry in chunk}
        previous_context = [
            {
                "index": entry.index,
                "source": normalize_subtitle_text(entry.text),
                "rewrite": rewrite_map.get(entry.index, normalize_subtitle_text(entry.text)),
            }
            for entry in entries[max(0, offset - 2) : offset]
        ]
        next_context = [
            {
                "index": entry.index,
                "source": normalize_subtitle_text(entry.text),
            }
            for entry in entries[offset + len(chunk) : offset + len(chunk) + 2]
        ]
        char_budgets = {
            entry.index: subtitle_char_budget(max(0.1, entry.end - entry.start))
            for entry in chunk
        }
        speech_budgets = {
            entry.index: narration_rewrite_speech_budgets(entry)
            for entry in chunk
        }
        payload_entries = [
            {
                "index": entry.index,
                "duration": round(max(0.1, entry.end - entry.start), 3),
                "max_chars": char_budgets[entry.index],
                "source_speech_units": subtitle_speech_units(entry.text),
                "preferred_speech_units": speech_budgets[entry.index][0],
                "max_speech_units": speech_budgets[entry.index][1],
                "text": normalize_subtitle_text(entry.text),
            }
            for entry in chunk
        ]
        items = request_rewrite_batch(
            ai_generator,
            previous_context,
            payload_entries,
            next_context,
            log_func=log_func,
        )
        if not items:
            empty_response_chunks += 1
            local_chunk_map = build_local_rewrite_map(chunk)
            if local_chunk_map:
                rewrite_map.update(local_chunk_map)
                local_diversified += len(local_chunk_map)
            continue

        accepted_chunk: Dict[int, str] = {}
        first_pass_candidates: Dict[int, str] = {}
        retry_payload_entries: List[Dict[str, object]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                index = int(item.get("index", 0))
            except (TypeError, ValueError):
                continue
            char_budget = char_budgets.get(index)
            speech_budget_pair = speech_budgets.get(index)
            if not char_budget or not speech_budget_pair:
                continue
            source_entry = chunk_by_index.get(index)
            candidate = normalize_spoken_narration_text(str(item.get("rewrite", "") or ""))
            if source_entry is not None:
                candidate = prefer_complete_narration_text(source_entry.text, candidate)
                candidate = fit_rewrite_candidate_to_timing(
                    source_entry,
                    candidate,
                    display_budget=char_budget,
                    speech_budget=speech_budget_pair[1],
                )
            if not candidate:
                continue
            if probably_incomplete_text(candidate) and source_entry is not None and not probably_incomplete_text(source_entry.text):
                continue
            first_pass_candidates[index] = candidate
            if source_entry is not None and rewrite_needs_more_variation(source_entry.text, candidate):
                retry_payload_entries.append(
                    {
                        "index": index,
                        "duration": round(max(0.1, source_entry.end - source_entry.start), 3),
                        "max_chars": char_budget,
                        "source_speech_units": subtitle_speech_units(source_entry.text),
                        "preferred_speech_units": speech_budget_pair[0],
                        "max_speech_units": speech_budget_pair[1],
                        "text": normalize_subtitle_text(source_entry.text),
                        "previous_rewrite": candidate,
                    }
                )
                continue
            accepted_chunk[index] = candidate

        pending_retry_indexes = {int(item["index"]) for item in retry_payload_entries}
        for entry in chunk:
            if entry.index in accepted_chunk or entry.index in pending_retry_indexes:
                continue
            if entry.index in first_pass_candidates:
                continue
            retry_payload_entries.append(
                {
                    "index": entry.index,
                    "duration": round(max(0.1, entry.end - entry.start), 3),
                    "max_chars": char_budgets[entry.index],
                    "source_speech_units": subtitle_speech_units(entry.text),
                    "preferred_speech_units": speech_budgets[entry.index][0],
                    "max_speech_units": speech_budgets[entry.index][1],
                    "text": normalize_subtitle_text(entry.text),
                    "previous_rewrite": "",
                }
            )

        retry_requested += len(retry_payload_entries)
        if retry_payload_entries:
            retry_items = request_rewrite_batch(
                ai_generator,
                previous_context,
                retry_payload_entries,
                next_context,
                force_variation=True,
                log_func=log_func,
            )
            for item in retry_items:
                if not isinstance(item, dict):
                    continue
                try:
                    index = int(item.get("index", 0))
                except (TypeError, ValueError):
                    continue
                char_budget = char_budgets.get(index)
                speech_budget_pair = speech_budgets.get(index)
                source_entry = chunk_by_index.get(index)
                if not char_budget or not speech_budget_pair or source_entry is None:
                    continue
                candidate = normalize_spoken_narration_text(str(item.get("rewrite", "") or ""))
                candidate = prefer_complete_narration_text(source_entry.text, candidate)
                candidate = fit_rewrite_candidate_to_timing(
                    source_entry,
                    candidate,
                    display_budget=char_budget,
                    speech_budget=speech_budget_pair[1],
                )
                if not candidate:
                    continue
                if probably_incomplete_text(candidate) and not probably_incomplete_text(source_entry.text):
                    continue
                if rewrite_needs_more_variation(source_entry.text, candidate):
                    continue
                accepted_chunk[index] = candidate
                retry_recovered += 1

        for payload in retry_payload_entries:
            index = int(payload["index"])
            if index in accepted_chunk:
                continue
            source_entry = chunk_by_index.get(index)
            char_budget = char_budgets.get(index)
            speech_budget_pair = speech_budgets.get(index)
            if source_entry is None or not char_budget or not speech_budget_pair:
                continue
            local_candidate = diversify_narration_locally(source_entry.text, char_budget)
            local_candidate = fit_rewrite_candidate_to_timing(
                source_entry,
                local_candidate,
                display_budget=char_budget,
                speech_budget=speech_budget_pair[1],
            )
            if local_candidate and not rewrite_needs_more_variation(source_entry.text, local_candidate):
                accepted_chunk[index] = local_candidate
                local_diversified += 1
                continue
            fallback_candidate = first_pass_candidates.get(index)
            if fallback_candidate and not rewrite_needs_more_variation(source_entry.text, fallback_candidate):
                accepted_chunk[index] = fallback_candidate

        rewrite_map.update(accepted_chunk)

    if log_func and retry_requested:
        log_func(
            "  AI rewrite diversity: "
            f"retry {retry_requested} lines, "
            f"recovered {retry_recovered}, "
            f"local fallback {local_diversified}"
        )
    elif log_func and local_diversified:
        log_func(f"  Local narration fallback: {local_diversified} entries")

    if log_func and empty_response_chunks:
        log_func(f"  AI rewrite empty chunks: {empty_response_chunks}")

    return rewrite_map


def classify_subtitle_entries(
    ai_generator: AINarrationGenerator,
    entries: Sequence[SubtitleEntry],
    chunk_size: int = 48,
    log_func: Optional[Callable[[str], None]] = None,
) -> Dict[str, List[Dict[str, str]]]:
    if not entries:
        return {"entries": []}
    if not ai_generator.api_key:
        return classify_entries_locally(entries)

    entry_by_index = {entry.index: entry for entry in entries}
    valid_types = {"narration", "dialogue", "original_subtitle", "watermark"}
    result_map: Dict[int, Dict[str, str]] = {}

    system_prompt = (
        "You classify Chinese short-drama reference subtitle lines using the whole script context. "
        "Return JSON only. "
        "Valid types are narration, dialogue, original_subtitle, watermark. "
        "narration means commentator voice-over, third-person plot summary, emotional explanation, transitions, or commentary that should enter rewrite and TTS. "
        "dialogue means a character is directly speaking, asking, replying, calling someone, threatening, promising, or talking to another character. "
        "original_subtitle means true source-video overlay text that should not be spoken as commentary, such as scene/location/time cards or obvious in-drama source captions that are not character dialogue. "
        "watermark means platform prompts, branding, episode tags, CTA, UI text, or non-story overlay text. "
        "Reference videos vary a lot: some are almost all narration, some are half narration, some contain long dialogue blocks, and some have sparse commentary. "
        "Do not force any fixed ratio. Infer the role from the whole-script narrative flow and neighboring lines. "
        "If adjacent short SRT rows are fragments of one third-person story sentence, classify those rows as narration, not original_subtitle. "
        "Do not classify a short narration fragment as dialogue just because it is short. "
        "Do not classify true character speech as narration just because it appears between narration lines. "
        "Only use original_subtitle when the text is clearly a source caption/card/overlay, not merely because it is short or contains a place word. "
        "Lightly correct only obvious OCR/ASR mistakes that are clear from context; otherwise keep the text. "
        "JSON format: {\"entries\":[{\"index\":1,\"type\":\"narration\",\"corrected\":\"...\"}]}"
    )

    def accept_items(
        items: object,
        *,
        allowed_indexes: Optional[set[int]] = None,
    ) -> int:
        if not isinstance(items, list):
            return 0
        accepted = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                index = int(item.get("index", 0))
            except (TypeError, ValueError):
                continue
            if allowed_indexes is not None and index not in allowed_indexes:
                continue
            source_entry = entry_by_index.get(index)
            if source_entry is None:
                continue
            entry_type = str(item.get("type", "") or "").strip().lower()
            if entry_type not in valid_types:
                continue
            corrected = cleanup_rewrite_text(str(item.get("corrected", "") or ""))
            if not corrected:
                corrected = normalize_subtitle_text(source_entry.text)
            result_map[index] = {
                "index": index,
                "type": entry_type,
                "original": source_entry.text,
                "corrected": corrected,
            }
            accepted += 1
        return accepted

    def payload_for(
        window: Sequence[SubtitleEntry],
        *,
        focus_indexes: Optional[set[int]] = None,
    ) -> List[Dict[str, object]]:
        return [
            {
                "index": entry.index,
                **({"focus": entry.index in focus_indexes} if focus_indexes is not None else {}),
                "start": round(entry.start, 3),
                "end": round(entry.end, 3),
                "duration": round(max(0.1, entry.end - entry.start), 3),
                "text": normalize_subtitle_text(entry.text),
            }
            for entry in window
        ]

    total_entries = len(entries)
    full_text_retry_rounds = 2
    contextual_retry_rounds = 4
    full_text_success_threshold = 0.90
    full_text_enabled = total_entries <= 320

    def current_coverage() -> float:
        return len(result_map) / max(1, total_entries)

    def missing_entries() -> List[SubtitleEntry]:
        return [entry for entry in entries if entry.index not in result_map]

    def missing_focus_ranges(max_focus_size: int) -> List[Tuple[int, int]]:
        missing_positions = [
            position
            for position, entry in enumerate(entries)
            if entry.index not in result_map
        ]
        if not missing_positions:
            return []

        ranges: List[Tuple[int, int]] = []
        start = missing_positions[0]
        previous = start
        count = 1
        for position in missing_positions[1:]:
            contiguous = position == previous + 1
            if contiguous and count < max_focus_size:
                previous = position
                count += 1
                continue
            ranges.append((start, previous + 1))
            start = position
            previous = position
            count = 1
        ranges.append((start, previous + 1))
        return ranges

    def run_full_text_pass(label: str, *, missing_only: bool) -> int:
        focus_indexes = {entry.index for entry in missing_entries()} if missing_only else None
        if missing_only and not focus_indexes:
            return 0
        payload = payload_for(entries, focus_indexes=focus_indexes)
        if missing_only:
            user_prompt = (
                "Re-check the full reference subtitle script below. "
                "Rows with focus=false are context only. "
                "Return final classifications only for focus=true rows that were still missing from a previous attempt. "
                "Use the whole-script narrative flow and return JSON only.\n\n"
                f"{json.dumps({'total_entries': total_entries, 'entries': payload}, ensure_ascii=False, indent=2)}"
            )
        else:
            user_prompt = (
                "Classify the full reference subtitle script below. "
                "Use the whole-script narrative flow, not a fixed narration/dialogue ratio. "
                "Return one final classification for every entry. Return JSON only.\n\n"
                f"{json.dumps({'total_entries': total_entries, 'entries': payload}, ensure_ascii=False, indent=2)}"
            )
        before = len(result_map)
        parsed = ai_generator.request_json_object(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.1,
            label=label,
            issue_recorder=ai_generator.note_ai_issue,
            log_func=log_func,
            timeout=180,
            max_tokens=8192,
            max_attempts=2,
        )
        accepted = 0
        if isinstance(parsed, dict):
            accepted = accept_items(parsed.get("entries"), allowed_indexes=focus_indexes)
        if log_func:
            gained = len(result_map) - before
            log_func(
                f"  {label} accepted: +{gained}, coverage {len(result_map)}/{total_entries}"
            )
        return accepted

    def run_contextual_pass(label: str, *, focus_size: int) -> int:
        before_total = len(result_map)
        context_radius = max(12, min(48, max(focus_size, chunk_size)))
        for start, end in missing_focus_ranges(max(1, focus_size)):
            focus_chunk = list(entries[start:end])
            if not focus_chunk:
                continue
            focus_indexes = {entry.index for entry in focus_chunk if entry.index not in result_map}
            if not focus_indexes:
                continue
            window_start = max(0, start - context_radius)
            window_end = min(len(entries), end + context_radius)
            window = list(entries[window_start:window_end])
            payload_entries = payload_for(window, focus_indexes=focus_indexes)
            user_prompt = (
                "Classify this focus window from a full short-drama reference script. "
                "Rows with focus=false are context only; return final classifications only for focus=true rows. "
                "These focus rows were missing from a previous classification attempt, so do not skip any focus row. "
                "Use the surrounding context to decide whether each row is narration, dialogue, original_subtitle, or watermark. "
                "Do not assume a fixed narration/dialogue ratio. Return JSON only.\n\n"
                f"{json.dumps({'total_entries': total_entries, 'entries': payload_entries}, ensure_ascii=False, indent=2)}"
            )
            parsed = ai_generator.request_json_object(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.1,
                label=label,
                issue_recorder=ai_generator.note_ai_issue,
                log_func=log_func,
                timeout=120,
                max_tokens=4096,
                max_attempts=2,
            )
            if isinstance(parsed, dict):
                accept_items(parsed.get("entries"), allowed_indexes=focus_indexes)
        gained_total = len(result_map) - before_total
        if log_func:
            log_func(
                f"  {label} accepted: +{gained_total}, coverage {len(result_map)}/{total_entries}"
            )
        return gained_total

    if full_text_enabled:
        run_full_text_pass("AI full-text classification", missing_only=False)
        if current_coverage() >= full_text_success_threshold:
            return {"entries": [result_map[index] for index in sorted(result_map)]}

        for round_index in range(full_text_retry_rounds):
            if current_coverage() >= full_text_success_threshold or not missing_entries():
                break
            run_full_text_pass(
                f"AI full-text classification recovery {round_index + 1}/{full_text_retry_rounds}",
                missing_only=True,
            )
            if current_coverage() >= full_text_success_threshold:
                return {"entries": [result_map[index] for index in sorted(result_map)]}

    for round_index in range(contextual_retry_rounds):
        if not missing_entries():
            break
        focus_size = chunk_size if round_index == 0 else max(12, chunk_size // 2)
        run_contextual_pass(
            f"AI contextual classification round {round_index + 1}/{contextual_retry_rounds}",
            focus_size=focus_size,
        )

    if log_func and missing_entries():
        log_func(
            f"  AI classification unresolved after retries: {len(missing_entries())} missing entries"
        )

    return {"entries": [result_map[index] for index in sorted(result_map)]}


class ConfigManager:
    def __init__(self, path: Path):
        self.path = path
        self.data = self._load()

    def _load(self) -> Dict[str, object]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                backup = self.path.with_suffix(".broken.json")
                try:
                    shutil.copy2(self.path, backup)
                except OSError:
                    pass
        return {"workspaces": [], "settings": {}}

    def save(self) -> None:
        self.path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def get_workspaces(self) -> List[Workspace]:
        return [Workspace(**item) for item in self.data.get("workspaces", [])]

    def add_workspace(self, workspace: Workspace) -> None:
        self.data.setdefault("workspaces", []).append(asdict(workspace))
        self.save()

    def update_workspace(self, workspace: Workspace) -> None:
        workspace.last_modified = datetime_now_text()
        items = self.data.setdefault("workspaces", [])
        for idx, item in enumerate(items):
            if item.get("id") == workspace.id:
                items[idx] = asdict(workspace)
                break
        else:
            items.append(asdict(workspace))
        self.save()

    def delete_workspace(self, workspace_id: str) -> None:
        items = self.data.setdefault("workspaces", [])
        self.data["workspaces"] = [item for item in items if item.get("id") != workspace_id]
        self.save()

    def get_setting(self, key: str, default=None):
        return self.data.get("settings", {}).get(key, default)

    def set_setting(self, key: str, value) -> None:
        self.data.setdefault("settings", {})[key] = value
        self.save()


def build_processed_subtitles(
    original_entries: Sequence[SubtitleEntry],
    raw_content: str,
    ai_generator: AINarrationGenerator,
    log_func: Optional[Callable[[str], None]] = None,
) -> ProcessedSubtitleBundle:
    if not original_entries:
        return ProcessedSubtitleBundle(
            [],
            [],
            {"narration": 0, "dialogue": 0, "original_subtitle": 0, "watermark": 0},
        )

    working_entries = preserve_reference_timeline_entries(original_entries)
    if log_func:
        log_func("  Subtitle timeline preserved: using reference SRT timestamps as-is")
    reviewed_correction_map = ai_generator.review_subtitle_ocr(working_entries, log_func=log_func)
    correction_count = 0
    if reviewed_correction_map:
        reviewed_entries: List[SubtitleEntry] = []
        for entry in working_entries:
            corrected = reviewed_correction_map.get(entry.index, entry.text)
            normalized_corrected = cleanup_rewrite_text(str(corrected or entry.text)) or normalize_subtitle_text(entry.text)
            if normalize_subtitle_text(normalized_corrected) != normalize_subtitle_text(entry.text):
                correction_count += 1
            reviewed_entries.append(clone_subtitle_entry(entry, text=normalized_corrected))
        working_entries = reviewed_entries
    if log_func and correction_count:
        log_func(f"  Full-text OCR review: {correction_count} entries corrected")

    working_entries, local_phrase_fix_count = repair_contextual_ocr_phrases(working_entries)
    if log_func and local_phrase_fix_count:
        log_func(f"  Context phrase repair: {local_phrase_fix_count} entries corrected")
    working_entries, full_text_ocr_fix_count = repair_full_text_ocr_consistency(working_entries)
    if log_func and full_text_ocr_fix_count:
        log_func(f"  Full-text consistency OCR repair: {full_text_ocr_fix_count} entries corrected")

    reference_content = entries_to_srt(working_entries) if working_entries else raw_content

    local_classification = classify_entries_locally(working_entries)
    local_classified_map = {
        int(item.get("index", 0)): item
        for item in local_classification.get("entries", [])
        if int(item.get("index", 0) or 0) > 0
    }
    classification = classify_subtitle_entries(ai_generator, working_entries, log_func=log_func)
    if not classification.get("entries"):
        classification = ai_generator.classify_srt(reference_content)
    ai_classified_map: Dict[int, Dict[str, str]] = {}
    for item in classification.get("entries", []):
        try:
            index = int(item.get("index", 0))
        except (TypeError, ValueError):
            continue
        if index <= 0:
            continue
        ai_classified_map[index] = item

    ai_classification_coverage = (
        len(ai_classified_map) / max(1, len(working_entries))
        if working_entries
        else 0.0
    )
    use_ai_primary = ai_classification_coverage >= 0.75
    classified_map: Dict[int, Dict[str, str]] = (
        dict(ai_classified_map)
        if use_ai_primary
        else dict(local_classified_map)
    )
    if use_ai_primary:
        for index, item in local_classified_map.items():
            classified_map.setdefault(index, item)
        if log_func:
            log_func(
                f"  AI subtitle classification primary: {len(ai_classified_map)}/{len(working_entries)} entries"
            )
    else:
        for index, item in ai_classified_map.items():
            classified_map[index] = item
    if not use_ai_primary and ai_classified_map and log_func:
        log_func(
            "  AI subtitle classification fallback: coverage too low, "
            f"using local classification as baseline + AI overrides ({len(ai_classified_map)}/{len(working_entries)} AI entries)"
        )

    merged_entries: List[SubtitleEntry] = []
    for entry in working_entries:
        item = classified_map.get(entry.index, {})
        entry_type = str(item.get("type", "narration") or "narration").strip().lower()
        if entry_type not in {"narration", "dialogue", "original_subtitle", "watermark"}:
            entry_type = "narration"
        corrected = str(item.get("corrected", entry.text) or entry.text).strip() or entry.text
        current = SubtitleEntry(
            index=entry.index,
            start=entry.start,
            end=entry.end,
            text=corrected,
            entry_type=entry_type,
        )
        merged_entries.append(current)

    if not use_ai_primary:
        merged_entries = classify_entries_with_whole_text_context(
            merged_entries,
            trust_existing_type=True,
        )
        merged_entries = refine_classified_entries(merged_entries)
        merged_entries = strengthen_classification(merged_entries)
        merged_entries = recover_narration_fragment_runs(merged_entries)

    watermark_count = sum(1 for entry in merged_entries if entry.entry_type == "watermark")
    cleaned_entries = [entry for entry in merged_entries if entry.entry_type != "watermark"]
    if not use_ai_primary:
        cleaned_entries = refine_classified_entries(cleaned_entries)
        cleaned_entries = strengthen_classification(cleaned_entries)
        cleaned_entries = recover_narration_fragment_runs(cleaned_entries)
    counts = {"narration": 0, "dialogue": 0, "original_subtitle": 0, "watermark": watermark_count}
    narration_seed: List[SubtitleEntry] = []
    for entry in cleaned_entries:
        counts[entry.entry_type] += 1
        if entry.entry_type == "narration":
            narration_seed.append(entry)

    rewrite_map: Dict[int, str] = {}
    if narration_seed:
        rewrite_map = rewrite_narration_entries(ai_generator, narration_seed, log_func=log_func)
        if rewrite_map and log_func:
            log_func(f"  Rewrite applied: {len(rewrite_map)} entries")
        if not rewrite_map:
            rewrite_input = entries_to_srt(narration_seed)
            rewritten_content = ai_generator.rewrite_srt_full(rewrite_input, log_func=log_func)
            rewritten_entries = parse_srt(rewritten_content)
            local_full_fallback = 0
            if len(rewritten_entries) == len(narration_seed):
                for source_entry, rewritten_entry in zip(narration_seed, rewritten_entries):
                    candidate = prefer_complete_narration_text(source_entry.text, rewritten_entry.text)
                    budget = subtitle_char_budget(max(0.1, source_entry.end - source_entry.start))
                    _, speech_budget = narration_rewrite_speech_budgets(source_entry)
                    candidate = fit_rewrite_candidate_to_timing(
                        source_entry,
                        candidate,
                        display_budget=budget,
                        speech_budget=speech_budget,
                    )
                    if not candidate:
                        candidate = diversify_narration_locally(source_entry.text, budget)
                        candidate = fit_rewrite_candidate_to_timing(
                            source_entry,
                            candidate,
                            display_budget=budget,
                            speech_budget=speech_budget,
                        )
                        if candidate:
                            local_full_fallback += 1
                    if not candidate or rewrite_needs_more_variation(source_entry.text, candidate):
                        continue
                    rewrite_map[source_entry.index] = candidate
                if log_func:
                    log_func(f"  AI rewrite accepted: {len(rewrite_map)} entries")
                    if local_full_fallback:
                        log_func(f"  AI rewrite local fallback: {local_full_fallback} entries")
            elif log_func:
                log_func("  AI full rewrite result count mismatch; fallback to corrected source text.")
            if not rewrite_map:
                local_rewrite_map = build_local_rewrite_map(narration_seed)
                if local_rewrite_map:
                    rewrite_map.update(local_rewrite_map)
                    if log_func:
                        log_func(f"  Local narration de-dup fallback: {len(local_rewrite_map)} entries")
                elif log_func:
                    last_issue = ai_generator.last_rewrite_issue.strip()
                    if last_issue:
                        log_func(f"  AI rewrite kept original narration; last issue: {last_issue}")
                    else:
                        log_func("  AI rewrite kept original narration; no acceptable changes were produced.")
    elif log_func:
        log_func("  No narration lines detected; rewrite step skipped.")
    final_entries: List[SubtitleEntry] = []
    narration_entries: List[SubtitleEntry] = []
    for entry in cleaned_entries:
        text = rewrite_map.get(entry.index, entry.text) if entry.entry_type == "narration" else entry.text
        if entry.entry_type == "narration":
            text = prefer_complete_narration_text(entry.text, text)
        prepared_text = normalize_subtitle_text(text)
        final_entry = SubtitleEntry(
            index=entry.index,
            start=entry.start,
            end=entry.end,
            text=prepared_text,
            entry_type=entry.entry_type,
        )
        final_entries.append(final_entry)
        if final_entry.entry_type == "narration":
            narration_entries.append(final_entry)

    if not final_entries:
        final_entries = [
            SubtitleEntry(
                entry.index,
                entry.start,
                entry.end,
                compact_subtitle_text(entry.text, max(0.1, entry.end - entry.start)),
            )
            for entry in working_entries
        ]
    narration_entries = [entry for entry in final_entries if entry.entry_type == "narration"]

    return ProcessedSubtitleBundle(final_entries, narration_entries, counts)


def extract_source_frames(
    videos: Sequence[Path],
    cache_dir: Path,
    video_processor: VideoProcessor,
    hasher: VisualHasher,
    frame_interval: float = FRAME_INTERVAL,
    log_func: Optional[Callable[[str], None]] = None,
) -> List[FrameSample]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "cache_info.json"
    try:
        cache = json.loads(cache_file.read_text(encoding="utf-8")) if cache_file.exists() else {}
    except Exception:
        cache = {}

    frames: List[FrameSample] = []
    global_index = 0
    for video_order, video_path in enumerate(videos, start=1):
        if log_func:
            log_func(f"  提取素材帧 {video_order}/{len(videos)}: {video_path.name}")
        abs_path = str(video_path.resolve())
        mtime = video_path.stat().st_mtime
        sanitized_abs_path = abs_path.replace(":", "").replace("\\", "_").replace("/", "_")
        frame_folder = cache_dir / f"src_{sanitized_abs_path}"
        frame_folder.mkdir(parents=True, exist_ok=True)

        cache_entry = cache.get(abs_path, {}) if isinstance(cache.get(abs_path, {}), dict) else {}
        cached_items = cache_entry.get("frames", [])
        cached_version = str(cache_entry.get("version", "") or "")
        cached_interval = float(cache_entry.get("frame_interval", 0.0) or 0.0)
        cache_reason = ""
        use_cache = (
            cache_entry.get("mtime") == mtime
            and cached_version == SOURCE_FRAME_CACHE_VERSION
            and abs(cached_interval - frame_interval) < 1e-6
            and bool(cached_items)
        )
        if not use_cache:
            if cache_entry and cached_version != SOURCE_FRAME_CACHE_VERSION:
                cache_reason = "哈希规则已更新，重建缓存"
            elif cache_entry and abs(cached_interval - frame_interval) >= 1e-6:
                cache_reason = "抽帧间隔已变化，重建缓存"
            elif cache_entry and cache_entry.get("mtime") != mtime:
                cache_reason = "检测到素材文件变化，重建缓存"
            else:
                cache_reason = "缓存缺失或不完整，重建缓存"
        frame_rows: List[Dict[str, object]] = []

        if use_cache:
            for item in cached_items:
                if isinstance(item, dict):
                    ts = float(item.get("ts", 0.0))
                    sig = tuple(item.get("sig", []))
                    flip_sig = tuple(item.get("flip_sig", []))
                else:
                    ts = float(item)
                    sig = ()
                    flip_sig = ()
                jpg_path = frame_folder / f"f_{ts:.1f}.jpg"
                if not jpg_path.exists():
                    use_cache = False
                    break
                if len(sig) != hasher.signature_parts or len(flip_sig) != hasher.signature_parts:
                    try:
                        sig = hasher.compute_signature_from_file(jpg_path)
                        flip_sig = hasher.compute_signature_from_file(jpg_path, flip_horizontal=True)
                    except Exception:
                        use_cache = False
                        break
                frame_rows.append({"ts": ts, "sig": list(sig), "flip_sig": list(flip_sig)})

        if not use_cache:
            if log_func:
                log_func(f"    {cache_reason}")
            frame_rows = []
            duration = video_processor.probe_duration(video_path)
            timestamp = 0.0
            while timestamp < duration:
                jpg_path = frame_folder / f"f_{timestamp:.1f}.jpg"
                if not jpg_path.exists():
                    run_subprocess_hidden(
                        [
                            str(video_processor.ffmpeg),
                            "-hide_banner",
                            "-loglevel",
                            "error",
                            "-ss",
                            f"{timestamp:.3f}",
                            "-i",
                            str(video_path),
                            "-frames:v",
                            "1",
                            "-q:v",
                            "2",
                            str(jpg_path),
                        ],
                        capture_output=True,
                        timeout=30,
                        check=False,
                    )
                if jpg_path.exists():
                    signature = hasher.compute_signature_from_file(jpg_path)
                    flipped_signature = hasher.compute_signature_from_file(jpg_path, flip_horizontal=True)
                    frame_rows.append({"ts": timestamp, "sig": list(signature), "flip_sig": list(flipped_signature)})
                timestamp += frame_interval
            cache[abs_path] = {
                "mtime": mtime,
                "version": SOURCE_FRAME_CACHE_VERSION,
                "frame_interval": frame_interval,
                "frames": frame_rows,
            }

        for local_index, item in enumerate(frame_rows):
            signature = tuple(int(part) for part in item["sig"])
            flipped_signature = tuple(int(part) for part in item.get("flip_sig", []))
            frames.append(
                FrameSample(
                    video_path=abs_path,
                    video_name=video_path.name,
                    video_order=video_order,
                    local_index=local_index,
                    global_index=global_index,
                    timestamp=float(item["ts"]),
                    signature=signature,
                    flipped_signature=flipped_signature,
                    frame_path=str((frame_folder / f"f_{float(item['ts']):.1f}.jpg").resolve()),
                )
            )
            global_index += 1

    cache_file.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    if log_func:
        log_func(f"素材帧总数: {len(frames)}")
    return frames


def extract_reference_frames(
    reference_video: Path,
    temp_dir: Path,
    video_processor: VideoProcessor,
    hasher: VisualHasher,
    frame_interval: float = FRAME_INTERVAL,
    log_func: Optional[Callable[[str], None]] = None,
) -> Tuple[List[ReferenceFrame], float]:
    duration = video_processor.probe_duration(reference_video)
    frames_dir = temp_dir / "reference_frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    frames: List[ReferenceFrame] = []
    timestamp = 0.0
    frame_index = 0
    while timestamp < duration:
        jpg_path = frames_dir / f"ref_{timestamp:.1f}.jpg"
        result = run_subprocess_hidden(
            [
                str(video_processor.ffmpeg),
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-y",
                "-ss",
                f"{timestamp:.3f}",
                "-i",
                str(reference_video),
                "-frames:v",
                "1",
                "-q:v",
                "2",
                str(jpg_path),
            ],
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0 or not jpg_path.exists():
            detail = result.stderr.decode("utf-8", errors="ignore")[:400].strip()
            raise RuntimeError(
                detail or f"reference frame extraction failed at {timestamp:.1f}s"
            )
        if jpg_path.exists():
            frames.append(
                ReferenceFrame(
                    index=frame_index,
                    timestamp=timestamp,
                    signature=hasher.compute_signature_from_file(jpg_path),
                    frame_path=str(jpg_path.resolve()),
                )
            )
            frame_index += 1
        timestamp += frame_interval
    if log_func:
        log_func(f"参考帧总数: {len(frames)}")
    return frames, duration


def evaluate_forward_path_similarity(
    reference_frames: Sequence[ReferenceFrame],
    source_frames: Sequence[FrameSample],
    hasher: VisualHasher,
    start_ref_index: int,
    start_source_pos: int,
    horizon: int = 4,
    start_flip: Optional[bool] = None,
) -> float:
    total = 0.0
    total_weight = 0.0
    previous_sample: Optional[FrameSample] = None
    active_flip = start_flip
    for offset in range(horizon):
        ref_pos = start_ref_index + offset
        source_pos = start_source_pos + offset
        if ref_pos >= len(reference_frames) or source_pos >= len(source_frames):
            break
        sample = source_frames[source_pos]
        if previous_sample is not None:
            if sample.global_index != previous_sample.global_index + 1:
                break
            if sample.video_order < previous_sample.video_order:
                break
        similarity, chosen_flip, _, _ = sample_signature_similarity(
            hasher,
            reference_frames[ref_pos].signature,
            sample,
            preferred_flip=active_flip,
        )
        active_flip = chosen_flip
        weight = 1.0 if offset == 0 else 1.15
        total += similarity * weight
        total_weight += weight
        previous_sample = sample
    return total / total_weight if total_weight else 0.0


def shortlist_candidate_priority(score: float, visual: float) -> float:
    return score + visual * 0.18


def update_match_candidate_shortlist(
    shortlist: List[Dict[str, object]],
    pos: int,
    sample: FrameSample,
    score: float,
    visual: float,
    flipped: bool,
    *,
    limit: int = 6,
) -> None:
    priority = shortlist_candidate_priority(score, visual)
    for item in shortlist:
        if int(item["pos"]) == pos:
            if priority > float(item["priority"]):
                item.update(
                    {
                        "sample": sample,
                        "score": score,
                        "visual": visual,
                        "flipped": flipped,
                        "priority": priority,
                    }
                )
            break
    else:
        shortlist.append(
            {
                "pos": pos,
                "sample": sample,
                "score": score,
                "visual": visual,
                "flipped": flipped,
                "priority": priority,
            }
        )
    shortlist.sort(key=lambda item: float(item["priority"]), reverse=True)
    del shortlist[limit:]


def refine_match_choice(
    reference_frame: ReferenceFrame,
    shortlist: Sequence[Dict[str, object]],
    hasher: VisualHasher,
    similarity_threshold: float,
    current_best_pos: int,
    current_best_score: float,
    frame_interval: float,
    last_match: Optional[FrameSample] = None,
    last_ref_time: float = 0.0,
    continuity_scale: float = 1.0,
) -> Optional[Dict[str, object]]:
    if not reference_frame.frame_path or not shortlist:
        return None

    best_choice: Optional[Dict[str, object]] = None
    best_final = -999.0
    max_pos_delta = max(8, int(4 / max(0.1, frame_interval)))
    for item in shortlist:
        sample = item.get("sample")
        if not isinstance(sample, FrameSample) or not sample.frame_path:
            continue
        candidate_pos = int(item["pos"])
        base_visual = float(item.get("visual", 0.0) or 0.0)
        base_score = float(item.get("score", 0.0) or 0.0)
        base_flipped = bool(item.get("flipped", False))
        if candidate_pos > current_best_pos + max_pos_delta:
            continue
        if base_score < current_best_score - 0.10:
            continue
        refined_visual, refined_flipped, refined_normal, refined_flipped_score = sample_refined_similarity(
            hasher,
            reference_frame,
            sample,
            preferred_flip=base_flipped,
        )
        combined_visual = refined_visual * 0.62 + base_visual * 0.38
        continuity_bias = base_score - base_visual * 1.35
        final_score = combined_visual * 1.35 + continuity_bias * 0.90
        if last_match is not None:
            pos_delta = abs(candidate_pos - current_best_pos)
            if pos_delta > 0:
                final_score -= min(0.05, pos_delta * 0.006)
            if sample.video_path == last_match.video_path:
                expected_time = last_match.timestamp + max(frame_interval, reference_frame.timestamp - last_ref_time)
                time_diff = abs(sample.timestamp - expected_time)
                final_score -= min(0.22, time_diff * 0.11 * continuity_scale)
        if refined_flipped != base_flipped:
            final_score -= 0.01
        if combined_visual < max(0.38, similarity_threshold - 0.10):
            final_score -= 0.05
        if final_score > best_final:
            best_choice = {
                "pos": candidate_pos,
                "sample": sample,
                "score": final_score,
                "visual": combined_visual,
                "flipped": refined_flipped,
                "refined_visual": refined_visual,
                "base_visual": base_visual,
                "refined_normal": refined_normal,
                "refined_flipped": refined_flipped_score,
            }
            best_final = final_score
    return best_choice


def find_segment_reset_candidate(
    reference_frames: Sequence[ReferenceFrame],
    source_frames: Sequence[FrameSample],
    hasher: VisualHasher,
    ref_index: int,
    current_best_pos: int,
    current_path_score: float,
    current_best_visual: float,
    current_best_flipped: bool,
    similarity_threshold: float,
    frame_interval: float,
) -> Optional[Dict[str, object]]:
    if not source_frames:
        return None

    ref = reference_frames[ref_index]
    coarse_step = 2 if len(source_frames) <= 6000 else 3
    skip_radius = max(6, int(4 / max(0.1, frame_interval)))
    shortlist: List[Dict[str, object]] = []
    for pos in range(0, len(source_frames), coarse_step):
        if abs(pos - current_best_pos) <= skip_radius:
            continue
        sample = source_frames[pos]
        visual, flipped, _, _ = sample_signature_similarity(
            hasher,
            ref.signature,
            sample,
            preferred_flip=current_best_flipped,
        )
        if visual < max(0.40, current_best_visual - 0.12):
            continue
        path_score = evaluate_forward_path_similarity(
            reference_frames,
            source_frames,
            hasher,
            ref_index,
            pos,
            horizon=5,
            start_flip=flipped,
        )
        priority = path_score * 1.05 + visual * 0.35
        if priority < current_path_score - 0.06:
            continue
        update_match_candidate_shortlist(shortlist, pos, sample, priority, visual, flipped, limit=8)

    best_choice: Optional[Dict[str, object]] = None
    best_score = -999.0
    for item in shortlist:
        center_pos = int(item["pos"])
        for pos in range(max(0, center_pos - 2), min(len(source_frames), center_pos + 3)):
            if abs(pos - current_best_pos) <= skip_radius:
                continue
            sample = source_frames[pos]
            shortlist_flip = bool(item.get("flipped", False))
            visual, flipped, _, _ = sample_signature_similarity(
                hasher,
                ref.signature,
                sample,
                preferred_flip=shortlist_flip,
            )
            if visual < max(0.42, current_best_visual - 0.10):
                continue
            path_score = evaluate_forward_path_similarity(
                reference_frames,
                source_frames,
                hasher,
                ref_index,
                pos,
                horizon=6,
                start_flip=flipped,
            )
            if path_score < current_path_score - 0.03:
                continue
            refined_visual = (
                sample_refined_similarity(
                    hasher,
                    ref,
                    sample,
                    preferred_flip=flipped,
                )[0]
                if ref.frame_path and sample.frame_path
                else 0.0
            )
            combined_visual = refined_visual * 0.60 + visual * 0.40 if refined_visual > 0 else visual
            final_score = path_score * 0.95 + combined_visual * 0.72
            if combined_visual < max(0.48, similarity_threshold - 0.06):
                final_score -= 0.05
            if final_score > best_score:
                best_choice = {
                    "pos": pos,
                    "sample": sample,
                    "visual": combined_visual,
                    "flipped": flipped,
                    "score": final_score,
                    "path_score": path_score,
                    "refined_visual": refined_visual,
                }
                best_score = final_score
    return best_choice


def match_frames(
    reference_frames: Sequence[ReferenceFrame],
    source_frames: Sequence[FrameSample],
    hasher: VisualHasher,
    frame_interval: float,
    similarity_threshold: float,
    attempt: int,
    log_func: Optional[Callable[[str], None]] = None,
) -> Tuple[List[Dict[str, object]], float, Dict[str, int]]:
    if not reference_frames or not source_frames:
        return [], 0.0, {
            "backtracks": 0,
            "low_sim": 0,
            "missed": len(reference_frames),
            "video_switches": 0,
            "reanchors": 0,
            "bridges": 0,
            "refinements": 0,
            "segment_resets": 0,
        }

    matches: List[Dict[str, object]] = []
    diagnostics = {
        "backtracks": 0,
        "low_sim": 0,
        "missed": 0,
        "video_switches": 0,
        "reanchors": 0,
        "bridges": 0,
        "refinements": 0,
        "segment_resets": 0,
    }
    search_start = 0
    confident = 0
    last_match: Optional[FrameSample] = None
    last_pos = -1
    last_flipped = False
    last_ref_time = 0.0
    continuity_scale = max(0.55, 1.0 - 0.12 * max(0, attempt - 1))
    low_streak = 0

    for ref_index, ref in enumerate(reference_frames):
        if search_start >= len(source_frames):
            diagnostics["missed"] += len(reference_frames) - len(matches)
            break

        best_pos = -1
        best_sample: Optional[FrameSample] = None
        best_score = -999.0
        best_visual = 0.0
        best_flipped = False
        best_reanchor_pos = -1
        best_reanchor_sample: Optional[FrameSample] = None
        best_reanchor_score = -999.0
        best_reanchor_visual = 0.0
        best_reanchor_flipped = False
        shortlist: List[Dict[str, object]] = []

        for pos in range(search_start, len(source_frames)):
            sample = source_frames[pos]
            preferred_flip = last_flipped if last_match is not None else None
            visual, flipped, _, _ = sample_signature_similarity(
                hasher,
                ref.signature,
                sample,
                preferred_flip=preferred_flip,
            )
            score = visual * 1.35
            reanchor_score = visual * 1.45

            if last_match is None:
                score -= min(0.20, pos * 0.00015)
                reanchor_score -= min(0.05, pos * 0.00003)
            else:
                gap_index = sample.global_index - last_match.global_index
                if gap_index < 0:
                    continue
                gap_ref = max(frame_interval, ref.timestamp - last_ref_time)
                expected_time = last_match.timestamp + gap_ref
                if sample.video_path == last_match.video_path:
                    time_diff = abs(sample.timestamp - expected_time)
                    if gap_index == 0:
                        score -= 0.10 * continuity_scale
                        reanchor_score -= 0.04 * continuity_scale
                    elif gap_index == 1:
                        score += 0.12
                        reanchor_score += 0.02
                    elif gap_index <= 3:
                        score += 0.06
                        reanchor_score += 0.01
                    score -= min(0.28, time_diff * 0.11 * continuity_scale)
                    reanchor_score -= min(0.08, time_diff * 0.018 * continuity_scale)
                else:
                    video_gap = sample.video_order - last_match.video_order
                    if video_gap < 0:
                        continue
                    score -= min(0.25, 0.06 * max(0, video_gap - 1) * continuity_scale)
                    score += max(0.0, 0.05 - sample.timestamp * 0.02)
                    reanchor_score -= min(0.05, 0.012 * max(0, video_gap - 1) * continuity_scale)
                    reanchor_score += max(0.0, 0.03 - sample.timestamp * 0.008)
                score -= min(0.16, max(0, gap_index - 6) * 0.0025 * continuity_scale)
                reanchor_score -= min(0.05, max(0, gap_index - 12) * 0.0004 * continuity_scale)
                if flipped == last_flipped:
                    score += 0.015
                    reanchor_score += 0.006
                else:
                    score -= 0.028 * continuity_scale
                    reanchor_score -= 0.010 * continuity_scale

            if score > best_score:
                best_pos = pos
                best_sample = sample
                best_score = score
                best_visual = visual
                best_flipped = flipped
            if reanchor_score > best_reanchor_score:
                best_reanchor_pos = pos
                best_reanchor_sample = sample
                best_reanchor_score = reanchor_score
                best_reanchor_visual = visual
                best_reanchor_flipped = flipped
            update_match_candidate_shortlist(
                shortlist,
                pos,
                sample,
                max(score, reanchor_score),
                visual,
                flipped,
            )
            if visual >= 0.995:
                break

        if best_sample is None:
            diagnostics["missed"] += 1
            continue

        if (
            last_match is not None
            and last_pos >= 0
            and ref.frame_path
            and last_match.frame_path
            and last_match.video_path == best_sample.video_path
            and ref.timestamp - last_ref_time <= frame_interval * 1.25
        ):
            hold_visual, hold_flipped, _, _ = sample_refined_similarity(
                hasher,
                ref,
                last_match,
                preferred_flip=last_flipped,
            )
            best_refined_visual = (
                sample_refined_similarity(
                    hasher,
                    ref,
                    best_sample,
                    preferred_flip=best_flipped,
                )[0]
                if best_sample.frame_path
                else best_visual
            )
            if hold_visual >= max(similarity_threshold, best_refined_visual + 0.04):
                best_pos = last_pos
                best_sample = last_match
                best_visual = hold_visual
                best_flipped = hold_flipped
                best_score = hold_visual * 1.35 - 0.08 * continuity_scale
                if hold_flipped == last_flipped:
                    best_score += 0.015

        if last_match and best_sample.video_path == last_match.video_path:
            jump_seconds = best_sample.timestamp - last_match.timestamp
            if jump_seconds >= frame_interval * 6:
                local_window_limit = min(len(source_frames), search_start + max(8, int(6 / frame_interval)))
                best_bridge_pos = -1
                best_bridge_sample: Optional[FrameSample] = None
                best_bridge_visual = 0.0
                best_bridge_flipped = best_flipped
                best_bridge_path_score = -999.0
                for pos in range(search_start, local_window_limit):
                    sample = source_frames[pos]
                    if sample.video_path != last_match.video_path:
                        break
                    if sample.timestamp - last_match.timestamp > frame_interval * 8:
                        break
                    visual, flipped, _, _ = sample_signature_similarity(
                        hasher,
                        ref.signature,
                        sample,
                        preferred_flip=last_flipped,
                    )
                    path_score = evaluate_forward_path_similarity(
                        reference_frames,
                        source_frames,
                        hasher,
                        ref_index,
                        pos,
                        start_flip=flipped,
                    )
                    if path_score > best_bridge_path_score:
                        best_bridge_pos = pos
                        best_bridge_sample = sample
                        best_bridge_visual = visual
                        best_bridge_flipped = flipped
                        best_bridge_path_score = path_score

                selected_path_score = evaluate_forward_path_similarity(
                    reference_frames,
                    source_frames,
                    hasher,
                    ref_index,
                    best_pos,
                    start_flip=best_flipped,
                )
                should_bridge = (
                    best_bridge_sample is not None
                    and best_bridge_pos < best_pos
                    and best_bridge_path_score >= selected_path_score + 0.10
                    and best_bridge_visual >= max(0.48, best_visual - 0.35)
                )
                if should_bridge:
                    if log_func and diagnostics["bridges"] < 8:
                        log_func(
                            "  Bridge rescue "
                            f"ref {ref.timestamp:.1f}s: "
                            f"{best_sample.video_name}@{best_sample.timestamp:.1f}s ({best_visual:.3f}) -> "
                            f"{best_bridge_sample.video_name}@{best_bridge_sample.timestamp:.1f}s "
                            f"({best_bridge_visual:.3f}), path {selected_path_score:.3f}->{best_bridge_path_score:.3f}"
                        )
                    best_pos = best_bridge_pos
                    best_sample = best_bridge_sample
                    best_visual = best_bridge_visual
                    best_flipped = best_bridge_flipped
                    gap_ref = max(frame_interval, ref.timestamp - last_ref_time)
                    expected_time = last_match.timestamp + gap_ref
                    gap_index = best_bridge_sample.global_index - last_match.global_index
                    best_score = best_bridge_visual * 1.35
                    if gap_index == 1:
                        best_score += 0.12
                    elif gap_index <= 3:
                        best_score += 0.06
                    best_score -= min(0.28, abs(best_bridge_sample.timestamp - expected_time) * 0.11 * continuity_scale)
                    best_score -= min(0.16, max(0, gap_index - 6) * 0.0025 * continuity_scale)
                    if best_bridge_flipped == last_flipped:
                        best_score += 0.015
                    else:
                        best_score -= 0.028 * continuity_scale
                    diagnostics["bridges"] += 1

        if last_match and best_reanchor_sample is not None:
            jump_frames = best_reanchor_sample.global_index - best_sample.global_index
            if best_reanchor_sample.video_path == last_match.video_path:
                jump_seconds = best_reanchor_sample.timestamp - last_match.timestamp
            else:
                jump_seconds = best_reanchor_sample.timestamp + frame_interval
            current_path_score = evaluate_forward_path_similarity(
                reference_frames,
                source_frames,
                hasher,
                ref_index,
                best_pos,
                start_flip=best_flipped,
            )
            reanchor_path_score = evaluate_forward_path_similarity(
                reference_frames,
                source_frames,
                hasher,
                ref_index,
                best_reanchor_pos,
                start_flip=best_reanchor_flipped,
            )
            strict_visual_floor = max(similarity_threshold + 0.01, 0.69 - 0.02 * max(0, attempt - 1))
            reanchor_visual_floor = max(similarity_threshold + 0.04, 0.73 - 0.02 * max(0, attempt - 1))
            path_supports_reanchor = (
                reanchor_path_score >= current_path_score - 0.02
                or (
                    best_reanchor_visual >= best_visual + 0.22
                    and reanchor_path_score >= current_path_score - 0.10
                )
            )
            should_reanchor = (
                best_reanchor_pos > best_pos
                and jump_frames >= 3
                and jump_seconds >= frame_interval * 4
                and best_visual < strict_visual_floor
                and best_reanchor_visual >= max(reanchor_visual_floor, best_visual + 0.10)
                and path_supports_reanchor
                and (
                    best_reanchor_score >= best_score - 0.02
                    or best_reanchor_visual >= best_visual + 0.16
                )
            )
            if should_reanchor:
                if log_func and diagnostics["reanchors"] < 8:
                    log_func(
                        "  Re-anchor "
                        f"ref {ref.timestamp:.1f}s: "
                        f"{best_sample.video_name}@{best_sample.timestamp:.1f}s ({best_visual:.3f}) -> "
                        f"{best_reanchor_sample.video_name}@{best_reanchor_sample.timestamp:.1f}s "
                        f"({best_reanchor_visual:.3f}), path {current_path_score:.3f}->{reanchor_path_score:.3f}"
                    )
                best_pos = best_reanchor_pos
                best_sample = best_reanchor_sample
                best_score = best_reanchor_score
                best_visual = best_reanchor_visual
                best_flipped = best_reanchor_flipped
                diagnostics["reanchors"] += 1

        if shortlist and best_sample is not None and best_visual < max(0.83, similarity_threshold + 0.10):
            refined_choice = refine_match_choice(
                ref,
                shortlist,
                hasher,
                similarity_threshold,
                best_pos,
                best_score,
                frame_interval,
                last_match=last_match,
                last_ref_time=last_ref_time,
                continuity_scale=continuity_scale,
            )
            if refined_choice is not None:
                refined_pos = int(refined_choice["pos"])
                refined_sample = refined_choice["sample"]
                refined_visual = float(refined_choice["visual"])
                refined_score = float(refined_choice["score"])
                refined_flipped = bool(refined_choice.get("flipped", False))
                if (
                    refined_pos != best_pos
                    and refined_visual >= max(best_visual + 0.03, similarity_threshold - 0.08)
                ) or refined_score > best_score + 0.03:
                    if log_func and diagnostics["refinements"] < 8:
                        log_func(
                            "  Refine match "
                            f"ref {ref.timestamp:.1f}s: "
                            f"{best_sample.video_name}@{best_sample.timestamp:.1f}s ({best_visual:.3f}) -> "
                            f"{refined_sample.video_name}@{refined_sample.timestamp:.1f}s "
                            f"({refined_visual:.3f})"
                        )
                    best_pos = refined_pos
                    best_sample = refined_sample
                    best_score = refined_score
                    best_visual = refined_visual
                    best_flipped = refined_flipped
                    diagnostics["refinements"] += 1

        current_path_score = evaluate_forward_path_similarity(
            reference_frames,
            source_frames,
            hasher,
            ref_index,
            best_pos,
            horizon=6,
            start_flip=best_flipped,
        )

        if best_visual >= similarity_threshold:
            low_streak = 0
        else:
            low_streak += 1

        if last_match is not None and (
            low_streak >= max(3, 6 - min(attempt, 3))
            or (
                best_visual < similarity_threshold - 0.05
                and current_path_score < similarity_threshold + 0.02
            )
        ):
            reset_choice = find_segment_reset_candidate(
                reference_frames,
                source_frames,
                hasher,
                ref_index,
                best_pos,
                current_path_score,
                best_visual,
                best_flipped,
                similarity_threshold,
                frame_interval,
            )
            if reset_choice is not None:
                reset_pos = int(reset_choice["pos"])
                reset_sample = reset_choice["sample"]
                reset_visual = float(reset_choice["visual"])
                reset_flipped = bool(reset_choice.get("flipped", False))
                reset_score = float(reset_choice["score"])
                reset_path_score = float(reset_choice["path_score"])
                far_enough = abs(reset_pos - best_pos) >= max(14, int(10 / max(0.1, frame_interval)))
                should_reset = (
                    far_enough
                    and reset_visual >= max(best_visual + 0.08, similarity_threshold - 0.03)
                    and reset_path_score >= current_path_score + 0.07
                )
                if should_reset:
                    if log_func and diagnostics["segment_resets"] < 8:
                        log_func(
                            "  Segment reset "
                            f"ref {ref.timestamp:.1f}s: "
                            f"{best_sample.video_name}@{best_sample.timestamp:.1f}s ({best_visual:.3f}) -> "
                            f"{reset_sample.video_name}@{reset_sample.timestamp:.1f}s "
                            f"({reset_visual:.3f}), path {current_path_score:.3f}->{reset_path_score:.3f}"
                        )
                    best_pos = reset_pos
                    best_sample = reset_sample
                    best_score = reset_score
                    best_visual = reset_visual
                    best_flipped = reset_flipped
                    current_path_score = reset_path_score
                    low_streak = 0
                    diagnostics["segment_resets"] += 1

        if last_match and best_sample.global_index < last_match.global_index:
            diagnostics["backtracks"] += 1
        if last_match and best_sample.video_path != last_match.video_path:
            diagnostics["video_switches"] += 1

        if best_visual >= similarity_threshold:
            confident += 1
        else:
            diagnostics["low_sim"] += 1

        matches.append(
            {
                "source_video": best_sample.video_path,
                "source_name": best_sample.video_name,
                "source_start": best_sample.timestamp,
                "source_flip": best_flipped,
                "duration": frame_interval,
                "similarity": best_visual,
                "score": best_score,
                "ref_time": ref.timestamp,
                "source_global_index": best_sample.global_index,
            }
        )
        search_start = best_pos
        last_match = best_sample
        last_pos = best_pos
        last_flipped = best_flipped
        last_ref_time = ref.timestamp

    if log_func and matches:
        log_func(
            f"  帧匹配完成: {len(matches)}/{len(reference_frames)}，"
            f"高置信 {confident}，低相似 {diagnostics['low_sim']}"
        )

    return matches, confident / len(reference_frames), diagnostics


def merge_matches(matches: Sequence[Dict[str, object]], tolerance: float = 0.05) -> List[SegmentJob]:
    if not matches:
        return []
    jobs: List[SegmentJob] = []
    current_video = str(matches[0]["source_video"])
    current_start = float(matches[0]["source_start"])
    current_duration = float(matches[0]["duration"])
    prev_match = matches[0]

    for match in matches[1:]:
        same_video = str(match["source_video"]) == current_video
        contiguous_source = abs(
            float(match["source_start"]) - (float(prev_match["source_start"]) + float(prev_match["duration"]))
        ) <= tolerance
        contiguous_ref = abs(
            float(match["ref_time"]) - (float(prev_match["ref_time"]) + float(prev_match["duration"]))
        ) <= tolerance
        if same_video and contiguous_source and contiguous_ref:
            current_duration += float(match["duration"])
        else:
            jobs.append(SegmentJob(current_video, current_start, current_duration, False))
            current_video = str(match["source_video"])
            current_start = float(match["source_start"])
            current_duration = float(match["duration"])
        prev_match = match

    jobs.append(SegmentJob(current_video, current_start, current_duration, False))
    return jobs


def trim_unstable_tail_matches(
    matches: Sequence[Dict[str, object]],
    similarity_floor: float,
    max_trim_duration: float = 12.0,
    window_size: int = 4,
) -> List[Dict[str, object]]:
    if len(matches) < max(3, window_size + 1):
        return list(matches)

    tail_start = len(matches) - 1
    tail_duration = 0.0
    while tail_start > 0 and tail_duration < max_trim_duration:
        tail_duration += float(matches[tail_start].get("duration", 0.0) or 0.0)
        tail_start -= 1
    tail_start = max(0, tail_start)

    trim_index: Optional[int] = None
    for index in range(tail_start, len(matches) - window_size + 1):
        current_similarity = float(matches[index].get("similarity", 0.0) or 0.0)
        window = matches[index : index + window_size]
        window_scores = [float(item.get("similarity", 0.0) or 0.0) for item in window]
        tail_scores = [float(item.get("similarity", 0.0) or 0.0) for item in matches[index:]]
        avg_window = sum(window_scores) / max(1, len(window_scores))
        avg_tail = sum(tail_scores) / max(1, len(tail_scores))
        low_ratio = sum(score < similarity_floor for score in tail_scores) / max(1, len(tail_scores))
        strong_rebound = any(score >= similarity_floor + 0.08 for score in tail_scores[1:])
        video_switches = sum(
            1
            for left, right in zip(matches[index:], matches[index + 1 :])
            if str(left.get("source_video", "")) != str(right.get("source_video", ""))
        )
        same_video_tail = len({str(item.get("source_video", "")) for item in matches[index:]}) == 1
        medium_support = sum(score >= similarity_floor - 0.05 for score in tail_scores)
        if same_video_tail and medium_support >= max(3, len(tail_scores) // 4):
            continue
        if (
            current_similarity < similarity_floor
            and avg_window < similarity_floor + 0.04
            and avg_tail < similarity_floor + 0.03
            and low_ratio >= 0.70
            and not strong_rebound
        ):
            trim_index = index
            break
        if (
            current_similarity < similarity_floor + 0.03
            and video_switches > 0
            and avg_window < similarity_floor + 0.08
            and avg_tail < similarity_floor + 0.05
            and low_ratio >= 0.65
            and not strong_rebound
        ):
            trim_index = index
            break

    if trim_index is not None and len(matches) - trim_index >= window_size:
        return list(matches[:trim_index])
    return list(matches)


def summarize_match_similarity(matches: Sequence[Dict[str, object]]) -> Dict[str, float]:
    if not matches:
        return {"avg": 0.0, "median": 0.0, "p75": 0.0, "p90": 0.0, "p95": 0.0}
    values = sorted(float(match.get("similarity", 0.0) or 0.0) for match in matches)
    avg = sum(values) / len(values)
    return {
        "avg": avg,
        "median": percentile_value(values, 0.5),
        "p75": percentile_value(values, 0.75),
        "p90": percentile_value(values, 0.90),
        "p95": percentile_value(values, 0.95),
    }


def assess_early_match_stability(
    matches: Sequence[Dict[str, object]],
    frame_interval: float,
    window_seconds: float = 12.0,
) -> Dict[str, float]:
    early_matches = [match for match in matches if float(match.get("ref_time", 0.0) or 0.0) <= window_seconds]
    if len(early_matches) < 2:
        return {
            "unique_videos": float(len({str(match.get("source_video", "")) for match in early_matches})),
            "video_switches": 0.0,
            "hard_jumps": 0.0,
            "penalty": 0.0,
        }

    video_switches = 0
    hard_jumps = 0
    for previous, current in zip(early_matches, early_matches[1:]):
        previous_video = str(previous.get("source_video", ""))
        current_video = str(current.get("source_video", ""))
        previous_start = float(previous.get("source_start", 0.0) or 0.0)
        current_start = float(current.get("source_start", 0.0) or 0.0)
        previous_ref = float(previous.get("ref_time", 0.0) or 0.0)
        current_ref = float(current.get("ref_time", 0.0) or 0.0)
        if current_video != previous_video:
            video_switches += 1
            continue
        ref_gap = max(frame_interval, current_ref - previous_ref)
        source_gap = max(0.0, current_start - previous_start)
        if source_gap > max(ref_gap + frame_interval * 3.0, frame_interval * 4.0):
            hard_jumps += 1

    unique_videos = len({str(match.get("source_video", "")) for match in early_matches})
    penalty = max(0, unique_videos - 1) * 0.18 + max(0, video_switches - 1) * 0.10 + hard_jumps * 0.03
    return {
        "unique_videos": float(unique_videos),
        "video_switches": float(video_switches),
        "hard_jumps": float(hard_jumps),
        "penalty": penalty,
    }


def match_quality_failure_reason(
    matches: Sequence[Dict[str, object]],
    confident_rate: float,
    diagnostics: Dict[str, int],
    audit: Optional[Dict[str, object]] = None,
) -> str:
    if not matches:
        return "没有可用匹配结果。"
    stats = summarize_match_similarity(matches)
    low_ratio = diagnostics.get("low_sim", 0) / max(1, len(matches))
    if audit and int(audit.get("samples", 0) or 0) >= 12:
        audit_avg = float(audit.get("avg", 0.0) or 0.0)
        audit_median = float(audit.get("median", 0.0) or 0.0)
        audit_low_ratio = float(audit.get("low_ratio", 0.0) or 0.0)
        suspect_times = audit.get("suspect_times", [])
        suspect_text = ""
        if isinstance(suspect_times, list) and suspect_times:
            suspect_text = " 可疑时点: " + ", ".join(f"{float(value):.1f}s" for value in suspect_times[:6])
        if audit_avg < 0.28 and audit_median < 0.22 and audit_low_ratio >= 0.55:
            return (
                "严格结构复审显示大量匹配帧并非同镜头，当前参考视频与素材无法稳定对齐。"
                "这通常不是单纯倒叙造成的，更常见于素材缺段、版源不同、裁切层叠差异过大。"
                + suspect_text
            )
    if confident_rate < 0.20 and low_ratio >= 0.75 and stats["median"] < 0.60:
        return (
            "帧匹配整体相似度过低，疑似参考片与素材并非同版画面，"
            "或存在较大的裁切 / 压制 / 叠层差异。"
        )
    if low_ratio >= 0.85 and stats["p90"] < 0.67:
        return "高相似度帧比例过低，当前素材无法稳定支撑画面重建。"
    return ""


def audit_match_alignment(
    matches: Sequence[Dict[str, object]],
    reference_frames: Sequence[ReferenceFrame],
    source_frame_index: Dict[Tuple[str, float], FrameSample],
    sample_step: int = AUDIT_SAMPLE_STEP,
) -> Dict[str, object]:
    if not matches or not reference_frames or not NUMPY_AVAILABLE:
        return {"avg": 0.0, "median": 0.0, "low": 0, "samples": 0, "low_ratio": 0.0, "suspect_times": []}

    scores: List[float] = []
    suspect_times: List[float] = []
    limit = min(len(matches), len(reference_frames))
    for index in range(0, limit, max(1, sample_step)):
        match = matches[index]
        ref = reference_frames[index]
        source_key = (
            str(match.get("source_video", "")),
            round(float(match.get("source_start", 0.0) or 0.0), 3),
        )
        sample = source_frame_index.get(source_key)
        if sample is None or not ref.frame_path or not sample.frame_path:
            continue
        score = structural_frame_similarity_from_paths(
            ref.frame_path,
            sample.frame_path,
            flip_right=bool(match.get("source_flip", False)),
        )
        scores.append(score)
        if score < AUDIT_LOW_SCORE and len(suspect_times) < 8:
            suspect_times.append(ref.timestamp)

    if not scores:
        return {"avg": 0.0, "median": 0.0, "low": 0, "samples": 0, "low_ratio": 0.0, "suspect_times": []}

    ordered_scores = sorted(scores)
    low_count = sum(value < AUDIT_LOW_SCORE for value in scores)
    return {
        "avg": sum(scores) / len(scores),
        "median": percentile_value(ordered_scores, 0.5),
        "low": low_count,
        "samples": len(scores),
        "low_ratio": low_count / len(scores),
        "suspect_times": suspect_times,
    }


def generate_silence(duration: float, output_path: Path, video_processor: VideoProcessor) -> None:
    codec_args = ["-c:a", "pcm_s16le"] if output_path.suffix.lower() == ".wav" else ["-c:a", "libmp3lame", "-b:a", "192k"]
    result = run_subprocess_hidden(
        [
            str(video_processor.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=channel_layout=stereo:sample_rate=48000",
            "-t",
            f"{max(duration, 0.01):.3f}",
            *codec_args,
            str(output_path),
        ],
        capture_output=True,
        timeout=60,
        check=False,
    )
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore")[:400] or "silence generation failed")


def atempo_chain(speed: float) -> str:
    if speed <= 0:
        raise ValueError("speed must be positive")
    values: List[float] = []
    remaining = speed
    while remaining > 2.0:
        values.append(2.0)
        remaining /= 2.0
    while remaining < 0.5:
        values.append(0.5)
        remaining /= 0.5
    values.append(remaining)
    return ",".join(f"atempo={value:.6f}" for value in values)


def choose_local_tts_fit_speed_factor(source_duration: float, target_duration: float) -> float:
    required_speed = source_duration / max(0.08, target_duration)
    if required_speed <= 1.01:
        return 1.0
    if required_speed <= LOCAL_TTS_MICRO_SPEED_FACTOR + 0.005:
        return clamp(required_speed, 1.0, LOCAL_TTS_MICRO_SPEED_FACTOR)
    return clamp(required_speed, 1.0, MAX_TTS_SPEED_FACTOR)


def build_tts_cleanup_filters(
    *,
    trim_silence: bool = True,
    volume_gain: float = 1.0,
    speed: Optional[float] = None,
    output_duration: Optional[float] = None,
) -> List[str]:
    filters: List[str] = []
    if trim_silence:
        filters.extend(
            [
                "silenceremove=start_periods=1:start_duration=0.05:start_threshold=-45dB",
                "areverse",
                "silenceremove=start_periods=1:start_duration=0.05:start_threshold=-45dB",
                "areverse",
            ]
        )
    if abs(volume_gain - 1.0) > 0.001:
        filters.append(f"volume={volume_gain:.3f}")
    if speed is not None and (speed > 1.01 or speed < 0.99):
        filters.append(atempo_chain(speed))
    if output_duration is not None:
        filters.append(f"atrim=duration={max(0.05, output_duration):.3f}")
    return filters


def prepare_tts_source_clip(
    source_path: Path,
    output_path: Path,
    video_processor: VideoProcessor,
) -> float:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result = run_subprocess_hidden(
        [
            str(video_processor.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-i",
            str(source_path),
            "-filter:a",
            ",".join(build_tts_cleanup_filters(trim_silence=True)),
            "-ar",
            "48000",
            "-ac",
            "2",
            "-c:a",
            "pcm_s16le",
            str(output_path),
        ],
        capture_output=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore")[:400] or "tts prepare failed")
    duration = video_processor.probe_duration(output_path)
    if duration <= 0.05:
        raise RuntimeError("tts prepared clip is empty after silence trim")
    return duration


def tts_group_schedulable_duration(group_state: Dict[str, object]) -> float:
    effective_duration = max(0.0, float(group_state.get("effective_duration", 0.0) or 0.0))
    if effective_duration > 0.0:
        return max(0.05, effective_duration)

    text = normalize_subtitle_text(str(group_state.get("text", "") or ""))
    render_rate = str(group_state.get("applied_rate", DEFAULT_TTS_RATE) or DEFAULT_TTS_RATE)
    estimated_duration = estimate_tts_render_duration(text, render_rate) if text else 0.0
    raw_duration = max(0.0, float(group_state.get("raw_duration", 0.0) or 0.0))
    if raw_duration > 0.0 and estimated_duration > 0.0:
        return max(0.05, min(raw_duration, estimated_duration * 1.12))
    if raw_duration > 0.0:
        return max(0.05, raw_duration)
    if estimated_duration > 0.0:
        return max(0.05, estimated_duration)
    return 0.05


def update_group_tts_source_metrics(
    group_state: Dict[str, object],
    render_rate: str,
    video_processor: VideoProcessor,
    log_func: Optional[Callable[[str], None]] = None,
) -> None:
    group_text = str(group_state.get("text", "") or "")
    raw_path = Path(str(group_state["raw_path"]))
    prepared_path = Path(str(group_state.get("prepared_path", ""))) if group_state.get("prepared_path") else None
    estimated_duration = estimate_tts_render_duration(group_text, render_rate)
    group_state["applied_rate"] = render_rate
    group_state["effective_path"] = str(raw_path)
    group_state["trim_silence_on_fit"] = True

    if not tts_audio_file_ready(raw_path):
        group_state["raw_duration"] = max(0.05, estimated_duration)
        group_state["effective_duration"] = max(0.05, estimated_duration)
        return

    raw_duration = max(0.05, video_processor.probe_duration(raw_path))
    group_state["raw_duration"] = raw_duration
    if prepared_path is None:
        group_state["effective_duration"] = max(0.05, min(raw_duration, estimated_duration * 1.12))
        return

    try:
        effective_duration = prepare_tts_source_clip(raw_path, prepared_path, video_processor)
    except Exception as exc:
        group_state["effective_duration"] = max(0.05, min(raw_duration, estimated_duration * 1.12))
        if log_func:
            detail = summarize_for_log(str(exc), limit=180) or "unknown prepare error"
            log_func(
                f"  TTS #{group_state.get('order', '?')} [{group_state.get('label', '?')}] using raw clip timing fallback: {detail}"
            )
        return

    group_state["effective_duration"] = max(0.05, effective_duration)
    group_state["effective_path"] = str(prepared_path)
    group_state["trim_silence_on_fit"] = False


def split_tts_recovery_chunks(text: str, split_depth: int = 0) -> List[str]:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return []

    budget_levels = [12, 10, 8, 6]
    start_level = min(max(0, split_depth), len(budget_levels) - 1)
    for budget in budget_levels[start_level:]:
        parts = split_text_at_budget(normalized, max_units=budget, min_units=2)
        if len(parts) > 1:
            return parts

    midpoint = len(normalized) // 2
    candidate_positions = sorted(set(break_positions(normalized) + [midpoint]))
    best_parts: List[str] = []
    best_distance: Optional[int] = None
    for position in candidate_positions:
        if position <= 0 or position >= len(normalized):
            continue
        left = normalize_subtitle_text(normalized[:position])
        right = normalize_subtitle_text(normalized[position:])
        if subtitle_speech_units(left) < 2 or subtitle_speech_units(right) < 2:
            continue
        distance = abs(position - midpoint)
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best_parts = [left, right]
    return best_parts


async def _synthesize_tts_async(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    volume: str = DEFAULT_TTS_VOLUME,
    pitch: str = DEFAULT_TTS_PITCH,
) -> None:
    communicate = edge_tts.Communicate(text=text, voice=voice, rate=rate, volume=volume, pitch=pitch)
    await asyncio.wait_for(
        communicate.save(str(output_path)),
        timeout=TTS_REQUEST_TIMEOUT_SECONDS,
    )


def tts_voice_candidates(preferred_voice: str) -> List[str]:
    ordered: List[str] = []
    for voice in [
        preferred_voice,
        DEFAULT_TTS_VOICE,
        LEGACY_DEFAULT_TTS_VOICE,
        "zh-CN-YunjianNeural",
        "zh-CN-YunhaoNeural",
        "zh-CN-YunyangNeural",
        "zh-CN-XiaoxiaoNeural",
    ]:
        normalized = (voice or "").strip()
        if normalized and normalized not in ordered:
            ordered.append(normalized)
    return ordered


def tts_attempt_settings(candidates: Sequence[str]) -> List[Tuple[str, str, str]]:
    attempts: List[Tuple[str, str, str]] = []
    for candidate in candidates:
        normalized = (candidate or "").strip()
        if not normalized:
            continue
        attempts.append((normalized, DEFAULT_TTS_VOLUME, DEFAULT_TTS_PITCH))
        neutral_settings = ("+0%", "+0Hz")
        if (DEFAULT_TTS_VOLUME, DEFAULT_TTS_PITCH) != neutral_settings:
            attempts.append((normalized, neutral_settings[0], neutral_settings[1]))
    return attempts


def is_backup_tts_enabled(settings: Optional[CloneSettings]) -> bool:
    return bool(settings and settings.enable_backup_tts)


def has_backup_tts_config(settings: Optional[CloneSettings]) -> bool:
    if not is_backup_tts_enabled(settings):
        return False
    return bool((settings.azure_tts_key or "").strip() and (settings.azure_tts_region or "").strip())


def has_any_tts_provider(settings: Optional[CloneSettings]) -> bool:
    return TTS_AVAILABLE or has_backup_tts_config(settings)


def azure_tts_endpoint(region: str) -> str:
    normalized_region = (region or "").strip().lower()
    return f"https://{normalized_region}.tts.speech.microsoft.com/cognitiveservices/v1"


def azure_tts_output_format_for_suffix(suffix: str) -> str:
    if (suffix or "").strip().lower() == ".wav":
        return AZURE_TTS_WAV_OUTPUT_FORMAT
    return AZURE_TTS_DEFAULT_OUTPUT_FORMAT


def azure_tts_voice_candidates(primary_voice: str, backup_voice: str) -> List[str]:
    preferred_voice = (backup_voice or "").strip() or (primary_voice or "").strip()
    return tts_voice_candidates(preferred_voice)


def build_azure_tts_ssml(text: str, voice: str, rate: str) -> str:
    safe_voice = html.escape((voice or "").strip() or DEFAULT_TTS_VOICE, quote=True)
    normalized_text = normalize_subtitle_text(text)
    safe_text = html.escape(normalized_text, quote=False)
    safe_rate = format_rate_percent(parse_rate_percent(rate or DEFAULT_TTS_RATE))
    return (
        '<speak version="1.0" xml:lang="zh-CN" '
        'xmlns="http://www.w3.org/2001/10/synthesis">'
        f'<voice name="{safe_voice}"><prosody rate="{safe_rate}">{safe_text}</prosody></voice>'
        "</speak>"
    )


def classify_azure_backup_tts_error(status_code: int, body_text: str) -> str:
    normalized = (body_text or "").strip().lower()
    if status_code == 429:
        return "quota"
    if status_code == 401:
        return "auth"
    if status_code == 403:
        if any(marker in normalized for marker in ["quota", "limit", "exceeded", "billing", "free tier"]):
            return "quota"
        return "auth"
    if status_code == 404:
        return "region"
    if status_code in {408, 409, 425, 500, 502, 503, 504}:
        return "transient"
    if any(marker in normalized for marker in ["quota", "out of quota", "exceeded", "free tier", "billing"]):
        return "quota"
    return "other"


def is_backup_tts_quota_error(error_text: str) -> bool:
    normalized = (error_text or "").strip().lower()
    return "azure backup tts" in normalized and "quota" in normalized


def is_backup_tts_missing_config_error(error_text: str) -> bool:
    normalized = (error_text or "").strip().lower()
    return "azure backup tts" in normalized and "missing key/region" in normalized


def is_backup_tts_auth_error(error_text: str) -> bool:
    normalized = (error_text or "").strip().lower()
    return "azure backup tts" in normalized and ("auth" in normalized or "region" in normalized)


def merge_tts_provider(current_provider: str, new_provider: str) -> str:
    normalized_current = (current_provider or "").strip()
    normalized_new = (new_provider or "").strip()
    if normalized_new == TTS_PROVIDER_AZURE:
        return normalized_new
    if not normalized_current or normalized_current == TTS_PROVIDER_CACHE:
        return normalized_new or normalized_current
    return normalized_current or normalized_new


def is_transient_tts_error(error_text: str) -> bool:
    normalized = (error_text or "").strip().lower()
    if not normalized:
        return True
    transient_markers = [
        "no audio was received",
        "timed out",
        "timeout",
        "timeouterror",
        "temporarily unavailable",
        "connection",
        "cannot connect",
        "clientconnector",
        "websocket",
        "ws",
        "reset by peer",
        "forcibly closed",
        "broken pipe",
        "eof",
        "server disconnected",
        "service unavailable",
        "internal error",
        "network is unreachable",
        "temporary failure",
        "name or service not known",
        "handshake",
        "transient error",
        "rate limit",
        "429",
        "502",
        "503",
        "504",
    ]
    return any(marker in normalized for marker in transient_markers)


def _synthesize_tts_cli(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    *,
    volume: str = DEFAULT_TTS_VOLUME,
    pitch: str = DEFAULT_TTS_PITCH,
) -> Tuple[bool, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "edge_tts",
        "--voice",
        voice,
        f"--rate={rate}",
        f"--volume={volume}",
        f"--pitch={pitch}",
        "--text",
        text,
        "--write-media",
        str(output_path),
    ]
    try:
        result = run_subprocess_hidden(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=120,
            check=False,
        )
    except Exception as exc:
        output_path.unlink(missing_ok=True)
        return False, str(exc).strip() or type(exc).__name__

    if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
        return True, ""

    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    output_path.unlink(missing_ok=True)
    return False, stderr[:400] or stdout[:400] or f"edge_tts cli failed: {result.returncode}"


def synthesize_tts_azure_rest(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    settings: Optional[CloneSettings],
    *,
    allow_voice_fallback: bool = True,
) -> TTSAttemptResult:
    if not is_backup_tts_enabled(settings):
        return TTSAttemptResult(False, (voice or "").strip(), "azure backup tts disabled", "")
    if not has_backup_tts_config(settings):
        return TTSAttemptResult(
            False,
            (voice or "").strip(),
            "azure backup tts missing key/region configuration",
            "",
        )

    assert settings is not None
    endpoint = azure_tts_endpoint(settings.azure_tts_region)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    requested_voice = (voice or "").strip() or DEFAULT_TTS_VOICE
    voice_candidates = (
        azure_tts_voice_candidates(requested_voice, settings.azure_tts_voice)
        if allow_voice_fallback
        else [((settings.azure_tts_voice or "").strip() or requested_voice)]
    )
    output_format = azure_tts_output_format_for_suffix(output_path.suffix or ".mp3")
    last_error = "azure backup tts unknown error"
    request_headers = {
        "Ocp-Apim-Subscription-Key": (settings.azure_tts_key or "").strip(),
        "Content-Type": "application/ssml+xml",
        "X-Microsoft-OutputFormat": output_format,
        "User-Agent": "drama-clone",
    }

    for candidate_voice in voice_candidates:
        normalized_voice = (candidate_voice or "").strip()
        if not normalized_voice:
            continue
        ssml_payload = build_azure_tts_ssml(text, normalized_voice, rate)
        for retry in range(2):
            try:
                response = requests.post(
                    endpoint,
                    headers=request_headers,
                    data=ssml_payload.encode("utf-8"),
                    timeout=AZURE_TTS_REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                last_error = f"azure backup tts request error: {summarize_for_log(str(exc), limit=260)}"
                output_path.unlink(missing_ok=True)
                if retry < 1:
                    time.sleep(1.2 * (retry + 1))
                    continue
                break

            if response.status_code == 200 and response.content:
                output_path.write_bytes(response.content)
                if output_path.exists() and output_path.stat().st_size > 0:
                    return TTSAttemptResult(True, normalized_voice, "", TTS_PROVIDER_AZURE)
                output_path.unlink(missing_ok=True)
                last_error = "azure backup tts returned empty audio payload"
                break

            output_path.unlink(missing_ok=True)
            body_preview = summarize_for_log(response.text or "", limit=260) or "<empty>"
            error_kind = classify_azure_backup_tts_error(response.status_code, body_preview)
            if error_kind == "quota":
                last_error = (
                    "azure backup tts quota exhausted or throttled: "
                    f"status {response.status_code}, body {body_preview}"
                )
            elif error_kind == "auth":
                last_error = (
                    "azure backup tts auth failed: "
                    f"status {response.status_code}, body {body_preview}"
                )
            elif error_kind == "region":
                last_error = (
                    "azure backup tts region invalid: "
                    f"status {response.status_code}, body {body_preview}"
                )
            elif error_kind == "transient":
                last_error = (
                    "azure backup tts transient error: "
                    f"status {response.status_code}, body {body_preview}"
                )
            else:
                last_error = (
                    "azure backup tts request failed: "
                    f"status {response.status_code}, body {body_preview}"
                )
            if error_kind == "transient" and retry < 1:
                time.sleep(1.2 * (retry + 1))
                continue
            break

    return TTSAttemptResult(False, requested_voice, last_error, TTS_PROVIDER_AZURE)


def synthesize_tts_with_fallback(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    *,
    allow_voice_fallback: bool = True,
) -> TTSAttemptResult:
    if not TTS_AVAILABLE:
        return TTSAttemptResult(False, (voice or "").strip(), "edge_tts not available", TTS_PROVIDER_EDGE)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    voice_candidates = tts_voice_candidates(voice) if allow_voice_fallback else [(voice or "").strip()]
    attempts = tts_attempt_settings(voice_candidates)

    last_error = "unknown tts error"
    for candidate_voice, volume, pitch in attempts:
        for retry in range(3):
            try:
                asyncio.run(
                    _synthesize_tts_async(
                        text,
                        candidate_voice,
                        rate,
                        output_path,
                        volume=volume,
                        pitch=pitch,
                    )
                )
                if output_path.exists() and output_path.stat().st_size > 0:
                    return TTSAttemptResult(True, candidate_voice, "", TTS_PROVIDER_EDGE)
            except Exception as exc:
                last_error = str(exc).strip() or type(exc).__name__
            output_path.unlink(missing_ok=True)
            if retry < 2:
                time.sleep(0.8 * (retry + 1))

    return TTSAttemptResult(False, (voice or "").strip(), last_error, TTS_PROVIDER_EDGE)


def synthesize_tts_resilient(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    *,
    allow_voice_fallback: bool = False,
) -> TTSAttemptResult:
    edge_result = synthesize_tts_with_fallback(
        text,
        voice,
        rate,
        output_path,
        allow_voice_fallback=allow_voice_fallback,
    )
    if edge_result.success:
        return edge_result

    last_error = edge_result.error_text
    if is_transient_tts_error(edge_result.error_text):
        for delay in (1.6, 3.2):
            time.sleep(delay)
            edge_result = synthesize_tts_with_fallback(
                text,
                voice,
                rate,
                output_path,
                allow_voice_fallback=allow_voice_fallback,
            )
            if edge_result.success:
                return edge_result
            last_error = edge_result.error_text or last_error

    if TTS_AVAILABLE:
        cli_candidates = tts_voice_candidates(voice) if allow_voice_fallback else [(voice or "").strip()]
        for candidate_voice, volume, pitch in tts_attempt_settings(cli_candidates):
            if not candidate_voice:
                continue
            cli_success, cli_error = _synthesize_tts_cli(
                text,
                candidate_voice,
                rate,
                output_path,
                volume=volume,
                pitch=pitch,
            )
            if cli_success:
                return TTSAttemptResult(True, candidate_voice, "", TTS_PROVIDER_EDGE)
            last_error = cli_error or last_error
            if is_transient_tts_error(cli_error):
                time.sleep(2.0)
                cli_success, cli_error = _synthesize_tts_cli(
                    text,
                    candidate_voice,
                    rate,
                    output_path,
                    volume=volume,
                    pitch=pitch,
                )
                if cli_success:
                    return TTSAttemptResult(True, candidate_voice, "", TTS_PROVIDER_EDGE)
                last_error = cli_error or last_error

    return TTSAttemptResult(False, (voice or "").strip(), last_error or "unknown tts error", TTS_PROVIDER_EDGE)


def resolve_tts_voice(
    preferred_voice: str,
    sample_text: str,
    rate: str,
    probe_dir: Path,
    video_processor: VideoProcessor,
    settings: Optional[CloneSettings] = None,
) -> Tuple[str, str]:
    locked_voice = (preferred_voice or "").strip() or DEFAULT_TTS_VOICE
    if not has_any_tts_provider(settings):
        return locked_voice, "no available TTS provider (edge_tts unavailable and backup TTS not configured)"

    probe_text = normalize_subtitle_text(sample_text) or "这是一次短剧解说测试。"
    probe_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^A-Za-z0-9]+", "_", locked_voice).strip("_") or "voice"
    probe_path = probe_dir / f"voice_probe_{safe_name}.mp3"
    result = synthesize_tts_resilient_validated(
        probe_text,
        locked_voice,
        rate,
        probe_path,
        video_processor,
        settings=settings,
        allow_voice_fallback=False,
        validation_rounds=1,
    )
    probe_path.unlink(missing_ok=True)
    if result.success:
        return locked_voice, ""
    return locked_voice, result.error_text or "unknown tts error"


def synthesize_tts(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    volume: str = DEFAULT_TTS_VOLUME,
    pitch: str = DEFAULT_TTS_PITCH,
) -> bool:
    if not TTS_AVAILABLE:
        return False
    try:
        asyncio.run(_synthesize_tts_async(text, voice, rate, output_path, volume=volume, pitch=pitch))
        return output_path.exists() and output_path.stat().st_size > 0
    except Exception:
        output_path.unlink(missing_ok=True)
        return False


def tts_audio_file_ready(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 0
    except OSError:
        return False


def fit_audio_clip(
    source_path: Path,
    target_duration: float,
    output_path: Path,
    video_processor: VideoProcessor,
    volume_gain: float = 1.0,
    max_speed_factor: float = MAX_TTS_SPEED_FACTOR,
    trim_silence: bool = True,
) -> float:
    source_duration = video_processor.probe_duration(source_path)
    is_wav = output_path.suffix.lower() == ".wav"
    codec_args = ["-c:a", "pcm_s16le"] if is_wav else ["-c:a", "libmp3lame", "-b:a", "192k"]
    output_audio_args = ["-ar", "48000", "-ac", "2"] if is_wav else []
    adjusted_duration = source_duration
    speed: Optional[float] = None
    if source_duration > target_duration + 0.03:
        speed = clamp(
            source_duration / max(0.08, target_duration),
            1.0,
            max(1.0, max_speed_factor),
        )
        if speed > 1.01:
            adjusted_duration = source_duration / speed
    elif source_duration + 0.03 < target_duration:
        desired_speed = source_duration / max(0.08, target_duration)
        if MIN_AUDIO_STRETCH_SPEED <= desired_speed < 0.99:
            speed = desired_speed
            adjusted_duration = source_duration / desired_speed
    output_duration = min(max(0.05, target_duration), max(0.05, adjusted_duration))
    filters = build_tts_cleanup_filters(
        trim_silence=trim_silence,
        volume_gain=volume_gain,
        speed=speed,
        output_duration=output_duration,
    )
    result = run_subprocess_hidden(
        [
            str(video_processor.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-i",
            str(source_path),
            "-filter:a",
            ",".join(filters),
            *output_audio_args,
            *codec_args,
            str(output_path),
        ],
        capture_output=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore")[:400] or "audio fit failed")
    return video_processor.probe_duration(output_path)


def concat_audio_files(parts: Sequence[Path], output_path: Path, video_processor: VideoProcessor) -> None:
    if not parts:
        raise RuntimeError("audio concat received no parts")
    if output_path.suffix.lower() == ".wav":
        expected_signature: Optional[Tuple[str, str, str]] = None
        for part in parts:
            audio_info = video_processor.probe_audio(part)
            current_signature = (
                audio_info.get("codec_name", ""),
                audio_info.get("sample_rate", ""),
                audio_info.get("channels", ""),
            )
            if expected_signature is None:
                expected_signature = current_signature
                continue
            if current_signature != expected_signature:
                raise RuntimeError(
                    "audio concat input mismatch: "
                    f"{part.name} has {current_signature}, expected {expected_signature}"
                )
    list_path = output_path.with_suffix(".concat.txt")
    codec_args = ["-c:a", "pcm_s16le"] if output_path.suffix.lower() == ".wav" else ["-c:a", "libmp3lame", "-b:a", "192k"]
    list_path.write_text(
        "\n".join(f"file '{part.resolve().as_posix()}'" for part in parts) + "\n",
        encoding="utf-8",
    )
    result = run_subprocess_hidden(
        [
            str(video_processor.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_path),
            *codec_args,
            str(output_path),
        ],
        capture_output=True,
        timeout=180,
        check=False,
    )
    list_path.unlink(missing_ok=True)
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore")[:400] or "audio concat failed")


def minimum_valid_tts_duration(text: str, rate: str) -> float:
    normalized = normalize_subtitle_text(text)
    units = max(1, subtitle_speech_units(normalized))
    minimum_duration = TTS_MIN_VALID_DURATION_SECONDS
    if units >= TTS_MIN_VALID_UNITS_FOR_DURATION_CHECK:
        minimum_duration = max(
            minimum_duration,
            estimate_tts_render_duration(normalized, rate) * TTS_MIN_VALID_DURATION_RATIO,
        )
    return minimum_duration


def validate_tts_audio_output(
    text: str,
    rate: str,
    output_path: Path,
    video_processor: VideoProcessor,
) -> Tuple[bool, str]:
    if not output_path.exists() or output_path.stat().st_size <= 0:
        return False, "tts output missing or empty"

    duration = max(0.0, video_processor.probe_duration(output_path))
    minimum_duration = minimum_valid_tts_duration(text, rate)
    if duration + 0.02 < minimum_duration:
        return False, f"tts output too short ({duration:.2f}s < {minimum_duration:.2f}s)"

    loudness = video_processor.probe_audio_volume(output_path)
    max_volume = loudness.get("max_volume")
    mean_volume = loudness.get("mean_volume")
    if (
        max_volume is not None
        and max_volume <= TTS_MIN_VALID_MAX_VOLUME_DB
        and (mean_volume is None or mean_volume <= TTS_MIN_VALID_MEAN_VOLUME_DB)
    ):
        if mean_volume is None:
            return False, f"tts output near silent (max {max_volume:.1f}dB)"
        return False, f"tts output near silent (mean {mean_volume:.1f}dB / max {max_volume:.1f}dB)"

    return True, ""


def tts_cache_root() -> Path:
    cache_dir = TTS_CACHE_DIR / TTS_CACHE_VERSION
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def tts_cache_request_key(
    text: str,
    voice: str,
    rate: str,
    *,
    volume: str = DEFAULT_TTS_VOLUME,
    pitch: str = DEFAULT_TTS_PITCH,
    suffix: str = ".mp3",
) -> str:
    normalized_text = normalize_subtitle_text(text)
    payload = json.dumps(
        {
            "text": normalized_text,
            "voice": (voice or "").strip(),
            "rate": (rate or "").strip(),
            "volume": (volume or "").strip(),
            "pitch": (pitch or "").strip(),
            "suffix": (suffix or ".mp3").lower(),
            "version": TTS_CACHE_VERSION,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def tts_cache_paths(
    text: str,
    voice: str,
    rate: str,
    *,
    volume: str = DEFAULT_TTS_VOLUME,
    pitch: str = DEFAULT_TTS_PITCH,
    suffix: str = ".mp3",
) -> Tuple[Path, Path]:
    normalized_suffix = (suffix or ".mp3").lower()
    if not normalized_suffix.startswith("."):
        normalized_suffix = f".{normalized_suffix}"
    cache_key = tts_cache_request_key(
        text,
        voice,
        rate,
        volume=volume,
        pitch=pitch,
        suffix=normalized_suffix,
    )
    cache_dir = tts_cache_root()
    return cache_dir / f"{cache_key}{normalized_suffix}", cache_dir / f"{cache_key}.json"


def load_tts_cache_metadata(metadata_path: Path) -> Dict[str, object]:
    if not metadata_path.exists():
        return {}
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def try_restore_tts_cache(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    video_processor: VideoProcessor,
    *,
    volume: str = DEFAULT_TTS_VOLUME,
    pitch: str = DEFAULT_TTS_PITCH,
) -> TTSAttemptResult:
    cache_audio_path, metadata_path = tts_cache_paths(
        text,
        voice,
        rate,
        volume=volume,
        pitch=pitch,
        suffix=output_path.suffix or ".mp3",
    )
    if not cache_audio_path.exists() or cache_audio_path.stat().st_size <= 0:
        return TTSAttemptResult(False, "", "", "")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cache_audio_path, output_path)
    is_valid, validation_error = validate_tts_audio_output(text, rate, output_path, video_processor)
    if not is_valid:
        output_path.unlink(missing_ok=True)
        cache_audio_path.unlink(missing_ok=True)
        metadata_path.unlink(missing_ok=True)
        return TTSAttemptResult(False, "", validation_error, "")

    metadata = load_tts_cache_metadata(metadata_path)
    used_voice = str(metadata.get("used_voice", "") or "").strip() or voice
    return TTSAttemptResult(True, used_voice, "", TTS_PROVIDER_CACHE)


def persist_tts_cache_entry(
    text: str,
    voice: str,
    used_voice: str,
    rate: str,
    output_path: Path,
    *,
    volume: str = DEFAULT_TTS_VOLUME,
    pitch: str = DEFAULT_TTS_PITCH,
    provider: str = TTS_PROVIDER_EDGE,
) -> None:
    if not output_path.exists() or output_path.stat().st_size <= 0:
        return
    cache_audio_path, metadata_path = tts_cache_paths(
        text,
        voice,
        rate,
        volume=volume,
        pitch=pitch,
        suffix=output_path.suffix or ".mp3",
    )
    cache_audio_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(output_path, cache_audio_path)
    metadata = {
        "voice_requested": (voice or "").strip(),
        "used_voice": (used_voice or "").strip() or (voice or "").strip(),
        "rate": (rate or "").strip(),
        "volume": (volume or "").strip(),
        "pitch": (pitch or "").strip(),
        "suffix": output_path.suffix or ".mp3",
        "text_units": subtitle_speech_units(text),
        "provider": (provider or "").strip() or TTS_PROVIDER_EDGE,
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")


def synthesize_tts_resilient_validated(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    video_processor: VideoProcessor,
    *,
    settings: Optional[CloneSettings] = None,
    allow_voice_fallback: bool = False,
    validation_rounds: int = 2,
) -> TTSAttemptResult:
    last_error = "unknown tts error"
    used_voice = (voice or "").strip() or DEFAULT_TTS_VOICE
    used_provider = ""
    cache_result = try_restore_tts_cache(text, voice, rate, output_path, video_processor)
    if cache_result.success:
        return TTSAttemptResult(True, cache_result.used_voice or used_voice, "", cache_result.provider)
    if cache_result.error_text:
        last_error = cache_result.error_text
    rounds = max(1, validation_rounds)
    for attempt_index in range(rounds):
        attempt_result = synthesize_tts_resilient(
            text,
            voice,
            rate,
            output_path,
            allow_voice_fallback=allow_voice_fallback,
        )
        if attempt_result.used_voice:
            used_voice = attempt_result.used_voice
        if attempt_result.provider:
            used_provider = merge_tts_provider(used_provider, attempt_result.provider)
        if not attempt_result.success:
            last_error = attempt_result.error_text or last_error
            if has_backup_tts_config(settings):
                backup_result = synthesize_tts_azure_rest(
                    text,
                    used_voice or voice,
                    rate,
                    output_path,
                    settings,
                    allow_voice_fallback=allow_voice_fallback,
                )
                if backup_result.used_voice:
                    used_voice = backup_result.used_voice
                if backup_result.provider:
                    used_provider = merge_tts_provider(used_provider, backup_result.provider)
                if backup_result.success:
                    attempt_result = backup_result
                else:
                    last_error = backup_result.error_text or last_error
            elif is_backup_tts_enabled(settings):
                last_error = "azure backup tts missing key/region configuration"
        if not attempt_result.success:
            if attempt_index + 1 < rounds and is_transient_tts_error(last_error):
                time.sleep(0.8 * (attempt_index + 1))
                continue
            break

        is_valid, validation_error = validate_tts_audio_output(text, rate, output_path, video_processor)
        if is_valid:
            persist_tts_cache_entry(
                text,
                voice,
                used_voice,
                rate,
                output_path,
                provider=attempt_result.provider or used_provider or TTS_PROVIDER_EDGE,
            )
            return TTSAttemptResult(True, used_voice, "", attempt_result.provider or used_provider or TTS_PROVIDER_EDGE)

        last_error = validation_error or last_error
        output_path.unlink(missing_ok=True)
        if has_backup_tts_config(settings) and attempt_result.provider != TTS_PROVIDER_AZURE:
            backup_result = synthesize_tts_azure_rest(
                text,
                used_voice or voice,
                rate,
                output_path,
                settings,
                allow_voice_fallback=allow_voice_fallback,
            )
            if backup_result.used_voice:
                used_voice = backup_result.used_voice
            if backup_result.provider:
                used_provider = merge_tts_provider(used_provider, backup_result.provider)
            if backup_result.success:
                backup_valid, backup_validation_error = validate_tts_audio_output(text, rate, output_path, video_processor)
                if backup_valid:
                    persist_tts_cache_entry(
                        text,
                        voice,
                        used_voice,
                        rate,
                        output_path,
                        provider=backup_result.provider or used_provider or TTS_PROVIDER_AZURE,
                    )
                    return TTSAttemptResult(
                        True,
                        used_voice,
                        "",
                        backup_result.provider or used_provider or TTS_PROVIDER_AZURE,
                    )
                last_error = backup_validation_error or last_error
                output_path.unlink(missing_ok=True)
            else:
                last_error = backup_result.error_text or last_error
        if attempt_index + 1 < rounds:
            time.sleep(0.8 * (attempt_index + 1))

    return TTSAttemptResult(False, used_voice, last_error, used_provider)


def synthesize_tts_segment(
    text: str,
    voice: str,
    rate: str,
    output_path: Path,
    video_processor: VideoProcessor,
    *,
    settings: Optional[CloneSettings] = None,
    allow_voice_fallback: bool = False,
    split_depth: int = 0,
) -> TTSAttemptResult:
    attempt_result = synthesize_tts_resilient_validated(
        text,
        voice,
        rate,
        output_path,
        video_processor,
        settings=settings,
        allow_voice_fallback=allow_voice_fallback,
    )
    if attempt_result.success:
        return attempt_result

    last_error = attempt_result.error_text
    used_voice = attempt_result.used_voice or (voice or "").strip() or DEFAULT_TTS_VOICE
    used_provider = attempt_result.provider
    if not allow_voice_fallback:
        attempt_result = synthesize_tts_resilient_validated(
            text,
            voice,
            rate,
            output_path,
            video_processor,
            settings=settings,
            allow_voice_fallback=True,
            validation_rounds=1,
        )
        if attempt_result.success:
            return attempt_result
        if attempt_result.used_voice:
            used_voice = attempt_result.used_voice
        used_provider = merge_tts_provider(used_provider, attempt_result.provider)
        last_error = attempt_result.error_text or last_error

    if split_depth >= MAX_TTS_SEGMENT_SPLIT_DEPTH:
        return TTSAttemptResult(False, used_voice, last_error, used_provider)

    chunk_texts = split_tts_recovery_chunks(text, split_depth=split_depth)
    if len(chunk_texts) <= 1:
        return TTSAttemptResult(False, used_voice, last_error, used_provider)

    recovered_voice = used_voice
    recovered_provider = used_provider
    chunk_paths: List[Path] = []
    try:
        for idx, chunk_text in enumerate(chunk_texts, start=1):
            chunk_path = output_path.with_name(f"{output_path.stem}_part{idx:02d}{output_path.suffix}")
            chunk_result = synthesize_tts_segment(
                chunk_text,
                voice,
                rate,
                chunk_path,
                video_processor,
                settings=settings,
                allow_voice_fallback=True,
                split_depth=split_depth + 1,
            )
            if not chunk_result.success:
                return TTSAttemptResult(
                    False,
                    chunk_result.used_voice or recovered_voice,
                    chunk_result.error_text or last_error,
                    merge_tts_provider(recovered_provider, chunk_result.provider),
                )
            if chunk_result.used_voice:
                recovered_voice = chunk_result.used_voice
            recovered_provider = merge_tts_provider(recovered_provider, chunk_result.provider)
            chunk_paths.append(chunk_path)
        concat_audio_files(chunk_paths, output_path, video_processor)
        output_valid, output_error = validate_tts_audio_output(text, rate, output_path, video_processor)
        if not output_valid:
            output_path.unlink(missing_ok=True)
            return TTSAttemptResult(False, recovered_voice, output_error or last_error, recovered_provider)
        persist_tts_cache_entry(
            text,
            voice,
            recovered_voice,
            rate,
            output_path,
            provider=recovered_provider or TTS_PROVIDER_EDGE,
        )
        return TTSAttemptResult(True, recovered_voice, "", recovered_provider or TTS_PROVIDER_EDGE)
    finally:
        for chunk_path in chunk_paths:
            chunk_path.unlink(missing_ok=True)


def log_tts_backup_notice_once(
    result: TTSAttemptResult,
    notice_state: Dict[str, bool],
    log_func: Optional[Callable[[str], None]],
) -> None:
    if log_func is None:
        return
    if result.success and result.provider == TTS_PROVIDER_AZURE and not notice_state.get("used"):
        notice_state["used"] = True
        log_func("  Backup TTS activated: Azure Speech 已接管当前失败片段")

    error_text = result.error_text or ""
    if error_text and is_backup_tts_quota_error(error_text) and not notice_state.get("quota"):
        notice_state["quota"] = True
        log_func("  Backup TTS warning: Azure Speech 免费额度可能已耗尽或被限流，请检查 F0 配额 / Key / Region")
    elif error_text and is_backup_tts_missing_config_error(error_text) and not notice_state.get("missing"):
        notice_state["missing"] = True
        log_func("  Backup TTS warning: 已启用 Azure 备用通道，但缺少 Key 或 Region，备用 TTS 当前不会生效")
    elif error_text and is_backup_tts_auth_error(error_text) and not notice_state.get("auth"):
        notice_state["auth"] = True
        log_func("  Backup TTS warning: Azure 备用通道鉴权失败，请检查 Key、Region 与资源状态")


def _legacy_build_tts_track_unused(
    entries: Sequence[SubtitleEntry],
    timeline_entries: Sequence[SubtitleEntry],
    total_duration: float,
    output_dir: Path,
    voice: str,
    rate: str,
    reference_gap: float,
    video_processor: VideoProcessor,
    ai_generator: Optional[AINarrationGenerator] = None,
    settings: Optional[CloneSettings] = None,
    log_func: Optional[Callable[[str], None]] = None,
) -> Tuple[Optional[Path], List[Tuple[float, float]], List[SubtitleEntry]]:
    if not entries:
        return None, [], list(entries)
    if not has_any_tts_provider(settings):
        raise RuntimeError("No available TTS provider: edge_tts unavailable and Azure backup TTS is not configured.")

    audio_dir = output_dir / "audio"
    raw_dir = audio_dir / "raw"
    aligned_dir = audio_dir / "aligned"
    raw_dir.mkdir(parents=True, exist_ok=True)
    aligned_dir.mkdir(parents=True, exist_ok=True)

    parts: List[Path] = []
    duck_intervals: List[Tuple[float, float]] = []
    cursor = 0.0
    min_gap_threshold = 0.03
    duck_release = 0.04
    tts_attempts = 0
    tts_failures = 0
    last_tts_error = ""
    backup_notice_state: Dict[str, bool] = {}
    rendered_entries: List[SubtitleEntry] = []
    target_gap = normalize_reference_gap(reference_gap)
    speech_entries: List[SubtitleEntry] = []
    for entry in entries:
        start = max(0.0, float(entry.start))
        end = max(start + 0.01, float(entry.end))
        entry_text = normalize_subtitle_text(entry.text)
        if end <= start + 0.01 or not entry_text:
            continue
        speech_entries.append(
            SubtitleEntry(
                index=entry.index,
                start=start,
                end=end,
                text=entry_text,
                entry_type=entry.entry_type,
            )
        )
    if not speech_entries:
        return None, [], list(entries)
    sample_entry = next(
        (
            normalize_subtitle_text(entry.text)
            for entry in speech_entries
            if normalize_subtitle_text(entry.text)
        ),
        "",
    )
    locked_voice, lock_error = resolve_tts_voice(voice, sample_entry, rate, raw_dir, video_processor, settings=settings)
    if log_func:
        log_func(f"  TTS voice locked: {locked_voice}")
        if lock_error:
            log_func(f"  TTS voice probe warning: {lock_error[:160]}")
        log_func(f"  TTS gap target: {target_gap:.2f}s")
    if lock_error:
        raise RuntimeError(f"所选 TTS 音色 {locked_voice} 当前不可用：{lock_error}")

    tts_join_map = ai_generator.plan_tts_sentence_links(speech_entries) if ai_generator else {}
    if log_func and tts_join_map:
        join_count = sum(1 for value in tts_join_map.values() if value)
        log_func(f"  TTS sentence review: {join_count} cross-line joins confirmed by AI")

    tts_groups = group_narration_entries_for_tts(speech_entries, target_gap, tts_join_map)
    for idx, group_entries in enumerate(tts_groups):
        order = idx + 1
        first_entry = group_entries[0]
        last_entry = group_entries[-1]
        group_start = first_entry.start
        group_text = join_narration_group_text(group_entries)
        if not group_text:
            continue

        if group_start > cursor + min_gap_threshold:
            silence_path = aligned_dir / f"gap_{order:03d}.wav"
            generate_silence(group_start - cursor, silence_path, video_processor)
            parts.append(silence_path)
            cursor = group_start

        raw_path = raw_dir / f"{order:03d}.mp3"
        aligned_path = aligned_dir / f"{order:03d}.wav"
        timeline_idx = timeline_positions.get(last_entry.index, -1)
        next_block_start: Optional[float] = None
        if 0 <= timeline_idx < len(timeline_entries) - 1:
            next_block_start = timeline_entries[timeline_idx + 1].start
        target_end = planned_tts_window_end(last_entry, next_block_start, total_duration, target_gap)
        target_duration = max(0.05, target_end - group_start)
        entry_rate = rate
        tts_attempts += 1
        result = synthesize_tts_segment(
            group_text,
            locked_voice,
            entry_rate,
            raw_path,
            video_processor,
            settings=settings,
        )
        log_tts_backup_notice_once(result, backup_notice_state, log_func)
        if result.success:
            fit_speed_factor = choose_local_tts_fit_speed_factor(
                max(0.05, video_processor.probe_duration(raw_path)),
                target_duration,
            )
            actual_duration = fit_audio_clip(
                raw_path,
                target_duration=target_duration,
                output_path=aligned_path,
                video_processor=video_processor,
                volume_gain=1.42,
                max_speed_factor=fit_speed_factor,
            )
        else:
            tts_failures += 1
            last_tts_error = result.error_text or last_tts_error
            generate_silence(target_duration, aligned_path, video_processor)
            actual_duration = target_duration
            if log_func:
                label = (
                    f"{first_entry.index}"
                    if len(group_entries) == 1
                    else f"{first_entry.index}-{last_entry.index}"
                )
                log_func(f"  TTS #{order} [{label}] failed: {result.error_text[:160]}")

        parts.append(aligned_path)
        voice_end = min(target_end, group_start + max(0.05, actual_duration))
        duck_intervals.append((group_start, min(target_end, voice_end + duck_release)))
        group_rendered_entries = distribute_group_rendered_entries(group_entries, group_start, voice_end)
        rendered_entries.extend(group_rendered_entries)
        cursor = voice_end
        if log_func:
            label = (
                f"{first_entry.index}"
                if len(group_entries) == 1
                else f"{first_entry.index}-{last_entry.index}"
            )
            log_func(
                f"  TTS #{order} [{label}]: {actual_duration:.2f}s / 目标 {target_duration:.2f}s / gap {target_gap:.2f}s"
            )

    if total_duration > cursor + min_gap_threshold:
        tail_path = aligned_dir / "tail.wav"
        generate_silence(total_duration - cursor, tail_path, video_processor)
        parts.append(tail_path)

    if not parts:
        return None, [], rendered_entries
    if tts_attempts > 0 and tts_failures >= tts_attempts:
        detail = summarize_for_log(last_tts_error, limit=220) or "unknown tts error"
        raise RuntimeError(f"TTS generation failed for all segments; last error: {detail}")
    if log_func and tts_failures > 0:
        log_func(f"  TTS fallback summary: {tts_failures}/{tts_attempts} segments used silence")

    output_path = output_dir / "output.wav"
    concat_audio_files(parts, output_path, video_processor)
    return output_path, duck_intervals, rendered_entries


def build_tts_track(
    entries: Sequence[SubtitleEntry],
    timeline_entries: Sequence[SubtitleEntry],
    total_duration: float,
    output_dir: Path,
    voice: str,
    rate: str,
    reference_gap: float,
    video_processor: VideoProcessor,
    ai_generator: Optional[AINarrationGenerator] = None,
    settings: Optional[CloneSettings] = None,
    log_func: Optional[Callable[[str], None]] = None,
) -> Tuple[Optional[Path], List[Tuple[float, float]], List[SubtitleEntry]]:
    if not entries:
        return None, [], list(entries)
    if not has_any_tts_provider(settings):
        raise RuntimeError("No available TTS provider: edge_tts unavailable and Azure backup TTS is not configured.")

    audio_dir = output_dir / "audio"
    raw_dir = audio_dir / "raw"
    aligned_dir = audio_dir / "aligned"
    raw_dir.mkdir(parents=True, exist_ok=True)
    aligned_dir.mkdir(parents=True, exist_ok=True)

    parts: List[Path] = []
    duck_intervals: List[Tuple[float, float]] = []
    cursor = 0.0
    min_gap_threshold = 0.03
    duck_release = 0.04
    tts_attempts = 0
    tts_failures = 0
    last_tts_error = ""
    backup_notice_state: Dict[str, bool] = {}
    rendered_entries: List[SubtitleEntry] = []
    target_gap = normalize_reference_gap(reference_gap)
    speech_entries: List[SubtitleEntry] = []
    for entry in entries:
        start = max(0.0, float(entry.start))
        end = max(start + 0.01, float(entry.end))
        entry_text = normalize_subtitle_text(entry.text)
        if end <= start + 0.01 or not entry_text:
            continue
        speech_entries.append(
            SubtitleEntry(
                index=entry.index,
                start=start,
                end=end,
                text=entry_text,
                entry_type=entry.entry_type,
            )
        )
    if not speech_entries:
        return None, [], list(entries)
    strict_timeline_duration = max(
        total_duration,
        max((entry.end for entry in timeline_entries), default=0.0),
        max((entry.end for entry in speech_entries), default=0.0),
    )

    sample_entry = next(
        (
            normalize_subtitle_text(entry.text)
            for entry in speech_entries
            if normalize_subtitle_text(entry.text)
        ),
        "",
    )
    locked_voice, lock_error = resolve_tts_voice(voice, sample_entry, rate, raw_dir, video_processor, settings=settings)
    if log_func:
        log_func(f"  TTS voice locked: {locked_voice}")
        if lock_error:
            log_func(f"  TTS voice probe warning: {lock_error[:160]}")
        log_func(f"  TTS gap target: {target_gap:.2f}s")
    if lock_error:
        raise RuntimeError(f"Selected TTS voice {locked_voice} is currently unavailable: {lock_error}")

    tts_join_map = plan_tts_sentence_links_locally(speech_entries, target_gap)
    if log_func:
        join_count = sum(1 for value in tts_join_map.values() if value)
        log_func(f"  TTS pause planner: {join_count} full-text joins selected by local sentence rules")

    tts_groups = group_narration_entries_for_tts(speech_entries, target_gap, tts_join_map)
    prepared_groups = build_prepared_tts_groups(tts_groups, raw_dir, strict_timeline_duration)

    def render_prepared_groups(render_rate: str) -> Tuple[int, int]:
        nonlocal last_tts_error
        attempts = 0
        failures = 0
        for group_state in prepared_groups:
            order = int(group_state["order"])
            group_text = str(group_state["text"])
            raw_path = Path(str(group_state["raw_path"]))
            attempts += 1
            result = synthesize_tts_segment(
                group_text,
                locked_voice,
                render_rate,
                raw_path,
                video_processor,
                settings=settings,
            )
            log_tts_backup_notice_once(result, backup_notice_state, log_func)
            if result.success and tts_audio_file_ready(raw_path):
                update_group_tts_source_metrics(
                    group_state,
                    render_rate,
                    video_processor,
                    log_func=log_func,
                )
                raw_duration = max(0.05, float(group_state.get("raw_duration", 0.05) or 0.05))
                if log_func and result.used_voice and result.used_voice != locked_voice:
                    log_func(
                        f"  TTS #{order} [{group_state['label']}] recovered with fallback voice {result.used_voice}"
                    )
            else:
                raw_duration = estimate_tts_render_duration(group_text, render_rate)
                failures += 1
                last_tts_error = result.error_text or last_tts_error
                if log_func:
                    log_func(f"  TTS #{order} [{group_state['label']}] failed: {result.error_text[:160]}")
            group_state["raw_duration"] = raw_duration
            if not result.success:
                group_state["effective_duration"] = raw_duration
                group_state["effective_path"] = str(raw_path)
                group_state["trim_silence_on_fit"] = True
            group_state["success"] = result.success and tts_audio_file_ready(raw_path)
            group_state["used_voice"] = result.used_voice
            group_state["used_provider"] = result.provider
            group_state["applied_rate"] = render_rate
        return attempts, failures

    applied_rate = rate
    rate_passes = 0
    refinement_passes = 0
    seen_group_layouts = {prepared_tts_group_signature(prepared_groups)}
    while True:
        tts_attempts, tts_failures = render_prepared_groups(applied_rate)
        split_candidate_groups, split_count = split_rendered_tts_groups_for_timing(
            prepared_groups,
            raw_dir,
            strict_timeline_duration,
            applied_rate,
        )
        if split_count > 0:
            split_signature = prepared_tts_group_signature(split_candidate_groups)
            if split_signature in seen_group_layouts:
                if log_func:
                    log_func("  TTS timing guard warning: split refinement repeated an earlier sentence layout; keeping the current layout to avoid an infinite loop")
            elif refinement_passes + 1 > MAX_TTS_GROUP_REFINEMENT_PASSES:
                if log_func:
                    log_func(
                        f"  TTS timing guard warning: sentence layout refinement exceeded {MAX_TTS_GROUP_REFINEMENT_PASSES} passes; keeping the current layout to avoid an infinite loop"
                    )
            else:
                prepared_groups = split_candidate_groups
                seen_group_layouts.add(split_signature)
                refinement_passes += 1
                if log_func:
                    log_func(
                        f"  TTS timing guard: split {split_count} sentence group(s) that could not fit within the {MAX_TTS_TIMELINE_OVERFLOW_SECONDS:.2f}s drift cap"
                    )
                continue
        merge_candidate_groups, merge_count = merge_underfilled_tts_groups_for_timing(
            prepared_groups,
            raw_dir,
            strict_timeline_duration,
            applied_rate,
        )
        if merge_count > 0:
            merge_signature = prepared_tts_group_signature(merge_candidate_groups)
            if merge_signature in seen_group_layouts:
                if log_func:
                    log_func("  TTS timing guard warning: merge refinement repeated an earlier sentence layout; keeping the current layout to avoid an infinite loop")
            elif refinement_passes + 1 > MAX_TTS_GROUP_REFINEMENT_PASSES:
                if log_func:
                    log_func(
                        f"  TTS timing guard warning: sentence layout refinement exceeded {MAX_TTS_GROUP_REFINEMENT_PASSES} passes; keeping the current layout to avoid an infinite loop"
                    )
            else:
                prepared_groups = merge_candidate_groups
                seen_group_layouts.add(merge_signature)
                refinement_passes += 1
                if log_func:
                    log_func(
                        f"  TTS timing guard: merged {merge_count} underfilled sentence group pair(s) to reduce hard trims and silent gaps"
                    )
                continue
        if not prepared_groups:
            break
        total_raw_duration = sum(
            max(0.05, tts_group_schedulable_duration(group)) for group in prepared_groups
        )
        total_reference_duration = sum(
            max(0.05, float(group.get("target_duration", 0.05) or 0.05)) for group in prepared_groups
        )
        if log_func:
            log_func(
                f"  TTS reference windows: core {total_raw_duration:.2f}s -> subtitle {total_reference_duration:.2f}s @ {applied_rate}"
            )
        suggested_rate = derive_uniform_tts_rate(prepared_groups, applied_rate)
        if abs(tts_rate_factor(suggested_rate) - tts_rate_factor(applied_rate)) < 0.03:
            break
        if rate_passes + 1 >= MAX_TTS_RESYNTH_PASSES:
            break
        if log_func:
            log_func(f"  TTS uniform rate recalibration: {applied_rate} -> {suggested_rate}")
        applied_rate = suggested_rate
        rate_passes += 1
    if log_func:
        log_func(f"  TTS uniform synth rate locked: {applied_rate}")

    schedule_stats = schedule_prepared_tts_groups(prepared_groups, strict_timeline_duration)
    if log_func and prepared_groups:
        log_func(
            f"  TTS sentence scheduling: drift cap {MAX_TTS_TIMELINE_OVERFLOW_SECONDS:.2f}s / "
            f"planned start drift <= {schedule_stats['max_start_drift']:.2f}s / "
            f"planned end drift <= {schedule_stats['max_end_drift']:.2f}s"
        )
        if schedule_stats["hard_trim_count"] > 0:
            log_func(
                f"  TTS timing guard: {int(schedule_stats['hard_trim_count'])} group(s) still require tail trim after applying the drift cap"
            )

    for group_state in prepared_groups:
        order = int(group_state["order"])
        strict_start = max(0.0, float(group_state.get("strict_start", 0.0) or 0.0))
        strict_end = max(strict_start + 0.05, float(group_state.get("strict_end", strict_start + 0.05) or (strict_start + 0.05)))
        scheduled_start = max(0.0, float(group_state.get("scheduled_start", strict_start) or strict_start))
        scheduled_end = max(
            scheduled_start + 0.05,
            float(group_state.get("scheduled_end", group_state.get("window_end", scheduled_start + 0.05)) or (scheduled_start + 0.05)),
        )
        aligned_path = aligned_dir / f"{order:03d}.wav"

        actual_start = max(scheduled_start, cursor)
        if actual_start > cursor + min_gap_threshold:
            silence_path = aligned_dir / f"gap_{order:03d}.wav"
            generate_silence(actual_start - cursor, silence_path, video_processor)
            parts.append(silence_path)
            cursor = actual_start

        available_duration = max(0.05, scheduled_end - actual_start)

        if bool(group_state.get("success")):
            raw_path = Path(str(group_state["raw_path"]))
            if not tts_audio_file_ready(raw_path):
                restore_rate = str(group_state.get("applied_rate", applied_rate) or applied_rate)
                restore_result = synthesize_tts_segment(
                    str(group_state.get("text", "") or ""),
                    locked_voice,
                    restore_rate,
                    raw_path,
                    video_processor,
                    settings=settings,
                )
                log_tts_backup_notice_once(restore_result, backup_notice_state, log_func)
                if restore_result.success and tts_audio_file_ready(raw_path):
                    group_state["success"] = True
                    update_group_tts_source_metrics(
                        group_state,
                        restore_rate,
                        video_processor,
                        log_func=log_func,
                    )
                    if restore_result.used_voice:
                        group_state["used_voice"] = restore_result.used_voice
                    if restore_result.provider:
                        group_state["used_provider"] = restore_result.provider
                    if log_func:
                        log_func(
                            f"  TTS #{order} [{group_state['label']}] raw clip was missing before final fit and has been regenerated"
                        )
                else:
                    group_state["success"] = False
                    last_tts_error = restore_result.error_text or "raw clip missing before final fit"
                    if log_func:
                        detail = summarize_for_log(last_tts_error, limit=180) or "unknown tts error"
                        log_func(
                            f"  TTS #{order} [{group_state['label']}] raw clip was missing before final fit and could not be regenerated: {detail}"
                        )

        if bool(group_state.get("success")):
            raw_duration = max(0.05, tts_group_schedulable_duration(group_state))
            target_duration = max(
                0.05,
                min(
                    available_duration,
                    float(group_state.get("target_duration", raw_duration) or raw_duration),
                    raw_duration,
                ),
            )
            if (
                raw_duration > target_duration + 0.03
                and raw_duration / max(MAX_TTS_SPEED_FACTOR, 1.0) > target_duration + 0.03
                and log_func
            ):
                log_func(
                    f"  TTS #{order} [{group_state['label']}] still exceeds its drift-capped sentence window at max speed; tail will be trimmed to stay within {MAX_TTS_TIMELINE_OVERFLOW_SECONDS:.2f}s subtitle drift"
            )
            fit_speed_factor = choose_local_tts_fit_speed_factor(raw_duration, target_duration)
            group_state["fit_speed_factor"] = fit_speed_factor
            fit_source_path = Path(str(group_state.get("effective_path", group_state["raw_path"]) or group_state["raw_path"]))
            if not tts_audio_file_ready(fit_source_path):
                fit_source_path = Path(str(group_state["raw_path"]))
            trim_silence = bool(group_state.get("trim_silence_on_fit", True))
            if fit_source_path == Path(str(group_state["raw_path"])):
                trim_silence = True
            actual_duration = fit_audio_clip(
                fit_source_path,
                target_duration=target_duration,
                output_path=aligned_path,
                video_processor=video_processor,
                volume_gain=1.42,
                max_speed_factor=fit_speed_factor,
                trim_silence=trim_silence,
            )
        else:
            target_duration = max(0.05, available_duration)
            generate_silence(target_duration, aligned_path, video_processor)
            actual_duration = target_duration

        parts.append(aligned_path)
        voice_end = min(scheduled_end, actual_start + max(0.05, actual_duration))
        duck_intervals.append((actual_start, min(strict_timeline_duration, voice_end + duck_release)))
        rendered_entries.extend(distribute_group_rendered_entries(list(group_state["entries"]), actual_start, voice_end))
        cursor = voice_end
        if log_func:
            fit_speed_factor = float(group_state.get("fit_speed_factor", 1.0) or 1.0)
            fit_note = (
                f" / local fit {fit_speed_factor:.02f}x"
                if fit_speed_factor > 1.01 and fit_speed_factor <= LOCAL_TTS_MICRO_SPEED_FACTOR + 0.005
                else ""
            )
            raw_probe_duration = max(0.05, float(group_state.get("raw_duration", actual_duration) or actual_duration))
            core_duration = max(0.05, tts_group_schedulable_duration(group_state))
            log_func(
                f"  TTS #{order} [{group_state['label']}]: raw {raw_probe_duration:.2f}s / core {core_duration:.2f}s -> final {actual_duration:.2f}s / "
                f"subtitle {strict_start:.2f}-{strict_end:.2f}s / speech {actual_start:.2f}-{voice_end:.2f}s / "
                f"rate {group_state.get('applied_rate', rate)}{fit_note}"
            )

    if strict_timeline_duration > cursor + min_gap_threshold:
        tail_path = aligned_dir / "tail.wav"
        generate_silence(strict_timeline_duration - cursor, tail_path, video_processor)
        parts.append(tail_path)

    if not parts:
        return None, [], rendered_entries
    if tts_attempts > 0 and tts_failures >= tts_attempts:
        detail = summarize_for_log(last_tts_error, limit=220) or "unknown tts error"
        raise RuntimeError(f"TTS generation failed for all segments; last error: {detail}")
    if log_func and tts_failures > 0:
        log_func(f"  TTS fallback summary: {tts_failures}/{tts_attempts} segments used silence")

    output_path = output_dir / "output.wav"
    concat_audio_files(parts, output_path, video_processor)
    return output_path, duck_intervals, rendered_entries


def mix_final_video(
    clean_video: Path,
    narration_audio: Path,
    duck_intervals: Sequence[Tuple[float, float]],
    output_path: Path,
    video_processor: VideoProcessor,
    duck_volume: float = DEFAULT_DUCK_VOLUME,
) -> None:
    video_duration = max(0.0, video_processor.probe_duration(clean_video))
    narration_duration = max(0.0, video_processor.probe_duration(narration_audio))
    background_has_audio = video_processor.has_audio_stream(clean_video)
    pad_duration = max(0.0, narration_duration - video_duration)
    video_filter = (
        f"[0:v]tpad=stop_mode=clone:stop_duration={pad_duration:.3f}[vout];"
        if pad_duration > 0.03
        else "[0:v]null[vout];"
    )
    if background_has_audio:
        active_expr = "+".join(
            f"between(t,{start:.3f},{end:.3f})" for start, end in duck_intervals if end > start
        ) or "0"
        background_volume = f"if(gt({active_expr},0),{duck_volume:.3f},1)"
        filter_complex = (
            f"{video_filter}"
            f"[0:a]aresample=48000,highpass=f=70,lowpass=f=12000,"
            f"acompressor=threshold=0.18:ratio=3.0:attack=15:release=180,"
            f"dynaudnorm=f=120:g=13:m=18,"
            f"volume='{background_volume}':eval=frame[bg];"
            f"[1:a]aresample=48000,highpass=f=90,lowpass=f=9000,"
            f"volume=1.06,"
            f"acompressor=threshold=0.10:ratio=3.5:attack=5:release=90,"
            f"dynaudnorm=f=70:g=11:m=14[vo];"
            f"[bg][vo]amix=inputs=2:weights='0.90 1.06':normalize=0:duration=longest,"
            f"volume=1.04,"
            f"acompressor=threshold=0.15:ratio=2.2:attack=10:release=160,"
            f"dynaudnorm=f=140:g=9:m=12,alimiter=limit=0.97[aout]"
        )
    else:
        filter_complex = (
            f"{video_filter}"
            f"[1:a]aresample=48000,highpass=f=90,lowpass=f=9000,"
            f"volume=1.06,"
            f"acompressor=threshold=0.10:ratio=3.5:attack=5:release=90,"
            f"dynaudnorm=f=70:g=11:m=14,"
            f"volume=1.04,"
            f"acompressor=threshold=0.15:ratio=2.2:attack=10:release=160,"
            f"dynaudnorm=f=140:g=9:m=12,alimiter=limit=0.97[aout]"
        )
    result = run_subprocess_hidden(
        [
            str(video_processor.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-i",
            str(clean_video),
            "-i",
            str(narration_audio),
            "-filter_complex",
            filter_complex,
            "-map",
            "[vout]",
            "-map",
            "[aout]",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            str(output_path),
        ],
        capture_output=True,
        timeout=600,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore")[:400] or "final mix failed")


def export_final_audio(video_path: Path, output_path: Path, video_processor: VideoProcessor) -> None:
    result = run_subprocess_hidden(
        [
            str(video_processor.ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-i",
            str(video_path),
            "-vn",
            "-map",
            "0:a:0",
            "-c:a",
            "libmp3lame",
            "-b:a",
            "192k",
            str(output_path),
        ],
        capture_output=True,
        timeout=300,
        check=False,
    )
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore")[:400] or "audio export failed")


def burn_subtitles_into_video(
    source_video: Path,
    subtitle_path: Path,
    output_path: Path,
    video_processor: VideoProcessor,
    subtitle_region: Optional[VideoMaskRegion] = None,
    log_func: Optional[Callable[[str], None]] = None,
) -> None:
    if not subtitle_path.exists() or subtitle_path.stat().st_size <= 0:
        shutil.copy2(source_video, output_path)
        return

    profile = video_processor.probe_video(source_video)
    video_width = int(profile["width"])
    video_height = int(profile["height"])
    subtitle_entries = build_delivery_subtitle_entries(
        parse_subtitle_content(load_text_file(subtitle_path), subtitle_path.suffix.lower())
    )
    if not subtitle_entries:
        shutil.copy2(source_video, output_path)
        return
    ass_content = entries_to_ass(subtitle_entries, video_width, video_height, subtitle_region)

    with tempfile.TemporaryDirectory(prefix="subtitle_burn_", dir=str(output_path.parent)) as temp_dir_text:
        temp_dir = Path(temp_dir_text)
        working_subtitle_path = temp_dir / "burn_subtitles.ass"
        working_subtitle_path.write_text(ass_content, encoding="utf-8-sig")
        filter_expr = "ass=burn_subtitles.ass"
        result = run_subprocess_hidden(
            [
                str(video_processor.ffmpeg),
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-y",
                "-i",
                str(source_video),
                "-vf",
                filter_expr,
                "-map",
                "0:v:0",
                "-map",
                "0:a?",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "20",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "copy",
                "-movflags",
                "+faststart",
                str(output_path),
            ],
            capture_output=True,
            timeout=1800,
            check=False,
            cwd=str(temp_dir),
        )
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore")[:600] or "subtitle burn failed")
    if log_func:
        region_text = (
            f"region {subtitle_region.x},{subtitle_region.y},{subtitle_region.width},{subtitle_region.height}"
            if subtitle_region is not None
            else "default bottom region"
        )
        log_func(f"  Delivery subtitles burned into video: {region_text}")


def _clamp_entries(entries: Sequence[SubtitleEntry], total_duration: float) -> List[SubtitleEntry]:
    clamped: List[SubtitleEntry] = []
    for entry in entries:
        start = clamp(entry.start, 0.0, total_duration)
        end = clamp(entry.end, 0.0, total_duration)
        if end <= start + 0.01:
            continue
        clamped.append(
            SubtitleEntry(
                index=entry.index,
                start=start,
                end=end,
                text=normalize_subtitle_text(entry.text),
                entry_type=entry.entry_type,
            )
        )
    return clamped


def run_clone_pipeline(
    settings: CloneSettings,
    ffmpeg: Path = DEFAULT_FFMPEG,
    ffprobe: Path = DEFAULT_FFPROBE,
    log_func: Optional[Callable[[str], None]] = None,
    progress_func: Optional[Callable[[float, str], None]] = None,
) -> CloneResult:
    if not PIL_AVAILABLE:
        raise RuntimeError("缺少 Pillow，无法分析视频帧。")

    def log(message: str) -> None:
        if log_func:
            log_func(message)

    def progress(value: float, text: str) -> None:
        if progress_func:
            progress_func(value, text)

    video_processor = VideoProcessor(ffmpeg, ffprobe)
    hasher = VisualHasher(16)
    ai_generator = AINarrationGenerator(
        settings.ai_api_key,
        settings.ai_model,
        settings.ai_api_url,
        fallback_models=settings.ai_fallback_models,
    )

    output_dir = settings.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = sanitize_stem(settings.output_stem)
    preferred_final_video_path = output_dir / f"{stem}.mp4"

    temp_context = tempfile.TemporaryDirectory(prefix="drama_clone_") if not settings.keep_temp else None
    temp_root = Path(temp_context.name) if temp_context else Path(tempfile.gettempdir()) / "drama_clone_keep_temp"
    temp_root.mkdir(parents=True, exist_ok=True)
    final_srt_path = temp_root / f"{stem}.srt"

    try:
        log("=== 开始处理 ===")
        progress(8, "分析字幕")

        raw_reference_text = entries_to_srt(settings.subtitle_entries)
        subtitle_bundle = build_processed_subtitles(
            settings.subtitle_entries,
            raw_reference_text,
            ai_generator,
            log_func=log,
        )
        log(
            "字幕分类: "
            f"解说 {subtitle_bundle.counts.get('narration', 0)} 条, "
            f"对白 {subtitle_bundle.counts.get('dialogue', 0)} 条, "
            f"原字幕 {subtitle_bundle.counts.get('original_subtitle', 0)} 条, "
            f"水印 {subtitle_bundle.counts.get('watermark', 0)} 条"
        )

        progress(25, "提取素材帧")
        videos = sorted(
            [
                path
                for path in settings.source_dir.iterdir()
                if path.is_file() and path.suffix.lower() in {".mp4", ".mov"}
            ],
            key=natural_path_key,
        )
        if not videos:
            raise RuntimeError("原素材目录中没有找到 mp4 或 mov。")
        frame_cache_dir = Path(__file__).parent / "frame_cache"
        source_frames = extract_source_frames(
            videos,
            frame_cache_dir,
            video_processor,
            hasher,
            frame_interval=settings.frame_interval,
            log_func=log,
        )
        source_frame_index = {
            (sample.video_path, round(sample.timestamp, 3)): sample
            for sample in source_frames
        }
        if not source_frames:
            raise RuntimeError("素材帧提取失败。")

        progress(45, "提取参考帧")
        reference_frames, reference_duration = extract_reference_frames(
            settings.reference_video,
            temp_root,
            video_processor,
            hasher,
            frame_interval=settings.frame_interval,
            log_func=log,
        )
        if not reference_frames:
            raise RuntimeError("参考帧提取失败。")

        progress(60, "匹配画面")
        strategies = [(0.72, 1), (0.70, 2), (0.67, 3), (0.64, 4), (0.60, 5)]
        best_matches: List[Dict[str, object]] = []
        best_rate = -1.0
        best_diag = {
            "backtracks": 0,
            "low_sim": 0,
            "missed": len(reference_frames),
            "video_switches": 0,
            "reanchors": 0,
            "bridges": 0,
            "refinements": 0,
        }
        best_stats = {"avg": 0.0, "median": 0.0, "p75": 0.0, "p90": 0.0, "p95": 0.0}
        best_audit: Dict[str, object] = {
            "avg": 0.0,
            "median": 0.0,
            "low": 0,
            "samples": 0,
            "low_ratio": 0.0,
            "suspect_times": [],
        }
        best_early = {"unique_videos": 0.0, "video_switches": 0.0, "hard_jumps": 0.0, "penalty": 0.0}
        best_quality = -999.0
        for threshold, attempt in strategies:
            effective_threshold = min(settings.match_threshold, threshold)
            log(f"  尝试匹配: 阈值 {effective_threshold:.2f}, 策略 {attempt}")
            matches, rate, diagnostics = match_frames(
                reference_frames,
                source_frames,
                hasher,
                settings.frame_interval,
                effective_threshold,
                attempt,
                log_func=log,
            )
            stats = summarize_match_similarity(matches)
            audit = audit_match_alignment(matches, reference_frames, source_frame_index)
            early = assess_early_match_stability(matches, settings.frame_interval)
            quality = (
                rate * 1.6
                + stats["avg"] * 0.70
                + stats["p90"] * 0.40
                + float(audit.get("avg", 0.0) or 0.0) * 0.90
                + float(audit.get("median", 0.0) or 0.0) * 0.50
                - diagnostics["low_sim"] * 0.001
                - diagnostics["missed"] * 0.03
                - int(audit.get("low", 0) or 0) * 0.004
                - float(early.get("penalty", 0.0) or 0.0)
            )
            log(
                "  匹配质量: "
                f"avg {stats['avg']:.3f} / median {stats['median']:.3f} / p90 {stats['p90']:.3f} / "
                f"audit {float(audit.get('avg', 0.0) or 0.0):.3f} / "
                f"early penalty {float(early.get('penalty', 0.0) or 0.0):.3f}"
            )
            if quality > best_quality:
                best_matches = matches
                best_rate = rate
                best_diag = diagnostics
                best_stats = stats
                best_audit = audit
                best_early = early
                best_quality = quality
            if rate >= 0.90 and diagnostics["low_sim"] <= max(2, len(reference_frames) // 25):
                break

        trimmed_matches = trim_unstable_tail_matches(
            best_matches,
            similarity_floor=max(0.66, min(settings.match_threshold, 0.72) - 0.03),
        )
        if len(trimmed_matches) != len(best_matches):
            removed = len(best_matches) - len(trimmed_matches)
            log(f"  尾段裁剪: 去除 {removed} 个低相似度尾帧，避免结尾漂移")
            best_matches = trimmed_matches
            best_stats = summarize_match_similarity(best_matches)
            best_audit = audit_match_alignment(best_matches, reference_frames, source_frame_index)

        log(
            "  最佳匹配汇总: "
            f"avg {best_stats['avg']:.3f} / median {best_stats['median']:.3f} / "
            f"p90 {best_stats['p90']:.3f} / low {best_diag['low_sim']} / "
            f"audit {float(best_audit.get('avg', 0.0) or 0.0):.3f} / "
            f"early penalty {float(best_early.get('penalty', 0.0) or 0.0):.3f}"
        )

        failure_reason = match_quality_failure_reason(best_matches, best_rate, best_diag, best_audit)
        if failure_reason:
            raise RuntimeError(f"{failure_reason} 请确认参考视频与素材是否属于同版画面。")

        if len(best_matches) < max(1, math.floor(len(reference_frames) * 0.70)):
            raise RuntimeError(
                f"匹配质量过低，仅匹配到 {len(best_matches)}/{len(reference_frames)} 帧。"
            )

        progress(72, "切片并拼接")
        profile = video_processor.probe_video(settings.reference_video)
        width = int(profile["width"])
        height = int(profile["height"])
        fps = fps_to_float(profile["fps"])

        jobs = merge_matches(best_matches)
        segment_dir = temp_root / "segments"
        segment_dir.mkdir(parents=True, exist_ok=True)
        concat_path = temp_root / "concat.txt"
        concat_lines: List[str] = []
        for idx, job in enumerate(jobs, start=1):
            segment_path = segment_dir / f"{idx:04d}.mp4"
            video_processor.cut_segment(
                source=Path(job.source_video),
                output=segment_path,
                start=job.start,
                duration=job.duration,
                width=width,
                height=height,
                fps=fps,
                hflip=False,
            )
            concat_lines.append(f"file '{segment_path.resolve().as_posix()}'")
        concat_path.write_text("\n".join(concat_lines) + "\n", encoding="utf-8")
        clean_video_path = temp_root / "clean_video.mp4"
        video_processor.concat_videos(concat_path, clean_video_path)
        clean_duration = video_processor.probe_duration(clean_video_path)
        log(f"清洁视频时长: {clean_duration:.2f}s")

        progress(84, "生成字幕和配音")
        progress(84, "Detect subtitle mask")
        delivery_video_path = clean_video_path
        subtitle_mask_region = detect_subtitle_mask_region(
            clean_video_path,
            temp_root / "subtitle_mask",
            video_processor,
            log_func=log,
        )
        if subtitle_mask_region is not None:
            subtitle_mask_region = tighten_subtitle_output_region(subtitle_mask_region, width, height)
            log(
                "  Subtitle mask output region refined: "
                f"{subtitle_mask_region.source} region "
                f"{subtitle_mask_region.x},{subtitle_mask_region.y},"
                f"{subtitle_mask_region.width},{subtitle_mask_region.height}"
            )
            masked_clean_video_path = temp_root / "clean_video_masked.mp4"
            apply_subtitle_mask_mosaic(
                delivery_video_path,
                masked_clean_video_path,
                subtitle_mask_region,
                video_processor,
                log_func=log,
            )
            delivery_video_path = masked_clean_video_path
            log(
                "  Subtitle mask applied: "
                f"{subtitle_mask_region.source} region "
                f"{subtitle_mask_region.x},{subtitle_mask_region.y},"
                f"{subtitle_mask_region.width},{subtitle_mask_region.height}"
            )
        else:
            log("  Subtitle mask skipped: no stable subtitle band detected")
        reference_timeline_entries = preserve_reference_timeline_entries(subtitle_bundle.all_entries)
        reference_timeline_end = max((entry.end for entry in reference_timeline_entries), default=0.0)
        narration_entries = [
            entry
            for entry in preserve_reference_timeline_entries(subtitle_bundle.narration_entries)
            if normalize_subtitle_text(entry.text) and entry.end > entry.start + 0.01
        ]
        if not narration_entries:
            log("未识别到可配音的解说词，本次跳过改写与 TTS。")
        reference_profile_entries = narration_entries or reference_timeline_entries
        reference_profile = analyze_reference_subtitle_profile(reference_profile_entries)
        requested_tts_rate = (settings.tts_rate or "").strip() or "+0%"
        reference_tts_rate = suggest_reference_tts_rate(reference_profile_entries)
        effective_tts_rate = min_rate_text(requested_tts_rate, reference_tts_rate)
        effective_tts_voice = choose_reference_tts_voice(settings.tts_voice, reference_profile_entries)
        target_gap = normalize_reference_gap(reference_profile["avg_gap"])
        log(
            f"Strict subtitle timeline: reference SRT end {reference_timeline_end:.2f}s / clean video {clean_duration:.2f}s"
        )
        log(
            f"Reference speech profile: median {reference_profile['median_cps']:.2f} cps / "
            f"p75 {reference_profile['p75_cps']:.2f} cps / avg gap {reference_profile['avg_gap']:.2f}s"
        )
        log(
            f"TTS profile applied: voice {effective_tts_voice} / requested rate {requested_tts_rate} / "
            f"reference rate {reference_tts_rate} / using {effective_tts_rate} / "
            f"target gap {target_gap:.2f}s"
        )
        if settings.enable_backup_tts:
            backup_voice = (settings.azure_tts_voice or "").strip() or effective_tts_voice
            if has_backup_tts_config(settings):
                log(
                    f"Backup TTS configured: Azure Speech / region {(settings.azure_tts_region or '').strip()} / voice {backup_voice}"
                )
            else:
                log("Backup TTS enabled but Azure key/region is missing; fallback path will stay inactive.")

        audio_path, duck_intervals, rendered_entries = build_tts_track(
            narration_entries,
            reference_timeline_entries,
            total_duration=max(clean_duration, reference_timeline_end),
            output_dir=temp_root,
            voice=effective_tts_voice,
            rate=effective_tts_rate,
            reference_gap=target_gap,
            video_processor=video_processor,
            ai_generator=ai_generator,
            settings=settings,
            log_func=log,
        )
        final_timeline_entries = preserve_reference_timeline_entries(reference_timeline_entries)
        output_entries = build_delivery_subtitle_entries(final_timeline_entries)
        write_srt(final_srt_path, output_entries)

        progress(94, "混合成片")
        staged_video_path = temp_root / "final_video_stage.mp4"
        if audio_path is not None:
            mix_final_video(
                delivery_video_path,
                audio_path,
                duck_intervals,
                staged_video_path,
                video_processor,
            )
        else:
            shutil.copy2(delivery_video_path, staged_video_path)

        finalized_video_path = staged_video_path
        if output_entries:
            burned_video_path = temp_root / "final_video_burned.mp4"
            burn_subtitles_into_video(
                finalized_video_path,
                final_srt_path,
                burned_video_path,
                video_processor,
                subtitle_region=subtitle_mask_region,
                log_func=log,
            )
            safe_unlink_file(finalized_video_path)
            finalized_video_path = burned_video_path
        else:
            log("  Delivery subtitles skipped: no visible subtitle entries to burn")
        safe_unlink_file(final_srt_path)

        final_video_path = move_output_file(
            finalized_video_path,
            preferred_final_video_path,
            log_func=log,
            artifact_label="video",
        )

        for obsolete_path in (
            output_dir / f"{stem}_clean.mp4",
            output_dir / f"{stem}.srt",
            output_dir / f"{stem}.mp3",
        ):
            safe_unlink_file(obsolete_path)

        diff_pct = abs(clean_duration - reference_duration) / max(reference_duration, 0.01) * 100
        log(
            f"时长对比: 参考 {reference_duration:.2f}s / 重建 {clean_duration:.2f}s / 差异 {diff_pct:.2f}%"
        )
        progress(100, "完成")

        return CloneResult(
            video_path=final_video_path,
            subtitle_path=None,
            audio_path=None,
            clean_video_path=final_video_path,
            reconstructed_duration=clean_duration,
            reference_duration=reference_duration,
            frame_matches=len(best_matches),
            reference_frames=len(reference_frames),
            confident_match_rate=best_rate,
            low_similarity_count=best_diag["low_sim"],
        )
    finally:
        if temp_context is not None:
            temp_context.cleanup()
