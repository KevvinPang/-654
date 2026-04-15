from __future__ import annotations

import argparse
from collections import Counter
import json
import multiprocessing
import os
import re
import shutil
import statistics
import sys
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import cv2

from subtitle_region_detector import detect_subtitle_region as detect_image_subtitle_region
from subtitle_region_detector import save_detection_preview as save_image_detection_preview

os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(__file__).resolve().parent / "subtitle_extractor_source" / "video-subtitle-extractor-main"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime" / "subtitle_output"
DEFAULT_TEMP_ROOT = PROJECT_ROOT / "runtime" / "subtitle_output" / "_temp"

if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

SubtitleAreaClass = None
ConfigObject = None
SubtitleExtractorClass = None
OcrRecogniserClass = None
GetCoordinatesFn = None
HardwareAcceleratorClass = None
SUBTITLE_LINE_JOIN_GAP = 54
SUBTITLE_NOISE_KEYWORDS = (
    "剧情演绎",
    "纯属虚构",
    "不良引导",
    "热门短剧",
    "新剧来袭",
    "点击头像",
    "点我头像",
    "搜剧名",
    "完整版",
    "关注",
    "点赞",
    "收藏",
    "转发",
    "评论区",
    "抖音",
    "douyin",
    "快手",
    "小红书",
    "bilibili",
    "下集",
    "上集",
)


@dataclass
class RawSubtitleBox:
    frame_no: int
    xmin: int
    xmax: int
    ymin: int
    ymax: int
    text: str

    @property
    def width(self) -> int:
        return max(0, self.xmax - self.xmin)

    @property
    def height(self) -> int:
        return max(0, self.ymax - self.ymin)

    @property
    def center_x(self) -> float:
        return (self.xmin + self.xmax) / 2.0

    @property
    def center_y(self) -> float:
        return (self.ymin + self.ymax) / 2.0


@dataclass
class SubtitleSegment:
    start_ms: int
    end_ms: int
    last_positive_ms: int
    variants: Counter[str]


@dataclass(frozen=True)
class LocalSubtitleArea:
    ymin: int
    ymax: int
    xmin: int
    xmax: int


def ensure_backend_loaded() -> None:
    global SubtitleAreaClass, ConfigObject, SubtitleExtractorClass, OcrRecogniserClass, GetCoordinatesFn, HardwareAcceleratorClass
    if (
        SubtitleAreaClass is not None
        and ConfigObject is not None
        and SubtitleExtractorClass is not None
        and OcrRecogniserClass is not None
        and GetCoordinatesFn is not None
        and HardwareAcceleratorClass is not None
    ):
        return
    from backend.bean.subtitle_area import SubtitleArea as LoadedSubtitleArea  # noqa: E402
    from backend.config import config as loaded_config  # noqa: E402
    from backend.tools.hardware_accelerator import HardwareAccelerator as LoadedHardwareAccelerator  # noqa: E402
    from backend.tools.ocr import OcrRecogniser as LoadedOcrRecogniser  # noqa: E402
    from backend.tools.ocr import get_coordinates as loaded_get_coordinates  # noqa: E402
    from backend.main import SubtitleExtractor as LoadedSubtitleExtractor  # noqa: E402

    SubtitleAreaClass = LoadedSubtitleArea
    ConfigObject = loaded_config
    SubtitleExtractorClass = LoadedSubtitleExtractor
    OcrRecogniserClass = LoadedOcrRecogniser
    GetCoordinatesFn = loaded_get_coordinates
    HardwareAcceleratorClass = LoadedHardwareAccelerator


def make_subtitle_area(ymin: int, ymax: int, xmin: int, xmax: int):
    if SubtitleAreaClass is None:
        try:
            ensure_backend_loaded()
        except Exception:
            return LocalSubtitleArea(ymin=ymin, ymax=ymax, xmin=xmin, xmax=xmax)
    return SubtitleAreaClass(ymin, ymax, xmin, xmax)


def parse_subtitle_area(value: str) -> tuple[int, int, int, int]:
    try:
        ymin, ymax, xmin, xmax = [int(part.strip()) for part in value.split(",")]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("subtitle-area must be ymin,ymax,xmin,xmax") from exc
    return ymin, ymax, xmin, xmax


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the subtitle extractor source code in batch mode.")
    parser.add_argument("--input", required=True, help="Input video path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory used to store generated srt files.")
    parser.add_argument("--output-name", default="", help="Optional output file name. Default: <video_stem>.srt")
    parser.add_argument("--subtitle-area", type=parse_subtitle_area, help="Subtitle area: ymin,ymax,xmin,xmax")
    parser.add_argument("--auto-subtitle-area", action="store_true", help="Auto-detect the subtitle area before the final extraction pass.")
    parser.add_argument("--language", default="ch", help="VSE language code, for example ch/en/japan.")
    parser.add_argument("--mode", default="accurate", choices=["auto", "fast", "accurate"], help="Extraction mode.")
    parser.add_argument("--extract-frequency", type=int, default=5, help="How many frames per second should be sampled for OCR.")
    parser.add_argument("--probe-extract-frequency", type=int, default=5, help="How many frames per second should be used for the auto-detect probe pass.")
    parser.add_argument("--temp-root", default=str(DEFAULT_TEMP_ROOT), help="Temporary working directory root.")
    parser.add_argument("--temp-name", default="", help="Optional temporary job directory name.")
    parser.add_argument("--generate-txt", action="store_true", help="Generate txt output together with srt.")
    parser.add_argument("--keep-temp", action="store_true", help="Keep temporary files for debugging.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite the output file when it already exists.")
    args = parser.parse_args(argv)
    if args.subtitle_area is None and not args.auto_subtitle_area:
        parser.error("Either --subtitle-area or --auto-subtitle-area is required.")
    return args


def set_config_value(option_item, value) -> None:
    ensure_backend_loaded()
    ConfigObject.set(option_item, value)


def configure_runtime(language: str, mode: str, extract_frequency: int, generate_txt: bool, keep_temp: bool) -> None:
    ensure_backend_loaded()
    set_config_value(ConfigObject.language, language)
    set_config_value(ConfigObject.mode, mode)
    set_config_value(ConfigObject.extractFrequency, int(extract_frequency))
    set_config_value(ConfigObject.generateTxt, bool(generate_txt))
    set_config_value(ConfigObject.debugNoDeleteCache, bool(keep_temp))


def configure_extractor_paths(extractor: Any, temp_root: Path, temp_name: str, output_path: Path) -> Path:
    job_temp_dir = temp_root / temp_name
    subtitle_temp_dir = job_temp_dir / "subtitle"
    extractor.temp_output_dir = str(job_temp_dir)
    extractor.frame_output_dir = str(job_temp_dir / "frames")
    extractor.subtitle_output_dir = str(subtitle_temp_dir)
    extractor.vsf_subtitle = str(subtitle_temp_dir / "raw_vsf.srt")
    extractor.raw_subtitle_path = str(subtitle_temp_dir / "raw.txt")
    extractor.subtitle_output_path = str(output_path)
    return job_temp_dir


def ensure_probe_directories(extractor: SubtitleExtractor, job_temp_dir: Path) -> None:
    shutil.rmtree(job_temp_dir, ignore_errors=True)
    Path(extractor.frame_output_dir).mkdir(parents=True, exist_ok=True)
    Path(extractor.subtitle_output_dir).mkdir(parents=True, exist_ok=True)


def load_raw_subtitle_boxes(raw_path: Path) -> list[RawSubtitleBox]:
    boxes: list[RawSubtitleBox] = []
    if not raw_path.exists():
        return boxes

    for line in raw_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        try:
            frame_no = int(parts[0])
            coords = [int(part.strip()) for part in parts[1].strip("()").split(",")]
        except ValueError:
            continue
        if len(coords) != 4:
            continue
        xmin, xmax, ymin, ymax = coords
        boxes.append(RawSubtitleBox(frame_no=frame_no, xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax, text=parts[2].strip()))
    return boxes


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(value, max_value))


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    position = clamp(ratio, 0.0, 1.0) * (len(ordered) - 1)
    low_index = int(position)
    high_index = min(low_index + 1, len(ordered) - 1)
    fraction = position - low_index
    return ordered[low_index] + (ordered[high_index] - ordered[low_index]) * fraction


def smooth_series(values: list[float], radius: int) -> list[float]:
    if radius <= 0 or not values:
        return values[:]
    prefix = [0.0]
    for value in values:
        prefix.append(prefix[-1] + value)
    smoothed: list[float] = []
    for index in range(len(values)):
        left = max(0, index - radius)
        right = min(len(values), index + radius + 1)
        smoothed.append((prefix[right] - prefix[left]) / max(1, right - left))
    return smoothed


def box_weight(box: RawSubtitleBox, frame_width: int, frame_height: int) -> float:
    width_ratio = clamp(box.width / max(1, frame_width), 0.0, 1.0)
    center_ratio = 1.0 - abs((box.center_x / max(1.0, frame_width)) - 0.5) * 2.0
    center_ratio = clamp(center_ratio, 0.0, 1.0)
    lower_ratio = clamp(box.center_y / max(1.0, frame_height), 0.0, 1.0)
    text_bonus = 0.8 + min(len(box.text.replace(" ", "")), 12) / 12.0
    weight = max(0.03, width_ratio) * (0.7 + center_ratio) * (0.9 + lower_ratio * 0.25) * text_bonus
    if width_ratio < 0.10:
        weight *= 0.55
    if box.height > frame_height * 0.12:
        weight *= 0.7
    return weight


def detect_watermark_keys(boxes: list[RawSubtitleBox], total_frames: int, frame_width: int) -> set[tuple[int, int, int, int]]:
    frame_groups: dict[tuple[int, int, int, int], set[int]] = {}
    sample_box: dict[tuple[int, int, int, int], RawSubtitleBox] = {}
    for box in boxes:
        key = (
            round(box.xmin / 24),
            round(box.xmax / 24),
            round(box.ymin / 12),
            round(box.ymax / 12),
        )
        frame_groups.setdefault(key, set()).add(box.frame_no)
        sample_box.setdefault(key, box)

    detected: set[tuple[int, int, int, int]] = set()
    for key, frames in frame_groups.items():
        box = sample_box[key]
        width_ratio = box.width / max(1, frame_width)
        coverage = len(frames) / max(1, total_frames)
        edge_like = box.center_x < frame_width * 0.3 or box.center_x > frame_width * 0.7
        if width_ratio < 0.08 and coverage >= 0.30:
            detected.add(key)
        elif width_ratio < 0.16 and coverage >= 0.50 and edge_like:
            detected.add(key)
    return detected


def filter_boxes_for_detection(boxes: list[RawSubtitleBox], frame_width: int, frame_height: int) -> tuple[list[RawSubtitleBox], dict]:
    if not boxes:
        return boxes, {"filtered_out": 0}
    unique_frames = len({box.frame_no for box in boxes})
    watermark_keys = detect_watermark_keys(boxes, unique_frames, frame_width)
    filtered: list[RawSubtitleBox] = []
    removed = 0
    for box in boxes:
        key = (
            round(box.xmin / 24),
            round(box.xmax / 24),
            round(box.ymin / 12),
            round(box.ymax / 12),
        )
        if key in watermark_keys:
            removed += 1
            continue
        if box.width <= 0 or box.height <= 0:
            removed += 1
            continue
        if box.ymin < 0 or box.ymax > frame_height or box.xmin < 0 or box.xmax > frame_width:
            continue
        filtered.append(box)
    return filtered, {"filtered_out": removed, "watermark_clusters": len(watermark_keys)}


def find_candidate_bands(histogram: list[float], frame_height: int) -> list[tuple[int, int]]:
    if not histogram:
        return []
    peak = max(histogram)
    if peak <= 0:
        return []
    average = sum(histogram) / len(histogram)
    threshold = max(peak * 0.52, average * 2.1)
    gap_limit = max(8, frame_height // 90)
    bands: list[tuple[int, int]] = []
    start = None
    below_gap = 0

    for index, value in enumerate(histogram):
        if value >= threshold:
            if start is None:
                start = index
            below_gap = 0
            continue
        if start is None:
            continue
        below_gap += 1
        if below_gap > gap_limit:
            bands.append((start, max(start, index - below_gap)))
            start = None
            below_gap = 0

    if start is not None:
        bands.append((start, len(histogram) - 1))

    if bands:
        return bands

    peak_index = max(range(len(histogram)), key=histogram.__getitem__)
    half_height = max(16, frame_height // 35)
    return [(max(0, peak_index - half_height), min(frame_height - 1, peak_index + half_height))]


def overlap_ratio(box: RawSubtitleBox, band: tuple[int, int]) -> float:
    band_ymin, band_ymax = band
    inter = max(0, min(box.ymax, band_ymax) - max(box.ymin, band_ymin))
    return inter / max(1, box.height)


def select_boxes_for_band(boxes: list[RawSubtitleBox], band: tuple[int, int]) -> list[RawSubtitleBox]:
    selected: list[RawSubtitleBox] = []
    for box in boxes:
        if overlap_ratio(box, band) >= 0.45:
            selected.append(box)
    return selected


def score_band(
    band: tuple[int, int],
    boxes: list[RawSubtitleBox],
    total_frames: int,
    frame_width: int,
    frame_height: int,
    peak: float,
) -> tuple[float, dict]:
    selected = select_boxes_for_band(boxes, band)
    if not selected:
        return -1.0, {}

    band_ymin, band_ymax = band
    band_center = (band_ymin + band_ymax) / 2.0
    band_height = max(1, band_ymax - band_ymin)
    unique_frames = len({box.frame_no for box in selected})
    coverage = unique_frames / max(1, total_frames)
    median_width_ratio = statistics.median(box.width / max(1, frame_width) for box in selected)
    median_center_ratio = statistics.median(box.center_x / max(1.0, frame_width) for box in selected)
    center_bonus = 1.0 - abs(median_center_ratio - 0.5) * 2.0
    lower_bonus = 0.85 + 0.15 * clamp(band_center / max(1.0, frame_height), 0.0, 1.0)
    size_penalty = 0.0 if band_height <= frame_height * 0.18 else 1.0
    band_strength = sum(box_weight(box, frame_width, frame_height) for box in selected) / max(1.0, peak)
    score = (
        coverage * 4.0
        + median_width_ratio * 3.0
        + center_bonus * 1.4
        + band_strength * 0.25
        + lower_bonus
        - size_penalty
    )
    return score, {
        "band": [band_ymin, band_ymax],
        "coverage": coverage,
        "median_width_ratio": median_width_ratio,
        "center_bonus": center_bonus,
        "lower_bonus": lower_bonus,
        "size_penalty": size_penalty,
        "selected_boxes": len(selected),
        "selected_frames": unique_frames,
    }


def fallback_subtitle_area(frame_width: int, frame_height: int):
    top_ratio = 0.72
    height_ratio = 0.12 if frame_height >= 1400 else 0.14
    bottom_ratio = min(0.87, top_ratio + height_ratio)
    return make_subtitle_area(
        int(frame_height * top_ratio),
        int(frame_height * bottom_ratio),
        int(frame_width * 0.05),
        int(frame_width * 0.95),
    )


def build_area_from_boxes(boxes: list[RawSubtitleBox], frame_width: int, frame_height: int):
    if not boxes:
        return fallback_subtitle_area(frame_width, frame_height)

    ymins = [box.ymin for box in boxes]
    ymaxs = [box.ymax for box in boxes]
    xmins = [box.xmin for box in boxes]
    xmaxs = [box.xmax for box in boxes]
    centers = [box.center_x for box in boxes]
    heights = [box.height for box in boxes]
    widths = [box.width for box in boxes]

    median_height = statistics.median(heights)
    median_width = statistics.median(widths)
    center_x = percentile(centers, 0.5)

    ymin = int(percentile(ymins, 0.10) - max(frame_height * 0.01, median_height * 0.7))
    ymax = int(percentile(ymaxs, 0.90) + max(frame_height * 0.01, median_height * 0.7))
    xmin = int(percentile(xmins, 0.05) - max(frame_width * 0.03, median_height * 2.0))
    xmax = int(percentile(xmaxs, 0.95) + max(frame_width * 0.03, median_height * 2.0))

    min_width = max(frame_width * 0.45, median_width * 1.5)
    current_width = xmax - xmin
    if current_width < min_width:
        half_width = min_width / 2.0
        xmin = int(center_x - half_width)
        xmax = int(center_x + half_width)

    ymin = int(clamp(ymin, 0, frame_height - 1))
    ymax = int(clamp(ymax, ymin + 1, frame_height))
    xmin = int(clamp(xmin, 0, frame_width - 1))
    xmax = int(clamp(xmax, xmin + 1, frame_width))
    return make_subtitle_area(ymin, ymax, xmin, xmax)


def detect_subtitle_area(
    raw_path: Path,
    frame_width: int,
    frame_height: int,
) -> tuple[SubtitleArea, dict]:
    original_boxes = load_raw_subtitle_boxes(raw_path)
    total_frames = len({box.frame_no for box in original_boxes})
    filtered_boxes, filter_report = filter_boxes_for_detection(original_boxes, frame_width, frame_height)
    boxes = filtered_boxes or original_boxes

    if not boxes:
        area = fallback_subtitle_area(frame_width, frame_height)
        return area, {
            "method": "fallback",
            "reason": "no_ocr_boxes",
            "frame_width": frame_width,
            "frame_height": frame_height,
            "subtitle_area": [area.ymin, area.ymax, area.xmin, area.xmax],
            **filter_report,
        }

    diff = [0.0] * (frame_height + 2)
    for box in boxes:
        weight = box_weight(box, frame_width, frame_height)
        ymin = int(clamp(box.ymin, 0, frame_height - 1))
        ymax = int(clamp(box.ymax, ymin + 1, frame_height))
        diff[ymin] += weight
        diff[ymax] -= weight

    histogram = [0.0] * frame_height
    running = 0.0
    for index in range(frame_height):
        running += diff[index]
        histogram[index] = running

    smoothed = smooth_series(histogram, max(6, frame_height // 80))
    peak = max(smoothed) if smoothed else 0.0
    bands = find_candidate_bands(smoothed, frame_height)

    best_score = -1.0
    best_band = None
    best_band_report: dict[str, float | int | list[int]] = {}
    for band in bands:
        score, band_report = score_band(band, boxes, total_frames, frame_width, frame_height, peak)
        if score > best_score:
            best_score = score
            best_band = band
            best_band_report = band_report

    if best_band is None:
        area = fallback_subtitle_area(frame_width, frame_height)
        return area, {
            "method": "fallback",
            "reason": "no_candidate_band",
            "frame_width": frame_width,
            "frame_height": frame_height,
            "subtitle_area": [area.ymin, area.ymax, area.xmin, area.xmax],
            **filter_report,
        }

    selected_boxes = select_boxes_for_band(boxes, best_band)
    area = build_area_from_boxes(selected_boxes, frame_width, frame_height)
    return area, {
        "method": "auto_detect",
        "frame_width": frame_width,
        "frame_height": frame_height,
        "raw_box_count": len(original_boxes),
        "usable_box_count": len(boxes),
        "candidate_band_count": len(bands),
        "selected_box_count": len(selected_boxes),
        "score": best_score,
        "best_band": list(best_band),
        "subtitle_area": [area.ymin, area.ymax, area.xmin, area.xmax],
        **best_band_report,
        **filter_report,
    }


def save_detection_preview(video_path: Path, image_path: Path, area: SubtitleArea) -> None:
    cap = cv2.VideoCapture(str(video_path))
    try:
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        target_frame = max(0, int(frame_count * 0.2))
        cap.set(cv2.CAP_PROP_POS_FRAMES, target_frame)
        ok, frame = cap.read()
        if not ok:
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ok, frame = cap.read()
        if not ok:
            return
        cv2.rectangle(frame, (area.xmin, area.ymin), (area.xmax, area.ymax), (0, 255, 0), 2)
        cv2.putText(
            frame,
            "Auto Subtitle Area",
            (area.xmin, max(0, area.ymin - 12)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 255, 0),
            2,
        )
        write_cv_image(image_path, frame)
    finally:
        cap.release()


def write_detection_report(report_path: Path, preview_path: Path, report: dict) -> None:
    payload = dict(report)
    payload["preview_image"] = str(preview_path)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def image_region_to_subtitle_area(region) -> Any:
    return make_subtitle_area(region.y, region.y + region.height, region.x, region.x + region.width)


def adjust_low_banner_region(area: Any, frame_width: int, frame_height: int) -> tuple[Any, dict] | None:
    y_ratio = area.ymin / max(1.0, frame_height)
    bottom_ratio = area.ymax / max(1.0, frame_height)
    height_ratio = (area.ymax - area.ymin) / max(1.0, frame_height)
    if y_ratio < 0.78 and not (bottom_ratio >= 0.95 and height_ratio >= 0.15):
        return None

    adjusted_ymax = min(
        int(frame_height * 0.84),
        area.ymin - max(12, int(round(frame_height * 0.008))),
    )
    adjusted_ymin = max(
        int(frame_height * 0.70),
        adjusted_ymax - max(120, int(round(frame_height * 0.14))),
    )
    if adjusted_ymax <= adjusted_ymin + 24:
        return None

    adjusted = make_subtitle_area(
        adjusted_ymin,
        adjusted_ymax,
        int(frame_width * 0.05),
        int(frame_width * 0.95),
    )
    return adjusted, {
        "reason": "detected_band_too_low",
        "original_area": [area.ymin, area.ymax, area.xmin, area.xmax],
        "adjusted_area": [adjusted.ymin, adjusted.ymax, adjusted.xmin, adjusted.xmax],
    }


def run_image_auto_detect(
    input_path: Path,
    output_path: Path,
    temp_root: Path,
    temp_name: str,
) -> tuple[Any | None, dict | None]:
    work_dir = temp_root / "_image_auto_detect" / temp_name
    region, report = detect_image_subtitle_region(input_path, work_dir)
    if region is None:
        report = dict(report or {})
        frame_width = int(report.get("frame_width") or 0)
        frame_height = int(report.get("frame_height") or 0)
        if frame_width > 0 and frame_height > 0:
            area = fallback_subtitle_area(frame_width, frame_height)
            preview_path = output_path.with_name(output_path.stem + ".auto_area.jpg")
            report_path = output_path.with_name(output_path.stem + ".auto_area.json")
            save_detection_preview(input_path, preview_path, area)
            report["subtitle_area"] = [area.ymin, area.ymax, area.xmin, area.xmax]
            report["detector"] = "shared_image_mask_fallback"
            report["preview_image"] = str(preview_path)
            report["report_path"] = str(report_path)
            write_detection_report(report_path, preview_path, report)
            print("AUTO_SUBTITLE_AREA", json.dumps(report["subtitle_area"], ensure_ascii=False))
            print("AUTO_SUBTITLE_REPORT", str(report_path))
            return area, report
        return None, report
    area = image_region_to_subtitle_area(region)
    adjusted = adjust_low_banner_region(area, int(report.get("frame_width") or 0), int(report.get("frame_height") or 0))
    if adjusted is not None:
        area, adjust_report = adjusted
        report["subtitle_area_adjustment"] = adjust_report
        report["detector"] = "shared_image_mask_adjusted"
    else:
        report["detector"] = "shared_image_mask"
    preview_path = output_path.with_name(output_path.stem + ".auto_area.jpg")
    report_path = output_path.with_name(output_path.stem + ".auto_area.json")
    save_detection_preview(input_path, preview_path, area)
    report = dict(report or {})
    report["subtitle_area"] = [area.ymin, area.ymax, area.xmin, area.xmax]
    report["preview_image"] = str(preview_path)
    report["report_path"] = str(report_path)
    write_detection_report(report_path, preview_path, report)
    print("AUTO_SUBTITLE_AREA", json.dumps(report["subtitle_area"], ensure_ascii=False))
    print("AUTO_SUBTITLE_REPORT", str(report_path))
    return area, report


def run_probe_extraction(
    input_path: Path,
    temp_root: Path,
    temp_name: str,
    language: str,
    extract_frequency: int,
    keep_temp: bool,
) -> tuple[Path, int, int]:
    configure_runtime(language=language, mode="fast", extract_frequency=extract_frequency, generate_txt=False, keep_temp=True)
    probe_output = temp_root / f"{temp_name}_probe.srt"
    ensure_backend_loaded()
    extractor = SubtitleExtractorClass(str(input_path))
    job_temp_dir = configure_extractor_paths(extractor, temp_root, f"{temp_name}_probe", probe_output)
    extractor.sub_area = None
    ensure_probe_directories(extractor, job_temp_dir)

    process = extractor.start_subtitle_ocr_async()
    extractor.extract_frame_by_fps()
    extractor.subtitle_ocr_task_queue.put((extractor.frame_count, -1, None, None, None, None))
    process.join()

    raw_path = Path(extractor.raw_subtitle_path)
    if not raw_path.exists():
        raise RuntimeError(f"probe pass did not produce raw OCR output: {raw_path}")

    return raw_path, extractor.frame_width, extractor.frame_height


def format_srt_timestamp(milliseconds: int) -> str:
    total_ms = max(0, int(milliseconds))
    hours, remainder = divmod(total_ms, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1_000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{millis:03d}"


def clean_subtitle_text(text: str) -> str:
    cleaned = str(text or "").strip()
    cleaned = cleaned.replace(" ", "")
    cleaned = re.sub(r"[\r\n\t]+", "", cleaned)
    cleaned = cleaned.strip("|¦`'\"[](){}<>")
    if not cleaned:
        return ""
    ascii_letters = len(re.findall(r"[A-Za-z]", cleaned))
    chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", cleaned))
    if chinese_chars == 0 and ascii_letters >= 2 and len(cleaned) <= 3:
        return ""
    if re.fullmatch(r"\d{1,4}", cleaned):
        return ""
    if not re.search(r"[0-9A-Za-z\u4e00-\u9fff]", cleaned):
        return ""
    return cleaned


def write_cv_image(image_path: Path, image) -> bool:
    image_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = image_path.suffix or ".png"
    ok, encoded = cv2.imencode(suffix, image)
    if not ok:
        return False
    image_path.write_bytes(encoded.tobytes())
    return True


def normalize_subtitle_text(text: str) -> str:
    normalized = str(text or "").strip().lower()
    normalized = normalized.replace("\n", "")
    normalized = re.sub(r"[\s\-_—–~`'\"“”‘’.,，。!！?？:：;；/\\|()\[\]{}<>]+", "", normalized)
    return normalized


def subtitle_text_quality_score(text: str) -> float:
    normalized = normalize_subtitle_text(text)
    if not normalized:
        return -999.0
    chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", text or ""))
    ascii_letters = len(re.findall(r"[A-Za-z]", text or ""))
    digits = len(re.findall(r"\d", text or ""))
    punctuation = len(re.findall(r"[^\w\u4e00-\u9fff]", text or ""))
    score = float(len(normalized))
    score += chinese_chars * 1.45
    score += digits * 0.25
    score -= ascii_letters * 0.70
    score -= punctuation * 0.20
    if chinese_chars == 0 and ascii_letters >= 2 and len(normalized) <= 3:
        score -= 8.0
    return score


def subtitle_noise_like_text(text: str) -> bool:
    raw = str(text or "").strip()
    normalized = normalize_subtitle_text(raw)
    lowered = normalized.lower()
    if not normalized:
        return False
    if "《" in raw and "》" in raw:
        return True
    if any(keyword in raw or keyword in normalized or keyword in lowered for keyword in SUBTITLE_NOISE_KEYWORDS):
        return True
    if re.search(r"第\s*\d+\s*[集话季]", raw):
        return True
    return False


def select_preferred_subtitle_text(lines: list[tuple[int | None, str]]) -> str:
    if not lines:
        return ""
    ranked = sorted(lines, key=lambda item: (item[0] is None, item[0] if item[0] is not None else 10**9))
    filtered = [(y, text) for y, text in ranked if not subtitle_noise_like_text(text)]
    if not filtered:
        return ""

    selected: list[str] = []
    previous_y: int | None = None
    for line_y, line_text in filtered:
        if not selected:
            selected.append(line_text)
            previous_y = line_y
            continue
        if previous_y is None or line_y is None or line_y - previous_y <= SUBTITLE_LINE_JOIN_GAP:
            selected.append(line_text)
            previous_y = line_y
            continue
        break
    return "\n".join(piece for piece in selected if piece)


def subtitles_are_similar(left: str, right: str) -> bool:
    left_normalized = normalize_subtitle_text(left)
    right_normalized = normalize_subtitle_text(right)
    if not left_normalized or not right_normalized:
        return False
    if left_normalized == right_normalized:
        return True
    if len(left_normalized) >= 4 and left_normalized in right_normalized:
        return True
    if len(right_normalized) >= 4 and right_normalized in left_normalized:
        return True
    return SequenceMatcher(None, left_normalized, right_normalized).ratio() >= 0.78


def choose_segment_text(segment: SubtitleSegment | None) -> str:
    if segment is None or not segment.variants:
        return ""
    ranked = sorted(
        segment.variants.items(),
        key=lambda item: (
            item[1],
            subtitle_text_quality_score(item[0]),
            len(item[0]),
        ),
        reverse=True,
    )
    return ranked[0][0]


def finalize_segment(segments: list[SubtitleSegment], segment: SubtitleSegment | None, sample_duration_ms: int) -> None:
    if segment is None:
        return
    text = choose_segment_text(segment)
    if not text:
        return
    minimum_duration = max(400, sample_duration_ms)
    segment.end_ms = max(segment.last_positive_ms, segment.start_ms + minimum_duration)
    if segment.end_ms <= segment.start_ms:
        segment.end_ms = segment.start_ms + minimum_duration
    segments.append(segment)


def merge_segments(segments: list[SubtitleSegment]) -> list[SubtitleSegment]:
    if not segments:
        return []
    merged: list[SubtitleSegment] = [segments[0]]
    for segment in segments[1:]:
        previous = merged[-1]
        if subtitles_are_similar(choose_segment_text(previous), choose_segment_text(segment)) and segment.start_ms - previous.end_ms <= 500:
            previous.end_ms = max(previous.end_ms, segment.end_ms)
            previous.last_positive_ms = max(previous.last_positive_ms, segment.last_positive_ms)
            previous.variants.update(segment.variants)
            continue
        merged.append(segment)
    return merged


def segment_vote_count(segment: SubtitleSegment) -> int:
    return int(sum(segment.variants.values()))


def segment_duration_ms(segment: SubtitleSegment) -> int:
    return max(0, int(segment.end_ms) - int(segment.start_ms))


def segment_rank(segment: SubtitleSegment) -> tuple[float, ...]:
    text = choose_segment_text(segment)
    chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", text or ""))
    return (
        float(segment_vote_count(segment)),
        float(subtitle_text_quality_score(text)),
        float(chinese_chars),
        float(segment_duration_ms(segment)),
        float(segment.start_ms),
    )


def bridge_merge_repeated_segments(segments: list[SubtitleSegment]) -> list[SubtitleSegment]:
    if len(segments) < 3:
        return segments
    bridged: list[SubtitleSegment] = []
    index = 0
    while index < len(segments):
        if index + 2 >= len(segments):
            bridged.extend(segments[index:])
            break
        first = segments[index]
        middle = segments[index + 1]
        third = segments[index + 2]
        first_text = choose_segment_text(first)
        third_text = choose_segment_text(third)
        if (
            first_text
            and third_text
            and subtitles_are_similar(first_text, third_text)
            and segment_duration_ms(middle) <= 700
            and middle.start_ms - first.start_ms <= 320
            and third.start_ms - middle.start_ms <= 320
        ):
            first.end_ms = max(first.end_ms, middle.end_ms, third.end_ms)
            first.last_positive_ms = max(first.last_positive_ms, middle.last_positive_ms, third.last_positive_ms)
            first.variants.update(middle.variants)
            first.variants.update(third.variants)
            bridged.append(first)
            index += 3
            continue
        bridged.append(first)
        index += 1
    return bridged


def collapse_overlapping_segments(segments: list[SubtitleSegment]) -> list[SubtitleSegment]:
    if not segments:
        return []
    collapsed: list[SubtitleSegment] = [segments[0]]
    for segment in segments[1:]:
        previous = collapsed[-1]
        previous_text = choose_segment_text(previous)
        current_text = choose_segment_text(segment)
        overlap_ms = previous.end_ms - segment.start_ms
        if overlap_ms <= 0:
            collapsed.append(segment)
            continue
        start_delta = segment.start_ms - previous.start_ms
        if subtitles_are_similar(previous_text, current_text):
            previous.end_ms = max(previous.end_ms, segment.end_ms)
            previous.last_positive_ms = max(previous.last_positive_ms, segment.last_positive_ms)
            previous.variants.update(segment.variants)
            continue
        if start_delta > 320:
            collapsed.append(segment)
            continue
        previous_duration = segment_duration_ms(previous)
        current_duration = segment_duration_ms(segment)
        minimum_duration = max(1, min(previous_duration, current_duration))
        overlap_ratio = overlap_ms / minimum_duration
        if overlap_ratio < 0.45 and minimum_duration > 600:
            collapsed.append(segment)
            continue
        if segment_rank(segment) >= segment_rank(previous):
            collapsed[-1] = segment
            continue
    return collapsed


def build_ocr_candidates(frame, area) -> list[Any]:
    crop = frame[max(0, area.ymin):max(area.ymin + 1, area.ymax), max(0, area.xmin):max(area.xmin + 1, area.xmax)]
    if crop.size == 0:
        return []
    scale = 2.0 if max(crop.shape[:2]) < 1000 else 1.5
    resized = cv2.resize(crop, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (3, 3), 0)
    normalized = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)
    _, binary = cv2.threshold(normalized, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    adaptive = cv2.adaptiveThreshold(
        normalized,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        11,
    )
    candidates: list[Any] = [
        cv2.cvtColor(binary, cv2.COLOR_GRAY2BGR),
        cv2.cvtColor(adaptive, cv2.COLOR_GRAY2BGR),
        resized,
    ]
    if float(binary.mean()) > 160:
        inverted = cv2.bitwise_not(binary)
        candidates.insert(1, cv2.cvtColor(inverted, cv2.COLOR_GRAY2BGR))
    return candidates


def build_frame_fingerprint(frame, area):
    crop = frame[max(0, area.ymin):max(area.ymin + 1, area.ymax), max(0, area.xmin):max(area.xmin + 1, area.xmax)]
    if crop.size == 0:
        return None
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (3, 3), 0)
    normalized = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)
    _, binary = cv2.threshold(normalized, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return cv2.resize(binary, (96, 32), interpolation=cv2.INTER_AREA)


def build_frame_text(dt_box, rec_res) -> str:
    if not rec_res:
        return ""
    coordinate_list = GetCoordinatesFn(dt_box) if GetCoordinatesFn is not None else []
    lines: list[tuple[int | None, str]] = []
    current_parts: list[str] = []
    current_y: int | None = None
    for index, (text, _score) in enumerate(rec_res):
        cleaned = clean_subtitle_text(text)
        if not cleaned:
            continue
        ymin = None
        if index < len(coordinate_list):
            ymin = int(coordinate_list[index][2])
        if current_y is None or ymin is None or abs(ymin - current_y) <= 24:
            current_parts.append(cleaned)
            if current_y is None and ymin is not None:
                current_y = ymin
            continue
        line_text = "".join(current_parts).strip()
        if line_text:
            lines.append((current_y, line_text))
        current_parts = [cleaned]
        current_y = ymin
    if current_parts:
        line_text = "".join(current_parts).strip()
        if line_text:
            lines.append((current_y, line_text))
    return select_preferred_subtitle_text(lines)


def build_rapidocr_text(result) -> str:
    if not result:
        return ""
    lines: list[tuple[int | None, str]] = []
    current_parts: list[str] = []
    current_y: int | None = None
    for item in result:
        if len(item) < 2:
            continue
        box, text = item[0], item[1]
        cleaned = clean_subtitle_text(text)
        if not cleaned:
            continue
        ymin = None
        if box:
            ymin = int(min(point[1] for point in box))
        if current_y is None or ymin is None or abs(ymin - current_y) <= 24:
            current_parts.append(cleaned)
            if current_y is None and ymin is not None:
                current_y = ymin
            continue
        line_text = "".join(current_parts).strip()
        if line_text:
            lines.append((current_y, line_text))
        current_parts = [cleaned]
        current_y = ymin
    if current_parts:
        line_text = "".join(current_parts).strip()
        if line_text:
            lines.append((current_y, line_text))
    return select_preferred_subtitle_text(lines)


def extract_frame_subtitle_text_with_rapidocr(ocr, frame, area) -> str:
    best_text = ""
    best_score = -999.0
    for candidate in build_ocr_candidates(frame, area):
        result, _ = ocr(candidate, use_cls=False)
        text = build_rapidocr_text(result)
        if not text:
            continue
        score = subtitle_text_quality_score(text)
        if score > best_score:
            best_text = text
            best_score = score
    return best_text


def extract_frame_subtitle_text(ocr, frame, area) -> str:
    if ocr.__class__.__module__.startswith("rapidocr_onnxruntime"):
        return extract_frame_subtitle_text_with_rapidocr(ocr, frame, area)
    best_text = ""
    best_score = -999.0
    for candidate in build_ocr_candidates(frame, area):
        dt_box, rec_res = ocr.predict(candidate)
        text = build_frame_text(dt_box, rec_res)
        if not text:
            continue
        score = subtitle_text_quality_score(text)
        if score > best_score:
            best_text = text
            best_score = score
    return best_text


def write_srt_output(output_path: Path, segments: list[SubtitleSegment]) -> None:
    lines: list[str] = []
    for index, segment in enumerate(segments, start=1):
        text = choose_segment_text(segment)
        if not text:
            continue
        lines.append(str(index))
        lines.append(f"{format_srt_timestamp(segment.start_ms)} --> {format_srt_timestamp(segment.end_ms)}")
        lines.append(text)
        lines.append("")
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def write_txt_output(output_path: Path, segments: list[SubtitleSegment]) -> None:
    text_lines = [choose_segment_text(segment) for segment in segments if choose_segment_text(segment)]
    output_path.with_suffix(".txt").write_text("\n".join(text_lines).strip() + "\n", encoding="utf-8")


def run_direct_ocr_extraction(
    input_path: Path,
    output_path: Path,
    area: Any,
    language: str,
    mode: str,
    extract_frequency: int,
    generate_txt: bool,
) -> None:
    ocr = None
    try:
        from rapidocr_onnxruntime import RapidOCR

        ocr = RapidOCR()
        print("OCR_BACKEND rapidocr")
    except Exception as exc:
        print(f"OCR_BACKEND_FALLBACK paddle ({exc})")

    if ocr is None:
        ensure_backend_loaded()
        hardware_accelerator = HardwareAcceleratorClass.instance()
        configure_runtime(language=language, mode=mode, extract_frequency=extract_frequency, generate_txt=generate_txt, keep_temp=False)
        ocr = OcrRecogniserClass()
        ocr.hardware_accelerator = hardware_accelerator

    capture = cv2.VideoCapture(str(input_path))
    if not capture.isOpened():
        raise RuntimeError(f"failed to open video for OCR: {input_path}")

    fps = float(capture.get(cv2.CAP_PROP_FPS) or 25.0)
    frame_count = int(round(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0))
    if frame_count <= 0:
        capture.release()
        raise RuntimeError(f"failed to read video frame count: {input_path}")

    effective_extract_frequency = max(1, int(extract_frequency))

    sample_step = max(1, int(round(fps / effective_extract_frequency)))
    sample_duration_ms = max(200, int(round(sample_step / max(fps, 1e-6) * 1000)))
    estimated_samples = max(1, (frame_count + sample_step - 1) // sample_step)
    video_duration_ms = int(round(frame_count / max(fps, 1e-6) * 1000))

    print("SUBTITLE_AREA", json.dumps([area.ymin, area.ymax, area.xmin, area.xmax], ensure_ascii=False))
    print("SUBTITLE_START", str(input_path), str(output_path))

    segments: list[SubtitleSegment] = []
    current_segment: SubtitleSegment | None = None
    empty_streak = 0
    last_fingerprint = None
    last_text = ""
    last_ocr_sample_index = 0

    try:
        sample_index = 0
        frame_no = 0
        while True:
            ok, frame = capture.read()
            if not ok:
                break
            if frame_no % sample_step != 0:
                frame_no += 1
                continue
            sample_index += 1

            start_ms = int(round(frame_no / max(fps, 1e-6) * 1000))
            end_ms = min(video_duration_ms, start_ms + sample_duration_ms)
            fingerprint = build_frame_fingerprint(frame, area)
            should_run_ocr = True
            if last_fingerprint is not None and fingerprint is not None:
                delta = float(cv2.absdiff(last_fingerprint, fingerprint).mean())
                if delta >= 6.0:
                    should_run_ocr = True
                elif last_text:
                    should_run_ocr = sample_index - last_ocr_sample_index >= 2
                else:
                    should_run_ocr = sample_index - last_ocr_sample_index >= max(1, int(extract_frequency))
            if should_run_ocr:
                text = extract_frame_subtitle_text(ocr, frame, area)
                last_text = text
                last_ocr_sample_index = sample_index
            else:
                text = last_text
            last_fingerprint = fingerprint

            if text:
                if current_segment is not None and subtitles_are_similar(choose_segment_text(current_segment), text):
                    current_segment.end_ms = max(current_segment.end_ms, end_ms)
                    current_segment.last_positive_ms = max(current_segment.last_positive_ms, end_ms)
                    current_segment.variants[text] += 1
                else:
                    finalize_segment(segments, current_segment, sample_duration_ms)
                    current_segment = SubtitleSegment(
                        start_ms=start_ms,
                        end_ms=end_ms,
                        last_positive_ms=end_ms,
                        variants=Counter({text: 1}),
                    )
                empty_streak = 0
            else:
                if current_segment is not None:
                    empty_streak += 1
                    if empty_streak <= 1:
                        current_segment.end_ms = max(current_segment.end_ms, end_ms)
                    else:
                        finalize_segment(segments, current_segment, sample_duration_ms)
                        current_segment = None
                        empty_streak = 0

            if sample_index == 1 or sample_index == estimated_samples or sample_index % max(1, estimated_samples // 10) == 0:
                print(f"OCR_PROGRESS {sample_index}/{estimated_samples}")
            frame_no += 1
    finally:
        capture.release()

    finalize_segment(segments, current_segment, sample_duration_ms)
    segments = merge_segments(segments)
    segments = bridge_merge_repeated_segments(segments)
    segments = collapse_overlapping_segments(segments)
    if not segments:
        raise RuntimeError("no subtitles were recognized from the selected subtitle area")

    write_srt_output(output_path, segments)
    if generate_txt:
        write_txt_output(output_path, segments)


def run_final_extraction(
    input_path: Path,
    output_path: Path,
    temp_root: Path,
    temp_name: str,
    area: SubtitleArea,
    language: str,
    mode: str,
    extract_frequency: int,
    generate_txt: bool,
    keep_temp: bool,
) -> None:
    run_direct_ocr_extraction(
        input_path=input_path,
        output_path=output_path,
        area=area,
        language=language,
        mode=mode,
        extract_frequency=extract_frequency,
        generate_txt=generate_txt,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"input video not found: {input_path}")

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_name = args.output_name or f"{input_path.stem}.srt"
    output_path = output_dir / output_name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not args.overwrite:
        print("SKIP_EXISTS", str(output_path))
        return 0

    temp_root = Path(args.temp_root).expanduser().resolve()
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_name = args.temp_name or input_path.stem

    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass

    detected_area = None
    if args.subtitle_area is not None:
        detected_area = make_subtitle_area(*args.subtitle_area)
    probe_root = temp_root / "_auto_detect"
    probe_raw_path = None

    if args.auto_subtitle_area and detected_area is None:
        detected_area, image_report = run_image_auto_detect(
            input_path=input_path,
            output_path=output_path,
            temp_root=temp_root,
            temp_name=temp_name,
        )
        if detected_area is None:
            capture = cv2.VideoCapture(str(input_path))
            try:
                frame_width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
                frame_height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
            finally:
                capture.release()
            if frame_width <= 0 or frame_height <= 0:
                raise RuntimeError(f"failed to inspect video size for subtitle area fallback: {input_path}")
            detected_area = fallback_subtitle_area(frame_width, frame_height)
            preview_path = output_path.with_name(output_path.stem + ".auto_area.jpg")
            report_path = output_path.with_name(output_path.stem + ".auto_area.json")
            save_detection_preview(input_path, preview_path, detected_area)
            report = dict(image_report or {})
            report["detector"] = "generic_fallback"
            report["subtitle_area"] = [detected_area.ymin, detected_area.ymax, detected_area.xmin, detected_area.xmax]
            write_detection_report(report_path, preview_path, report)
            print("AUTO_SUBTITLE_AREA", json.dumps(report["subtitle_area"], ensure_ascii=False))
            print("AUTO_SUBTITLE_REPORT", str(report_path))

    assert detected_area is not None

    run_final_extraction(
        input_path=input_path,
        output_path=output_path,
        temp_root=temp_root,
        temp_name=temp_name,
        area=detected_area,
        language=args.language,
        mode=args.mode,
        extract_frequency=args.extract_frequency,
        generate_txt=args.generate_txt,
        keep_temp=args.keep_temp,
    )

    if not output_path.exists():
        raise RuntimeError(f"subtitle extraction finished but output file was not created: {output_path}")

    print("SUBTITLE_DONE", str(output_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
