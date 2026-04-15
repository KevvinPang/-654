from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Optional, Sequence

import cv2
import numpy as np
from PIL import Image, ImageFilter, ImageOps


SUBTITLE_MASK_SAMPLE_INTERVAL = 0.9
SUBTITLE_MASK_MIN_SAMPLES = 18
SUBTITLE_MASK_MAX_SAMPLES = 72
SUBTITLE_MASK_LEFT_RATIO = 0.05
SUBTITLE_MASK_RIGHT_RATIO = 0.95
SUBTITLE_MASK_TOP_RATIO = 0.64
SUBTITLE_MASK_BOTTOM_RATIO = 0.96
SUBTITLE_MASK_MIN_CONFIDENCE = 0.18
SUBTITLE_MASK_FALLBACK_SIGNAL = 0.09
SUBTITLE_MASK_DEFAULT_X_MARGIN_RATIO = 0.06
SUBTITLE_MASK_DEFAULT_TOP_RATIO = 0.84
SUBTITLE_MASK_DEFAULT_HEIGHT_RATIO = 0.085
SUBTITLE_MASK_COMPONENT_MIN_FRAMES = 5
SUBTITLE_MASK_COMPONENT_MIN_WIDTH_RATIO = 0.11
SUBTITLE_MASK_COMPONENT_MAX_HEIGHT_RATIO = 0.24
SUBTITLE_MASK_OUTPUT_MIN_HEIGHT_RATIO = 0.090
SUBTITLE_MASK_OUTPUT_MAX_HEIGHT_RATIO = 0.155
SUBTITLE_MASK_OUTPUT_TOP_PADDING_RATIO = 0.14
SUBTITLE_MASK_OUTPUT_BOTTOM_PADDING_RATIO = 0.32

RESAMPLE_LANCZOS = getattr(getattr(Image, "Resampling", Image), "LANCZOS", Image.LANCZOS)


@dataclass
class VideoMaskRegion:
    x: int
    y: int
    width: int
    height: int
    confidence: float
    source: str


def write_cv_image(image_path: Path, image) -> bool:
    image_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = image_path.suffix or ".png"
    ok, encoded = cv2.imencode(suffix, image)
    if not ok:
        return False
    image_path.write_bytes(encoded.tobytes())
    return True


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(value, max_value))


def crop_box_ratio(
    width: int,
    height: int,
    left: float,
    top: float,
    right: float,
    bottom: float,
) -> tuple[int, int, int, int]:
    return (
        max(0, min(width - 1, int(width * left))),
        max(0, min(height - 1, int(height * top))),
        max(1, min(width, int(width * right))),
        max(1, min(height, int(height * bottom))),
    )


def crop_image_ratio(image: Image.Image, left: float, top: float, right: float, bottom: float) -> Image.Image:
    width, height = image.size
    return image.crop(crop_box_ratio(width, height, left, top, right, bottom))


def smooth_numeric_profile(values: Sequence[float], window: int) -> np.ndarray:
    normalized_window = max(1, int(window))
    if normalized_window % 2 == 0:
        normalized_window += 1
    array = np.asarray(values, dtype=np.float32)
    if normalized_window <= 1 or array.size <= 2:
        return array
    kernel = np.ones(normalized_window, dtype=np.float32) / float(normalized_window)
    return np.convolve(array, kernel, mode="same")


def build_video_sample_timestamps(duration: float) -> list[float]:
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

    timestamps: list[float] = []
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


def extract_video_sample_frames(video_path: Path, sample_dir: Path) -> tuple[list[Path], dict]:
    sample_dir.mkdir(parents=True, exist_ok=True)
    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        raise RuntimeError(f"failed to open video for sampling: {video_path}")
    try:
        fps = float(capture.get(cv2.CAP_PROP_FPS) or 25.0)
        frame_count = int(round(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0))
        duration = frame_count / fps if fps > 0 and frame_count > 0 else 0.0
        timestamps = build_video_sample_timestamps(duration)
        sample_paths: list[Path] = []
        for index, timestamp in enumerate(timestamps, start=1):
            frame_no = max(0, int(round(timestamp * fps)))
            capture.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
            ok, frame = capture.read()
            if not ok:
                continue
            sample_path = sample_dir / f"sample_{index:03d}.jpg"
            if write_cv_image(sample_path, frame):
                sample_paths.append(sample_path)
        width = int(round(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or 0))
        height = int(round(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0))
    finally:
        capture.release()
    return sample_paths, {"width": width, "height": height, "duration": duration}


def build_subtitle_detection_mask(image: Image.Image):
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
) -> Optional[tuple[int, int, int, int, float]]:
    if mask is None or not getattr(mask, "size", 0):
        return None

    mask_height, mask_width = mask.shape
    if mask_height < 20 or mask_width < 32:
        return None

    merged = mask.astype(np.uint8) * 255
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
    best_box: Optional[tuple[int, int, int, int, float]] = None
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


def find_profile_segments(profile: Sequence[float], threshold: float) -> list[tuple[int, int, float]]:
    segments: list[tuple[int, int, float]] = []
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
    return VideoMaskRegion(x=x, y=y, width=region_width, height=region_height, confidence=confidence, source="fallback")


def tighten_subtitle_output_region(region: VideoMaskRegion, video_width: int, video_height: int) -> VideoMaskRegion:
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
        y1 -= int(math.ceil(missing * 0.42))
        y2 += int(math.floor(missing * 0.58))
    elif current_height > max_height:
        excess = current_height - max_height
        trim_top = int(round(excess * 0.66))
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
        x=0,
        y=y1,
        width=max(1, video_width),
        height=max(1, y2 - y1),
        confidence=region.confidence,
        source=f"{region.source}-band",
    )


def refine_subtitle_output_region_with_masks(
    region: VideoMaskRegion,
    masks: Sequence[np.ndarray],
    *,
    crop_top: int,
    scale_y: float,
    focus_x1: int,
    focus_x2: int,
    video_width: int,
    video_height: int,
    log_func: Optional[Callable[[str], None]] = None,
) -> VideoMaskRegion:
    if not masks:
        return region

    mask_height, mask_width = masks[0].shape
    if mask_height <= 0 or mask_width <= 0:
        return region

    local_x1 = max(0, min(mask_width - 1, int(focus_x1)))
    local_x2 = max(local_x1 + 1, min(mask_width, int(focus_x2)))
    min_focus_width = max(12, int(round(mask_width * 0.20)))
    if local_x2 - local_x1 < min_focus_width:
        local_x1 = max(0, int(round(mask_width * 0.08)))
        local_x2 = min(mask_width, int(round(mask_width * 0.92)))

    row_profiles = np.vstack(
        [
            mask[:, local_x1:local_x2].mean(axis=1) if local_x2 > local_x1 else mask.mean(axis=1)
            for mask in masks
        ]
    ).astype(np.float32)
    if not getattr(row_profiles, "size", 0):
        return region

    row_presence_threshold = max(0.018, float(np.quantile(row_profiles, 0.55)) * 1.12)
    row_presence = np.mean(row_profiles >= row_presence_threshold, axis=0).astype(np.float32)
    row_strength = np.quantile(row_profiles, 0.72, axis=0).astype(np.float32)
    if float(row_strength.max()) <= 1e-6:
        return region
    row_strength = row_strength / max(1e-6, float(row_strength.max()))
    row_profile = smooth_numeric_profile(row_presence * 0.54 + row_strength * 0.46, max(3, int(mask_height * 0.03)))

    local_y1 = max(0, min(mask_height - 1, int(round((region.y - crop_top) / max(1e-6, scale_y)))))
    local_y2 = max(local_y1 + 1, min(mask_height, int(round((region.y + region.height - crop_top) / max(1e-6, scale_y)))))
    band_height = max(1, local_y2 - local_y1)
    edge_window = max(3, int(round(band_height * 0.14)))
    scan_window = max(12, int(round(band_height * 0.38)))

    band_slice = row_profile[local_y1:local_y2]
    strong_threshold = max(
        0.030,
        float(np.quantile(row_profile, 0.68)) * 0.48,
        float(band_slice.mean()) * 0.62 if band_slice.size else 0.030,
    )
    support_threshold = max(0.022, strong_threshold * 0.72)

    top_edge_slice = row_profile[local_y1 : min(mask_height, local_y1 + edge_window)]
    bottom_edge_slice = row_profile[max(0, local_y2 - edge_window) : local_y2]
    top_edge_touch = bool(top_edge_slice.size and float(top_edge_slice.max()) >= strong_threshold)
    bottom_edge_touch = bool(bottom_edge_slice.size and float(bottom_edge_slice.max()) >= strong_threshold)
    if not top_edge_touch and not bottom_edge_touch:
        return region

    expanded_y1 = local_y1
    expanded_y2 = local_y2
    if top_edge_touch:
        probe_start = max(0, local_y1 - scan_window)
        probe_slice = row_profile[probe_start:local_y1]
        support_slice = row_presence[probe_start:local_y1]
        top_candidates = np.flatnonzero((probe_slice >= support_threshold) | (support_slice >= 0.08))
        if top_candidates.size:
            expanded_y1 = probe_start + int(top_candidates[0])
        else:
            expanded_y1 = max(0, local_y1 - edge_window)
    if bottom_edge_touch:
        probe_end = min(mask_height, local_y2 + scan_window)
        probe_slice = row_profile[local_y2:probe_end]
        support_slice = row_presence[local_y2:probe_end]
        bottom_candidates = np.flatnonzero((probe_slice >= support_threshold) | (support_slice >= 0.08))
        if bottom_candidates.size:
            expanded_y2 = local_y2 + int(bottom_candidates[-1] + 1)
        else:
            expanded_y2 = min(mask_height, local_y2 + edge_window)

    expanded_height = max(1, expanded_y2 - expanded_y1)
    if top_edge_touch:
        expanded_y1 = max(0, expanded_y1 - max(2, int(round(expanded_height * 0.04))))
    if bottom_edge_touch:
        expanded_y2 = min(mask_height, expanded_y2 + max(2, int(round(expanded_height * 0.08))))

    refined_y1 = crop_top + int(round(expanded_y1 * scale_y))
    refined_y2 = crop_top + int(round(expanded_y2 * scale_y))
    refined_y1 = max(0, min(video_height - 1, min(int(region.y), refined_y1)))
    refined_y2 = max(refined_y1 + 1, min(video_height, max(int(region.y + region.height), refined_y2)))
    refined_region = VideoMaskRegion(
        x=0,
        y=refined_y1,
        width=max(1, video_width),
        height=max(1, refined_y2 - refined_y1),
        confidence=region.confidence,
        source=f"{region.source}-edges",
    )
    if log_func:
        touched_parts: list[str] = []
        if top_edge_touch:
            touched_parts.append("上沿")
        if bottom_edge_touch:
            touched_parts.append("下沿")
        log_func(
            "  Subtitle region edge refine: "
            + "/".join(touched_parts)
            + f" 触边，扩展到 x={refined_region.x}, y={refined_region.y}, "
            + f"w={refined_region.width}, h={refined_region.height}, confidence {refined_region.confidence:.2f}"
        )
    return refined_region


def detect_subtitle_region(
    video_path: Path,
    work_dir: Path,
    log_func: Optional[Callable[[str], None]] = None,
) -> tuple[Optional[VideoMaskRegion], dict]:
    sample_dir = work_dir / "subtitle_mask_samples"
    sample_paths, profile = extract_video_sample_frames(video_path, sample_dir)
    width = int(profile["width"])
    height = int(profile["height"])
    report = {"frame_width": width, "frame_height": height, "sample_count": len(sample_paths), "method": "image_mask"}

    if width < 160 or height < 160:
        report["reason"] = "video_too_small"
        return None, report

    masks = [load_subtitle_detection_mask(str(path.resolve())) for path in sample_paths]
    masks = [mask for mask in masks if mask is not None and getattr(mask, "size", 0)]
    report["usable_sample_count"] = len(masks)
    if len(masks) < 6:
        report["reason"] = "too_few_usable_samples"
        return None, report

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
    component_boxes: list[tuple[int, int, int, int, float]] = []
    sample_row_profiles: list[np.ndarray] = []
    sample_band_tops: list[int] = []
    sample_band_bottoms: list[int] = []
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

    sample_hint_top: Optional[int] = None
    sample_hint_bottom: Optional[int] = None
    if len(sample_band_tops) >= min_component_frames:
        sample_hint_top = int(round(float(np.quantile(sample_band_tops, 0.18))))
        sample_hint_bottom = int(round(float(np.quantile(sample_band_bottoms, 0.82))))

    row_floor = max(0.010, float(np.quantile(row_profiles, 0.35)))
    row_presence = np.mean(row_profiles >= max(0.022, row_floor * 1.7), axis=0)
    row_strength = np.quantile(row_profiles, 0.70, axis=0)
    if float(row_strength.max()) <= 1e-6:
        report["reason"] = "row_strength_empty"
        return None, report
    row_strength = row_strength / max(1e-6, float(row_strength.max()))
    row_profile = row_presence * 0.60 + row_strength * 0.40
    row_profile *= np.linspace(0.82, 1.42, len(row_profile), dtype=np.float32)
    row_profile = smooth_numeric_profile(row_profile, max(5, int(len(row_profile) * 0.05)))

    peak_row = int(np.argmax(row_profile))
    strong_threshold = max(0.16, float(np.quantile(row_profile, 0.84)) * 0.92, float(row_profile[peak_row]) * 0.70)
    best_top = peak_row
    best_bottom = peak_row + 1
    while best_top > 0 and row_profile[best_top - 1] >= strong_threshold:
        best_top -= 1
    while best_bottom < len(row_profile) and row_profile[best_bottom] >= strong_threshold:
        best_bottom += 1

    support_threshold = max(0.050, float(np.quantile(row_profile, 0.70)) * 0.45, float(row_profile[peak_row]) * 0.24)
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

    leak_threshold = max(0.032, float(np.quantile(row_profile, 0.62)) * 0.42, float(row_profile[peak_row]) * 0.16)
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
    column_probe_bottom = min(mask_height, best_bottom + max(4, int(round((best_bottom - best_top) * 0.75))))
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
    selected_segment: Optional[tuple[int, int, float]] = None
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

    report.update(
        {
            "band_signal": band_signal,
            "presence_signal": presence_signal,
            "column_signal": column_signal,
            "confidence": confidence,
            "component_confidence": component_confidence,
        }
    )

    if confidence < SUBTITLE_MASK_MIN_CONFIDENCE:
        peak_signal = max(confidence, float(row_profile.max()))
        if peak_signal < SUBTITLE_MASK_FALLBACK_SIGNAL:
            report["reason"] = "low_confidence"
            return None, report
        fallback = fallback_subtitle_mask_region(
            width,
            height,
            detected_top=detected_y1,
            detected_bottom=detected_y2,
            confidence=peak_signal,
        )
        report["region"] = asdict(fallback)
        report["reason"] = "fallback"
        return fallback, report

    region = VideoMaskRegion(
        x=detected_x1,
        y=detected_y1,
        width=max(1, detected_x2 - detected_x1),
        height=max(1, detected_y2 - detected_y1),
        confidence=confidence,
        source="auto",
    )
    region = tighten_subtitle_output_region(region, width, height)
    region = refine_subtitle_output_region_with_masks(
        region,
        masks,
        crop_top=crop_top,
        scale_y=scale_y,
        focus_x1=mask_x1,
        focus_x2=mask_x2,
        video_width=width,
        video_height=height,
        log_func=log_func,
    )
    report["region"] = asdict(region)
    if log_func:
        log_func(
            "  Subtitle region detected: "
            f"x={region.x}, y={region.y}, w={region.width}, h={region.height}, confidence {region.confidence:.2f}"
        )
    return region, report


def save_detection_preview(video_path: Path, image_path: Path, region: VideoMaskRegion) -> None:
    capture = cv2.VideoCapture(str(video_path))
    try:
        frame_count = int(round(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0))
        frame_no = max(0, int(frame_count * 0.2))
        capture.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
        ok, frame = capture.read()
        if not ok:
            capture.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ok, frame = capture.read()
        if not ok:
            return
        cv2.rectangle(
            frame,
            (region.x, region.y),
            (min(frame.shape[1] - 1, region.x + region.width), min(frame.shape[0] - 1, region.y + region.height)),
            (0, 255, 0),
            2,
        )
        cv2.putText(
            frame,
            f"Subtitle Region ({region.source}, {region.confidence:.2f})",
            (region.x, max(20, region.y - 10)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 255, 0),
            2,
        )
        write_cv_image(image_path, frame)
    finally:
        capture.release()
