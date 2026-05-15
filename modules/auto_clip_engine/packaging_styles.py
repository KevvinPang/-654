from __future__ import annotations

from copy import deepcopy
from typing import Any


OUTPUT_PACKAGING_NONE = "none"
DEFAULT_OUTPUT_PACKAGING_STYLE = "douyin_handwritten"
DEFAULT_OUTPUT_PACKAGING_FONT = "style_default"
DEFAULT_OUTPUT_PACKAGING_TITLE_ALIGN = "left"
DEFAULT_OUTPUT_PACKAGING_BOTTOM_TEXT = "热门短剧 剧情需要 请勿模仿"

OUTPUT_PACKAGING_FONTS: dict[str, dict[str, str]] = {
    DEFAULT_OUTPUT_PACKAGING_FONT: {
        "id": DEFAULT_OUTPUT_PACKAGING_FONT,
        "label": "跟随款式",
        "description": "使用当前包装款式自带的推荐字体。",
        "font_file": "",
    },
    "mashan_zheng": {
        "id": "mashan_zheng",
        "label": "马善政手写",
        "description": "粗一些的手写艺术字，适合短剧标题。",
        "font_file": "MaShanZheng-Regular.ttf",
    },
    "long_cang": {
        "id": "long_cang",
        "label": "龙藏手写",
        "description": "细长一点的书写感，画面更轻。",
        "font_file": "LongCang-Regular.ttf",
    },
    "zcool_kuaile": {
        "id": "zcool_kuaile",
        "label": "站酷快乐体",
        "description": "更醒目的综艺感字体，适合强提示。",
        "font_file": "ZCOOLKuaiLe-Regular.ttf",
    },
}

OUTPUT_PACKAGING_STYLES: dict[str, dict[str, Any]] = {
    OUTPUT_PACKAGING_NONE: {
        "id": OUTPUT_PACKAGING_NONE,
        "label": "不使用包装",
        "description": "保留原成片画面，不叠加上下遮挡和提示文字。",
        "enabled": False,
    },
    "douyin_handwritten": {
        "id": "douyin_handwritten",
        "label": "款式 B 参考图手写版",
        "description": "参考图 B 的手写标题感，上下遮挡对称，边缘柔黑但不过度压画面。",
        "enabled": True,
        "font_file": "MaShanZheng-Regular.ttf",
        "top_height_ratio": 0.245,
        "bottom_height_ratio": 0.245,
        "top_edge_height_ratio": 0.044,
        "bottom_edge_height_ratio": 0.044,
        "top_edge_alpha": 0.80,
        "bottom_edge_alpha": 0.80,
        "top_gradient_alpha": 0.66,
        "bottom_gradient_alpha": 0.66,
        "gradient_steps": 48,
        "top_gradient_curve": 0.88,
        "bottom_gradient_curve": 0.88,
        "title_size_ratio": 0.025,
        "title_min_size": 19,
        "title_max_size": 46,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.010,
        "title_border_ratio": 0.105,
        "bottom_size_ratio": 0.023,
        "bottom_min_size": 18,
        "bottom_max_size": 42,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.011,
        "bottom_align": "right",
        "bottom_border_ratio": 0.095,
    },
    "v8_deep_gradient": {
        "id": "v8_deep_gradient",
        "label": "款式 A 窄幕黑边",
        "description": "重新设计为短剧窄黑边字幕条，边缘更像烧录黑幕，不走 B/C 的雾化风格。",
        "enabled": True,
        "font_file": "LongCang-Regular.ttf",
        "top_height_ratio": 0.120,
        "bottom_height_ratio": 0.155,
        "top_edge_height_ratio": 0.060,
        "bottom_edge_height_ratio": 0.088,
        "top_edge_alpha": 0.96,
        "bottom_edge_alpha": 0.98,
        "top_gradient_alpha": 0.22,
        "bottom_gradient_alpha": 0.30,
        "gradient_steps": 46,
        "top_gradient_curve": 2.60,
        "bottom_gradient_curve": 2.20,
        "title_size_ratio": 0.023,
        "title_min_size": 18,
        "title_max_size": 42,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.010,
        "title_border_ratio": 0.100,
        "bottom_size_ratio": 0.022,
        "bottom_min_size": 18,
        "bottom_max_size": 40,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.011,
        "bottom_align": "right",
        "bottom_border_ratio": 0.090,
    },
    "narrow_premium": {
        "id": "narrow_premium",
        "label": "款式 C 电影厚雾边",
        "description": "最边缘接近实黑，向画面内大幅渐隐，适合需要强聚焦的成片。",
        "enabled": True,
        "font_file": "MaShanZheng-Regular.ttf",
        "top_height_ratio": 0.245,
        "bottom_height_ratio": 0.265,
        "top_edge_height_ratio": 0.034,
        "bottom_edge_height_ratio": 0.072,
        "top_edge_alpha": 0.92,
        "bottom_edge_alpha": 0.96,
        "top_gradient_alpha": 0.80,
        "bottom_gradient_alpha": 0.86,
        "gradient_steps": 48,
        "top_gradient_curve": 0.90,
        "bottom_gradient_curve": 0.86,
        "title_size_ratio": 0.025,
        "title_min_size": 19,
        "title_max_size": 46,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.010,
        "title_border_ratio": 0.110,
        "bottom_size_ratio": 0.023,
        "bottom_min_size": 18,
        "bottom_max_size": 42,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.011,
        "bottom_align": "right",
        "bottom_border_ratio": 0.100,
    },
    "premium_silk_balanced": {
        "id": "premium_silk_balanced",
        "label": "款式 D1 深幕平衡版",
        "description": "保留参考图 B 的手写感，上下遮挡对称，边缘更厚，整体更稳。",
        "enabled": True,
        "font_file": "MaShanZheng-Regular.ttf",
        "top_height_ratio": 0.265,
        "bottom_height_ratio": 0.265,
        "top_edge_height_ratio": 0.066,
        "bottom_edge_height_ratio": 0.066,
        "top_edge_alpha": 0.92,
        "bottom_edge_alpha": 0.92,
        "top_gradient_alpha": 0.66,
        "bottom_gradient_alpha": 0.66,
        "gradient_steps": 48,
        "top_gradient_curve": 1.02,
        "bottom_gradient_curve": 1.02,
        "title_size_ratio": 0.028,
        "title_min_size": 20,
        "title_max_size": 54,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.024,
        "title_border_ratio": 0.095,
        "bottom_size_ratio": 0.023,
        "bottom_min_size": 18,
        "bottom_max_size": 44,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.014,
        "bottom_align": "right",
        "bottom_border_ratio": 0.085,
    },
    "premium_silk_soft": {
        "id": "premium_silk_soft",
        "label": "款式 D2 深幕柔化版",
        "description": "遮挡面积略大，中间过渡更柔，适合画面偏亮的成片。",
        "enabled": True,
        "font_file": "MaShanZheng-Regular.ttf",
        "top_height_ratio": 0.285,
        "bottom_height_ratio": 0.285,
        "top_edge_height_ratio": 0.052,
        "bottom_edge_height_ratio": 0.052,
        "top_edge_alpha": 0.88,
        "bottom_edge_alpha": 0.88,
        "top_gradient_alpha": 0.72,
        "bottom_gradient_alpha": 0.72,
        "gradient_steps": 48,
        "top_gradient_curve": 0.82,
        "bottom_gradient_curve": 0.82,
        "title_size_ratio": 0.028,
        "title_min_size": 20,
        "title_max_size": 54,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.024,
        "title_border_ratio": 0.095,
        "bottom_size_ratio": 0.023,
        "bottom_min_size": 18,
        "bottom_max_size": 44,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.014,
        "bottom_align": "right",
        "bottom_border_ratio": 0.085,
    },
    "premium_mist_controlled": {
        "id": "premium_mist_controlled",
        "label": "款式 F1 收雾标准版",
        "description": "C 方向的收雾版，雾感更克制，边缘仍保持明确遮挡。",
        "enabled": True,
        "font_file": "MaShanZheng-Regular.ttf",
        "top_height_ratio": 0.235,
        "bottom_height_ratio": 0.265,
        "top_edge_height_ratio": 0.046,
        "bottom_edge_height_ratio": 0.066,
        "top_edge_alpha": 0.95,
        "bottom_edge_alpha": 0.97,
        "top_gradient_alpha": 0.68,
        "bottom_gradient_alpha": 0.76,
        "gradient_steps": 48,
        "top_gradient_curve": 1.18,
        "bottom_gradient_curve": 1.08,
        "title_size_ratio": 0.028,
        "title_min_size": 20,
        "title_max_size": 54,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.024,
        "title_border_ratio": 0.095,
        "bottom_size_ratio": 0.023,
        "bottom_min_size": 18,
        "bottom_max_size": 44,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.014,
        "bottom_align": "right",
        "bottom_border_ratio": 0.085,
    },
    "premium_mist_deep_edge": {
        "id": "premium_mist_deep_edge",
        "label": "款式 F2 收雾厚边版",
        "description": "比 F1 更压暗，底部更稳，适合亮度偏高或需要强聚焦的画面。",
        "enabled": True,
        "font_file": "MaShanZheng-Regular.ttf",
        "top_height_ratio": 0.250,
        "bottom_height_ratio": 0.280,
        "top_edge_height_ratio": 0.056,
        "bottom_edge_height_ratio": 0.080,
        "top_edge_alpha": 0.96,
        "bottom_edge_alpha": 0.98,
        "top_gradient_alpha": 0.70,
        "bottom_gradient_alpha": 0.80,
        "gradient_steps": 48,
        "top_gradient_curve": 1.30,
        "bottom_gradient_curve": 1.16,
        "title_size_ratio": 0.028,
        "title_min_size": 20,
        "title_max_size": 54,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.024,
        "title_border_ratio": 0.095,
        "bottom_size_ratio": 0.023,
        "bottom_min_size": 18,
        "bottom_max_size": 44,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.014,
        "bottom_align": "right",
        "bottom_border_ratio": 0.085,
    },
    "premium_theatre_vignette": {
        "id": "premium_theatre_vignette",
        "label": "款式 H1 剧场暗角版",
        "description": "上下面边缘带轻微暗角感，不做左右遮挡，画面更有剧场感。",
        "enabled": True,
        "font_file": "MaShanZheng-Regular.ttf",
        "top_height_ratio": 0.235,
        "bottom_height_ratio": 0.260,
        "top_edge_height_ratio": 0.052,
        "bottom_edge_height_ratio": 0.074,
        "top_edge_alpha": 0.90,
        "bottom_edge_alpha": 0.95,
        "top_gradient_alpha": 0.62,
        "bottom_gradient_alpha": 0.74,
        "gradient_steps": 48,
        "top_gradient_curve": 1.12,
        "bottom_gradient_curve": 1.02,
        "title_size_ratio": 0.028,
        "title_min_size": 20,
        "title_max_size": 54,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.024,
        "title_border_ratio": 0.095,
        "bottom_size_ratio": 0.023,
        "bottom_min_size": 18,
        "bottom_max_size": 44,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.014,
        "bottom_align": "right",
        "bottom_border_ratio": 0.085,
    },
    "premium_theatre_dense": {
        "id": "premium_theatre_dense",
        "label": "款式 H2 剧场浓影版",
        "description": "暗角更重、底部更稳，整体更戏剧化。",
        "enabled": True,
        "font_file": "LongCang-Regular.ttf",
        "top_height_ratio": 0.250,
        "bottom_height_ratio": 0.275,
        "top_edge_height_ratio": 0.058,
        "bottom_edge_height_ratio": 0.086,
        "top_edge_alpha": 0.92,
        "bottom_edge_alpha": 0.97,
        "top_gradient_alpha": 0.66,
        "bottom_gradient_alpha": 0.78,
        "gradient_steps": 48,
        "top_gradient_curve": 1.16,
        "bottom_gradient_curve": 1.05,
        "title_size_ratio": 0.029,
        "title_min_size": 20,
        "title_max_size": 56,
        "title_margin_x_ratio": 0.045,
        "title_margin_top_ratio": 0.024,
        "title_border_ratio": 0.095,
        "bottom_size_ratio": 0.024,
        "bottom_min_size": 18,
        "bottom_max_size": 45,
        "bottom_margin_x_ratio": 0.045,
        "bottom_margin_bottom_ratio": 0.014,
        "bottom_align": "right",
        "bottom_border_ratio": 0.085,
    },
}


def packaging_style_options() -> list[dict[str, str]]:
    return [
        {
            "id": style_id,
            "label": str(style.get("label") or style_id),
            "description": str(style.get("description") or ""),
        }
        for style_id, style in OUTPUT_PACKAGING_STYLES.items()
    ]


def packaging_font_options() -> list[dict[str, str]]:
    return [
        {
            "id": font_id,
            "label": str(font.get("label") or font_id),
            "description": str(font.get("description") or ""),
        }
        for font_id, font in OUTPUT_PACKAGING_FONTS.items()
    ]


def normalize_output_packaging_style(value: object) -> str:
    style_id = str(value or "").strip()
    if not style_id:
        return OUTPUT_PACKAGING_NONE
    return style_id if style_id in OUTPUT_PACKAGING_STYLES else DEFAULT_OUTPUT_PACKAGING_STYLE


def normalize_output_packaging_font(value: object) -> str:
    font_id = str(value or "").strip()
    if not font_id:
        return DEFAULT_OUTPUT_PACKAGING_FONT
    return font_id if font_id in OUTPUT_PACKAGING_FONTS else DEFAULT_OUTPUT_PACKAGING_FONT


def get_output_packaging_style(style_id: object) -> dict[str, Any]:
    normalized = normalize_output_packaging_style(style_id)
    return deepcopy(OUTPUT_PACKAGING_STYLES.get(normalized) or OUTPUT_PACKAGING_STYLES[OUTPUT_PACKAGING_NONE])


def apply_output_packaging_font(style: dict[str, Any], font_id: object) -> dict[str, Any]:
    resolved = deepcopy(style)
    normalized = normalize_output_packaging_font(font_id)
    font = OUTPUT_PACKAGING_FONTS.get(normalized) or {}
    font_file = str(font.get("font_file") or "").strip()
    if font_file:
        resolved["font_file"] = font_file
    resolved["font_id"] = normalized
    return resolved


def normalize_output_packaging_title_align(value: object) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"left", "center", "right"} else DEFAULT_OUTPUT_PACKAGING_TITLE_ALIGN
