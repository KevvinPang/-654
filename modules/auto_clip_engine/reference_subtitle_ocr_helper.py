from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional, Sequence


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_box(points: object) -> list[int]:
    xs: list[int] = []
    ys: list[int] = []
    for point in points if isinstance(points, list) else []:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        try:
            xs.append(int(round(float(point[0]))))
            ys.append(int(round(float(point[1]))))
        except (TypeError, ValueError):
            continue
    if not xs or not ys:
        return [0, 0, 0, 0]
    return [min(xs), min(ys), max(xs), max(ys)]


def _append_line(
    lines: list[dict],
    text_parts: list[str],
    score_state: list[float],
    box: object,
    text: object,
    score_value: object,
) -> None:
    text_value = str(text or "").strip()
    if not text_value:
        return
    try:
        score = float(score_value or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    lines.append(
        {
            "text": text_value,
            "score": score,
            "box": _normalize_box(box),
        }
    )
    text_parts.append(text_value)
    score_state[0] += score
    score_state[1] += 1.0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    request_path = Path(args.request)
    output_path = Path(args.output)
    payload = _load_json(request_path)

    subtitle_source = Path(str(payload.get("subtitle_extractor_source") or "")).expanduser()
    if subtitle_source.exists():
        sys.path.insert(0, str(subtitle_source))

    os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")

    rapid_ocr = None
    recogniser = None
    try:
        from rapidocr_onnxruntime import RapidOCR

        rapid_ocr = RapidOCR()
    except Exception:
        from backend.tools.ocr import OcrRecogniser

        recogniser = OcrRecogniser()

    results: list[dict] = []
    for item in payload.get("images") or []:
        key = str(item.get("key") or "").strip()
        path = Path(str(item.get("path") or "")).expanduser()
        if not key or not path.exists():
            continue

        lines: list[dict] = []
        text_parts: list[str] = []
        score_state = [0.0, 0.0]
        if rapid_ocr is not None:
            rec_result, _ = rapid_ocr(str(path), use_cls=False)
            for rec_item in rec_result or []:
                if not isinstance(rec_item, (list, tuple)) or len(rec_item) < 2:
                    continue
                box = rec_item[0]
                text = rec_item[1]
                score = rec_item[2] if len(rec_item) >= 3 else 0.0
                _append_line(lines, text_parts, score_state, box, text, score)
        else:
            dt_box, rec_res = recogniser.predict(str(path))
            for box, rec_item in zip(dt_box, rec_res):
                text = rec_item[0] if isinstance(rec_item, (list, tuple)) and rec_item else ""
                score = rec_item[1] if isinstance(rec_item, (list, tuple)) and len(rec_item) >= 2 else 0.0
                _append_line(lines, text_parts, score_state, box, text, score)

        results.append(
            {
                "key": key,
                "joined_text": "".join(text_parts),
                "avg_score": score_state[0] / max(1.0, score_state[1]),
                "lines": lines,
            }
        )

    _write_json(output_path, {"results": results})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
