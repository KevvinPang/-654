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

    from backend.tools.ocr import OcrRecogniser

    recogniser = OcrRecogniser()
    results: list[dict] = []
    for item in payload.get("images") or []:
        key = str(item.get("key") or "").strip()
        path = Path(str(item.get("path") or "")).expanduser()
        if not key or not path.exists():
            continue

        dt_box, rec_res = recogniser.predict(str(path))
        lines: list[dict] = []
        text_parts: list[str] = []
        score_sum = 0.0
        score_count = 0
        for box, rec_item in zip(dt_box, rec_res):
            text = str(rec_item[0] or "").strip()
            try:
                score = float(rec_item[1] or 0.0)
            except (TypeError, ValueError):
                score = 0.0
            if not text:
                continue
            lines.append(
                {
                    "text": text,
                    "score": score,
                    "box": _normalize_box(box),
                }
            )
            text_parts.append(text)
            score_sum += score
            score_count += 1

        results.append(
            {
                "key": key,
                "joined_text": "".join(text_parts),
                "avg_score": score_sum / max(1, score_count),
                "lines": lines,
            }
        )

    _write_json(output_path, {"results": results})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
