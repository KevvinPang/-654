from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import re
import traceback
from pathlib import Path
from collections.abc import Iterable as IterableABC
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_debug_event(path: Path, stage: str, **payload: object) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        record: Dict[str, object] = {
            "time": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
        }
        record.update(payload)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", "", text)
    return text


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_text(result: object) -> str:
    for key in ("text", "transcription", "content"):
        if hasattr(result, key):
            text = _clean_text(getattr(result, key))
            if text:
                return text
        if isinstance(result, dict):
            text = _clean_text(result.get(key))
            if text:
                return text
    return _clean_text(result)


def _extract_language(result: object) -> str:
    if hasattr(result, "language"):
        return str(getattr(result, "language") or "")
    if isinstance(result, dict):
        return str(result.get("language") or "")
    return ""


def _iter_timestamp_items(raw_timestamps: object) -> Iterable[object]:
    if raw_timestamps is None:
        return []
    items = getattr(raw_timestamps, "items", None)
    if isinstance(items, (list, tuple)):
        return items
    if isinstance(raw_timestamps, (list, tuple)):
        return raw_timestamps
    if isinstance(raw_timestamps, IterableABC) and not isinstance(raw_timestamps, (str, bytes, dict)):
        return raw_timestamps
    return []


def _extract_token_timestamp(item: object) -> Optional[Tuple[str, float, float]]:
    if isinstance(item, dict):
        text = _clean_text(item.get("text") or item.get("word") or item.get("char") or item.get("token"))
        start = _to_float(
            item.get("start_time", item.get("start", item.get("begin", item.get("begin_time")))),
            -1.0,
        )
        end = _to_float(
            item.get("end_time", item.get("end", item.get("finish", item.get("end_time")))),
            -1.0,
        )
    else:
        text = _clean_text(getattr(item, "text", "") or getattr(item, "word", "") or getattr(item, "char", ""))
        start = _to_float(
            getattr(item, "start_time", getattr(item, "start", getattr(item, "begin", -1.0))),
            -1.0,
        )
        end = _to_float(
            getattr(item, "end_time", getattr(item, "end", getattr(item, "finish", -1.0))),
            -1.0,
        )
    if not text or start < 0 or end <= start:
        return None
    return text, start, end


def _sentence_info_from_token_timestamps(
    text: str,
    raw_timestamps: object,
    fallback_start: float = 0.0,
    fallback_end: float = 0.0,
) -> Tuple[List[Dict[str, object]], List[List[float]]]:
    token_items = [
        item
        for item in (_extract_token_timestamp(raw) for raw in _iter_timestamp_items(raw_timestamps))
        if item is not None
    ]
    if not token_items:
        return [], []

    timestamp_pairs = [[round(start * 1000.0, 3), round(end * 1000.0, 3)] for _text, start, end in token_items]
    starts = [start for _token, start, _end in token_items]
    ends = [end for _token, _start, end in token_items]
    full_start = min(starts)
    full_end = max(ends)
    if full_end <= full_start:
        full_start = fallback_start
        full_end = fallback_end

    average_token_duration = max(0.04, (full_end - full_start) / max(1, len(token_items)))
    gap_threshold = max(0.14, min(0.32, average_token_duration * 1.35))
    punctuation_gap_threshold = max(0.06, min(gap_threshold, average_token_duration * 0.75))
    terminal_punctuation = set("\u3002\uff01\uff1f!?\uff1b;")
    soft_punctuation = set("\uff0c\u3001,:\uff1a")

    def should_split(index: int) -> bool:
        left_text, _left_start, left_end = token_items[index]
        _right_text, right_start, _right_end = token_items[index + 1]
        gap = max(0.0, right_start - left_end)
        if gap >= gap_threshold:
            return True
        if left_text and left_text[-1] in terminal_punctuation and gap >= punctuation_gap_threshold:
            return True
        if left_text and left_text[-1] in soft_punctuation and gap >= max(punctuation_gap_threshold, 0.10):
            return True
        return False

    def build_segment(start_index: int, end_index: int) -> Optional[Dict[str, object]]:
        segment_items = token_items[start_index:end_index]
        if not segment_items:
            return None
        segment_text = _clean_text("".join(token for token, _start, _end in segment_items))
        if not segment_text:
            return None
        segment_start = min(start for _token, start, _end in segment_items)
        segment_end = max(end for _token, _start, end in segment_items)
        if segment_end <= segment_start:
            return None
        segment_pairs = [
            [round(start * 1000.0, 3), round(end * 1000.0, 3)]
            for _token, start, end in segment_items
        ]
        return {
            "start": round(max(0.0, segment_start) * 1000.0, 3),
            "end": round(max(max(0.0, segment_start), segment_end) * 1000.0, 3),
            "text": segment_text,
            "timestamp": segment_pairs,
        }

    sentence_info: List[Dict[str, object]] = []
    segment_start_index = 0
    for index in range(len(token_items) - 1):
        if not should_split(index):
            continue
        segment = build_segment(segment_start_index, index + 1)
        if segment is not None:
            sentence_info.append(segment)
        segment_start_index = index + 1
    tail_segment = build_segment(segment_start_index, len(token_items))
    if tail_segment is not None:
        sentence_info.append(tail_segment)

    if not sentence_info:
        joined_text = _clean_text("".join(token for token, _start, _end in token_items)) or text
        sentence_info = [
            {
                "start": round(max(0.0, full_start) * 1000.0, 3),
                "end": round(max(max(0.0, full_start), full_end) * 1000.0, 3),
                "text": joined_text,
                "timestamp": timestamp_pairs,
            }
        ]
    return sentence_info, timestamp_pairs


def _sentence_info_from_plain_text(text: str, audio_duration: float) -> List[Dict[str, object]]:
    if not text:
        return []
    end = max(0.05, audio_duration)
    return [
        {
            "start": 0.0,
            "end": round(end * 1000.0, 3),
            "text": text,
            "timestamp": [],
        }
    ]


def _audio_duration_seconds(audio_path: Path) -> float:
    try:
        import wave

        if audio_path.suffix.lower() == ".wav":
            with wave.open(str(audio_path), "rb") as handle:
                frame_rate = float(handle.getframerate() or 0)
                if frame_rate > 0:
                    return float(handle.getnframes()) / frame_rate
    except Exception:
        pass
    return 0.0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    request_path = Path(args.request)
    output_path = Path(args.output)
    debug_path = output_path.with_suffix(output_path.suffix + ".debug.jsonl")
    error_path = output_path.with_suffix(output_path.suffix + ".error.json")
    payload = _load_json(request_path)

    audio_path = Path(str(payload.get("audio_path") or "")).expanduser()
    model_path = str(payload.get("model_path") or "").strip()
    aligner_path = str(payload.get("aligner_path") or "").strip()
    language = str(payload.get("language") or "Chinese").strip() or "Chinese"
    return_time_stamps = bool(payload.get("return_time_stamps", True))
    max_new_tokens = int(payload.get("max_new_tokens") or 2048)
    max_batch_size = int(payload.get("max_inference_batch_size") or 1)
    device_map = str(payload.get("device_map") or "cpu").strip() or "cpu"
    dtype_name = str(payload.get("dtype") or "bfloat16").strip().lower()

    _append_debug_event(
        debug_path,
        "request_loaded",
        audio_path=str(audio_path),
        model_path=model_path,
        aligner_path=aligner_path,
        language=language,
        return_time_stamps=return_time_stamps,
        device_map=device_map,
        dtype=dtype_name,
    )

    try:
        import torch
        from qwen_asr import Qwen3ASRModel

        dtype = torch.float32 if dtype_name in {"float32", "fp32"} else torch.bfloat16
        model_kwargs: Dict[str, object] = {
            "dtype": dtype,
            "device_map": device_map,
            "max_inference_batch_size": max_batch_size,
            "max_new_tokens": max_new_tokens,
        }
        if return_time_stamps and aligner_path and Path(aligner_path).exists():
            model_kwargs["forced_aligner"] = aligner_path
            model_kwargs["forced_aligner_kwargs"] = {
                "dtype": dtype,
                "device_map": device_map,
            }

        _append_debug_event(debug_path, "model_load_start", has_forced_aligner="forced_aligner" in model_kwargs)
        model = Qwen3ASRModel.from_pretrained(model_path, **model_kwargs)
        _append_debug_event(debug_path, "model_loaded", has_forced_aligner=bool(getattr(model, "forced_aligner", None)))
        _append_debug_event(debug_path, "transcribe_start")
        results = model.transcribe(
            audio=str(audio_path),
            language=language,
            return_time_stamps=bool("forced_aligner" in model_kwargs),
        )
        _append_debug_event(
            debug_path,
            "transcribe_done",
            result_type=type(results).__name__,
            result_count=len(results) if isinstance(results, list) else None,
        )
        result = results[0] if isinstance(results, list) and results else results

        text = _extract_text(result)
        language_result = _extract_language(result) or language
        raw_timestamps = getattr(result, "time_stamps", None)
        if raw_timestamps is None and isinstance(result, dict):
            raw_timestamps = result.get("time_stamps") or result.get("timestamps")

        duration = _audio_duration_seconds(audio_path)
        sentence_info, timestamp = _sentence_info_from_token_timestamps(text, raw_timestamps, 0.0, duration)
        if not sentence_info:
            sentence_info = _sentence_info_from_plain_text(text, duration)
            timestamp = []

        _write_json(
            output_path,
            {
                "engine": "qwen_asr",
                "language": language_result,
                "text": text,
                "timestamp": timestamp,
                "sentence_info": sentence_info,
            },
        )
        _append_debug_event(
            debug_path,
            "output_written",
            text_length=len(text),
            sentence_count=len(sentence_info),
            timestamp_count=len(timestamp),
        )
        return 0
    except BaseException as exc:
        error_payload = {
            "engine": "qwen_asr",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
        _append_debug_event(debug_path, "error", **error_payload)
        _write_json(error_path, error_payload)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
