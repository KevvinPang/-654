from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    request_path = Path(args.request)
    output_path = Path(args.output)
    payload = _load_json(request_path)

    funasr_source = Path(str(payload.get("funasr_source") or "")).expanduser()
    if funasr_source.exists():
        sys.path.insert(0, str(funasr_source))

    from funasr import AutoModel
    from funasr.utils.postprocess_utils import rich_transcription_postprocess

    engine = str(payload.get("engine") or "funasr").strip().lower()
    if engine == "sensevoice":
        sensevoice_source = Path(str(payload.get("sensevoice_source") or "")).expanduser()
        if sensevoice_source.exists():
            sys.path.insert(0, str(sensevoice_source))
    model_kwargs = {
        "model": str(payload.get("model") or "paraformer-zh"),
        "vad_model": str(payload.get("vad_model") or "fsmn-vad"),
        "vad_kwargs": {"max_single_segment_time": int(payload.get("max_single_segment_time") or 60000)},
        "disable_update": True,
    }
    generate_kwargs = {
        "input": str(payload.get("audio_path") or ""),
        "cache": {},
        "batch_size_s": float(payload.get("batch_size_s") or 0),
        "pred_timestamp": True,
        "return_raw_text": True,
        "sentence_timestamp": True,
    }
    if engine == "sensevoice":
        sensevoice_source = Path(str(payload.get("sensevoice_source") or "")).expanduser()
        remote_code = sensevoice_source / "model.py"
        model_kwargs["trust_remote_code"] = True
        if remote_code.exists():
            model_kwargs["remote_code"] = "model"
        model_kwargs["punc_model"] = str(payload.get("punc_model") or "ct-punc")
        generate_kwargs["language"] = str(payload.get("language") or "auto")
        generate_kwargs["use_itn"] = True
        generate_kwargs["merge_vad"] = True
        generate_kwargs["merge_length_s"] = float(payload.get("merge_length_s") or 15)
    else:
        model_kwargs["punc_model"] = str(payload.get("punc_model") or "ct-punc")

    model = AutoModel(**model_kwargs)
    try:
        result = model.generate(**generate_kwargs)
    except Exception:
        if engine != "sensevoice":
            raise
        fallback_generate_kwargs = dict(generate_kwargs)
        fallback_generate_kwargs.pop("pred_timestamp", None)
        fallback_generate_kwargs.pop("sentence_timestamp", None)
        result = model.generate(**fallback_generate_kwargs)
    item = result[0] if isinstance(result, list) and result else {}
    if not isinstance(item, dict):
        item = {}

    text = rich_transcription_postprocess(str(item.get("text") or ""))
    sentence_info = item.get("sentence_info") or []
    normalized_sentence_info = []
    if isinstance(sentence_info, list):
        for sentence_item in sentence_info:
            if not isinstance(sentence_item, dict):
                continue
            normalized_item = dict(sentence_item)
            normalized_item["text"] = rich_transcription_postprocess(str(sentence_item.get("text") or ""))
            normalized_sentence_info.append(normalized_item)

    _write_json(
        output_path,
        {
            "engine": engine,
            "text": text,
            "timestamp": item.get("timestamp") or [],
            "sentence_info": normalized_sentence_info,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
