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

    model = AutoModel(
        model=str(payload.get("model") or "paraformer-zh"),
        vad_model=str(payload.get("vad_model") or "fsmn-vad"),
        punc_model=str(payload.get("punc_model") or "ct-punc"),
        vad_kwargs={"max_single_segment_time": int(payload.get("max_single_segment_time") or 60000)},
        disable_update=True,
    )
    result = model.generate(
        input=str(payload.get("audio_path") or ""),
        cache={},
        batch_size_s=float(payload.get("batch_size_s") or 0),
        pred_timestamp=True,
        return_raw_text=True,
        sentence_timestamp=True,
    )
    item = result[0] if isinstance(result, list) and result else {}
    if not isinstance(item, dict):
        item = {}

    _write_json(
        output_path,
        {
            "text": str(item.get("text") or ""),
            "timestamp": item.get("timestamp") or [],
            "sentence_info": item.get("sentence_info") or [],
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
