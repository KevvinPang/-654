from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


def _default_speechbrain_source() -> Optional[Path]:
    env_value = str(__import__("os").environ.get("SERVER_AUTO_CLIP_SPEECHBRAIN_SOURCE", "")).strip()
    if env_value:
        path = Path(env_value).expanduser()
        if path.exists():
            return path
    candidate = Path.home() / "Desktop" / "speechbrain-develop"
    if candidate.exists():
        return candidate
    return None


def _normalize_waveform(signal, sample_rate: int):
    import torch
    import torchaudio

    if signal.ndim == 1:
        signal = signal.unsqueeze(0)
    if signal.ndim != 2:
        raise RuntimeError(f"unexpected waveform shape: {tuple(signal.shape)}")
    if signal.shape[0] > 1:
        signal = torch.mean(signal, dim=0, keepdim=True)
    if sample_rate != 16000:
        signal = torchaudio.functional.resample(signal, sample_rate, 16000)
        sample_rate = 16000
    signal = signal.to(dtype=torch.float32)
    peak = torch.max(torch.abs(signal))
    if float(peak) > 0.0:
        signal = signal / peak.clamp(min=1e-6)
    return signal, sample_rate


def _slice_segment(signal, sample_rate: int, start: float, end: float):
    import torch

    total_samples = int(signal.shape[-1])
    duration = max(0.0, float(end) - float(start))
    pad = 0.08
    desired_min = 0.45
    start = max(0.0, float(start) - pad)
    end = min(total_samples / float(sample_rate), float(end) + pad)
    if end - start < desired_min:
        center = (start + end) * 0.5
        half = desired_min * 0.5
        start = max(0.0, center - half)
        end = min(total_samples / float(sample_rate), center + half)
        if end - start < desired_min:
            start = max(0.0, end - desired_min)
    start_index = max(0, min(total_samples, int(round(start * sample_rate))))
    end_index = max(start_index + 1, min(total_samples, int(round(end * sample_rate))))
    clip = signal[:, start_index:end_index]
    if clip.shape[-1] < int(desired_min * sample_rate):
        pad_amount = int(desired_min * sample_rate) - int(clip.shape[-1])
        clip = torch.nn.functional.pad(clip, (0, max(0, pad_amount)))
    return clip


def _encode_segment(recognizer, signal, sample_rate: int, start: float, end: float):
    import torch

    clip = _slice_segment(signal, sample_rate, start, end)
    wav = clip.squeeze(0).unsqueeze(0)
    lengths = torch.tensor([1.0], dtype=torch.float32)
    embedding = recognizer.encode_batch(wav, lengths, normalize=False)
    return embedding.reshape(-1).detach().cpu()


def _cosine_similarity(left, right) -> float:
    import torch

    if left is None or right is None:
        return 0.0
    left_norm = torch.linalg.norm(left)
    right_norm = torch.linalg.norm(right)
    if float(left_norm) <= 1e-8 or float(right_norm) <= 1e-8:
        return 0.0
    return float(torch.dot(left, right) / (left_norm * right_norm))


def _load_request(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def _write_output(path: Path, payload: Dict[str, object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    request_path = Path(args.request)
    output_path = Path(args.output)
    payload = _load_request(request_path)

    speechbrain_source = Path(str(payload.get("speechbrain_source") or "")).expanduser()
    if not speechbrain_source.exists():
        speechbrain_source = _default_speechbrain_source() or speechbrain_source
    if not speechbrain_source.exists():
        raise RuntimeError("speechbrain source path not found")
    sys.path.insert(0, str(speechbrain_source))

    import torchaudio
    from speechbrain.inference.speaker import SpeakerRecognition
    from speechbrain.utils.fetching import LocalStrategy

    audio_path = Path(str(payload.get("audio_path") or ""))
    signal, sample_rate = torchaudio.load(str(audio_path))
    signal, sample_rate = _normalize_waveform(signal, sample_rate)

    model_source = str(payload.get("model_source") or "speechbrain/spkrec-ecapa-voxceleb")
    model_cache_dir = Path(str(payload.get("model_cache_dir") or (Path.home() / ".cache" / "speechbrain" / "spkrec-ecapa-voxceleb")))
    model_cache_dir.mkdir(parents=True, exist_ok=True)
    recognizer = SpeakerRecognition.from_hparams(
        source=model_source,
        savedir=str(model_cache_dir),
        local_strategy=LocalStrategy.COPY,
    )

    seed_groups = payload.get("seed_groups") or {}
    entries = payload.get("entries") or []

    centroids: Dict[str, object] = {}
    seed_stats: Dict[str, int] = {}
    for label in ("narration", "dialogue"):
        items = seed_groups.get(label) or []
        vectors: List[object] = []
        for item in items:
            try:
                vector = _encode_segment(
                    recognizer,
                    signal,
                    sample_rate,
                    float(item.get("start", 0.0) or 0.0),
                    float(item.get("end", 0.0) or 0.0),
                )
            except Exception:
                continue
            vectors.append(vector)
        seed_stats[label] = len(vectors)
        if vectors:
            import torch

            centroids[label] = torch.mean(torch.stack(vectors, dim=0), dim=0)

    output_entries: List[Dict[str, object]] = []
    for item in entries:
        try:
            index = int(item.get("index", 0))
            start = float(item.get("start", 0.0) or 0.0)
            end = float(item.get("end", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        try:
            vector = _encode_segment(recognizer, signal, sample_rate, start, end)
        except Exception:
            continue
        output_entries.append(
            {
                "index": index,
                "narration_similarity": round(_cosine_similarity(vector, centroids.get("narration")), 6),
                "dialogue_similarity": round(_cosine_similarity(vector, centroids.get("dialogue")), 6),
            }
        )

    _write_output(
        output_path,
        {
            "entries": output_entries,
            "seed_stats": seed_stats,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
