from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


CAMPPLUS_COMMON_CONF = {
    "model_id": "iic/speech_campplus_sv_zh-cn_16k-common",
    "revision": "v1.0.0",
    "model_ckpt": "campplus_cn_common.bin",
    "embedding_model": {
        "obj": "speakerlab.models.campplus.DTDNN.CAMPPlus",
        "args": {
            "feat_dim": 80,
            "embedding_size": 192,
        },
    },
    "feature_extractor": {
        "obj": "speakerlab.process.processor.FBank",
        "args": {
            "n_mels": 80,
            "sample_rate": 16000,
            "mean_nor": True,
        },
    },
}


def _load_request(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def _write_output(path: Path, payload: Dict[str, object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


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


def _encode_segment(embedding_model, feature_extractor, signal, sample_rate: int, start: float, end: float):
    import torch

    clip = _slice_segment(signal, sample_rate, start, end)
    feat = feature_extractor(clip).unsqueeze(0)
    with torch.no_grad():
        embedding = embedding_model(feat).detach().reshape(-1).cpu()
    return embedding


def _cosine_similarity(left, right) -> float:
    import torch

    if left is None or right is None:
        return 0.0
    left_norm = torch.linalg.norm(left)
    right_norm = torch.linalg.norm(right)
    if float(left_norm) <= 1e-8 or float(right_norm) <= 1e-8:
        return 0.0
    return float(torch.dot(left, right) / (left_norm * right_norm))


def _mean_vectors(vectors: Sequence[object]):
    import torch

    if not vectors:
        return None
    return torch.mean(torch.stack(list(vectors), dim=0), dim=0)


def _best_group_similarity(vector, centroid, seed_vectors: Sequence[object]) -> float:
    best = _cosine_similarity(vector, centroid)
    for seed_vector in seed_vectors:
        best = max(best, _cosine_similarity(vector, seed_vector))
    return best


def _group_similarity_for_entry(
    entry_index: int,
    vector,
    centroid,
    seed_items: Sequence[Tuple[int, object]],
) -> float:
    if not seed_items:
        return _cosine_similarity(vector, centroid)

    filtered_seed_vectors = [seed_vector for seed_index, seed_vector in seed_items if int(seed_index) != int(entry_index)]
    if len(filtered_seed_vectors) != len(seed_items):
        effective_centroid = _mean_vectors(filtered_seed_vectors)
    else:
        effective_centroid = centroid
    return _best_group_similarity(vector, effective_centroid, filtered_seed_vectors)


def _build_model_conf(model_source: str) -> Dict[str, object]:
    if model_source == CAMPPLUS_COMMON_CONF["model_id"]:
        return dict(CAMPPLUS_COMMON_CONF)
    raise RuntimeError(f"unsupported speakerlab model: {model_source}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    payload = _load_request(Path(args.request))

    speakerlab_source = Path(str(payload.get("speakerlab_source") or "")).expanduser()
    if not speakerlab_source.exists():
        raise RuntimeError("speakerlab source path not found")
    sys.path.insert(0, str(speakerlab_source))
    os.environ.setdefault("MODELSCOPE_LOG_LEVEL", "40")

    import torch
    import torchaudio
    from speakerlab.utils.builder import build
    from speakerlab.utils.config import Config
    from speakerlab.utils.utils import download_model_from_modelscope

    audio_path = Path(str(payload.get("audio_path") or ""))
    signal, sample_rate = torchaudio.load(str(audio_path))
    signal, sample_rate = _normalize_waveform(signal, sample_rate)

    model_source = str(payload.get("model_source") or CAMPPLUS_COMMON_CONF["model_id"])
    model_cache_dir = Path(
        str(payload.get("model_cache_dir") or (Path.home() / ".cache" / "speakerlab"))
    )
    model_cache_dir.mkdir(parents=True, exist_ok=True)

    conf = _build_model_conf(model_source)
    cache_dir = download_model_from_modelscope(
        str(conf["model_id"]),
        str(conf["revision"]),
        str(model_cache_dir),
    )
    pretrained_model_path = Path(cache_dir) / str(conf["model_ckpt"])
    config = Config(conf)
    feature_extractor = build("feature_extractor", config)
    embedding_model = build("embedding_model", config)
    pretrained_state = torch.load(str(pretrained_model_path), map_location="cpu")
    embedding_model.load_state_dict(pretrained_state)
    embedding_model.eval()

    seed_groups = payload.get("seed_groups") or {}
    entries = payload.get("entries") or []

    centroids: Dict[str, object] = {}
    seed_vectors_by_label: Dict[str, List[Tuple[int, object]]] = {}
    seed_stats: Dict[str, int] = {}
    for label in ("narration", "dialogue"):
        items = seed_groups.get(label) or []
        vectors: List[Tuple[int, object]] = []
        for item in items:
            try:
                index = int(item.get("index", 0) or 0)
                vector = _encode_segment(
                    embedding_model,
                    feature_extractor,
                    signal,
                    sample_rate,
                    float(item.get("start", 0.0) or 0.0),
                    float(item.get("end", 0.0) or 0.0),
                )
            except Exception:
                continue
            vectors.append((index, vector))
        seed_vectors_by_label[label] = vectors
        seed_stats[label] = len(vectors)
        if vectors:
            centroids[label] = _mean_vectors([vector for _index, vector in vectors])

    output_entries: List[Dict[str, object]] = []
    for item in entries:
        try:
            index = int(item.get("index", 0))
            start = float(item.get("start", 0.0) or 0.0)
            end = float(item.get("end", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        try:
            vector = _encode_segment(
                embedding_model,
                feature_extractor,
                signal,
                sample_rate,
                start,
                end,
            )
        except Exception:
            continue
        output_entries.append(
            {
                "index": index,
                "narration_similarity": round(
                    _group_similarity_for_entry(
                        index,
                        vector,
                        centroids.get("narration"),
                        seed_vectors_by_label.get("narration", []),
                    ),
                    6,
                ),
                "dialogue_similarity": round(
                    _group_similarity_for_entry(
                        index,
                        vector,
                        centroids.get("dialogue"),
                        seed_vectors_by_label.get("dialogue", []),
                    ),
                    6,
                ),
            }
        )

    _write_output(
        Path(args.output),
        {
            "engine": "speakerlab_campp",
            "entries": output_entries,
            "seed_stats": seed_stats,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
