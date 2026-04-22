from __future__ import annotations

import argparse
import sys
from pathlib import Path

import librosa
import numpy as np
import soundfile as sf
import torch


def _make_padding(width: int, cropsize: int, offset: int) -> tuple[int, int, int]:
    left = offset
    roi_size = cropsize - offset * 2
    if roi_size == 0:
        roi_size = cropsize
    right = roi_size - (width % roi_size) + left
    return left, right, roi_size


def _match_shape(wave: np.ndarray, target: np.ndarray) -> np.ndarray:
    if wave.shape == target.shape:
        return wave
    output = np.zeros_like(target)
    channels = min(output.shape[0], wave.shape[0])
    samples = min(output.shape[1], wave.shape[1])
    output[:channels, :samples] = wave[:channels, :samples]
    return output


def run_denoise(input_path: Path, output_path: Path, uvr_root: Path, model_path: Path) -> None:
    sys.path.insert(0, str(uvr_root))
    from lib_v5.vr_network import nets_new  # type: ignore

    wave, sample_rate = sf.read(str(input_path), always_2d=True, dtype="float32")
    if sample_rate != 44100:
        raise RuntimeError(f"UVR denoise expects 44100 Hz wav, got {sample_rate}")

    source = np.asfortranarray(wave.T)
    if source.shape[0] == 1:
        source = np.repeat(source, 2, axis=0)

    n_fft = 2048
    hop_length = 1024
    cropsize = 256
    batchsize = 2

    model = nets_new.CascadedNet(n_fft, nout=16, nout_lstm=128)
    state = torch.load(str(model_path), map_location="cpu")
    model.load_state_dict(state)
    model.to("cpu")
    model.eval()

    spec_left = librosa.stft(np.asfortranarray(source[0]), n_fft=n_fft, hop_length=hop_length)
    spec_right = librosa.stft(np.asfortranarray(source[1]), n_fft=n_fft, hop_length=hop_length)
    source_spec = np.asfortranarray([spec_left, spec_right])
    source_mag = np.abs(source_spec)
    source_phase = np.angle(source_spec)

    frame_count = source_mag.shape[2]
    pad_left, _, roi_size = _make_padding(frame_count, cropsize, model.offset)
    mag_padded = np.pad(source_mag, ((0, 0), (0, 0), (pad_left, roi_size - (frame_count % roi_size) + pad_left)), mode="constant")
    max_mag = max(float(mag_padded.max()), 1e-8)
    mag_padded /= max_mag

    patches = (mag_padded.shape[2] - 2 * model.offset) // roi_size
    dataset = []
    for patch_index in range(patches):
        start = patch_index * roi_size
        dataset.append(mag_padded[:, :, start : start + cropsize])
    if not dataset:
        raise RuntimeError("UVR denoise input is too short")
    dataset_array = np.asarray(dataset, dtype=np.float32)

    with torch.no_grad():
        masks = []
        for start in range(0, patches, batchsize):
            batch = torch.from_numpy(dataset_array[start : start + batchsize]).to("cpu")
            prediction = model.predict_mask(batch)
            prediction = prediction.detach().cpu().numpy()
            prediction = np.concatenate(prediction, axis=2)
            masks.append(prediction)
        mask = np.concatenate(masks, axis=2)

    mask = mask[:, :, :frame_count]
    denoised_spec = (1 - mask) * source_mag * np.exp(1.0j * source_phase)
    denoised_left = librosa.istft(np.asfortranarray(denoised_spec[0]), hop_length=hop_length)
    denoised_right = librosa.istft(np.asfortranarray(denoised_spec[1]), hop_length=hop_length)
    denoised = np.asfortranarray([denoised_left, denoised_right])
    denoised = _match_shape(denoised, source)
    denoised = np.clip(denoised.T, -1.0, 1.0)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    sf.write(str(output_path), denoised, sample_rate)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run UVR DeNoise Lite for pause-analysis audio.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--uvr-root", required=True)
    parser.add_argument("--model", required=True)
    args = parser.parse_args()

    run_denoise(
        input_path=Path(args.input),
        output_path=Path(args.output),
        uvr_root=Path(args.uvr_root),
        model_path=Path(args.model),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
