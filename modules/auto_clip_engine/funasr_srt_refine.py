import argparse
from difflib import SequenceMatcher
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence


@dataclass
class SrtEntry:
    index: int
    start_ms: int
    end_ms: int
    text: str


@dataclass
class CharSpan:
    start_ms: int
    end_ms: int
    ch: str

    @property
    def center_ms(self) -> int:
        return (self.start_ms + self.end_ms) // 2


def parse_srt_timestamp(value: str) -> int:
    hh, mm, rest = value.split(":")
    ss, ms = rest.split(",")
    return ((int(hh) * 60 + int(mm)) * 60 + int(ss)) * 1000 + int(ms)


def format_srt_timestamp(ms: int) -> str:
    ms = max(0, int(ms))
    hh, rem = divmod(ms, 3600_000)
    mm, rem = divmod(rem, 60_000)
    ss, ms = divmod(rem, 1000)
    return f"{hh:02d}:{mm:02d}:{ss:02d},{ms:03d}"


def parse_srt(path: Path) -> List[SrtEntry]:
    content = path.read_text(encoding="utf-8-sig")
    blocks = [block.strip() for block in content.split("\n\n") if block.strip()]
    result: List[SrtEntry] = []
    for block in blocks:
        lines = [line.rstrip("\r") for line in block.splitlines() if line.strip()]
        if len(lines) < 3:
            continue
        try:
            index = int(lines[0].strip())
            time_range = lines[1].strip()
            start_text, end_text = [item.strip() for item in time_range.split("-->")]
            text = "".join(lines[2:]).strip()
        except Exception:
            continue
        result.append(
            SrtEntry(
                index=index,
                start_ms=parse_srt_timestamp(start_text),
                end_ms=parse_srt_timestamp(end_text),
                text=text,
            )
        )
    return result


def extract_sentence_info(payload: object) -> List[dict]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and isinstance(item.get("sentence_info"), list):
                return list(item["sentence_info"])
    if isinstance(payload, dict) and isinstance(payload.get("sentence_info"), list):
        return list(payload["sentence_info"])
    return []


def build_char_spans(sentence_info: Sequence[dict]) -> List[CharSpan]:
    spans: List[CharSpan] = []
    for item in sentence_info:
        text = str(item.get("text") or "").replace(" ", "").strip()
        start_ms = int(float(item.get("start") or 0))
        end_ms = int(float(item.get("end") or 0))
        if not text or end_ms <= start_ms:
            continue
        chars = [ch for ch in text if ch.strip()]
        if not chars:
            continue
        total = max(end_ms - start_ms, len(chars))
        step = total / len(chars)
        for idx, ch in enumerate(chars):
            ch_start = int(round(start_ms + idx * step))
            ch_end = int(round(start_ms + (idx + 1) * step))
            spans.append(CharSpan(start_ms=ch_start, end_ms=max(ch_start + 1, ch_end), ch=ch))
    return spans


def distance_to_window(center_ms: int, start_ms: int, end_ms: int) -> int:
    if center_ms < start_ms:
        return start_ms - center_ms
    if center_ms > end_ms:
        return center_ms - end_ms
    return 0


def slice_visible_text(value: str, start_visible: int, count: int | None = None) -> str:
    if count is not None and count <= 0:
        return ""
    chars: List[str] = []
    visible_idx = 0
    started = False
    collected = 0
    for ch in value:
        is_visible = ch.strip() and ch not in "，。？！,.!?、：；“”\"'"
        if is_visible and visible_idx < start_visible:
            visible_idx += 1
            continue
        if is_visible:
            started = True
        if started:
            chars.append(ch)
            if is_visible:
                visible_idx += 1
                collected += 1
                if count is not None and collected >= count:
                    break
    return "".join(chars).strip()


def assign_sentence_texts_to_entries(
    entries: Sequence[SrtEntry],
    sentence_info: Sequence[dict],
    *,
    margin_ms: int,
) -> List[str]:
    assigned_text: List[List[str]] = [[] for _ in entries]
    if not entries or not sentence_info:
        return ["" for _ in entries]

    entry_centers = [
        (entry.start_ms + entry.end_ms) // 2
        for entry in entries
    ]

    for item in sentence_info:
        text = str(item.get("text") or "").replace(" ", "").strip()
        start_ms = int(float(item.get("start") or 0))
        end_ms = int(float(item.get("end") or 0))
        if not text or end_ms <= start_ms:
            continue

        matched_indices = [
            idx
            for idx, center_ms in enumerate(entry_centers)
            if start_ms - margin_ms <= center_ms <= end_ms + margin_ms
        ]
        if not matched_indices:
            continue

        counts = [max(1, visible_char_count(entries[idx].text)) for idx in matched_indices]
        cursor = 0
        for pos, idx in enumerate(matched_indices):
            if pos == len(matched_indices) - 1:
                part = slice_visible_text(text, cursor)
            else:
                part = slice_visible_text(text, cursor, counts[pos])
            cursor += counts[pos]
            if part:
                assigned_text[idx].append(part)

    return ["".join(parts).strip() for parts in assigned_text]


def normalize_text(value: str) -> str:
    return (
        value.replace(" ", "")
        .replace("　", "")
        .replace("，", "")
        .replace("。", "")
        .replace("？", "")
        .replace("！", "")
        .replace(",", "")
        .replace(".", "")
        .replace("?", "")
        .replace("!", "")
        .strip()
    )


def visible_char_count(value: str) -> int:
    return len([ch for ch in value if ch.strip() and ch not in "，。？！,.!?、：；“”\"'"])


def take_visible_chars(value: str, count: int) -> str:
    if count <= 0:
        return ""
    chars: List[str] = []
    visible = 0
    for ch in value:
        chars.append(ch)
        if ch.strip() and ch not in "，。？！,.!?、：；“”\"'":
            visible += 1
            if visible >= count:
                break
    return "".join(chars).strip()


def choose_refined_text(original: str, candidate: str) -> str:
    original_norm = normalize_text(original)
    candidate_norm = normalize_text(candidate)
    if not candidate_norm:
        return original
    if candidate_norm == original_norm:
        return candidate
    original_len = visible_char_count(original_norm)
    candidate_len = visible_char_count(candidate_norm)
    if candidate_norm.startswith(original_norm):
        return take_visible_chars(candidate, original_len)
    if candidate_len < max(1, len(original_norm) // 3):
        return original
    if candidate_len > max(original_len + 2, int(original_len * 1.35) + 1):
        return original
    matcher = SequenceMatcher(a=original_norm, b=candidate_norm)
    if matcher.ratio() < 0.72:
        return original
    matching_blocks = [block for block in matcher.get_matching_blocks() if block.size > 0]
    if not matching_blocks:
        return original
    first_block = matching_blocks[0]
    if first_block.a > 1 or first_block.b > 1:
        return original
    return candidate


def write_srt(path: Path, entries: Sequence[SrtEntry]) -> None:
    chunks: List[str] = []
    for idx, entry in enumerate(entries, start=1):
        chunks.append(
            "\n".join(
                [
                    str(idx),
                    f"{format_srt_timestamp(entry.start_ms)} --> {format_srt_timestamp(entry.end_ms)}",
                    entry.text.strip(),
                ]
            )
        )
    path.write_text("\n\n".join(chunks) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Refine old SRT text with FunASR sentence_info while keeping original timings.")
    parser.add_argument("--old-srt", required=True)
    parser.add_argument("--funasr-json", required=True)
    parser.add_argument("--output-srt", required=True)
    parser.add_argument("--margin-ms", type=int, default=180)
    args = parser.parse_args()

    old_entries = parse_srt(Path(args.old_srt))
    funasr_payload = json.loads(Path(args.funasr_json).read_text(encoding="utf-8"))
    sentence_info = extract_sentence_info(funasr_payload)
    assigned_texts = assign_sentence_texts_to_entries(
        old_entries,
        sentence_info,
        margin_ms=args.margin_ms,
    )

    refined_entries: List[SrtEntry] = []
    for entry, candidate in zip(old_entries, assigned_texts):
        refined_entries.append(
            SrtEntry(
                index=entry.index,
                start_ms=entry.start_ms,
                end_ms=entry.end_ms,
                text=choose_refined_text(entry.text, candidate),
            )
        )

    write_srt(Path(args.output_srt), refined_entries)


if __name__ == "__main__":
    main()
