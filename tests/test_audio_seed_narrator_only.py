import sys
import json
import subprocess
import tempfile
import unittest
import wave
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENGINE_DIR = PROJECT_ROOT / "modules" / "auto_clip_engine"
if str(ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(ENGINE_DIR))

import drama_clone_core as core


def entry(index, start, end, text, entry_type="narration"):
    return core.SubtitleEntry(
        index=index,
        start=start,
        end=end,
        text=text,
        entry_type=entry_type,
    )


def write_silence_wav(path: Path, duration: float = 1.0, sample_rate: int = 16000):
    frame_count = int(duration * sample_rate)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(b"\x00\x00" * frame_count)


class AudioSeedNarratorOnlyTests(unittest.TestCase):
    def test_narration_only_seed_map_drops_dialogue_seed(self):
        result = core.narration_only_audio_seed_map(
            {
                1: {"label": "narration_seed", "confidence": 0.95},
                2: {"label": "dialogue_seed", "confidence": 0.99},
                3: {"label": "uncertain", "confidence": 0.90},
            }
        )

        self.assertEqual(set(result), {1})
        self.assertEqual(result[1]["label"], "narration_seed")

    def test_limited_narration_seed_map_prefers_primary_over_expansion(self):
        entries = [entry(i, float(i), float(i) + 0.8, f"line {i}") for i in range(1, 8)]
        seed_map = {
            1: {"label": "narration_seed", "confidence": 0.80, "source": "local_high_precision_seed"},
            2: {"label": "narration_seed", "confidence": 0.81, "source": "local_high_precision_seed"},
            3: {"label": "narration_seed", "confidence": 0.82, "source": "local_high_precision_seed"},
            4: {"label": "narration_seed", "confidence": 0.83, "source": "local_high_precision_seed"},
            5: {"label": "narration_seed", "confidence": 0.84, "source": "local_high_precision_seed"},
            6: {"label": "narration_seed", "confidence": 0.85, "source": "local_high_precision_seed"},
            7: {"label": "narration_seed", "confidence": 0.99, "source": "hard_speaker_expansion"},
        }

        limited = core.limit_narration_audio_seed_map(entries, seed_map, limit=6, prefer_primary=True)

        self.assertEqual(set(limited), {1, 2, 3, 4, 5, 6})
        self.assertNotIn(7, limited)

    def test_local_seed_supplement_never_adds_dialogue_seed(self):
        profiles = {
            1: core.AudioSegmentProfile(1, 0.0, 1.0, 1.0, 0.9, 0.9, -20.0, 0.1, 140.0, 0.9, (1.0, 0.0)),
            2: core.AudioSegmentProfile(2, 1.1, 2.0, 0.9, 0.9, 0.9, -20.0, 0.1, 150.0, 0.9, (0.0, 1.0)),
        }
        hints = {
            1: {"narration": 0.1, "dialogue": 4.0, "neighbor_dialogue": True},
            2: {"narration": 4.0, "dialogue": 0.1},
        }

        result = core.supplement_audio_seed_labels_locally(
            [
                entry(1, 0.0, 1.0, "你给我等着", "dialogue"),
                entry(2, 1.1, 2.0, "没想到下一秒", "narration"),
            ],
            hints,
            profiles,
            {1: {"label": "dialogue_seed", "confidence": 0.99}},
        )

        self.assertNotIn(1, result)
        self.assertIn(2, result)
        self.assertTrue(all(item["label"] == "narration_seed" for item in result.values()))

    def test_hard_narration_results_expand_secondary_seeds(self):
        entries = [
            entry(1, 0.00, 1.00, "男人站在门口"),
            entry(2, 1.10, 2.10, "没想到下一秒"),
            entry(3, 2.20, 3.20, "众人都愣住了"),
            entry(4, 3.30, 4.30, "你给我等着", "dialogue"),
        ]
        base_seed_map = {
            1: {"label": "narration_seed", "confidence": 0.96},
            4: {"label": "dialogue_seed", "confidence": 0.98},
        }
        similarity_map = {
            1: {"narration_similarity": 0.93, "dialogue_similarity": 0.0},
            2: {"narration_similarity": 0.94, "dialogue_similarity": 0.0},
            3: {"narration_similarity": 0.92, "dialogue_similarity": 0.0},
            4: {"narration_similarity": 0.18, "dialogue_similarity": 0.0},
        }

        expanded = core.expanded_narration_seed_map_from_hard_speaker_results(
            entries,
            base_seed_map,
            similarity_map,
        )

        self.assertIn(2, expanded)
        self.assertIn(3, expanded)
        self.assertNotIn(4, expanded)
        self.assertEqual(expanded[2]["source"], "hard_speaker_expansion")
        self.assertTrue(all(item["label"] == "narration_seed" for item in expanded.values()))

    def test_hard_narration_seed_expansion_reruns_once(self):
        original_speakerlab = core.build_speakerlab_similarity_map
        original_resolve = core.resolve_speechbrain_runtime
        original_run = core.run_subprocess_hidden
        calls = {"run": 0}
        seen_seed_counts = []

        def fake_speakerlab(*args, **kwargs):
            return {}

        def fake_resolve():
            return Path(sys.executable), ENGINE_DIR

        def fake_run_subprocess(command, **kwargs):
            calls["run"] += 1
            request_path = Path(command[command.index("--request") + 1])
            output_path = Path(command[command.index("--output") + 1])
            request_payload = json.loads(request_path.read_text(encoding="utf-8"))
            seed_groups = request_payload.get("seed_groups") or {}
            self.assertEqual(seed_groups.get("dialogue"), [])
            seen_seed_counts.append(len(seed_groups.get("narration") or []))
            if calls["run"] == 1:
                payload = {
                    "entries": [
                        {"index": 1, "narration_similarity": 0.93, "dialogue_similarity": 0.0},
                        {"index": 2, "narration_similarity": 0.94, "dialogue_similarity": 0.0},
                        {"index": 3, "narration_similarity": 0.92, "dialogue_similarity": 0.0},
                    ],
                    "seed_stats": {"narration": 1, "dialogue": 0},
                }
            else:
                payload = {
                    "entries": [
                        {"index": 1, "narration_similarity": 0.95, "dialogue_similarity": 0.0},
                        {"index": 2, "narration_similarity": 0.95, "dialogue_similarity": 0.0},
                    {"index": 3, "narration_similarity": 0.95, "dialogue_similarity": 0.0},
                    ],
                    "seed_stats": {"narration": 3, "dialogue": 0},
                }
            output_path.write_text(json.dumps(payload), encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        try:
            core.build_speakerlab_similarity_map = fake_speakerlab
            core.resolve_speechbrain_runtime = fake_resolve
            core.run_subprocess_hidden = fake_run_subprocess
            with tempfile.TemporaryDirectory() as temp_dir:
                audio_path = Path(temp_dir) / "audio.wav"
                write_silence_wav(audio_path, duration=3.2)
                result = core.build_speechbrain_similarity_map(
                    audio_path,
                    [
                        entry(1, 0.00, 1.00, "男人站在门口"),
                        entry(2, 1.10, 2.10, "没想到下一秒"),
                        entry(3, 2.20, 3.20, "众人都愣住了"),
                    ],
                    {
                        1: {"label": "narration_seed", "confidence": 0.96},
                        99: {"label": "dialogue_seed", "confidence": 0.99},
                    },
                )
        finally:
            core.build_speakerlab_similarity_map = original_speakerlab
            core.resolve_speechbrain_runtime = original_resolve
            core.run_subprocess_hidden = original_run

        self.assertEqual(calls["run"], 2)
        self.assertEqual(seen_seed_counts, [1, 3])
        self.assertEqual(result[2]["narration_similarity"], 0.95)

    def test_protected_audio_dialogue_survives_boundary_repair(self):
        entries = [
            entry(1, 0.0, 1.0, "前一条"),
            entry(2, 1.1, 1.4, "中间短句", "dialogue"),
            entry(3, 1.5, 2.4, "后一条"),
        ]
        repaired = core.repair_final_classification_boundaries(
            entries,
            override_meta={
                2: {"type": "dialogue", "confidence": 0.69, "source": "audio_speaker_speechbrain"}
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_unprotected_dialogue_island_is_not_swallowed_without_narration_audio(self):
        entries = [
            entry(1, 0.0, 1.0, "前一条"),
            entry(2, 1.08, 1.52, "你到底想干什么", "dialogue"),
            entry(3, 1.60, 2.5, "后一条"),
        ]

        stabilized = core.stabilize_audio_classification_runs(entries, override_meta={})

        self.assertEqual(stabilized[1].entry_type, "dialogue")

    def test_text_neighbor_hints_do_not_mark_narration_or_dialogue_neighbors(self):
        entries = [
            entry(1, 0.0, 1.0, "男人站在门口"),
            entry(2, 1.1, 2.0, "你到底想干什么", "dialogue"),
            entry(3, 2.1, 3.0, "没想到下一秒"),
        ]

        hint = core.subtitle_audio_text_hints(entries, 1)

        self.assertFalse(hint["neighbor_narration"])
        self.assertFalse(hint["neighbor_dialogue"])

    def test_first_person_text_alone_is_not_direct_dialogue_anchor(self):
        self.assertFalse(core.dialogue_like_text("我以为这件事就这样结束了"))
        self.assertFalse(core.strong_direct_dialogue_text("我以为这件事就这样结束了"))

    def test_stable_voice_cluster_can_seed_first_person_narration(self):
        entries = [
            entry(1, 0.0, 1.0, "我以为这件事就这样结束了"),
            entry(2, 1.2, 2.2, "谁知道下一秒"),
            entry(3, 2.4, 3.4, "我才发现事情不简单"),
            entry(4, 3.6, 4.6, "你到底想干什么", "dialogue"),
        ]
        profiles = {
            1: core.AudioSegmentProfile(1, 0.0, 1.0, 1.0, 0.86, 0.82, -20.0, 0.08, 150.0, 0.88, (1.0, 0.0, 0.0)),
            2: core.AudioSegmentProfile(2, 1.2, 2.2, 1.0, 0.87, 0.83, -20.0, 0.08, 151.0, 0.89, (0.99, 0.01, 0.0)),
            3: core.AudioSegmentProfile(3, 2.4, 3.4, 1.0, 0.85, 0.82, -20.0, 0.08, 149.0, 0.88, (0.98, 0.02, 0.0)),
            4: core.AudioSegmentProfile(4, 3.6, 4.6, 1.0, 0.86, 0.82, -20.0, 0.08, 190.0, 0.88, (0.0, 1.0, 0.0)),
        }
        hints = {
            1: {"narration": 0.2, "dialogue": 0.1},
            2: {"narration": 2.5, "dialogue": 0.0},
            3: {"narration": 0.2, "dialogue": 0.1},
            4: {"narration": 0.0, "dialogue": 4.0},
        }

        seeds = core.build_stable_voice_seed_map(entries, hints, profiles, limit=6)

        self.assertIn(1, seeds)
        self.assertIn(3, seeds)
        self.assertNotIn(4, seeds)
        self.assertTrue(all(item["label"] == "narration_seed" for item in seeds.values()))
        self.assertTrue(all(item["source"] == "stable_voice_cluster" for item in seeds.values()))

    def test_audio_pause_beats_visual_join_in_tts_planning(self):
        entries = [
            entry(1, 0.00, 1.00, "男人站在门口"),
            entry(2, 1.03, 2.00, "女人转身离开"),
        ]

        join_map = core.plan_tts_sentence_links_locally(
            entries,
            0.06,
            prefer_sentence_pauses=True,
            boundary_audio_pause_map={1: core.STRICT_TTS_VISUAL_JOIN_AUDIO_BLOCK_MIN_SECONDS + 0.01},
            boundary_visual_join_map={1: True},
        )

        self.assertFalse(join_map[1])

    def test_visual_break_only_forces_when_audio_is_ambiguous(self):
        entries = [
            entry(1, 0.00, 1.00, "男人刚要开口"),
            entry(2, 1.03, 2.00, "却被女人打断"),
        ]

        clear_audio_join_map = core.plan_tts_sentence_links_locally(
            entries,
            0.06,
            prefer_sentence_pauses=True,
            boundary_audio_pause_map={1: 0.0},
            boundary_visual_pause_map={1: core.STRICT_TTS_BOUNDARY_AUDIO_PAUSE_MIN_SECONDS + 0.02},
        )
        ambiguous_audio_join_map = core.plan_tts_sentence_links_locally(
            entries,
            0.06,
            prefer_sentence_pauses=True,
            boundary_audio_pause_map={1: 0.03},
            boundary_visual_pause_map={1: core.STRICT_TTS_BOUNDARY_AUDIO_PAUSE_MIN_SECONDS + 0.02},
        )

        self.assertTrue(clear_audio_join_map[1])
        self.assertFalse(ambiguous_audio_join_map[1])

    def test_strict_audio_timing_requires_audio_subtitles(self):
        settings_off = core.CloneSettings(
            reference_video=Path("ref.mp4"),
            source_dir=Path("."),
            output_dir=Path("."),
            subtitle_entries=[],
            prefer_funasr_audio_subtitles=False,
            prefer_funasr_sentence_pauses=True,
        )
        settings_on = core.CloneSettings(
            reference_video=Path("ref.mp4"),
            source_dir=Path("."),
            output_dir=Path("."),
            subtitle_entries=[],
            prefer_funasr_audio_subtitles=True,
            prefer_funasr_sentence_pauses=True,
        )

        self.assertFalse(core.strict_audio_timing_enabled(settings_off))
        self.assertTrue(core.strict_audio_timing_enabled(settings_on))

    def test_visual_internal_split_requires_strong_audio_support(self):
        self.assertGreater(core.REFERENCE_VISUAL_TTS_INTERNAL_SPLIT_MIN_AUDIO_SUPPORT, 0.57)


if __name__ == "__main__":
    unittest.main()
