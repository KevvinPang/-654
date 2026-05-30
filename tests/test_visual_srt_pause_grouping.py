import sys
import unittest
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


class VisualSrtPauseGroupingTests(unittest.TestCase):
    def test_visual_same_line_merges_mid_word_audio_splits_before_tts(self):
        audio_entries = [
            entry(1, 15.00, 15.63, "直接将烟"),
            entry(2, 15.73, 16.33, "头精准"),
            entry(3, 16.40, 17.23, "弹在混混身上"),
        ]
        visual_entries = [
            entry(1, 15.00, 17.20, "直接将烟头精准弹在混混身上"),
        ]

        merged, fix_count = core.build_primary_entries_from_funasr_and_visual(
            audio_entries,
            visual_entries,
        )

        self.assertGreaterEqual(fix_count, 1)
        self.assertEqual(len(merged), 1)
        self.assertEqual(core.normalize_subtitle_text(merged[0].text), "直接将烟头精准弹在混混身上")

    def test_visual_same_line_merges_repeated_character_word_split(self):
        audio_entries = [
            entry(1, 26.00, 27.00, "便将所有混"),
            entry(2, 27.02, 28.40, "混尽数打倒在地"),
        ]
        visual_entries = [
            entry(1, 26.00, 28.40, "便将所有混混尽数打倒在地"),
        ]

        merged, _ = core.build_primary_entries_from_funasr_and_visual(audio_entries, visual_entries)

        self.assertEqual(len(merged), 1)
        self.assertEqual(core.normalize_subtitle_text(merged[0].text), "便将所有混混尽数打倒在地")

    def test_visual_same_line_merges_short_sentence_fragments(self):
        audio_entries = [
            entry(1, 10.00, 10.42, "按道理"),
            entry(2, 10.56, 10.92, "你改叫我"),
            entry(3, 11.04, 11.36, "一声姐夫"),
        ]
        visual_entries = [
            entry(1, 10.00, 11.36, "按道理你改叫我一声姐夫"),
        ]

        merged, fix_count = core.build_primary_entries_from_funasr_and_visual(
            audio_entries,
            visual_entries,
        )

        self.assertGreaterEqual(fix_count, 1)
        self.assertEqual(len(merged), 1)
        self.assertEqual(core.normalize_subtitle_text(merged[0].text), "按道理你改叫我一声姐夫")

    def test_visual_same_line_does_not_cross_audio_type_boundary(self):
        audio_entries = [
            entry(1, 3.00, 4.10, "\u7537\u4eba\u4e00\u58f0\u4ee4\u4e0b", "narration"),
            entry(2, 4.14, 4.58, "\u8d77\u98de", "dialogue"),
        ]
        visual_entries = [
            entry(1, 3.00, 4.58, "\u7537\u4eba\u4e00\u58f0\u4ee4\u4e0b\u8d77\u98de"),
        ]

        merged, _ = core.build_primary_entries_from_funasr_and_visual(
            audio_entries,
            visual_entries,
        )
        _pause_map, visual_join_map, _stats = core.build_visual_subtitle_tts_boundary_evidence(
            merged,
            visual_entries,
        )

        self.assertEqual(len(merged), 2)
        self.assertEqual([item.entry_type for item in merged], ["narration", "dialogue"])
        self.assertEqual(core.normalize_subtitle_text(merged[0].text), "\u7537\u4eba\u4e00\u58f0\u4ee4\u4e0b")
        self.assertEqual(core.normalize_subtitle_text(merged[1].text), "\u8d77\u98de")
        self.assertNotIn(1, visual_join_map)

    def test_visual_sentence_alignment_trims_cross_sentence_audio_spillover(self):
        audio_entries = [
            entry(1, 25.35, 26.35, "仅紧三拳两脚"),
            entry(2, 26.35, 27.55, "便将所有混混尽数"),
            entry(3, 27.63, 28.29, "打倒在地一"),
            entry(4, 28.37, 29.29, "旁的美女见小伙"),
            entry(5, 29.29, 29.97, "这么有实力"),
        ]
        visual_entries = [
            entry(1, 25.40, 26.40, "仅仅三拳两脚"),
            entry(2, 26.40, 28.40, "便将所有混混尽数打倒在地"),
            entry(3, 28.40, 30.00, "一旁的美女见小伙这么有实力"),
        ]

        merged, fix_count = core.build_primary_entries_from_funasr_and_visual(
            audio_entries,
            visual_entries,
        )

        self.assertGreaterEqual(fix_count, 1)
        self.assertEqual(
            [core.normalize_subtitle_text(item.text) for item in merged],
            [
                "仅仅三拳两脚",
                "便将所有混混尽数打倒在地",
                "一旁的美女见小伙这么有实力",
            ],
        )

    def test_visual_sentence_alignment_rejects_unmatched_watermark_tail(self):
        audio_entries = [
            entry(1, 36.31, 38.23, "可他殊不知"),
        ]
        visual_entries = [
            entry(1, 36.40, 38.40, "可他殊不知灵迹剪刀"),
        ]

        merged, _ = core.build_primary_entries_from_funasr_and_visual(
            audio_entries,
            visual_entries,
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(core.normalize_subtitle_text(merged[0].text), "可他殊不知")

    def test_visual_sentence_alignment_repairs_prefix_spillover_into_next_line(self):
        audio_entries = [
            entry(1, 39.81, 40.45, "竟是当地赫"),
            entry(2, 40.53, 42.29, "赫有名黑道大佬的掌上千金"),
        ]
        visual_entries = [
            entry(1, 39.80, 41.00, "竟是当地赫赫有名"),
            entry(2, 41.00, 42.40, "黑道大佬的掌上千金"),
        ]

        merged, fix_count = core.build_primary_entries_from_funasr_and_visual(
            audio_entries,
            visual_entries,
        )

        self.assertGreaterEqual(fix_count, 1)
        self.assertEqual(
            [core.normalize_subtitle_text(item.text) for item in merged],
            ["竟是当地赫赫有名", "黑道大佬的掌上千金"],
        )
        self.assertGreaterEqual(merged[1].start, merged[0].end)

    def test_visual_srt_sentence_alignment_protects_whole_sentence_tts_join(self):
        entries = [
            entry(1, 38.23, 39.03, "眼前这位外表"),
            entry(2, 39.12, 39.69, "柔弱的美女"),
        ]
        visual_entries = [
            entry(1, 38.40, 39.80, "眼前这位外表柔弱的美女"),
        ]

        _pause_map, visual_join_map, _stats = core.build_visual_subtitle_tts_boundary_evidence(
            entries,
            visual_entries,
        )
        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            0.07,
            boundary_audio_pause_map={1: 0.08},
            boundary_visual_join_map=visual_join_map,
        )

        self.assertTrue(visual_join_map[1])
        self.assertTrue(join_map[1])
        self.assertEqual(len(groups), 1)
        self.assertEqual(core.join_narration_group_text(groups[0]), "眼前这位外表柔弱的美女")

    def test_visual_no_break_can_protect_short_fragments_when_audio_is_ambiguous(self):
        entries = [
            entry(1, 10.00, 10.42, "按道理"),
            entry(2, 10.56, 10.92, "你改叫我"),
            entry(3, 11.04, 11.36, "一声姐夫"),
        ]

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            0.09,
            boundary_audio_pause_map={1: 0.04, 2: 0.04},
            boundary_visual_join_map={1: True, 2: True},
        )

        self.assertTrue(join_map[1])
        self.assertTrue(join_map[2])
        self.assertEqual(len(groups), 1)
        self.assertEqual(core.join_narration_group_text(groups[0]), "按道理你改叫我一声姐夫")

    def test_clear_audio_pause_still_overrides_visual_no_break(self):
        entries = [
            entry(1, 10.00, 10.42, "按道理"),
            entry(2, 10.56, 10.92, "你改叫我"),
        ]

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            0.09,
            boundary_audio_pause_map={1: 0.22},
            boundary_visual_join_map={1: True},
        )

        self.assertFalse(join_map[1])
        self.assertEqual(len(groups), 2)

    def test_compact_midword_token_split_is_rejected_even_with_strong_audio_candidate(self):
        item = {
            "text": "直接将烟头精准",
            "start": 0,
            "end": 1400,
            "timestamp": [
                [0, 180],
                [180, 360],
                [360, 540],
                [540, 650],
                [950, 1080],
                [1080, 1220],
                [1220, 1400],
            ],
        }

        split_entries = core.split_funasr_sentence_item_by_audio_timing(item)

        self.assertEqual(len(split_entries), 1)
        self.assertEqual(core.normalize_subtitle_text(split_entries[0].text), "直接将烟头精准")


if __name__ == "__main__":
    unittest.main()
