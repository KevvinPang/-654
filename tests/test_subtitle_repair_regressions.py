import math
import struct
import tempfile
import unittest
import wave
from pathlib import Path

from modules import subtitle_batch_runner as subtitle_runner
from modules.auto_clip_engine import drama_clone_core as core


def entry(index, start, end, text, entry_type="narration"):
    return core.SubtitleEntry(index, float(start), float(end), text, entry_type)


def repair_visual_with_audio(visual_entries, audio_entries):
    repaired, _, _ = core.refine_reference_entries_with_funasr(visual_entries, audio_entries)
    repaired, _ = core.drop_isolated_visual_ocr_noise_entries(repaired)
    return repaired


def write_sine_wav(path, intervals, duration=2.6, sample_rate=16000):
    total_samples = int(duration * sample_rate)
    frames = bytearray()
    for sample_index in range(total_samples):
        time_value = sample_index / sample_rate
        active = any(start <= time_value <= end for start, end in intervals)
        if active:
            value = 0.42 * math.sin(2.0 * math.pi * 180.0 * time_value)
        else:
            value = 0.0
        frames.extend(struct.pack("<h", int(max(-1.0, min(1.0, value)) * 32767)))
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(bytes(frames))


def sine_samples(intervals, duration=2.6, sample_rate=16000):
    if not core.NUMPY_AVAILABLE:
        return None, sample_rate
    import numpy as np

    total_samples = int(duration * sample_rate)
    samples = np.zeros(total_samples, dtype=np.float32)
    for sample_index in range(total_samples):
        time_value = sample_index / sample_rate
        if any(start <= time_value <= end for start, end in intervals):
            samples[sample_index] = 0.42 * math.sin(2.0 * math.pi * 180.0 * time_value)
    return samples, sample_rate


class DummyAINarrationGenerator:
    def review_subtitle_ocr(self, entries, log_func=None):
        return {}

    def rewrite_srt_full(self, srt_content, log_func=None):
        return ""


class RaisingAI:
    api_key = "test-key"
    last_rewrite_issue = ""
    last_ai_issue = ""

    def request_json_object(self, **kwargs):
        raise AssertionError("AI should not be called for this scenario")

    def note_ai_issue(self, *args, **kwargs):
        pass

    def note_rewrite_issue(self, *args, **kwargs):
        pass


class CapturingRewriteAI(RaisingAI):
    def __init__(self):
        self.calls = 0
        self.last_prompt = ""

    def request_json_object(self, **kwargs):
        self.calls += 1
        self.last_prompt = str(kwargs.get("user_prompt") or "")
        return {"entries": []}


def audio_profile_for(entry_item, vector=(1.0, 0.0, 0.0)):
    return core.AudioSegmentProfile(
        index=entry_item.index,
        start=entry_item.start,
        end=entry_item.end,
        duration=max(0.1, entry_item.end - entry_item.start),
        speech_ratio=0.55,
        voiced_ratio=0.55,
        rms_db=-22.0,
        spectral_flatness=0.08,
        pitch_hz=180.0,
        confidence=0.92,
        feature_vector=tuple(vector),
    )


class SubtitleTextRepairRegressionTests(unittest.TestCase):
    def test_ai_pause_planner_is_disabled(self):
        generator = core.AINarrationGenerator(
            api_key="test-key",
            model="dummy",
            api_url="http://127.0.0.1/unused",
        )
        generator.request_json_object = lambda **kwargs: {"entries": [{"index": 1, "join_next": True}]}

        result = generator.plan_tts_sentence_links(
            [
                entry(1, 0.0, 0.8, "\u5973\u4eba\u8f6c\u8eab"),
                entry(2, 0.8, 1.6, "\u7537\u4eba\u8ffd\u4e86\u4e0a\u6765"),
            ]
        )

        self.assertEqual(result, {})

    def test_ai_rewrite_ignores_non_narration_entries_even_if_called_directly(self):
        result = core.rewrite_narration_entries(
            RaisingAI(),
            [
                entry(1, 0.0, 1.0, "\u6162\u7740", "dialogue"),
                entry(2, 1.0, 2.0, "\u7b2c\u4e09\u96c6", "original_subtitle"),
            ],
        )

        self.assertEqual(result, {})

    def test_ai_rewrite_payload_contains_only_narration_entries(self):
        ai = CapturingRewriteAI()
        result = core.rewrite_narration_entries(
            ai,
            [
                entry(1, 0.0, 1.0, "\u7537\u4eba\u6ca1\u60f3\u5230", "narration"),
                entry(2, 1.0, 1.5, "\u6162\u7740", "dialogue"),
            ],
        )

        self.assertEqual(result, {})
        self.assertEqual(ai.calls, 1)
        self.assertIn("\u7537\u4eba\u6ca1\u60f3\u5230", ai.last_prompt)
        self.assertNotIn("\u6162\u7740", ai.last_prompt)

    def test_ai_seed_detection_skips_when_local_seeds_are_clear(self):
        entries = [
            entry(1, 0.0, 1.0, "\u7537\u4eba\u6ca1\u60f3\u5230"),
            entry(2, 1.2, 2.2, "\u4e0b\u4e00\u79d2\u5973\u4eba\u51b2\u4e86\u8fc7\u6765"),
            entry(3, 2.4, 3.4, "\u6b64\u65f6\u738b\u864e\u8fd8\u5728\u5bb6\u91cc"),
            entry(4, 3.6, 4.2, "\u6162\u7740", "dialogue"),
        ]
        profiles = {item.index: audio_profile_for(item) for item in entries}
        hints = {
            1: {"narration": 1.1, "dialogue": 0.0},
            2: {"narration": 1.2, "dialogue": 0.0},
            3: {"narration": 1.2, "dialogue": 0.0},
            4: {"narration": 0.0, "dialogue": 1.4},
        }

        self.assertFalse(core.audio_seed_detection_needs_ai(entries, hints, profiles))
        result = core.detect_ai_audio_seed_labels(RaisingAI(), entries, hints, profiles)
        self.assertEqual(result, {})

    def test_ai_seed_detection_runs_only_for_ambiguous_local_seed_state(self):
        entries = [
            entry(1, 0.0, 0.8, "\u8fd9\u65f6\u7537\u4eba"),
            entry(2, 0.9, 1.7, "\u4f60\u7ed9\u6211\u7b49\u7740"),
        ]
        profiles = {item.index: audio_profile_for(item) for item in entries}
        hints = {
            1: {"narration": 0.55, "dialogue": 0.42},
            2: {"narration": 0.42, "dialogue": 0.55},
        }

        self.assertTrue(core.audio_seed_detection_needs_ai(entries, hints, profiles))

    @unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for mask stability tests")
    def test_dynamic_subtitle_mask_holds_previous_text_pixels(self):
        current = core.np.zeros((12, 18), dtype=core.np.uint8)
        current[5, 7] = 255

        stabilized, previous, hold = core.stabilize_subtitle_inpaint_mask(
            current,
            None,
            0,
            6,
        )
        self.assertIsNotNone(stabilized)
        self.assertEqual(int(stabilized[5, 7]), 255)
        self.assertEqual(hold, 6)

        held, previous, hold = core.stabilize_subtitle_inpaint_mask(
            None,
            previous,
            hold,
            6,
        )
        self.assertIsNotNone(held)
        self.assertEqual(int(held[5, 7]), 255)
        self.assertEqual(hold, 5)

    @unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for mask stability tests")
    def test_dynamic_subtitle_mask_unions_recent_text_without_full_band_growth(self):
        previous = core.np.zeros((12, 18), dtype=core.np.uint8)
        previous[5, 7] = 255
        current = core.np.zeros((12, 18), dtype=core.np.uint8)
        current[5, 8] = 255

        stabilized, _previous, hold = core.stabilize_subtitle_inpaint_mask(
            current,
            previous,
            4,
            6,
        )

        self.assertEqual(int(stabilized[5, 7]), 255)
        self.assertEqual(int(stabilized[5, 8]), 255)
        self.assertEqual(int(core.np.count_nonzero(stabilized)), 2)
        self.assertEqual(hold, 6)

    def test_dynamic_subtitle_mask_hold_scales_with_fps(self):
        self.assertGreater(core.subtitle_mask_dynamic_hold_frames(50.0), core.SUBTITLE_MASK_BOX_HOLD_FRAMES)
        self.assertLessEqual(
            core.subtitle_mask_dynamic_hold_frames(120.0),
            core.SUBTITLE_MASK_DYNAMIC_MAX_HOLD_FRAMES,
        )

    @unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for mask alpha tests")
    def test_fixed_subtitle_band_fades_out_at_vertical_edges(self):
        alpha = core.build_fixed_subtitle_band_alpha(100, 20)

        self.assertIsNotNone(alpha)
        self.assertLess(float(alpha[0, 10]), 0.02)
        self.assertLess(float(alpha[-1, 10]), 0.02)
        self.assertGreater(float(alpha[50, 10]), 0.55)
        self.assertGreater(float(alpha[25, 10]), float(alpha[5, 10]))
        self.assertGreater(float(alpha[50, 10]), float(alpha[10, 10]))

    def test_subtitle_mask_cover_region_stays_tight_and_anchors_burn(self):
        detected = core.VideoMaskRegion(165, 1228, 729, 516, 0.41, "auto")
        cover = core.expand_subtitle_mask_cover_region(detected, 1080, 1920)
        layout = core.build_subtitle_burn_layout(1080, 1920, cover)
        subtitle_baseline_y = 1920 - int(layout["margin_v"])

        self.assertEqual(cover.x, 0)
        self.assertEqual(cover.width, 1080)
        self.assertLessEqual(cover.height, int(round(1920 * core.SUBTITLE_MASK_COVER_MAX_HEIGHT_RATIO)) + 2)
        self.assertGreaterEqual(subtitle_baseline_y, cover.y)
        self.assertLessEqual(subtitle_baseline_y, cover.y + cover.height)

        detected_anchor = core.VideoMaskRegion(
            165,
            1228,
            729,
            96,
            0.41,
            "auto",
            anchor_x=165,
            anchor_y=1228,
            anchor_width=729,
            anchor_height=96,
        )
        cover_with_anchor = core.expand_subtitle_mask_cover_region(detected_anchor, 1080, 1920)
        anchored_layout = core.build_subtitle_burn_layout(1080, 1920, cover_with_anchor)
        anchored_baseline_y = 1920 - int(anchored_layout["margin_v"])
        self.assertGreaterEqual(anchored_baseline_y, 1228)
        self.assertLessEqual(anchored_baseline_y, 1228 + 96 + anchored_layout["font_size"])

    def test_subtitle_mask_intervals_merge_visual_and_audio_events(self):
        audio_entry = entry(1, 1.00, 1.50, "\u4ed6\u521a\u8d70\u4e0b\u5a5a\u8f66", "narration")
        visual_entry = entry(2, 1.56, 1.90, "\u4ed6\u521a\u8d70\u4e0b\u5a5a\u8f66", "narration")

        intervals = core.build_subtitle_mask_intervals(
            [audio_entry],
            [visual_entry],
            total_duration=3.0,
        )

        self.assertEqual(len(intervals), 1)
        self.assertLessEqual(intervals[0][0], 0.95)
        self.assertGreaterEqual(intervals[0][1], 2.05)
        self.assertTrue(core.time_in_subtitle_mask_intervals(1.70, intervals))
        self.assertFalse(core.time_in_subtitle_mask_intervals(2.50, intervals))

    def test_narrator_only_near_reject_similarity_stays_narration_gray(self):
        espnet_item = {
            "speaker_engine": "espnet_wavlm",
            "narration_similarity": 0.558,
            "dialogue_similarity": 0.0,
            "entry_duration": 1.56,
            "has_context_window": 1.0,
        }
        unispeech_item = {
            "speaker_engine": "unispeech_sat_large_sv",
            "narration_similarity": 0.768,
            "dialogue_similarity": 0.0,
            "entry_duration": 1.56,
            "has_context_window": 1.0,
        }

        self.assertEqual(core.espnet_wavlm_similarity_label(espnet_item), "")
        self.assertEqual(core.unispeech_sat_similarity_label(unispeech_item), "")
        self.assertEqual(core.speaker_gray_fallback_override_from_item(espnet_item)["type"], "narration")
        self.assertEqual(core.speaker_gray_fallback_override_from_item(unispeech_item)["type"], "narration")

    def test_narrator_only_hard_reject_still_marks_dialogue(self):
        espnet_item = {
            "speaker_engine": "espnet_wavlm",
            "narration_similarity": 0.31,
            "dialogue_similarity": 0.0,
            "entry_duration": 1.20,
            "has_context_window": 1.0,
        }
        unispeech_item = {
            "speaker_engine": "unispeech_sat_large_sv",
            "narration_similarity": 0.60,
            "dialogue_similarity": 0.0,
            "entry_duration": 1.20,
            "has_context_window": 1.0,
        }

        self.assertEqual(core.espnet_wavlm_similarity_label(espnet_item), "dialogue")
        self.assertEqual(core.unispeech_sat_similarity_label(unispeech_item), "dialogue")

    def test_visual_delivery_split_rejects_word_internal_boundary(self):
        source = entry(
            1,
            0.0,
            2.8,
            "\u53ea\u56e0\u6821\u957f\u589e\u52a0\u4e86\u4e00\u6761\u65b0\u89c4"
            "\u5c31\u662f\u6bcf\u6350\u6b3e\u4e94\u767e\u4e07\u53ef\u4ee5\u7ed9\u5b69\u5b50\u52a0\u5341\u5206",
        )
        visual = [
            entry(
                1,
                0.0,
                1.4,
                "\u53ea\u56e0\u6821\u957f\u589e\u52a0\u4e86\u4e00\u6761\u65b0\u89c4"
                "\u5c31\u662f\u6bcf\u6350\u6b3e\u4e94\u767e\u4e07\u53ef",
            ),
            entry(2, 1.4, 2.8, "\u4ee5\u7ed9\u5b69\u5b50\u52a0\u5341\u5206"),
        ]

        split_entries, split_count = core.split_long_delivery_entries_by_visual_subtitles(
            [source],
            visual,
            fps=50.0,
            max_units=18,
        )

        self.assertGreaterEqual(split_count, 1)
        self.assertEqual("".join(item.text for item in split_entries), source.text)
        for left, right in zip(split_entries, split_entries[1:]):
            self.assertFalse(left.text.endswith("\u53ef") and right.text.startswith("\u4ee5"))
            self.assertFalse(core.split_crosses_display_protected_word(left.text, right.text))

    def test_visual_delivery_split_rejects_duplicate_boundary_character(self):
        source = entry(1, 0.0, 2.2, "\u521a\u597d\u8d85\u8fc7\u4e86\u5973\u513f\u6210\u4e3a\u4e86\u7b2c\u4e00\u540d")
        visual = [
            entry(1, 0.0, 1.1, "\u521a\u597d\u8d85\u8fc7\u4e86\u5973\u513f\u6210"),
            entry(2, 1.1, 2.2, "\u6210\u4e3a\u4e86\u7b2c\u4e00\u540d"),
        ]

        split_entries, _ = core.split_long_delivery_entries_by_visual_subtitles(
            [source],
            visual,
            fps=50.0,
            max_units=12,
        )

        self.assertEqual("".join(item.text for item in split_entries), source.text)
        for left, right in zip(split_entries, split_entries[1:]):
            self.assertFalse(left.text.endswith("\u6210") and right.text.startswith("\u6210"))

    def test_audio_sentence_info_is_repaired_from_qwen_master_text(self):
        parsed = {
            "text": "因为被他们欺负的老实人终于爆发了原来就在三天前",
            "sentence_info": [
                {
                    "text": "因为被他们欺负的老实人终爆发了原来就在三天前",
                    "start": 6320,
                    "end": 11040,
                    "timestamp": [],
                }
            ],
        }
        entries, fix_count = core.build_audio_first_entries_from_parsed_transcription(parsed)

        self.assertGreaterEqual(fix_count, 1)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].text, "因为被他们欺负的老实人终于爆发了原来就在三天前")
        self.assertAlmostEqual(entries[0].start, 6.32, places=2)
        self.assertAlmostEqual(entries[0].end, 11.04, places=2)

    def test_dual_srt_visual_split_does_not_merge_audio_pause_boundary(self):
        visual = [
            entry(1, 0.0, 2.0, "终于爆发了原来就在三天前"),
        ]
        audio = [
            entry(1, 0.0, 0.8, "终爆发了"),
            entry(2, 1.2, 2.0, "原来就在三天前"),
        ]
        settings = core.CloneSettings(
            reference_video=Path("reference.mp4"),
            source_dir=Path("."),
            output_dir=Path("."),
            subtitle_entries=audio,
            visual_subtitle_entries=visual,
            prefer_funasr_audio_subtitles=True,
            disable_ai_subtitle_review=True,
            disable_ai_narration_rewrite=True,
            prefer_funasr_sentence_pauses=True,
        )
        bundle = core.build_processed_subtitles(
            audio,
            "",
            DummyAINarrationGenerator(),
            settings=settings,
        )

        texts = [item.text for item in bundle.all_entries]
        self.assertEqual(len(bundle.all_entries), 2)
        self.assertEqual(texts, ["终于爆发了", "原来就在三天前"])
        self.assertAlmostEqual(bundle.all_entries[0].end, 0.8, places=2)
        self.assertAlmostEqual(bundle.all_entries[1].start, 1.2, places=2)

    def test_dual_srt_keeps_audio_text_while_visual_only_repairs_tiny_gaps(self):
        visual = [
            entry(1, 0.0, 1.2, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929"),
            entry(2, 1.2, 2.6, "\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
            entry(3, 2.6, 3.4, "\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c"),
            entry(4, 6.4, 7.6, "\u56e0\u4e3a\u88ab\u4ed6\u4eec\u6b3a\u8d1f\u7684\u8001\u5b9e\u4eba"),
            entry(5, 7.6, 8.4, "\u7ec8\u4e8e\u7206\u53d1\u4e86"),
            entry(6, 8.4, 9.4, "\u539f\u6765\u5c31\u57283\u5929\u524d"),
            entry(7, 9.4, 10.4, "\u6751\u91cc\u6700\u8001\u5b9e\u7684\u7537\u4eba"),
            entry(8, 10.4, 11.2, "\u674e\u6811\u6839\u7ed3\u5a5a"),
        ]
        audio = [
            entry(1, 0.0, 1.546, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709\u6751"),
            entry(2, 1.654, 3.28, "\u6c11\u628a\u8def\u62e6\u4f4f\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c"),
            entry(3, 6.32, 8.16, "\u56e0\u4e3a\u88ab\u4ed6\u4eec\u6b3a\u8d1f\u7684\u8001\u5b9e\u4eba\u7ec8\u7206\u53d1\u4e86"),
            entry(4, 8.32, 11.04, "\u539f\u6765\u5c31\u5728\u4e09\u5929\u524d\u6751\u91cc\u6700\u8001\u5b9e\u7684\u7537\u4eba\u674e\u53d4\u8ddf\u7ed3\u5a5a"),
        ]
        settings = core.CloneSettings(
            reference_video=Path("reference.mp4"),
            source_dir=Path("."),
            output_dir=Path("."),
            subtitle_entries=audio,
            visual_subtitle_entries=visual,
            prefer_funasr_audio_subtitles=True,
            disable_ai_subtitle_review=True,
            disable_ai_narration_rewrite=True,
            prefer_funasr_sentence_pauses=True,
        )
        bundle = core.build_processed_subtitles(
            audio,
            "",
            DummyAINarrationGenerator(),
            settings=settings,
        )

        texts = [item.text for item in bundle.all_entries]
        self.assertEqual(texts[0], audio[0].text)
        self.assertEqual(texts[1], "\u6c11\u628a\u8def\u62e6\u4f4f\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c")
        self.assertEqual(texts[2], "\u56e0\u4e3a\u88ab\u4ed6\u4eec\u6b3a\u8d1f\u7684\u8001\u5b9e\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86")
        self.assertEqual(texts[3], audio[3].text)
        self.assertNotIn("\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c", texts)
        self.assertNotIn("\u7ec8\u4e8e\u7206\u53d1\u4e86", texts)

    def test_audio_cannot_clip_reliable_visual_tail(self):
        visual = [
            entry(1, 0.20, 1.60, "女人嫁进农村的第一天"),
            entry(2, 1.60, 3.60, "就被恶霸拿拖拉机挡住了去路"),
        ]
        audio = [
            entry(1, 0.17, 0.83, "女人嫁进农"),
            entry(2, 0.91, 1.31, "村的第一"),
            entry(3, 1.39, 2.45, "天就被恶霸拿拖拉"),
            entry(4, 2.53, 3.39, "机挡住了去路"),
        ]
        repaired = repair_visual_with_audio(visual, audio)
        self.assertEqual(repaired[0].text, "女人嫁进农村的第一天")
        self.assertEqual(repaired[1].text, "就被恶霸拿拖拉机挡住了去路")

    def test_visual_noise_is_removed_without_deleting_split_ai_terms(self):
        self.assertEqual(core.clean_visual_ocr_noise_text("1:\n随即"), "随即")
        self.assertTrue(core.isolated_visual_ocr_noise_text("H44"))
        self.assertTrue(core.isolated_visual_ocr_noise_text("5.0"))
        self.assertFalse(core.isolated_visual_ocr_noise_text("AI"))

        entries = [
            entry(1, 0.0, 0.6, "这是"),
            entry(2, 0.6, 0.9, "H44"),
            entry(3, 0.9, 1.4, "下一句"),
            entry(4, 1.4, 1.7, "AI"),
            entry(5, 1.7, 2.2, "换脸"),
        ]
        kept, dropped = core.drop_isolated_visual_ocr_noise_entries(entries)
        self.assertEqual(dropped, 1)
        self.assertEqual([item.text for item in kept], ["这是", "下一句", "AI", "换脸"])

    def test_cascaded_visual_noise_cluster_is_removed(self):
        entries = [
            entry(1, 0.0, 0.4, "福"),
            entry(2, 0.4, 0.7, "E1"),
            entry(3, 0.7, 1.0, "5.0"),
            entry(4, 1.0, 1.5, "大吉"),
        ]
        kept, dropped = core.drop_isolated_visual_ocr_noise_entries(entries)
        self.assertEqual(dropped, 2)
        self.assertEqual([item.text for item in kept], ["福", "大吉"])

    def test_audio_can_restore_missing_tail_without_spilling_next_line(self):
        visual = [
            entry(1, 148.50, 149.50, "丈母娘也是放下狠"),
            entry(2, 149.50, 151.30, "话但凡今天的婚礼有任何的差池"),
        ]
        audio = [
            entry(1, 148.19, 149.39, "丈母娘也是放下狠话"),
            entry(2, 149.39, 151.05, "但凡今天的婚礼有任何的差池"),
        ]
        repaired, fix_count = core.repair_audio_tail_spillover_from_next_entry(visual, audio)
        self.assertGreaterEqual(fix_count, 1)
        self.assertEqual(repaired[0].text, "丈母娘也是放下狠话")
        self.assertEqual(repaired[1].text, "但凡今天的婚礼有任何的差池")

    def test_funasr_candidate_cannot_append_neighbor_prefix_with_ocr_variant(self):
        merged = core.merge_visual_text_with_funasr_correction(
            "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929",
            "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709",
            next_visual_text="\u7ade\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f",
        )
        self.assertEqual(merged, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929")

    def test_tail_spillover_repair_does_not_steal_complete_visual_prefix(self):
        cases = [
            ("\u5374\u7528\u62d6\u62c9\u673a", "\u5c06\u8fdb\u6751\u552f\u4e00\u7684\u8def\u7ed9\u5835\u4f4f"),
            ("\u5c31\u6cfc\u4e86\u8fc7\u53bb", "\u738b\u864e\u6ca1\u60f3\u5230"),
            ("\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929", "\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
        ]
        for left_text, right_text in cases:
            with self.subTest(left=left_text, right=right_text):
                visual = [entry(1, 0.0, 1.0, left_text), entry(2, 1.0, 2.0, right_text)]
                audio = [entry(1, 0.0, 1.0, left_text), entry(2, 1.0, 2.0, right_text)]
                repaired, fix_count = core.repair_audio_tail_spillover_from_next_entry(visual, audio)
                self.assertEqual(fix_count, 0)
                self.assertEqual([item.text for item in repaired], [left_text, right_text])

    def test_cross_boundary_audio_segment_cannot_override_visual_boundary(self):
        visual = [
            entry(1, 13.0, 13.6, "\u5374\u7528\u62d6\u62c9\u673a"),
            entry(2, 13.6, 15.2, "\u5c06\u8fdb\u6751\u552f\u4e00\u7684\u8def\u7ed9\u5835\u4f4f"),
        ]
        audio = [
            entry(1, 12.93, 13.89, "\u5374\u7528\u62d6\u62c9\u673a\u5c06\u8fdb"),
            entry(2, 13.97, 15.07, "\u6751\u552f\u4e00\u7684\u8def\u7ed9\u5835\u4f4f"),
        ]
        repaired, fix_count = core.repair_audio_tail_spillover_from_next_entry(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual([item.text for item in repaired], [item.text for item in visual])

    def test_non_crossing_audio_segment_can_restore_true_tail(self):
        visual = [
            entry(1, 148.50, 149.50, "\u4e08\u6bcd\u5a18\u4e5f\u662f\u653e\u4e0b\u72e0"),
            entry(2, 149.50, 151.30, "\u8bdd\u4f46\u51e1\u4eca\u5929\u7684\u5a5a\u793c\u6709\u4efb\u4f55\u7684\u5dee\u6c60"),
        ]
        audio = [
            entry(1, 148.19, 149.39, "\u4e08\u6bcd\u5a18\u4e5f\u662f\u653e\u4e0b\u72e0\u8bdd"),
            entry(2, 149.39, 151.05, "\u4f46\u51e1\u4eca\u5929\u7684\u5a5a\u793c\u6709\u4efb\u4f55\u7684\u5dee\u6c60"),
        ]
        repaired, fix_count = core.repair_audio_tail_spillover_from_next_entry(visual, audio)
        self.assertEqual(fix_count, 1)
        self.assertEqual(repaired[0].text, "\u4e08\u6bcd\u5a18\u4e5f\u662f\u653e\u4e0b\u72e0\u8bdd")
        self.assertEqual(repaired[1].text, "\u4f46\u51e1\u4eca\u5929\u7684\u5a5a\u793c\u6709\u4efb\u4f55\u7684\u5dee\u6c60")

    def test_refine_keeps_visual_boundary_when_audio_spans_neighbor_prefix(self):
        visual = [
            entry(1, 0.0, 1.2, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929"),
            entry(2, 1.2, 2.6, "\u7ade\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
            entry(3, 13.0, 13.6, "\u5374\u7528\u62d6\u62c9\u673a"),
            entry(4, 13.6, 15.2, "\u5c06\u8fdb\u6751\u552f\u4e00\u7684\u8def\u7ed9\u5835\u4f4f"),
        ]
        audio = [
            entry(1, 0.0, 1.2, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709"),
            entry(2, 1.2, 2.6, "\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
            entry(3, 13.0, 13.6, "\u5374\u7528\u62d6\u62c9\u673a"),
            entry(4, 13.6, 15.2, "\u5c06\u8fdb\u6751\u552f\u4e00\u7684\u8def\u7ed9\u5835\u4f4f"),
        ]
        repaired = repair_visual_with_audio(visual, audio)
        self.assertEqual(
            [item.text for item in repaired],
            [item.text for item in visual],
        )

    def test_natural_sentence_prefix_is_not_moved_to_previous_line(self):
        visual = [
            entry(1, 144.90, 146.70, "他为这场婚礼已经准备了大半年"),
            entry(2, 146.70, 148.50, "定然会风风光光的将她娶进门"),
        ]
        audio = [
            entry(1, 144.95, 146.55, "他为这场婚礼已经准备了大半年"),
            entry(2, 146.65, 148.30, "定然会风风光光的将她娶进门"),
        ]
        repaired, fix_count = core.repair_audio_tail_spillover_from_next_entry(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual([item.text for item in repaired], [item.text for item in visual])

    def test_garbled_audio_does_not_replace_reliable_visual_text(self):
        visual = [entry(1, 126.89, 128.49, "把清单上的东西全都买齐")]
        audio = [entry(1, 126.61, 128.05, "把清单上的东西全都满气")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "把清单上的东西全都买齐")

    def test_audio_neighbor_prefix_does_not_pollute_next_visual_line(self):
        visual = [entry(1, 10.0, 11.0, "女人刚到门口")]
        audio = [entry(1, 10.0, 11.0, "女人刚到门口没想到")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "女人刚到门口")

    def test_audio_can_restore_tiny_tail_on_reliable_visual_line(self):
        visual = [entry(1, 10.0, 11.0, "结婚第一")]
        audio = [entry(1, 10.0, 11.0, "结婚第一天")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 1)
        self.assertEqual(repaired[0].text, "结婚第一天")

    def test_reliable_latin_token_is_not_removed_by_audio_candidate(self):
        visual = [entry(1, 20.0, 21.0, "原来这是AI换脸")]
        audio = [entry(1, 20.0, 21.0, "原来这是换脸")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "原来这是AI换脸")

    def test_visual_numeric_amount_survives_homophone_audio_noise(self):
        visual = [entry(1, 30.0, 31.0, "到账4000万")]
        audio = [entry(1, 30.0, 31.0, "到账死千万")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "到账4000万")

    def test_continuous_narration_duck_bridge_covers_tts_internal_gap(self):
        timeline = [
            entry(151, 179.57, 180.67, "可是城里苏家的千金人"),
            entry(152, 180.73, 181.87, "女方家里人比较好面子"),
            entry(153, 182.60, 183.33, "特意交代他们"),
        ]
        groups = [{"entries": [item]} for item in timeline]
        bridged, bridge_count, bridge_total = core.bridge_continuous_narration_duck_intervals(
            [(179.59, 181.77), (182.61, 183.23), (183.40, 184.60)],
            groups,
            timeline,
        )
        self.assertEqual(bridge_count, 2)
        self.assertAlmostEqual(bridge_total, 1.01, places=2)
        self.assertEqual(bridged[0], (179.59, 182.61))
        self.assertEqual(bridged[1], (182.61, 183.40))

    def test_continuous_narration_duck_bridge_stops_at_source_audio(self):
        timeline = [
            entry(1, 10.0, 11.0, "解说一"),
            entry(2, 11.0, 12.0, "慢着", entry_type="dialogue"),
            entry(3, 12.0, 13.0, "解说二"),
        ]
        groups = [{"entries": [timeline[0]]}, {"entries": [timeline[2]]}]
        bridged, bridge_count, bridge_total = core.bridge_continuous_narration_duck_intervals(
            [(10.0, 10.8), (12.0, 12.8)],
            groups,
            timeline,
        )
        self.assertEqual(bridge_count, 0)
        self.assertEqual(bridge_total, 0.0)
        self.assertEqual(bridged, [(10.0, 10.8), (12.0, 12.8)])

    def test_repeated_visual_phrase_is_deduped_when_audio_supports_it(self):
        visual = [entry(1, 0.0, 1.2, "第一名一名男子冲了出来")]
        audio = [entry(1, 0.0, 1.2, "第一名男子冲了出来")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertGreaterEqual(fix_count, 1)
        self.assertEqual(repaired[0].text, "第一名男子冲了出来")

    def test_audio_candidate_cannot_add_neighbor_prefix_to_reliable_visual(self):
        self.assertTrue(
            core.audio_candidate_has_boundary_pollution(
                "但凡今天的婚礼有任何的差池",
                "话但凡今天的婚礼有任何的差池",
                previous_visual_text="丈母娘也是放下狠",
            )
        )

    def test_audio_candidate_cannot_append_short_next_visual_line(self):
        visual = [
            entry(1, 95.40, 97.40, "\u4eca\u5929\u975e\u5f97\u6559\u8bad\u6559\u8bad\u4f60\u4e0d\u884c"),
            entry(2, 97.40, 98.00, "\u662f\u5417"),
        ]
        audio = [
            entry(1, 95.38, 97.26, "\u7136\u800c\u975e\u5f97\u6559\u8bad\u6559\u8bad\u4f60\u4e0d\u884c"),
            entry(2, 97.32, 97.78, "\u662f\u5417"),
        ]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual([item.text for item in repaired], [item.text for item in visual])


    def test_audio_strong_transition_prefix_does_not_pollute_visual_line(self):
        visual = [entry(1, 0.0, 1.0, "男人冲进大厅")]
        audio = [entry(1, 0.0, 1.0, "没想到男人冲进大厅")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "男人冲进大厅")

    def test_audio_call_to_action_suffix_is_not_added_to_clean_visual(self):
        visual = [entry(1, 0.0, 1.0, "男人转身离开")]
        audio = [entry(1, 0.0, 1.0, "男人转身离开关注我看全集")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "男人转身离开")

    def test_audio_can_restore_amount_unit_tail_preserving_digits(self):
        visual = [entry(1, 0.0, 1.0, "到账4000")]
        audio = [entry(1, 0.0, 1.0, "到账4000万")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 1)
        self.assertEqual(repaired[0].text, "到账4000万")

    def test_audio_cannot_change_amount_digits_when_extending(self):
        visual = [entry(1, 0.0, 1.0, "到账4000万")]
        audio = [entry(1, 0.0, 1.0, "到账5000万元")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "到账4000万")

    def test_audio_homophone_cannot_replace_short_reliable_dialogue_text(self):
        visual = [entry(1, 0.0, 1.0, "不转", "dialogue")]
        audio = [entry(1, 0.0, 1.0, "不赚", "dialogue")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "不转")

    def test_audio_candidate_cannot_append_watermark_to_short_dialogue(self):
        visual = [entry(1, 0.0, 1.0, "王虎", "dialogue")]
        audio = [entry(1, 0.0, 1.0, "王虎关注我", "dialogue")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "王虎")

    def test_watermark_audio_sentence_is_not_inserted_as_supplement(self):
        audio_entry = entry(1, 2.0, 3.0, "关注我看全集")
        self.assertFalse(core.should_insert_funasr_sentence(audio_entry, []))

    def test_short_audio_sentence_is_not_inserted_as_supplement(self):
        audio_entry = entry(1, 2.0, 3.0, "慢着")
        self.assertFalse(core.should_insert_funasr_sentence(audio_entry, []))

    def test_funasr_merge_blocks_neighbor_transition_suffix(self):
        merged = core.merge_visual_text_with_funasr_correction("女人刚下车", "女人刚下车没想到")
        self.assertEqual(merged, "女人刚下车")

    def test_funasr_merge_blocks_previous_tail_prefix(self):
        merged = core.merge_visual_text_with_funasr_correction(
            "男人转身离开",
            "话音刚落男人转身离开",
            previous_visual_text="话音刚落",
        )
        self.assertEqual(merged, "男人转身离开")

    def test_contextual_ocr_correction_allows_same_length_typo(self):
        self.assertTrue(core.safe_contextual_ocr_correction_variant("紧上的二小姐", "车上的二小姐"))
        self.assertEqual(
            core.subtitle_ocr_correction_rejection_reason("紧上的二小姐", "车上的二小姐"),
            "",
        )

    def test_contextual_ocr_correction_rejects_numeric_anchor_change(self):
        reason = core.subtitle_ocr_correction_rejection_reason("到账4000万", "到账5000万")
        self.assertEqual(reason, "numeric anchor changed")

    def test_pause_safe_rewrite_keeps_pause_boundaries(self):
        reason = core.pause_safe_rewrite_rejection_reason("女人逃走，男人追来", "女人逃走男人追来")
        self.assertEqual(reason, "pause punctuation changed")

    def test_pause_safe_rewrite_keeps_numeric_anchors(self):
        reason = core.pause_safe_rewrite_rejection_reason("到账4000万。", "到账5000万。")
        self.assertEqual(reason, "numeric anchor changed")

    def test_delivery_filters_isolated_original_ocr_glyphs(self):
        delivered = core.build_delivery_subtitle_entries(
            [
                entry(1, 0.0, 0.3, "C", "original_subtitle"),
                entry(2, 0.4, 1.1, "慢着", "original_subtitle"),
            ]
        )
        self.assertEqual([item.text for item in delivered], ["慢着"])

    def test_delivery_keeps_valid_original_dialogue_subtitle(self):
        delivered = core.build_delivery_subtitle_entries(
            [entry(1, 0.0, 1.0, "给你机会你不要", "original_subtitle")]
        )
        self.assertEqual([item.text for item in delivered], ["给你机会你不要"])

    def test_boundary_locked_delivery_does_not_merge_two_reference_srt_lines(self):
        reference = [
            entry(1, 0.0, 1.0, "得救"),
            entry(2, 1.08, 2.0, "一番交谈"),
        ]
        rendered = [entry(1, 0.0, 2.0, "得救一番交谈")]
        delivered = core.build_boundary_locked_delivery_subtitle_entries(reference, rendered, fps=30)
        self.assertEqual([item.text for item in delivered], ["得救", "一番交谈"])

    def test_visual_display_split_does_not_touch_short_audio_pause_group(self):
        audio_group = [entry(1, 0.0, 1.5, "shortline")]
        visual = [
            entry(1, 0.0, 0.7, "short"),
            entry(2, 0.8, 1.5, "line"),
        ]

        delivered, split_count = core.split_long_delivery_entries_by_visual_subtitles(
            audio_group,
            visual,
            fps=30,
        )

        self.assertEqual(split_count, 0)
        self.assertEqual([item.text for item in delivered], ["shortline"])

    def test_display_delivery_does_not_split_short_sentence(self):
        delivered = core.build_display_delivery_subtitle_entries([entry(1, 0.0, 1.2, "男人刚刚下车")])
        self.assertEqual([item.text for item in delivered], ["男人刚刚下车"])

    def test_display_delivery_splits_long_two_sentence_narration(self):
        delivered = core.build_display_delivery_subtitle_entries(
            [entry(1, 0.0, 2.2, "男人刚刚下车。没想到杀手就冲了上来。")]
        )
        self.assertEqual(len(delivered), 2)
        self.assertTrue(delivered[0].text.startswith("男人刚刚下车"))
        self.assertTrue(delivered[1].text.startswith("没想到杀手就冲了上来"))

    def test_reliable_visual_boundary_pollution_detects_suffix_without_neighbor(self):
        self.assertTrue(
            core.reliable_visual_audio_extension_looks_like_boundary_pollution(
                "女人刚到门口",
                "女人刚到门口没想到",
            )
        )

    def test_reliable_visual_boundary_pollution_allows_tiny_tail(self):
        self.assertFalse(
            core.reliable_visual_audio_extension_looks_like_boundary_pollution(
                "结婚第一",
                "结婚第一天",
            )
        )

    def test_audio_does_not_delete_natural_reduplication(self):
        visual = [entry(1, 0.0, 1.0, "男人冷冷地看着他")]
        audio = [entry(1, 0.0, 1.0, "男人冷地看着他")]
        repaired, fix_count = core.repair_entries_with_audio_text_support(visual, audio)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired[0].text, "男人冷冷地看着他")


class SyntheticSubtitleStressMatrixTests(unittest.TestCase):
    def repair_pair(self, visual_text, audio_text, entry_type="narration"):
        repaired, fix_count = core.repair_entries_with_audio_text_support(
            [entry(1, 0.0, 1.0, visual_text, entry_type)],
            [entry(1, 0.0, 1.0, audio_text, entry_type)],
        )
        return repaired[0].text, fix_count

    def assert_not_changed_by_audio(self, visual_text, audio_text, entry_type="narration"):
        repaired_text, fix_count = self.repair_pair(visual_text, audio_text, entry_type=entry_type)
        self.assertEqual(fix_count, 0)
        self.assertEqual(repaired_text, visual_text)

    def assert_repaired_by_audio(self, visual_text, audio_text, entry_type="narration"):
        repaired_text, fix_count = self.repair_pair(visual_text, audio_text, entry_type=entry_type)
        self.assertEqual(fix_count, 1)
        self.assertEqual(repaired_text, audio_text)

    def test_transition_prefix_and_suffix_pollution_matrix(self):
        base_texts = [
            "\u5973\u4eba\u521a\u5230\u95e8\u53e3",
            "\u7537\u4eba\u51b2\u8fdb\u5927\u5385",
            "\u5c0f\u4f19\u62ac\u5934\u770b\u53bb",
            "\u8001\u7237\u5b50\u6c14\u5f97\u53d1\u6296",
        ]
        prefixes = [
            "\u6ca1\u60f3\u5230",
            "\u8c01\u77e5",
            "\u54ea\u6599",
            "\u54ea\u77e5",
            "\u5c82\u6599",
            "\u8c01\u6599",
            "\u7ed3\u679c",
            "\u4e8e\u662f",
            "\u5e76\u4e14",
            "\u7136\u800c",
            "\u4e0d\u6599",
            "\u5374",
            "\u53c8",
            "\u8bdd\u97f3\u521a\u843d",
        ]
        suffixes = prefixes + [
            "\u5173\u6ce8\u6211",
            "\u5173\u6ce8\u6211\u770b\u5168\u96c6",
        ]
        for base_text in base_texts:
            for prefix in prefixes:
                with self.subTest(direction="prefix", base=base_text, affix=prefix):
                    self.assert_not_changed_by_audio(base_text, prefix + base_text)
            for suffix in suffixes:
                with self.subTest(direction="suffix", base=base_text, affix=suffix):
                    self.assert_not_changed_by_audio(base_text, base_text + suffix)

    def test_short_tail_restoration_matrix(self):
        cases = [
            ("\u653e\u4e0b\u72e0", "\u653e\u4e0b\u72e0\u8bdd"),
            ("\u6482\u4e0b\u72e0", "\u6482\u4e0b\u72e0\u8bdd"),
            ("\u7559\u4e0b\u72e0", "\u7559\u4e0b\u72e0\u8bdd"),
            ("AI\u6362", "AI\u6362\u8138"),
            ("\u7b2c\u4e00", "\u7b2c\u4e00\u5929"),
            ("\u8179\u4e2d\u80ce", "\u8179\u4e2d\u80ce\u513f"),
        ]
        for visual_text, audio_text in cases:
            with self.subTest(visual=visual_text, audio=audio_text):
                self.assert_repaired_by_audio(visual_text, audio_text)

    def test_short_weak_particle_tail_is_not_added_matrix(self):
        unsafe_cases = [
            ("\u5c0f\u4f19\u8bf4", "\u5c0f\u4f19\u8bf4\u554a"),
            ("\u7537\u4eba\u8d70", "\u7537\u4eba\u8d70\u5427"),
            ("\u5979\u62ac\u5934", "\u5979\u62ac\u5934\u5462"),
            ("\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00", "\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00\u554a"),
            ("\u5973\u4eba\u521a\u8d70\u5230\u95e8\u53e3", "\u5973\u4eba\u521a\u8d70\u5230\u95e8\u53e3\u5427"),
        ]
        for visual_text, audio_text in unsafe_cases:
            with self.subTest(visual=visual_text, audio=audio_text):
                self.assert_not_changed_by_audio(visual_text, audio_text)
        self.assert_repaired_by_audio("\u5973\u4eba\u54ed", "\u5973\u4eba\u54ed\u4e86")

    def test_too_short_dialogue_is_not_extended_by_audio_tail(self):
        cases = [
            ("\u738b\u864e", "\u738b\u864e\u554a"),
            ("\u7238", "\u7238\u7238"),
            ("\u6162\u7740", "\u6162\u7740\u554a"),
            ("\u4e0d\u8f6c", "\u4e0d\u8f6c\u4e86"),
        ]
        for visual_text, audio_text in cases:
            with self.subTest(visual=visual_text, audio=audio_text):
                self.assert_not_changed_by_audio(visual_text, audio_text, entry_type="dialogue")

    def test_numeric_tail_and_numeric_change_matrix(self):
        safe_tail_cases = [
            ("\u5230\u8d264000", "\u5230\u8d264000\u4e07"),
            ("\u8f6c\u8d265000", "\u8f6c\u8d265000\u4e07"),
            ("\u8d54\u507f300", "\u8d54\u507f300\u4e07"),
        ]
        unsafe_change_cases = [
            ("\u5230\u8d264000\u4e07", "\u5230\u8d265000\u4e07"),
            ("\u8f6c\u8d265000\u4e07", "\u8f6c\u8d26500\u4e07"),
            ("\u8d54\u507f300\u4e07", "\u8d54\u507f3000\u4e07"),
        ]
        for visual_text, audio_text in safe_tail_cases:
            with self.subTest(kind="safe_tail", visual=visual_text, audio=audio_text):
                self.assert_repaired_by_audio(visual_text, audio_text)
        for visual_text, audio_text in unsafe_change_cases:
            with self.subTest(kind="unsafe_change", visual=visual_text, audio=audio_text):
                self.assert_not_changed_by_audio(visual_text, audio_text)

    def test_semantic_numeric_anchor_detection_matrix(self):
        positive_cases = [
            "\u8d54\u507f300\u4e07",
            "\u5230\u8d264000",
            "\u7b2c\u4e00\u5929",
            "\u7b2c12\u96c6",
        ]
        negative_cases = [
            "\u7f8e7",
            "H44",
            "01",
        ]
        for text in positive_cases:
            with self.subTest(kind="positive", text=text):
                self.assertTrue(core.subtitle_text_has_semantic_numeric_anchor(text))
        for text in negative_cases:
            with self.subTest(kind="negative", text=text):
                self.assertFalse(core.subtitle_text_has_semantic_numeric_anchor(text))

    def test_long_repeated_visual_phrase_is_deduped_with_audio_support(self):
        visual_text = "\u5fc5\u987b\u6253\u6389\u8179\u4e2d\u80ce\u513f\u5fc5\u987b\u6253\u6389\u8179\u4e2d\u80ce\u513f"
        audio_text = "\u5fc5\u987b\u6253\u6389\u8179\u4e2d\u80ce\u513f"
        repaired_text, fix_count = self.repair_pair(visual_text, audio_text)
        self.assertEqual(fix_count, 1)
        self.assertEqual(repaired_text, audio_text)

    def test_latin_token_matrix(self):
        safe_tail_cases = [
            ("AI\u6362", "AI\u6362\u8138"),
            ("VIP\u5ba2", "VIP\u5ba2\u6237"),
        ]
        unsafe_missing_cases = [
            ("AI\u6362\u8138", "\u6362\u8138"),
            ("VIP\u5ba2\u6237", "\u5ba2\u6237"),
        ]
        for visual_text, audio_text in safe_tail_cases:
            with self.subTest(kind="safe_tail", visual=visual_text, audio=audio_text):
                self.assert_repaired_by_audio(visual_text, audio_text)
        for visual_text, audio_text in unsafe_missing_cases:
            with self.subTest(kind="unsafe_missing", visual=visual_text, audio=audio_text):
                self.assert_not_changed_by_audio(visual_text, audio_text)

    def test_isolated_ocr_noise_cluster_matrix(self):
        entries = [
            entry(1, 0.0, 0.5, "\u5973\u4eba\u9192\u6765"),
            entry(2, 0.5, 0.7, "H44"),
            entry(3, 0.7, 0.9, "E1"),
            entry(4, 0.9, 1.4, "\u7537\u4eba\u79bb\u5f00"),
            entry(5, 1.4, 1.6, "5.0"),
            entry(6, 1.6, 2.1, "\u4e0b\u4e00\u79d2"),
        ]
        kept, dropped = core.drop_isolated_visual_ocr_noise_entries(entries)
        self.assertEqual(dropped, 3)
        self.assertEqual([item.text for item in kept], ["\u5973\u4eba\u9192\u6765", "\u7537\u4eba\u79bb\u5f00", "\u4e0b\u4e00\u79d2"])

    def test_isolated_digit_ocr_noise_matrix(self):
        entries = [
            entry(1, 0.0, 0.5, "\u7b2c\u4e00\u5929"),
            entry(2, 0.5, 0.7, "1"),
            entry(3, 0.7, 0.9, "01"),
            entry(4, 0.9, 1.4, "\u7537\u4eba\u51fa\u95e8"),
        ]
        kept, dropped = core.drop_isolated_visual_ocr_noise_entries(entries)
        self.assertEqual(dropped, 2)
        self.assertEqual([item.text for item in kept], ["\u7b2c\u4e00\u5929", "\u7537\u4eba\u51fa\u95e8"])

    def test_valid_ai_token_is_kept_inside_visual_cluster(self):
        entries = [
            entry(1, 0.0, 0.5, "\u8fd9\u662f"),
            entry(2, 0.5, 0.7, "AI"),
            entry(3, 0.7, 1.2, "\u6362\u8138"),
        ]
        kept, dropped = core.drop_isolated_visual_ocr_noise_entries(entries)
        self.assertEqual(dropped, 0)
        self.assertEqual([item.text for item in kept], ["\u8fd9\u662f", "AI", "\u6362\u8138"])

    def test_delivery_timeline_normalizes_overlap_and_tiny_duration(self):
        delivered = core.build_delivery_subtitle_entries(
            [
                entry(1, 0.00, 1.00, "\u7b2c\u4e00\u53e5"),
                entry(2, 0.95, 1.50, "\u7b2c\u4e8c\u53e5"),
                entry(3, 1.50, 1.51, "\u7b2c\u4e09\u53e5"),
            ],
            fps=30,
        )
        self.assertEqual([item.text for item in delivered], ["\u7b2c\u4e00\u53e5", "\u7b2c\u4e8c\u53e5", "\u7b2c\u4e09\u53e5"])
        for left, right in zip(delivered, delivered[1:]):
            self.assertGreaterEqual(right.start, left.end + 0.02)
        self.assertGreaterEqual(delivered[-1].end - delivered[-1].start, 1.0 / 30.0)

    def test_delivery_timeline_sorts_rendered_source_audio_windows(self):
        delivered = core.build_delivery_subtitle_entries(
            [
                entry(44, 43.40, 50.80, "\u751a\u81f3\u8fde\u4e00\u53e5\u8bdd\u90fd"),
                entry(45, 38.76, 39.50, "\u4e0d\u8bf4\u671d\u7740\u738b\u864e"),
                entry(46, 51.00, 51.02, "\u77ed\u53e5"),
            ],
            fps=30,
        )

        self.assertEqual(
            [item.text for item in delivered],
            ["\u4e0d\u8bf4\u671d\u7740\u738b\u864e", "\u751a\u81f3\u8fde\u4e00\u53e5\u8bdd\u90fd", "\u77ed\u53e5"],
        )
        for item in delivered:
            self.assertGreater(item.end, item.start)
        for left, right in zip(delivered, delivered[1:]):
            self.assertGreaterEqual(right.start, left.end + 0.02)

    def test_delivery_timeline_filters_unplaceable_same_start_overlap(self):
        delivered = core.build_delivery_subtitle_entries(
            [
                entry(1, 1.00, 1.01, "\u88ab\u6324\u6389"),
                entry(2, 1.00, 1.40, "\u6b63\u5e38\u5b57\u5e55"),
            ],
            fps=30,
        )

        self.assertEqual([item.text for item in delivered], ["\u6b63\u5e38\u5b57\u5e55"])
        self.assertGreater(delivered[0].end, delivered[0].start)

    def test_funasr_sentence_is_inserted_when_visual_time_covers_unrelated_text(self):
        audio_entry = entry(2, 0.90, 1.50, "\u6ca1\u60f3\u5230\u7ba1\u5bb6\u51b2\u4e86\u8fdb\u6765")
        visual_entries = [
            entry(1, 0.00, 1.80, "\u7537\u4eba\u521a\u5230\u95e8\u53e3"),
            entry(2, 1.10, 2.20, "\u5973\u4eba\u8f6c\u8eab\u79bb\u5f00"),
        ]

        self.assertTrue(core.should_insert_funasr_sentence(audio_entry, visual_entries))

    def test_funasr_sentence_is_not_inserted_when_visual_text_covers_it_across_split(self):
        audio_entry = entry(2, 0.90, 1.50, "\u6ca1\u60f3\u5230\u7ba1\u5bb6\u51b2\u4e86\u8fdb\u6765")
        visual_entries = [
            entry(1, 0.90, 1.18, "\u6ca1\u60f3\u5230"),
            entry(2, 1.18, 1.50, "\u7ba1\u5bb6\u51b2\u4e86\u8fdb\u6765"),
        ]

        self.assertFalse(core.should_insert_funasr_sentence(audio_entry, visual_entries))

    def test_watermark_classification_and_delivery_filter_matrix(self):
        watermark_texts = [
            "\u5173\u6ce8\u6211\u770b\u5168\u96c6",
            "\u70b9\u8d5e\u6536\u85cf",
            "\u53f3\u4e0b\u89d2\u641c\u5267\u540d",
            "\u7b2c12\u96c6",
            "\u7b2c\u5341\u4e8c\u96c6",
        ]
        for text in watermark_texts:
            with self.subTest(text=text):
                self.assertEqual(core.heuristic_entry_type(text), "watermark")
                delivered = core.build_delivery_subtitle_entries([entry(1, 0.0, 1.0, text, "watermark")])
                self.assertEqual(delivered, [])

    def test_strong_narration_connectors_classify_as_narration(self):
        narration_texts = [
            "\u4e0e\u6b64\u540c\u65f6\u7537\u4eba\u7ec8\u4e8e\u8d76\u5230",
            "\u6ca1\u60f3\u5230\u5973\u4eba\u8f6c\u8eab\u79bb\u5f00",
            "\u968f\u540e\u4f17\u4eba\u4e00\u62e5\u800c\u4e0a",
            "\u7ed3\u679c\u8001\u7237\u5b50\u6c14\u5f97\u53d1\u6296",
        ]
        for text in narration_texts:
            with self.subTest(text=text):
                self.assertEqual(core.heuristic_entry_type(text), "narration")

    def test_reliable_visual_homophone_audio_matrix(self):
        cases = [
            ("\u8f66\u4e0a\u7684\u4e8c\u5c0f\u59d0", "\u8eab\u4e0a\u7684\u4e8c\u5c0f\u59d0"),
            ("\u8fd9\u4e0d\u8f6c", "\u8fd9\u4e0d\u8d5a"),
            ("\u98de\u8f66\u66b4\u8d70\u65cf", "\u98de\u8f66\u66b4\u8d70\u5f92"),
            ("\u7ed9\u4f60\u8f6c5000\u4e07", "\u7ed9\u4f60\u8f6c5000\u5757"),
        ]
        for visual_text, audio_text in cases:
            with self.subTest(visual=visual_text, audio=audio_text):
                self.assert_not_changed_by_audio(visual_text, audio_text)

    def test_tail_spillover_does_not_move_transition_prefix_matrix(self):
        cases = [
            (
                "\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00",
                "\u5374\u88ab\u5973\u4eba\u62e6\u4f4f",
                "\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00\u5374",
                "\u88ab\u5973\u4eba\u62e6\u4f4f",
            ),
            (
                "\u5973\u4eba\u521a\u8d76\u5230",
                "\u4e8e\u662f\u7acb\u523b\u62a5\u8b66",
                "\u5973\u4eba\u521a\u8d76\u5230\u4e8e\u662f",
                "\u7acb\u523b\u62a5\u8b66",
            ),
        ]
        for left_text, right_text, left_audio, right_audio in cases:
            with self.subTest(left=left_text, right=right_text):
                visual = [entry(1, 0.0, 1.0, left_text), entry(2, 1.0, 2.0, right_text)]
                audio = [entry(1, 0.0, 1.0, left_audio), entry(2, 1.0, 2.0, right_audio)]
                repaired, fix_count = core.repair_audio_tail_spillover_from_next_entry(visual, audio)
                self.assertEqual(fix_count, 0)
                self.assertEqual([item.text for item in repaired], [left_text, right_text])

    def test_boundary_locked_delivery_preserves_reference_group_texts(self):
        reference = [
            entry(1, 0.0, 0.8, "\u4e0b\u4e00\u79d2"),
            entry(2, 0.86, 1.5, "\u7537\u4eba\u51b2\u4e86\u8fdb\u6765"),
            entry(3, 1.58, 2.2, "\u5973\u4eba\u5413\u4e86\u4e00\u8df3"),
        ]
        rendered = [entry(1, 0.0, 2.2, "\u4e0b\u4e00\u79d2\u7537\u4eba\u51b2\u4e86\u8fdb\u6765\u5973\u4eba\u5413\u4e86\u4e00\u8df3")]
        delivered = core.build_boundary_locked_delivery_subtitle_entries(reference, rendered, fps=30)
        self.assertEqual([item.text for item in delivered], [item.text for item in reference])

    def test_funasr_neighbor_boundary_matrix(self):
        cases = [
            ("\u5973\u4eba\u521a\u4e0b\u8f66", "\u5973\u4eba\u521a\u4e0b\u8f66\u6ca1\u60f3\u5230", "", "\u6ca1\u60f3\u5230\u6740\u624b\u8ffd\u6765"),
            ("\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00", "\u8bdd\u97f3\u521a\u843d\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00", "\u8bdd\u97f3\u521a\u843d", ""),
            ("\u7ed9\u4f60\u673a\u4f1a\u4f60\u4e0d\u8981", "\u4e0d\u8f6c\u7ed9\u4f60\u673a\u4f1a\u4f60\u4e0d\u8981", "\u4e0d\u8f6c", ""),
            ("\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929", "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709", "", "\u7ade\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
        ]
        for visual_text, audio_text, previous_text, next_text in cases:
            with self.subTest(visual=visual_text, audio=audio_text):
                merged = core.merge_visual_text_with_funasr_correction(
                    visual_text,
                    audio_text,
                    previous_visual_text=previous_text,
                    next_visual_text=next_text,
                )
                self.assertEqual(merged, visual_text)

    def test_short_neighbor_prefix_ownership_stress_matrix(self):
        left_texts = [
            "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929",
            "\u5374\u7528\u62d6\u62c9\u673a",
            "\u5c31\u6cfc\u4e86\u8fc7\u53bb",
            "\u5973\u4eba\u521a\u8d70\u4e0b\u5a5a\u8f66",
            "\u4ed6\u8f6c\u8eab\u770b\u5411\u4f17\u4eba",
            "\u8001\u7237\u5b50\u6c14\u5f97\u53d1\u6296",
        ]
        right_texts = [
            "\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f",
            "\u5c06\u8fdb\u6751\u552f\u4e00\u7684\u8def\u7ed9\u5835\u4f4f",
            "\u738b\u864e\u6ca1\u60f3\u5230\u5979\u8fd9\u4e48\u5f6a",
            "\u4e0b\u8f66\u89e3\u51b3\u9ebb\u70e6",
            "\u6ca1\u60f3\u5230\u6740\u624b\u51b2\u4e86\u4e0a\u6765",
            "\u7ed3\u679c\u4f17\u4eba\u4e00\u62e5\u800c\u4e0a",
            "\u54ea\u6599\u4e0b\u4e00\u79d2\u4ed6\u5c31\u5012\u5728\u5730\u4e0a",
            "\u4e0e\u6b64\u540c\u65f6\u95e8\u5916\u4f20\u6765\u811a\u6b65\u58f0",
        ]
        for left_text in left_texts:
            for right_text in right_texts:
                with self.subTest(left=left_text, right=right_text):
                    visual = [entry(1, 0.0, 1.0, left_text), entry(2, 1.0, 2.0, right_text)]
                    audio = [entry(1, 0.0, 1.0, left_text), entry(2, 1.0, 2.0, right_text)]
                    repaired = repair_visual_with_audio(visual, audio)
                    self.assertEqual([item.text for item in repaired], [left_text, right_text])

    def test_audio_visual_fusion_large_boundary_stress_matrix(self):
        left_texts = [
            "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929",
            "\u5374\u7528\u62d6\u62c9\u673a",
            "\u5c31\u6cfc\u4e86\u8fc7\u53bb",
            "\u5973\u4eba\u521a\u8d70\u4e0b\u5a5a\u8f66",
            "\u7537\u4eba\u8f6c\u8eab\u770b\u5411\u4f17\u4eba",
            "\u8001\u7237\u5b50\u6c14\u5f97\u53d1\u6296",
            "\u4ed6\u6ca1\u60f3\u5230\u8fd9\u79cd\u5973\u4eba\u7adf\u771f\u6562",
            "\u4eca\u5929\u975e\u5f97\u6559\u8bad\u6559\u8bad\u4f60\u4e0d\u884c",
            "\u8fd9\u4e00\u5bb6\u4eba\u7ec8\u4e8e\u7b49\u5230\u673a\u4f1a",
            "\u5c0f\u4f19\u88ab\u4f17\u4eba\u8e39\u5012\u5728\u5730",
        ]
        right_texts = [
            "\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f",
            "\u5c06\u8fdb\u6751\u552f\u4e00\u7684\u8def\u7ed9\u5835\u4f4f",
            "\u738b\u864e\u6ca1\u60f3\u5230\u5979\u8fd9\u4e48\u5f6a",
            "\u4e0b\u8f66\u89e3\u51b3\u9ebb\u70e6",
            "\u6ca1\u60f3\u5230\u6740\u624b\u51b2\u4e86\u4e0a\u6765",
            "\u7ed3\u679c\u4f17\u4eba\u4e00\u62e5\u800c\u4e0a",
            "\u54ea\u6599\u4e0b\u4e00\u79d2\u4ed6\u5c31\u5012\u5728\u5730\u4e0a",
            "\u662f\u5417",
            "\u7136\u800c\u8fd8\u6ca1\u7b49\u4ed6\u8fc7\u53bb",
            "\u4e0e\u6b64\u540c\u65f6\u95e8\u5916\u4f20\u6765\u811a\u6b65\u58f0",
            "\u8f66\u4e0a\u7684\u4e8c\u5c0f\u59d0\u88ab\u5413\u5f97\u8138\u8272\u53d1\u767d",
            "\u98de\u8f66\u66b4\u8d70\u65cf\u7acb\u523b\u56f4\u4e86\u4e0a\u6765",
        ]
        for left_text in left_texts:
            for right_text in right_texts:
                max_stolen_units = min(3, max(1, len(core.subtitle_variant_signature(right_text)) - 2))
                for stolen_units in range(1, max_stolen_units + 1):
                    stolen_prefix = core.funasr_slice_visible_range(right_text, 0, stolen_units)
                    remainder_text = core.funasr_slice_visible_range(
                        right_text,
                        stolen_units,
                        core.funasr_visible_char_count(right_text),
                    )
                    if not stolen_prefix or not remainder_text:
                        continue
                    visual_right = (
                        "\u7ade" + right_text[1:]
                        if right_text.startswith("\u7adf")
                        else right_text
                    )
                    visual = [
                        entry(1, 0.0, 1.0, left_text),
                        entry(2, 1.0, 2.0, visual_right),
                    ]
                    audio = [
                        entry(1, 0.0, 1.16, left_text + stolen_prefix),
                        entry(2, 1.18, 2.0, remainder_text),
                    ]
                    with self.subTest(left=left_text, right=right_text, stolen=stolen_units):
                        repaired = repair_visual_with_audio(visual, audio)
                        self.assertEqual([item.text for item in repaired[:2]], [left_text, visual_right])

        true_tail_cases = [
            (
                "\u4e08\u6bcd\u5a18\u4e5f\u662f\u653e\u4e0b\u72e0",
                "\u8bdd\u4f46\u51e1\u4eca\u5929\u7684\u5a5a\u793c\u6709\u5dee\u6c60",
                "\u4e08\u6bcd\u5a18\u4e5f\u662f\u653e\u4e0b\u72e0\u8bdd",
                "\u4f46\u51e1\u4eca\u5929\u7684\u5a5a\u793c\u6709\u5dee\u6c60",
            ),
            (
                "\u5fc5\u987b\u6253\u6389\u8179\u4e2d\u80ce",
                "\u513f\u624d\u80fd\u8fdb\u95e8",
                "\u5fc5\u987b\u6253\u6389\u8179\u4e2d\u80ce\u513f",
                "\u624d\u80fd\u8fdb\u95e8",
            ),
            (
                "\u539f\u6765\u8fd9\u662fAI\u6362",
                "\u8138\u624d\u5bfc\u81f4\u8bef\u4f1a",
                "\u539f\u6765\u8fd9\u662fAI\u6362\u8138",
                "\u624d\u5bfc\u81f4\u8bef\u4f1a",
            ),
            (
                "\u7ed3\u5a5a\u7b2c\u4e00",
                "\u5929\u5c31\u88ab\u62e6\u4f4f",
                "\u7ed3\u5a5a\u7b2c\u4e00\u5929",
                "\u5c31\u88ab\u62e6\u4f4f",
            ),
            (
                "\u94f6\u884c\u5361\u5230\u8d264000",
                "\u4e07\u540e\u4f17\u4eba\u50bb\u773c",
                "\u94f6\u884c\u5361\u5230\u8d264000\u4e07",
                "\u540e\u4f17\u4eba\u50bb\u773c",
            ),
        ]
        for left_text, right_text, expected_left, expected_right in true_tail_cases:
            visual = [entry(1, 10.0, 11.0, left_text), entry(2, 11.0, 12.0, right_text)]
            audio = [entry(1, 10.0, 10.9, expected_left), entry(2, 10.92, 12.0, expected_right)]
            with self.subTest(kind="true_tail", left=left_text, right=right_text):
                repaired = repair_visual_with_audio(visual, audio)
                self.assertEqual([item.text for item in repaired[:2]], [expected_left, expected_right])

        homophone_cases = [
            ("\u8f66\u4e0a\u7684\u4e8c\u5c0f\u59d0", "\u8eab\u4e0a\u7684\u4e8c\u5c0f\u59d0"),
            ("\u8fd9\u4e0d\u8f6c", "\u8fd9\u4e0d\u8d5a"),
            ("\u98de\u8f66\u66b4\u8d70\u65cf", "\u98de\u8f66\u66b4\u8d70\u5f92"),
            (
                "\u4eca\u5929\u975e\u5f97\u6559\u8bad\u6559\u8bad\u4f60\u4e0d\u884c",
                "\u7136\u800c\u975e\u5f97\u6559\u8bad\u6559\u8bad\u4f60\u4e0d\u884c",
            ),
        ]
        for visual_text, audio_text in homophone_cases:
            with self.subTest(kind="homophone", visual=visual_text, audio=audio_text):
                repaired = repair_visual_with_audio(
                    [entry(1, 20.0, 21.0, visual_text)],
                    [entry(1, 20.0, 21.0, audio_text)],
                )
                self.assertEqual([item.text for item in repaired[:1]], [visual_text])


class StrictTtsPausePlanningRegressionTests(unittest.TestCase):
    def test_measured_strict_tts_rate_raises_global_base_when_rendered_voice_is_slow(self):
        groups = [
            {
                "text": "\u6751\u6c11\u90fd\u56f4\u4e86\u4e0a\u6765",
                "target_duration": 1.50,
                "effective_duration": 2.00,
                "applied_rate": "+6%",
                "success": True,
            },
            {
                "text": "\u738b\u864e\u5374\u4e00\u70b9\u4e5f\u4e0d\u5728\u4e4e",
                "target_duration": 2.00,
                "effective_duration": 2.70,
                "applied_rate": "+6%",
                "success": True,
            },
        ]

        suggested_rate, stats = core.derive_measured_strict_tts_rate(groups, "+6%")

        self.assertGreaterEqual(core.tts_rate_factor(suggested_rate), 1.20)
        self.assertLessEqual(core.tts_rate_factor(suggested_rate), 1.28)
        self.assertGreater(stats["pressure_count"], 0)
        self.assertGreater(stats["observed_ratio"], 1.25)

    def test_measured_strict_tts_rate_keeps_base_when_rendered_voice_already_fits(self):
        groups = [
            {
                "text": "\u4f17\u4eba\u542c\u540e\u90fd\u6123\u4f4f\u4e86",
                "target_duration": 1.60,
                "effective_duration": 1.50,
                "applied_rate": "+18%",
                "success": True,
            }
        ]

        suggested_rate, stats = core.derive_measured_strict_tts_rate(groups, "+18%")

        self.assertEqual(suggested_rate, "+18%")
        self.assertEqual(stats["pressure_count"], 0)

    def test_strict_rate_plan_uses_measured_audio_instead_of_text_density_spikes(self):
        groups = [
            {
                "text": "\u8fd9\u662f\u4e00\u53e5\u5b57\u6570\u5f88\u591a\u4f46\u5df2\u7ecf\u80fd\u653e\u8fdb\u53c2\u8003\u53d1\u58f0\u7a97\u53e3\u7684\u89e3\u8bf4",
                "target_duration": 3.00,
                "effective_duration": 2.40,
                "applied_rate": "+52%",
                "success": True,
            },
            {
                "text": "\u4e0b\u4e00\u53e5\u4e5f\u4fdd\u6301\u7a33\u5b9a\u8bed\u901f",
                "target_duration": 2.00,
                "effective_duration": 1.70,
                "applied_rate": "+52%",
                "success": True,
            },
        ]

        rates, stats = core.plan_strict_tts_synth_rates(groups, "+52%")

        self.assertEqual(rates, ["+52%", "+52%"])
        self.assertAlmostEqual(stats["anchor_factor"], core.tts_rate_factor("+52%"), places=2)

    def test_strict_tts_schedule_targets_reference_speech_window_even_when_tts_is_shorter(self):
        groups = [
            {
                "strict_start": 0.0,
                "strict_end": 2.0,
                "window_end": 2.0,
                "latest_start": 0.0,
                "target_duration": 2.0,
                "effective_duration": 1.30,
                "raw_duration": 1.30,
                "text": "\u7537\u4eba\u8d70\u4e86\u8fc7\u6765",
            }
        ]

        stats = core.schedule_prepared_tts_groups(
            groups,
            total_duration=3.0,
            prefer_strict_windows=True,
        )

        self.assertEqual(stats["hard_trim_count"], 0.0)
        self.assertAlmostEqual(groups[0]["target_duration"], 2.0, places=2)
        self.assertAlmostEqual(groups[0]["scheduled_start"], 0.0, places=2)
        self.assertAlmostEqual(groups[0]["scheduled_end"], 2.0, places=2)

    def test_strict_audio_fit_can_slow_tts_to_reference_speech_window(self):
        speed, output_duration = core.compute_audio_fit_plan(
            source_duration=1.30,
            target_duration=2.00,
            max_speed_factor=core.STRICT_TTS_MAX_LOCAL_SPEED_FACTOR,
            min_speed_factor=core.STRICT_TTS_MIN_LOCAL_SLOWDOWN_FACTOR,
            allow_slowdown_to_fill=True,
            allow_tail_trim=False,
        )

        self.assertIsNotNone(speed)
        self.assertAlmostEqual(speed, 0.65, places=2)
        self.assertAlmostEqual(output_duration, 2.00, places=2)

    def test_strict_audio_fit_keeps_padding_when_tts_is_still_shorter_than_reference_window(self):
        speed, output_duration = core.compute_audio_fit_plan(
            source_duration=1.00,
            target_duration=2.20,
            max_speed_factor=core.STRICT_TTS_MAX_LOCAL_SPEED_FACTOR,
            min_speed_factor=core.STRICT_TTS_MIN_LOCAL_SLOWDOWN_FACTOR,
            allow_slowdown_to_fill=True,
            allow_tail_trim=False,
        )

        self.assertAlmostEqual(speed, core.STRICT_TTS_MIN_LOCAL_SLOWDOWN_FACTOR, places=2)
        self.assertAlmostEqual(output_duration, 2.20, places=2)

    def test_reference_audio_activity_window_tightens_overlong_asr_span(self):
        if not core.NUMPY_AVAILABLE:
            self.skipTest("numpy unavailable")
        sample_rate = 1000
        samples = core.np.zeros(int(sample_rate * 4.0), dtype=core.np.float32)
        for sample_index in range(int(1.0 * sample_rate), int(1.85 * sample_rate)):
            time_value = sample_index / sample_rate
            samples[sample_index] = 0.35 * math.sin(2.0 * math.pi * 180.0 * time_value)
        groups = [
            {
                "strict_start": 0.50,
                "strict_end": 3.00,
                "window_start": 0.50,
                "window_end": 3.00,
                "latest_start": 0.78,
                "target_duration": 2.50,
                "text": "\u53ef\u4e0b\u4e00\u79d2",
            }
        ]

        count = core.apply_reference_audio_activity_tts_windows(
            groups,
            total_duration=4.0,
            primary_samples=samples,
            primary_sample_rate=sample_rate,
            overflow_seconds=0.28,
        )

        self.assertEqual(count, 1)
        self.assertGreater(groups[0]["strict_start"], 0.90)
        self.assertLess(groups[0]["strict_end"], 2.00)
        self.assertLess(groups[0]["target_duration"], 1.10)

    def test_reference_activity_duration_is_recorded_without_forcing_a_shrink(self):
        if not core.NUMPY_AVAILABLE:
            self.skipTest("numpy unavailable")
        sample_rate = 1000
        samples = core.np.zeros(int(sample_rate * 2.0), dtype=core.np.float32)
        for sample_index in range(int(0.08 * sample_rate), int(1.28 * sample_rate)):
            time_value = sample_index / sample_rate
            samples[sample_index] = 0.35 * math.sin(2.0 * math.pi * 180.0 * time_value)
        groups = [
            {
                "strict_start": 0.00,
                "strict_end": 1.38,
                "window_start": 0.00,
                "window_end": 1.38,
                "latest_start": 0.28,
                "target_duration": 1.38,
                "text": "\u6d3b\u4eba\u8fd8\u80fd\u5c3f\u618b\u6b7b",
            }
        ]

        count = core.apply_reference_audio_activity_tts_windows(
            groups,
            total_duration=2.0,
            primary_samples=samples,
            primary_sample_rate=sample_rate,
            overflow_seconds=0.28,
        )

        self.assertEqual(count, 0)
        self.assertIn("reference_activity_duration", groups[0])
        self.assertGreater(groups[0]["reference_activity_duration"], 1.0)

    def test_strict_activity_fit_source_trims_non_speech_tail_before_slowdown(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source_path = Path(temp_dir) / "source.wav"
            output_path = Path(temp_dir) / "activity.wav"
            write_sine_wav(source_path, [(0.20, 0.82)], duration=1.50, sample_rate=16000)

            result = core.prepare_wav_tts_activity_fit_source(
                source_path,
                output_path,
                core.VideoProcessor(),
            )

            self.assertIsNotNone(result)
            _path, trimmed_duration, speech_duration = result
            self.assertTrue(output_path.exists())
            self.assertLess(trimmed_duration, 1.20)
            self.assertGreater(speech_duration, 0.55)

    def test_reference_rate_window_caps_implausibly_long_short_sentence(self):
        groups = [
            {
                "strict_start": 203.71,
                "strict_end": 207.15,
                "window_start": 203.71,
                "window_end": 207.15,
                "latest_start": 203.99,
                "target_duration": 3.44,
                "text": "\u559c\u4e0d\u559c\u6b22",
            }
        ]

        count = core.apply_reference_audio_activity_tts_windows(
            groups,
            total_duration=220.0,
            reference_profile={"median_cps": 7.7, "p75_cps": 8.2, "avg_gap": 0.18},
            overflow_seconds=0.28,
        )

        self.assertEqual(count, 1)
        self.assertLess(groups[0]["target_duration"], 1.20)
        self.assertLess(groups[0]["strict_end"], 205.0)

    def test_tiny_reference_speech_window_merges_into_adjacent_sentence_group(self):
        groups = [
            [entry(1, 0.00, 1.00, "\u7537\u4eba\u521a\u8981\u5f00\u53e3")],
            [entry(2, 1.08, 1.42, "\u53ef\u662f")],
            [entry(3, 1.50, 2.60, "\u738b\u864e\u5df2\u7ecf\u8d70\u4e86\u8fdb\u6765")],
        ]
        join_map = {1: False, 2: False}

        merged, merge_count = core.merge_tiny_reference_window_tts_groups(groups, join_map)

        self.assertEqual(merge_count, 1)
        self.assertEqual([[item.index for item in group] for group in merged], [[1], [2, 3]])
        self.assertTrue(join_map[2])

    def test_tiny_reference_speech_window_keeps_real_pause_boundary(self):
        groups = [
            [entry(1, 0.00, 0.42, "\u4e0b\u4e00\u79d2")],
            [entry(2, 0.82, 1.80, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86")],
        ]

        merged, merge_count = core.merge_tiny_reference_window_tts_groups(groups)

        self.assertEqual(merge_count, 0)
        self.assertEqual([[item.index for item in group] for group in merged], [[1], [2]])

    def test_audio_zero_pause_overrides_visual_break_hint(self):
        entries = [
            entry(1, 0.00, 0.60, "\u98de\u8f66\u66b4\u8d70"),
            entry(2, 0.61, 1.20, "\u65cf\u62e6\u4f4f\u53bb\u8def"),
        ]
        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            0.08,
            boundary_audio_pause_map={1: 0.0},
            boundary_visual_pause_map={1: core.STRICT_TTS_BOUNDARY_AUDIO_PAUSE_MIN_SECONDS},
            boundary_visual_join_map=None,
        )
        self.assertTrue(join_map.get(1))
        self.assertEqual([[item.text for item in group] for group in groups], [[entries[0].text, entries[1].text]])

    def test_strict_audio_window_join_ignores_local_group_caps(self):
        entries = [
            entry(index, index * 0.42, index * 0.42 + 0.36, f"\u89e3\u8bf4{index}")
            for index in range(1, 7)
        ]
        pause_map = {entry_item.index: 0.0 for entry_item in entries[:-1]}

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            reference_gap=0.10,
            boundary_audio_pause_map=pause_map,
            boundary_visual_pause_map={2: core.STRICT_TTS_PERCEPTIBLE_SENTENCE_PAUSE_SECONDS},
            boundary_visual_join_map=None,
        )

        self.assertEqual([[item.index for item in group] for group in groups], [[1, 2, 3, 4, 5, 6]])
        self.assertTrue(all(join_map.get(entry_item.index) for entry_item in entries[:-1]))

    def test_audio_continuity_owns_sentence_window_even_when_subtitle_gap_is_wide(self):
        entries = [
            entry(1, 0.00, 0.70, "\u6751\u6c11\u90fd\u8bf4\u6751\u957f"),
            entry(2, 0.92, 1.80, "\u4e00\u5bb6\u6d3b\u8be5\u88ab\u5835"),
        ]

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            reference_gap=0.10,
            boundary_audio_pause_map={1: 0.0},
        )

        self.assertTrue(join_map.get(1))
        self.assertEqual([[item.index for item in group] for group in groups], [[1, 2]])

    def test_missing_audio_probe_falls_back_to_perceptible_subtitle_gap(self):
        entries = [
            entry(1, 0.00, 0.70, "\u7b2c\u4e00\u53e5\u8bf4\u5b8c"),
            entry(2, 0.88, 1.80, "\u7b2c\u4e8c\u53e5\u518d\u8bf4"),
        ]

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            reference_gap=0.10,
            boundary_audio_pause_map={},
        )

        self.assertFalse(join_map.get(1))
        self.assertEqual([[item.index for item in group] for group in groups], [[1], [2]])

    def test_audio_pause_overrides_visual_join_hint(self):
        entries = [
            entry(1, 0.00, 0.60, "\u98de\u8f66\u66b4\u8d70"),
            entry(2, 0.64, 1.20, "\u65cf\u62e6\u4f4f\u53bb\u8def"),
        ]
        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            0.08,
            boundary_audio_pause_map={1: 0.22},
            boundary_visual_pause_map=None,
            boundary_visual_join_map={1: True},
        )
        self.assertFalse(join_map.get(1))
        self.assertEqual([[item.text for item in group] for group in groups], [[entries[0].text], [entries[1].text]])

    def test_perceptible_audio_pause_blocks_structural_tts_join(self):
        entries = [
            entry(1, 0.00, 0.70, "\u7537\u4eba\u51b3\u5b9a\u628a\u94b1\u4ea4\u7ed9"),
            entry(2, 0.84, 1.40, "\u5973\u4eba\u4fdd\u7ba1"),
        ]
        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            0.08,
            boundary_audio_pause_map={1: core.STRICT_TTS_PERCEPTIBLE_SENTENCE_PAUSE_SECONDS},
            boundary_visual_pause_map=None,
            boundary_visual_join_map=None,
        )
        self.assertFalse(join_map.get(1))
        self.assertEqual([[item.text for item in group] for group in groups], [[entries[0].text], [entries[1].text]])

    def test_visual_break_hint_is_ignored_for_tts_pause_planning(self):
        entries = [
            entry(1, 0.00, 0.60, "\u98de\u8f66\u66b4\u8d70"),
            entry(2, 0.64, 1.20, "\u65cf\u62e6\u4f4f\u53bb\u8def"),
        ]
        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            0.08,
            boundary_audio_pause_map={},
            boundary_visual_pause_map={1: core.STRICT_TTS_BOUNDARY_AUDIO_PAUSE_MIN_SECONDS},
            boundary_visual_join_map=None,
        )
        self.assertTrue(join_map.get(1))
        self.assertEqual([[item.text for item in group] for group in groups], [[entries[0].text, entries[1].text]])

    def test_dialogue_source_audio_protection_subtracts_tts_duck_intervals(self):
        entries = [
            entry(1, 0.00, 0.90, "\u968f\u540e\u7537\u4eba\u8d70\u4e86\u8fc7\u6765", "narration"),
            entry(2, 1.00, 1.80, "\u6162\u7740", "dialogue"),
            entry(3, 2.00, 2.80, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        protected = core.build_source_audio_protect_intervals(
            entries,
            source_handoff_starts={2: 0.92},
            pad_seconds=0.02,
        )
        remaining_duck = core.subtract_time_intervals([(0.0, 3.0)], protected)

        self.assertEqual(protected, [(0.90, 1.82)])
        self.assertEqual(remaining_duck, [(0.0, 0.9), (1.82, 3.0)])

    def test_zero_second_source_handoff_start_is_respected(self):
        entries = [
            entry(1, 1.00, 1.80, "\u8d77\u98de", "dialogue"),
        ]

        protected = core.build_source_audio_protect_intervals(
            entries,
            source_handoff_starts={1: 0.0},
        )
        remaining_duck = core.subtract_time_intervals([(0.0, 2.0)], protected)

        self.assertEqual(protected, [(0.0, 1.8)])
        self.assertEqual(remaining_duck, [(1.8, 2.0)])


class DualSrtFusionStressTests(unittest.TestCase):
    def build_bundle(self, visual, audio):
        settings = core.CloneSettings(
            reference_video=Path("reference.mp4"),
            source_dir=Path("."),
            output_dir=Path("."),
            subtitle_entries=audio,
            visual_subtitle_entries=visual,
            prefer_funasr_audio_subtitles=True,
            disable_ai_subtitle_review=True,
            disable_ai_narration_rewrite=True,
            prefer_funasr_sentence_pauses=True,
        )
        return core.build_processed_subtitles(
            audio,
            "",
            DummyAINarrationGenerator(),
            settings=settings,
        )

    def test_visual_missing_middle_sentence_is_restored_even_when_time_is_covered(self):
        visual = [
            entry(1, 0.0, 1.1, "\u7537\u4eba\u521a\u5230\u95e8\u53e3"),
            entry(2, 1.1, 2.8, "\u5973\u4eba\u8f6c\u8eab\u79bb\u5f00"),
        ]
        audio = [
            entry(1, 0.0, 0.9, "\u7537\u4eba\u521a\u5230\u95e8\u53e3"),
            entry(2, 0.95, 1.55, "\u6ca1\u60f3\u5230\u7ba1\u5bb6\u51b2\u4e86\u8fdb\u6765"),
            entry(3, 1.6, 2.8, "\u5973\u4eba\u8f6c\u8eab\u79bb\u5f00"),
        ]
        bundle = self.build_bundle(visual, audio)

        self.assertIn("\u6ca1\u60f3\u5230\u7ba1\u5bb6\u51b2\u4e86\u8fdb\u6765", [item.text for item in bundle.all_entries])

    def test_visual_missing_short_dialogue_can_be_restored_with_speaker_type(self):
        visual = [
            entry(1, 0.0, 1.1, "\u7537\u4eba\u521a\u5230\u95e8\u53e3"),
            entry(2, 1.6, 2.8, "\u5973\u4eba\u8f6c\u8eab\u79bb\u5f00"),
        ]
        audio = [
            entry(1, 0.0, 0.9, "\u7537\u4eba\u521a\u5230\u95e8\u53e3"),
            entry(2, 0.95, 1.35, "\u6162\u7740"),
            entry(3, 1.6, 2.8, "\u5973\u4eba\u8f6c\u8eab\u79bb\u5f00"),
        ]
        settings = core.CloneSettings(
            reference_video=Path("reference.mp4"),
            source_dir=Path("."),
            output_dir=Path("."),
            subtitle_entries=audio,
            visual_subtitle_entries=visual,
            prefer_funasr_audio_subtitles=True,
            disable_ai_subtitle_review=True,
            disable_ai_narration_rewrite=True,
            prefer_funasr_sentence_pauses=True,
        )
        original_build_overrides = core.build_audio_classification_overrides
        original_run_funasr = core.run_funasr_reference_transcription
        try:
            core.run_funasr_reference_transcription = lambda *args, **kwargs: []
            core.build_audio_classification_overrides = lambda processed_entries, *args, **kwargs: {
                item.index: {
                    "type": "dialogue",
                    "confidence": 0.74,
                    "source": "audio_speaker_espnet_wavlm",
                }
                for item in processed_entries
                if item.text == "\u6162\u7740"
            }
            bundle = core.build_processed_subtitles(
                audio,
                "",
                DummyAINarrationGenerator(),
                reference_video=Path("reference.mp4"),
                video_processor=object(),
                settings=settings,
            )
        finally:
            core.build_audio_classification_overrides = original_build_overrides
            core.run_funasr_reference_transcription = original_run_funasr

        restored = [item for item in bundle.all_entries if item.text == "\u6162\u7740"]
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0].entry_type, "dialogue")

    def test_audio_primary_keeps_audio_sentence_when_visual_missing_whole_sentence(self):
        visual = [
            entry(1, 0.0, 1.1, "\u7537\u4eba\u521a\u5230\u95e8\u53e3"),
            entry(2, 1.6, 2.8, "\u5973\u4eba\u8f6c\u8eab\u79bb\u5f00"),
        ]
        audio = [
            entry(1, 0.0, 0.9, "\u7537\u4eba\u521a\u5230\u95e8\u53e3"),
            entry(2, 0.95, 1.55, "\u6ca1\u60f3\u5230\u7ba1\u5bb6\u51b2\u4e86\u8fdb\u6765"),
            entry(3, 1.6, 2.8, "\u5973\u4eba\u8f6c\u8eab\u79bb\u5f00"),
        ]

        merged, _fix_count, audio_add_count, _split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual([item.text for item in merged], [item.text for item in audio])
        self.assertEqual(audio_add_count, 0)

    def test_visual_missing_head_and_tail_sentences_are_restored(self):
        visual = [
            entry(1, 0.6, 1.8, "\u6751\u91cc\u4eba\u90fd\u770b\u50bb\u4e86"),
        ]
        audio = [
            entry(1, 0.0, 0.55, "\u5927\u5a5a\u5f53\u5929"),
            entry(2, 0.6, 1.8, "\u6751\u91cc\u4eba\u90fd\u770b\u50bb\u4e86"),
            entry(3, 1.9, 2.6, "\u6ca1\u60f3\u5230\u65b0\u90ce\u8f6c\u8eab\u5c31\u8d70"),
        ]
        bundle = self.build_bundle(visual, audio)
        texts = [item.text for item in bundle.all_entries]

        self.assertIn("\u5927\u5a5a\u5f53\u5929", texts)
        self.assertIn("\u6751\u91cc\u4eba\u90fd\u770b\u50bb\u4e86", texts)
        self.assertIn("\u6ca1\u60f3\u5230\u65b0\u90ce\u8f6c\u8eab\u5c31\u8d70", texts)

    def test_audio_only_promo_sentence_is_not_inserted(self):
        visual = [
            entry(1, 0.0, 1.0, "\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00"),
        ]
        audio = [
            entry(1, 0.0, 1.0, "\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00"),
            entry(2, 1.1, 2.0, "\u5173\u6ce8\u6211\u770b\u540e\u7eed"),
        ]
        bundle = self.build_bundle(visual, audio)

        self.assertNotIn("\u5173\u6ce8\u6211\u770b\u540e\u7eed", [item.text for item in bundle.all_entries])

    def test_visual_corrects_audio_typo_without_deleting_audio_pause_boundary(self):
        visual = [
            entry(1, 0.0, 0.8, "\u5230\u8d264000\u4e07"),
            entry(2, 1.1, 1.8, "\u5973\u4eba\u7ec8\u4e8e\u677e\u4e86\u53e3\u6c14"),
        ]
        audio = [
            entry(1, 0.0, 0.8, "\u5230\u8d26\u56db\u5343\u4e07"),
            entry(2, 1.1, 1.8, "\u5973\u4eba\u7ec8\u4e8e\u677e\u4e86\u53e3\u6c14"),
        ]
        bundle = self.build_bundle(visual, audio)
        texts = [item.text for item in bundle.all_entries]

        self.assertEqual(texts[:2], ["\u5230\u8d264000\u4e07", "\u5973\u4eba\u7ec8\u4e8e\u677e\u4e86\u53e3\u6c14"])
        self.assertLess(bundle.all_entries[0].end, bundle.all_entries[1].start)

    def test_audio_complete_word_survives_visual_missing_middle_character(self):
        visual = [
            entry(1, 0.0, 1.0, "\u8001\u5b9e\u4eba\u7ec8\u7206\u53d1\u4e86"),
        ]
        audio = [
            entry(1, 0.0, 1.0, "\u8001\u5b9e\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86"),
        ]
        bundle = self.build_bundle(visual, audio)

        self.assertEqual([item.text for item in bundle.all_entries], ["\u8001\u5b9e\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86"])

    def test_audio_complete_word_survives_visual_missing_tail_character(self):
        visual = [
            entry(1, 52.2, 54.2, "\u674e\u6811\u6839\u4e00\u76f4\u7ed9\u4ed6\u9053"),
        ]
        audio = [
            entry(1, 52.0, 53.92, "\u7b49\u5230\u4e86\u665a\u4e0a\uff0c\u674e\u53d4\u8ddf\u4e00\u76f4\u7ed9\u4ed6\u9053\u6b49\uff0c"),
        ]

        merged, _fix_count, _audio_add_count, _split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertIn("\u9053\u6b49", "".join(item.text for item in merged))

    def test_visual_whole_sentence_noise_does_not_insert_into_audio_primary_timeline(self):
        audio = [
            entry(1, 44.80, 47.04, "\u542c\u5230\u8fd9\u8bdd\u8d75\u79c0\u83b2\u5c06\u7caa\u6876\u5f80\u5730\u4e0a\u4e00\u6254\uff0c\u8bf4"),
            entry(2, 47.28, 48.80, "\u6d3b\u4eba\u8fd8\u80fd\u5c3f\u618b\u6b7b"),
            entry(3, 49.12, 51.92, "\u4ed6\u4e5f\u4e0d\u5728\u4e4e\u4efb\u4f55\u7684\u98ce\u4fd7\uff0c\u8ba9\u674e\u53d4\u8ddf\u62b1\u7740\u81ea\u5df1\u5c31\u56de\u4e86\u5bb6"),
        ]
        visual = [
            entry(1, 44.80, 45.40, "\u542c\u5230\u8fd9\u8bdd"),
            entry(2, 45.40, 47.00, "\u8d75\u79c0\u83b2\u5c06\u7caa\u6876\u5f80\u5730\u4e0a\u4e00\u6254"),
            entry(3, 47.00, 47.40, "\u8bf4\u4ed6\u4e5f\u4e0d\u5728\u4e4e\u4efb\u4f55\u7684\u98ce\u4fd7"),
            entry(4, 47.40, 49.20, "\u6d3b\u4eba\u8fd8\u80fd\u8ba9\u5c3f\u618b\u6b7b\u554a"),
            entry(5, 49.20, 50.40, "\u5979\u4e5f\u4e0d\u5728\u4e4e\u4efb\u4f55\u7684\u98ce\u4fd7"),
            entry(6, 50.40, 52.20, "\u8ba9\u674e\u6811\u6839\u62b1\u7740\u81ea\u5df1\u5c31\u56de\u4e86\u5bb6"),
        ]

        merged, _fix_count, audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(audio_add_count, 0)
        self.assertEqual(split_count, 0)
        self.assertEqual(len(merged), 3)
        self.assertEqual(core.subtitle_variant_signature(merged[0].text), core.subtitle_variant_signature(audio[0].text))
        self.assertIn("\u6d3b\u4eba\u8fd8\u80fd", merged[1].text)
        self.assertIn("\u618b\u6b7b", merged[1].text)
        self.assertNotEqual(merged[1].text, "\u8bf4\u4ed6\u4e5f\u4e0d\u5728\u4e4e\u4efb\u4f55\u7684\u98ce\u4fd7")
        self.assertNotIn("\u4e0d\u5728\u4e4e\u4efb\u4f55\u7684\u98ce\u4fd7", merged[1].text)
        self.assertEqual(sum(1 for item in merged if "\u4e0d\u5728\u4e4e\u4efb\u4f55\u7684\u98ce\u4fd7" in item.text), 1)

    def test_visual_text_assist_only_extends_audio_by_tiny_missing_characters(self):
        self.assertEqual(
            core.merge_funasr_text_with_visual_support(
                "\u8001\u5b9e\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86",
                "\u8001\u5b9e\u4eba\u7ec8\u7206\u53d1\u4e86",
            ),
            "\u8001\u5b9e\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86",
        )
        self.assertEqual(
            core.merge_funasr_text_with_visual_support(
                "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709\u6751",
                "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f",
            ),
            "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709\u6751",
        )

    def test_visual_support_can_safely_complete_one_missing_audio_tail_character(self):
        audio = [entry(1, 0.0, 1.0, "\u5973\u4eba\u8d70\u8fdb\u5927")]
        visual = [entry(1, 0.0, 1.0, "\u5973\u4eba\u8d70\u8fdb\u5927\u5385")]

        merged, fix_count, audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(core.merge_funasr_text_with_visual_support(audio[0].text, visual[0].text), visual[0].text)
        self.assertEqual([item.text for item in merged], [visual[0].text])
        self.assertEqual(fix_count, 1)
        self.assertEqual(audio_add_count, 0)
        self.assertEqual(split_count, 0)

    def test_visual_support_cannot_replace_wrong_audio_character(self):
        audio = [entry(1, 0.0, 1.0, "\u7537\u4eba\u62ff\u8d77\u624b\u673a")]
        visual = [entry(1, 0.0, 1.0, "\u7537\u4eba\u62ff\u8d77\u6c34\u673a")]

        merged, fix_count, audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(core.merge_funasr_text_with_visual_support(audio[0].text, visual[0].text), audio[0].text)
        self.assertEqual([item.text for item in merged], [audio[0].text])
        self.assertEqual(fix_count, 0)
        self.assertEqual(audio_add_count, 0)
        self.assertEqual(split_count, 0)

    def test_visual_support_cannot_delete_audio_head_fragment(self):
        audio = [entry(1, 0.0, 1.2, "\u6ca1\u60f3\u5230\u7537\u4eba\u62ff\u8d77\u624b\u673a")]
        visual = [entry(1, 0.0, 1.2, "\u7537\u4eba\u62ff\u8d77\u624b\u673a")]

        merged, fix_count, audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(core.merge_funasr_text_with_visual_support(audio[0].text, visual[0].text), audio[0].text)
        self.assertEqual([item.text for item in merged], [audio[0].text])
        self.assertEqual(fix_count, 0)
        self.assertEqual(audio_add_count, 0)
        self.assertEqual(split_count, 0)

    def test_visual_support_cannot_steal_whole_prefix_into_audio_text(self):
        audio = [entry(1, 0.0, 1.0, "\u7537\u4eba\u521a\u8fdb\u95e8")]
        visual = [entry(1, 0.0, 1.0, "\u6ca1\u60f3\u5230\u7537\u4eba\u521a\u8fdb\u95e8")]

        merged, fix_count, audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(core.merge_funasr_text_with_visual_support(audio[0].text, visual[0].text), audio[0].text)
        self.assertEqual([item.text for item in merged], [audio[0].text])
        self.assertEqual(fix_count, 0)
        self.assertEqual(audio_add_count, 0)
        self.assertEqual(split_count, 0)

    def test_audio_existing_character_is_kept_when_visual_is_shorter(self):
        audio = [entry(1, 0.0, 1.0, "\u8001\u5b9e\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86")]
        visual = [entry(1, 0.0, 1.0, "\u8001\u5b9e\u4eba\u7ec8\u7206\u53d1\u4e86")]

        merged, fix_count, audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(core.merge_funasr_text_with_visual_support(audio[0].text, visual[0].text), audio[0].text)
        self.assertEqual([item.text for item in merged], [audio[0].text])
        self.assertEqual(fix_count, 0)
        self.assertEqual(audio_add_count, 0)
        self.assertEqual(split_count, 0)

    def test_visual_ocr_noise_cannot_pollute_audio_primary_text(self):
        audio = [entry(1, 0.0, 1.0, "\u7537\u4eba\u62ff\u8d77\u624b\u673a")]
        visual = [entry(1, 0.0, 1.0, "\u7537\u4eba\u62ffA\u8d77\u624b1\u673a")]

        merged, fix_count, audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(core.merge_funasr_text_with_visual_support(audio[0].text, visual[0].text), audio[0].text)
        self.assertEqual([item.text for item in merged], [audio[0].text])
        self.assertEqual(fix_count, 0)
        self.assertEqual(audio_add_count, 0)
        self.assertEqual(split_count, 0)

    def test_visual_ocr_prefers_main_subtitle_geometry_over_side_text(self):
        lines = [
            subtitle_runner.OcrTextLine(
                "\u5305\u5b50\u8bf4\u5267",
                xmin=12,
                xmax=92,
                ymin=72,
                ymax=92,
                canvas_width=720,
                canvas_height=220,
            ),
            subtitle_runner.OcrTextLine(
                "\u771f\u6b63\u7684\u4e3b\u5b57\u5e55",
                xmin=128,
                xmax=592,
                ymin=94,
                ymax=132,
                canvas_width=720,
                canvas_height=220,
            ),
        ]

        self.assertEqual(subtitle_runner.select_preferred_subtitle_text(lines), "\u771f\u6b63\u7684\u4e3b\u5b57\u5e55")

    def test_visual_ocr_keeps_centered_two_line_subtitle_by_geometry(self):
        lines = [
            subtitle_runner.OcrTextLine(
                "\u4e00\u53e5\u6bd4\u8f83\u957f\u7684\u4e3b\u5b57\u5e55",
                xmin=94,
                xmax=626,
                ymin=54,
                ymax=92,
                canvas_width=720,
                canvas_height=220,
            ),
            subtitle_runner.OcrTextLine(
                "\u7b2c\u4e8c\u884c\u4ecd\u7136\u662f\u5b57\u5e55",
                xmin=142,
                xmax=578,
                ymin=98,
                ymax=136,
                canvas_width=720,
                canvas_height=220,
            ),
        ]

        self.assertEqual(
            subtitle_runner.select_preferred_subtitle_text(lines),
            "\u4e00\u53e5\u6bd4\u8f83\u957f\u7684\u4e3b\u5b57\u5e55\n\u7b2c\u4e8c\u884c\u4ecd\u7136\u662f\u5b57\u5e55",
        )

    def test_aligned_visual_grouping_does_not_override_audio_primary_boundaries(self):
        visual = [
            entry(1, 0.0, 1.2, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929"),
            entry(2, 1.2, 2.6, "\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
            entry(3, 2.6, 3.4, "\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c"),
        ]
        audio = [
            entry(1, 0.0, 1.546, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709\u6751"),
            entry(2, 1.654, 3.28, "\u6c11\u628a\u8def\u62e6\u4f4f\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c"),
        ]
        bundle = self.build_bundle(visual, audio)
        texts = [item.text for item in bundle.all_entries]

        self.assertEqual(texts[0], audio[0].text)
        self.assertTrue(texts[1].startswith(audio[1].text))
        self.assertLessEqual(
            core.funasr_visible_char_count(texts[1]) - core.funasr_visible_char_count(audio[1].text),
            1,
        )
        self.assertFalse(any("\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f\u6c11\u628a\u8def" in text for text in texts))

    def test_visual_tail_completion_dedupes_next_audio_prefix(self):
        visual = [
            entry(1, 0.0, 1.2, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929"),
            entry(2, 1.2, 2.6, "\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
            entry(3, 2.6, 3.4, "\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c"),
            entry(4, 3.4, 4.8, "\u770b\u70ed\u95f9\u7684\u6751\u6c11\u8bae\u8bba\u7eb7\u7eb7"),
        ]
        audio = [
            entry(1, 0.0, 1.546, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709\u6751"),
            entry(2, 1.654, 3.28, "\u6c11\u628a\u8def\u62e6\u4f4f\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c"),
            entry(3, 3.36, 3.96, "\u793c\u3002\u770b\u70ed\u95f9\u7684\u6751"),
        ]

        merged, _fix_count, audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(audio_add_count, 0)
        self.assertEqual(split_count, 0)
        self.assertEqual(merged[1].text, "\u6c11\u628a\u8def\u62e6\u4f4f\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c")
        self.assertEqual(merged[2].text, "\u770b\u70ed\u95f9\u7684\u6751")
        self.assertNotIn(
            "\u846c\u793c\u793c",
            core.normalize_tts_boundary_alignment_text("".join(item.text for item in merged)),
        )

    def test_sub_perceptible_audio_gap_stays_joined_without_visual_tts_help(self):
        entries = [
            entry(1, 0.0, 1.546, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709\u6751"),
            entry(2, 1.654, 3.28, "\u6c11\u628a\u8def\u62e6\u4f4f\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c"),
        ]

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            reference_gap=0.10,
            boundary_audio_pause_map={1: 0.108},
            boundary_visual_join_map={1: True},
        )

        self.assertTrue(join_map.get(1))
        self.assertEqual([[item.index for item in group] for group in groups], [[1, 2]])

    def test_asr_fragment_gap_below_perceptible_pause_does_not_create_tts_pause(self):
        entries = [
            entry(1, 0.0, 1.546, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929\u7adf\u6709\u6751"),
            entry(2, 1.654, 3.28, "\u6c11\u628a\u8def\u62e6\u4f4f\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c"),
        ]

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            reference_gap=0.10,
            boundary_audio_pause_map={1: 0.108},
            boundary_visual_join_map=None,
        )

        self.assertTrue(join_map.get(1))
        self.assertEqual([[item.index for item in group] for group in groups], [[1, 2]])

    def test_visual_join_does_not_override_very_strong_audio_pause(self):
        entries = [
            entry(1, 0.0, 1.0, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929"),
            entry(2, 1.3, 2.0, "\u7adf\u6709\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
        ]

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            reference_gap=0.10,
            boundary_audio_pause_map={1: 0.24},
            boundary_visual_join_map={1: True},
        )

        self.assertFalse(join_map.get(1))
        self.assertEqual([[item.index for item in group] for group in groups], [[1], [2]])

    def test_visual_join_does_not_override_perceptible_audio_pause(self):
        entries = [
            entry(1, 0.0, 1.0, "\u738b\u864e\u6ca1\u60f3\u5230"),
            entry(2, 1.14, 2.0, "\u8fd9\u4e2a\u5973\u4eba\u7adf\u8fd9\u4e48\u5f6a"),
        ]

        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            reference_gap=0.10,
            boundary_audio_pause_map={1: core.STRICT_TTS_PERCEPTIBLE_SENTENCE_PAUSE_SECONDS},
            boundary_visual_join_map={1: True},
        )

        self.assertFalse(join_map.get(1))
        self.assertEqual([[item.index for item in group] for group in groups], [[1], [2]])

    def test_visual_srt_only_splits_long_display_text_not_tts_pause(self):
        entries = [
            entry(
                1,
                0.0,
                3.0,
                "\u8bf4\u7740\u5c31\u8ba9\u4eba\u628a\u72d7\u5e26\u56de\u53bb\u4ed6\u4eec\u6740\u4e86\u5927\u9ec4\u5c31\u662f\u4e3a\u4e86\u5403\u8089",
            )
        ]
        visual = [
            entry(1, 0.0, 1.4, "\u8bf4\u7740\u5c31\u8ba9\u4eba\u628a\u72d7\u5e26\u56de\u53bb"),
            entry(2, 1.4, 3.0, "\u4ed6\u4eec\u6740\u4e86\u5927\u9ec4\u5c31\u662f\u4e3a\u4e86\u5403\u8089"),
        ]

        delivered, split_count = core.split_long_delivery_entries_by_visual_subtitles(
            entries,
            visual,
            fps=30,
        )
        groups, join_map = core.plan_strict_audio_tts_groups(
            entries,
            reference_gap=0.10,
            boundary_audio_pause_map={},
            boundary_visual_join_map={},
        )

        self.assertEqual(split_count, 1)
        self.assertEqual([item.text for item in delivered], [item.text for item in visual])
        self.assertEqual([[item.index for item in group] for group in groups], [[1]])
        self.assertEqual(join_map, {})

    def test_long_delivery_display_falls_back_when_visual_text_is_not_exact(self):
        entries = [
            entry(
                1,
                57.38,
                68.10,
                "甚至还告诉公婆和丈夫，既然自己进了这个家，以后谁都别再想欺负咱们。可她话音刚落，王虎就带着人冲进了家里。这家伙声称这大婚之日没人闹喜怎么能行？说着还想对秀莲动手动脚",
            )
        ]
        visual = [
            entry(1, 57.40, 58.80, "甚至还告诉公婆和丈夫"),
            entry(2, 58.80, 60.20, "既然自己进了这个家"),
            entry(3, 60.20, 61.60, "以后谁都别再想欺负咱们"),
            entry(4, 61.60, 62.40, "可他话音刚落"),
            entry(5, 62.40, 64.00, "王虎就带着人冲进了家里"),
            entry(6, 64.00, 65.40, "这家伙声称这大婚之日"),
            entry(7, 65.40, 66.40, "没人闹戏怎么能行"),
            entry(8, 66.40, 68.40, "说着还想对秀莲动手动脚"),
        ]

        delivered, split_count = core.split_long_delivery_entries_by_visual_subtitles(
            entries,
            visual,
            fps=50,
        )

        self.assertGreaterEqual(split_count, 1)
        self.assertGreater(len(delivered), 1)
        self.assertTrue(all(core.subtitle_display_units(item.text) <= core.MAX_SUBTITLE_CHARS + 4 for item in delivered))
        self.assertEqual("".join(item.text for item in delivered), entries[0].text)

    def test_reported_narration_tail_is_not_split_as_dialogue_without_direct_evidence(self):
        source = entry(1, 4.04, 6.08, "\u6c11\u8bae\u8bba\u7eb7\u7eb7\uff0c\u90fd\u8bf4\u6751\u957f\u4e00\u5bb6\u6d3b\u8be5\u88ab\u5835")

        self.assertIsNone(core.split_mixed_dialogue_tail_entry(source))

    def test_audio_narrator_override_blocks_text_mixed_tail_split(self):
        source = entry(1, 4.04, 6.08, "\u6c11\u8bae\u8bba\u7eb7\u7eb7\uff0c\u90fd\u8bf4\u6751\u957f\u4e00\u5bb6\u6d3b\u8be5\u88ab\u5835")

        split_entries, split_count = core.split_mixed_reported_speech_entries(
            [source],
            override_meta={
                1: {
                    "type": "narration",
                    "confidence": 0.80,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(split_count, 0)
        self.assertEqual(split_entries, [source])

    def test_post_audio_stage_does_not_create_text_only_dialogue_tail(self):
        source = entry(1, 4.04, 6.08, "\u7537\u4eba\u51b7\u51b7\u5730\u8bf4\u7238")

        pre_audio_entries, pre_audio_count = core.split_mixed_reported_speech_entries([source])
        post_audio_entries, post_audio_count = core.split_mixed_reported_speech_entries(
            [source],
            override_meta={},
        )

        self.assertEqual(pre_audio_count, 1)
        self.assertEqual([item.entry_type for item in pre_audio_entries], ["narration", "dialogue"])
        self.assertEqual(post_audio_count, 0)
        self.assertEqual(post_audio_entries, [source])

    def test_mixed_dialogue_head_is_split_before_speaker_review(self):
        source = entry(
            1,
            98.08,
            105.04,
            "\u5417\uff1f\u7136\u800c\u8fd8\u6ca1\u7b49\u4ed6\u8fc7\u53bb\uff0c\u79c0\u83b2\u53c8\u62ff\u51fa\u4e00\u628a\u5200",
            "dialogue",
        )

        split_entries, split_count = core.split_mixed_reported_speech_entries([source])

        self.assertEqual(split_count, 1)
        self.assertEqual([item.entry_type for item in split_entries], ["dialogue", "narration"])
        self.assertEqual(split_entries[0].text, "\u5417")
        self.assertTrue(split_entries[1].text.startswith("\u7136\u800c\u8fd8\u6ca1\u7b49\u4ed6\u8fc7\u53bb"))

    def test_mixed_dialogue_head_requires_narration_transition(self):
        source = entry(1, 10.0, 12.0, "\u662f\u5417\uff1f\u4f60\u8fd8\u6562\u8fd9\u6837\u8ddf\u6211\u8bf4\u8bdd", "dialogue")

        split_entries, split_count = core.split_mixed_reported_speech_entries([source])

        self.assertEqual(split_count, 0)
        self.assertEqual(split_entries, [source])

    def test_short_speaker_verification_window_is_expanded(self):
        source = entry(1, 10.00, 10.26, "\u6162\u7740", "dialogue")

        window_start, window_end = core.speaker_verification_window_for_entry(source)
        payload = core.speaker_verification_payload_for_entry(source)

        self.assertLess(window_start, source.start)
        self.assertGreater(window_end, source.end)
        self.assertGreaterEqual(window_end - window_start, core.SPEECHBRAIN_MIN_SEGMENT_SECONDS)
        self.assertIsNotNone(payload)

    def test_short_speaker_verification_uses_in_entry_activity_when_samples_available(self):
        if not core.NUMPY_AVAILABLE:
            self.skipTest("numpy not available")

        sample_rate = 16000
        samples = core.np.zeros(int(sample_rate * 2.0), dtype=core.np.float32)
        active_start = int(round(1.09 * sample_rate))
        active_end = int(round(1.19 * sample_rate))
        tone_times = core.np.arange(active_end - active_start, dtype=core.np.float32) / float(sample_rate)
        samples[active_start:active_end] = 0.32 * core.np.sin(2.0 * core.math.pi * 220.0 * tone_times)
        source = entry(1, 1.00, 1.26, "\u6162\u7740", "dialogue")

        window_start, window_end = core.speaker_verification_window_for_entry(source, samples, sample_rate)
        payload = core.speaker_verification_payload_for_entry(source, samples, sample_rate)

        self.assertGreaterEqual(window_start, source.start)
        self.assertLessEqual(window_end, source.end)
        self.assertLess(window_end - window_start, core.SPEECHBRAIN_MIN_SEGMENT_SECONDS)
        self.assertIsNotNone(payload)
        self.assertFalse(payload["allow_context_extension"])
        self.assertEqual(payload["pad_seconds"], 0.0)
        self.assertGreaterEqual(payload["min_duration"], core.SPEECHBRAIN_MIN_SEGMENT_SECONDS)

    def test_long_audio_sentence_can_be_split_by_aligned_visual_subtitles(self):
        audio = [
            entry(
                1,
                0.0,
                6.0,
                "\u8bf4\u7740\u5c31\u8ba9\u4eba\u628a\u72d7\u5e26\u56de\u53bb\u4ed6\u4eec\u6740\u4e86\u5927\u9ec4\u5c31\u662f\u4e3a\u4e86\u5403\u8089\u7b49\u5230\u4ed6\u4eec\u8d70\u540e\u79c0\u83b2\u544a\u8bc9\u4e08\u592b\u4e0d\u8981\u7740\u6025",
            )
        ]
        visual = [
            entry(1, 0.0, 1.6, "\u8bf4\u7740\u5c31\u8ba9\u4eba\u628a\u72d7\u5e26\u56de\u53bb"),
            entry(2, 1.6, 3.4, "\u4ed6\u4eec\u6740\u4e86\u5927\u9ec4\u5c31\u662f\u4e3a\u4e86\u5403\u8089"),
            entry(3, 3.4, 6.0, "\u7b49\u5230\u4ed6\u4eec\u8d70\u540e\u79c0\u83b2\u544a\u8bc9\u4e08\u592b\u4e0d\u8981\u7740\u6025"),
        ]

        split_entries, split_count = core.split_audio_entries_by_aligned_visual_segments(audio, visual)

        self.assertGreaterEqual(split_count, 2)
        self.assertEqual([item.text for item in split_entries], [item.text for item in visual])

    def test_unrelated_visual_noise_does_not_replace_audio_primary_sentence(self):
        visual = [
            entry(1, 20.0, 21.0, "H44"),
            entry(2, 21.1, 22.0, "\u7f8e7"),
        ]
        audio = [
            entry(1, 20.0, 21.0, "\u539f\u6765\u8fd9\u662fAI\u6362\u8138"),
            entry(2, 21.1, 22.0, "\u7537\u4eba\u7acb\u523b\u610f\u8bc6\u5230\u4e0d\u5bf9"),
        ]
        merged, _fix_count, audio_add_count, _split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual([item.text for item in merged], [item.text for item in audio])
        self.assertEqual(audio_add_count, 0)

    def test_visual_missing_whole_sentence_inserts_audio_without_consuming_neighbors(self):
        visual = [
            entry(1, 0.0, 1.0, "\u7236\u4eb2\u7ad9\u5728\u95e8\u53e3"),
            entry(2, 2.0, 3.0, "\u4f17\u4eba\u90fd\u6c89\u9ed8\u4e86"),
        ]
        audio = [
            entry(1, 0.0, 1.0, "\u7236\u4eb2\u7ad9\u5728\u95e8\u53e3"),
            entry(2, 1.05, 1.95, "\u6ca1\u60f3\u5230\u5973\u513f\u7a81\u7136\u5f00\u53e3"),
            entry(3, 2.0, 3.0, "\u4f17\u4eba\u90fd\u6c89\u9ed8\u4e86"),
        ]
        bundle = self.build_bundle(visual, audio)

        self.assertEqual(
            [item.text for item in bundle.all_entries],
            [item.text for item in audio],
        )

    def test_dual_srt_does_not_merge_audio_pause_boundary_from_local_rules(self):
        audio = [
            entry(1, 37.840, 39.520, "\u751a\u81f3\u8fde\u4e00\u53e5\u8bdd\u90fd\u4e0d\u8bf4\uff0c\u671d\u7740\u738b\u864e"),
            entry(2, 39.600, 40.720, "\u51e0\u4eba\u8eab\u4e0a\u5c31\u6cfc\u4e86\u8fc7\u53bb"),
            entry(3, 40.880, 41.680, "\u738b\u864e\u6ca1\u60f3\u5230\u8fd9"),
        ]
        visual = [
            entry(1, 37.8, 39.0, "\u751a\u81f3\u8fde\u4e00\u53e5\u8bdd\u90fd\u4e0d\u8bf4"),
            entry(2, 39.0, 40.2, "\u671d\u7740\u738b\u864e\u51e0\u4eba\u8eab\u4e0a"),
            entry(3, 40.2, 41.0, "\u5c31\u6cfc\u4e86\u8fc7\u53bb"),
            entry(4, 41.0, 41.6, "\u738b\u864e\u6ca1\u60f3\u5230"),
        ]

        merged, _fix_count, _audio_add_count, _split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        texts = [item.text for item in merged]
        self.assertIn("\u51e0\u4eba\u8eab\u4e0a\u5c31\u6cfc\u4e86\u8fc7\u53bb", texts)
        self.assertIn("\u738b\u864e\u6ca1\u60f3\u5230\u8fd9", texts)
        self.assertFalse(any("\u5c31\u6cfc\u4e86\u8fc7\u53bb\u738b\u864e" in text for text in texts))

    def test_short_aligned_visual_track_does_not_force_extra_audio_split(self):
        visual = [
            entry(1, 0.0, 2.0, "\u7ec8\u4e8e\u7206\u53d1\u4e86\u539f\u6765\u5c31\u5728\u4e09\u5929\u524d"),
        ]
        audio = [
            entry(1, 0.0, 0.8, "\u7ec8\u7206\u53d1\u4e86"),
            entry(2, 1.2, 2.0, "\u539f\u6765\u5c31\u5728\u4e09\u5929\u524d"),
        ]
        merged, _fix_count, _audio_add_count, split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual(split_count, 0)
        self.assertEqual([item.text for item in merged], ["\u7ec8\u4e8e\u7206\u53d1\u4e86", "\u539f\u6765\u5c31\u5728\u4e09\u5929\u524d"])

    def test_divergent_visual_srt_does_not_interleave_audio_primary_text(self):
        audio = [
            entry(1, 0.0, 1.0, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929"),
            entry(2, 1.1, 2.0, "\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
        ]
        visual = [
            entry(1, 0.0, 1.0, "\u7236\u5973\u4fe9\u9707\u60ca\u7684\u53d1\u73b0"),
            entry(2, 1.1, 2.0, "\u52b3\u65af\u83b1\u65af\u53e4\u65af\u7279"),
        ]
        merged, _fix_count, audio_add_count, _split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual([item.text for item in merged], ["\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929", "\u6751\u6c11\u628a\u8def\u62e6\u4f4f"])
        self.assertEqual(audio_add_count, 0)

    def test_dense_divergent_visual_track_falls_back_to_visual_instead_of_wrong_audio(self):
        visual = [
            entry(1, 0.0, 1.0, "\u7236\u5973\u4fe9\u9707\u60ca\u7684\u53d1\u73b0"),
            entry(2, 1.1, 2.0, "\u52b3\u65af\u83b1\u65af\u53e4\u65af\u7279"),
            entry(3, 2.1, 3.0, "\u7adf\u7136\u8ddf\u59bb\u5b50\u7684\u8f66\u4e00\u6837"),
            entry(4, 3.1, 4.0, "\u7537\u4eba\u51b3\u5b9a\u5148\u53bb\u9881\u5956\u5178\u793c"),
            entry(5, 4.1, 5.0, "\u6ca1\u60f3\u5230\u66b4\u53d1\u6237\u4e5f\u6765\u4e86"),
            entry(6, 5.1, 6.0, "\u6821\u957f\u5f53\u573a\u5ba3\u5e03\u65b0\u89c4"),
            entry(7, 6.1, 7.0, "\u7537\u4eba\u51b7\u9759\u62ff\u51fa\u9ed1\u5361"),
            entry(8, 7.1, 8.0, "\u4f17\u4eba\u8fd9\u624d\u77e5\u9053\u4ed6\u7684\u8eab\u4efd"),
        ]
        audio = [
            entry(1, 0.0, 1.0, "\u6751\u957f\u513f\u5b50\u5927\u5a5a\u8fd9\u5929"),
            entry(2, 1.1, 2.0, "\u6751\u6c11\u628a\u8def\u62e6\u4f4f"),
            entry(3, 2.1, 3.0, "\u7ed9\u5bb6\u91cc\u7684\u72d7\u529e\u846c\u793c"),
            entry(4, 3.1, 4.0, "\u56e0\u4e3a\u88ab\u4ed6\u4eec\u6b3a\u8d1f"),
            entry(5, 4.1, 5.0, "\u8001\u5b9e\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86"),
            entry(6, 5.1, 6.0, "\u539f\u6765\u5c31\u5728\u4e09\u5929\u524d"),
            entry(7, 6.1, 7.0, "\u6751\u957f\u7684\u513f\u5b50\u738b\u864e"),
            entry(8, 7.1, 8.0, "\u5e26\u7740\u51e0\u4e2a\u5730\u75de\u8d76\u5230\u73b0\u573a"),
        ]
        merged, _fix_count, _audio_add_count, _split_count = core.build_dual_srt_audio_primary_display_entries(audio, visual)

        self.assertEqual([item.text for item in merged], [item.text for item in visual])

    def test_audio_primary_repairs_unreadable_asr_word_fragments(self):
        audio = [
            entry(1, 6.32, 6.72, "\u56e0\u4e3a\u88ab\u4ed6"),
            entry(2, 6.80, 7.38, "\u4eec\u6b3a\u8d1f\u7684\u8001\u5b9e"),
            entry(3, 7.50, 8.16, "\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86"),
            entry(4, 11.20, 11.96, "\u6751\u957f\u7684\u513f\u5b50\u738b"),
            entry(5, 12.04, 13.40, "\u864e\u548c\u51e0\u4e2a\u5730\u75de\u5374\u7528\u62d6\u62c9"),
            entry(6, 13.48, 13.88, "\u673a\u5c06\u8fdb"),
            entry(7, 13.96, 14.88, "\u6751\u552f\u4e00\u8def\u7ed9\u5835\u4f4f"),
        ]
        repaired, fix_count = core.repair_audio_first_sentence_entries(audio)

        self.assertGreaterEqual(fix_count, 1)
        self.assertEqual(
            [item.text for item in repaired],
            [
                "\u56e0\u4e3a\u88ab\u4ed6\u4eec\u6b3a\u8d1f\u7684\u8001\u5b9e\u4eba\u7ec8\u4e8e\u7206\u53d1\u4e86",
                "\u6751\u957f\u7684\u513f\u5b50\u738b\u864e\u548c\u51e0\u4e2a\u5730\u75de\u5374\u7528\u62d6\u62c9\u673a\u5c06\u8fdb\u6751\u552f\u4e00\u8def\u7ed9\u5835\u4f4f",
            ],
        )


@unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for synthetic audio boundary tests")
class CleanAudioTailRegressionTests(unittest.TestCase):
    def run_tail_retime(self, speech_intervals, override_meta=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            wav_path = temp_root / "clean.wav"
            fake_video = temp_root / "clean.mp4"
            fake_video.write_bytes(b"fake")
            write_sine_wav(wav_path, speech_intervals)

            original_extractor = core.extract_reference_audio_for_classification
            try:
                core.extract_reference_audio_for_classification = lambda *args, **kwargs: wav_path
                return core.retime_narration_runs_by_clean_audio_tail(
                    [
                        entry(1, 0.40, 1.00, "慢着", "dialogue"),
                        entry(2, 1.50, 2.10, "随后男人继续解释", "narration"),
                    ],
                    fake_video,
                    video_processor=object(),
                    override_meta=override_meta,
                )
            finally:
                core.extract_reference_audio_for_classification = original_extractor

    def test_dialogue_tail_in_subtitle_gap_is_protected(self):
        repaired = self.run_tail_retime([(0.82, 1.46)])
        self.assertGreaterEqual(repaired[0].end, 1.45)
        self.assertGreaterEqual(repaired[1].start, 1.50)

    def test_silent_gap_does_not_shift_boundary(self):
        repaired = self.run_tail_retime([(0.40, 0.95)])
        self.assertAlmostEqual(repaired[0].end, 1.00, places=2)
        self.assertAlmostEqual(repaired[1].start, 1.50, places=2)

    def test_audio_speaker_locked_boundary_is_not_retimed_by_clean_audio_tail(self):
        repaired = self.run_tail_retime(
            [(0.82, 1.46)],
            override_meta={
                1: {"type": "dialogue", "confidence": 0.82, "source": "audio_speaker_espnet_wavlm"},
                2: {"type": "narration", "confidence": 0.82, "source": "audio_speaker_unispeech_sat"},
            },
        )
        self.assertAlmostEqual(repaired[0].end, 1.00, places=2)
        self.assertAlmostEqual(repaired[1].start, 1.50, places=2)


class AudioClassificationOverrideRegressionTests(unittest.TestCase):
    def test_no_seed_narrator_family_does_not_absorb_moderately_similar_voice(self):
        self.assertFalse(
            core.audio_cluster_belongs_to_narrator_family(
                cluster_id=2,
                narrator_cluster_id=0,
                has_narrator_seed=False,
                family_similarity=0.90,
                cluster_to_narrator_similarity=0.90,
                cluster_score=9.5,
                best_cluster_score=10.0,
                narration_advantage=0.20,
                ai_dialogue_density=0.0,
                ai_narration_density=0.0,
            )
        )

    def test_no_seed_narrator_family_keeps_only_near_identical_voice(self):
        self.assertTrue(
            core.audio_cluster_belongs_to_narrator_family(
                cluster_id=2,
                narrator_cluster_id=0,
                has_narrator_seed=False,
                family_similarity=0.982,
                cluster_to_narrator_similarity=0.982,
                cluster_score=9.8,
                best_cluster_score=10.0,
                narration_advantage=0.42,
                ai_dialogue_density=0.0,
                ai_narration_density=0.0,
            )
        )

    def test_no_seed_narrator_family_rejects_merely_close_character_voice(self):
        self.assertFalse(
            core.audio_cluster_belongs_to_narrator_family(
                cluster_id=2,
                narrator_cluster_id=0,
                has_narrator_seed=False,
                family_similarity=0.95,
                cluster_to_narrator_similarity=0.95,
                cluster_score=9.6,
                best_cluster_score=10.0,
                narration_advantage=0.30,
                ai_dialogue_density=0.0,
                ai_narration_density=0.0,
            )
        )

    def test_model_dialogue_override_survives_local_stabilization(self):
        entries = [
            entry(1, 0.00, 0.90, "\u7537\u4eba\u521a\u8d70\u5230\u95e8\u53e3", "narration"),
            entry(2, 1.00, 1.70, "\u6162\u7740", "narration"),
            entry(3, 1.78, 2.60, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        repaired = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "dialogue",
                    "confidence": 0.76,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_speaker_verification_expands_short_narration_fragments(self):
        entries = [
            entry(13, 23.84, 24.60, "\u6bd5\u7adf\u6751\u957f\u513f\u5b50\u591a", "narration"),
            entry(14, 24.68, 25.08, "\u800c\u4e14", "narration"),
            entry(15, 25.16, 25.48, "\u90fd\u662f\u6df7", "narration"),
            entry(16, 25.56, 26.56, "\u5728\u6751\u91cc\u65e0\u4eba\u6562\u60f9", "narration"),
            entry(17, 26.80, 28.64, "\u738b\u864e\u8ba9\u5c0f\u5f1f\u4eec\u62e6\u4f4f\u674e\u53d4\u8ddf\u548c\u4ed6\u7239", "narration"),
            entry(18, 28.80, 29.40, "\u4ed6\u8d70\u5230\u5a5a\u8f66", "narration"),
            entry(19, 29.48, 29.96, "\u65c1\u8fd8\u60f3", "narration"),
            entry(20, 30.04, 30.88, "\u8d5a\u65b0\u5ab3\u5987\u7684\u4fbf\u5b9c", "narration"),
        ]

        context = core.build_speaker_verification_context_window_map(entries)

        self.assertEqual(context[14], (23.84, 25.08))
        self.assertEqual(context[15], (25.16, 26.56))
        self.assertEqual(context[18], (28.80, 29.96))

    def test_speaker_verification_context_does_not_cross_dialogue_boundary(self):
        entries = [
            entry(1, 0.00, 0.80, "\u7537\u4eba\u8d76\u5230\u95e8\u53e3", "narration"),
            entry(2, 0.88, 1.24, "\u6162\u7740", "dialogue"),
            entry(3, 1.32, 2.10, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        context = core.build_speaker_verification_context_window_map(entries)

        self.assertNotIn(2, context)

    @unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for audio pause grouping")
    def test_speaker_verification_context_uses_real_audio_pause_groups(self):
        entries = [
            entry(13, 23.84, 24.60, "\u6bd5\u7adf\u6751\u957f\u513f\u5b50\u591a", "narration"),
            entry(14, 24.68, 25.08, "\u800c\u4e14", "narration"),
            entry(15, 25.16, 25.48, "\u90fd\u662f\u6df7", "narration"),
            entry(16, 25.56, 26.56, "\u5728\u6751\u91cc\u65e0\u4eba\u6562\u60f9", "narration"),
        ]
        samples, sample_rate = sine_samples([(23.84, 26.56)], duration=27.0)

        context = core.build_speaker_verification_context_window_map(entries, samples, sample_rate)

        self.assertEqual(context[14], (23.84, 26.56))
        self.assertEqual(context[15], (23.84, 26.56))
        self.assertEqual(context[16], (23.84, 26.56))

    @unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for audio pause grouping")
    def test_speaker_verification_context_falls_back_for_uncovered_short_fragments(self):
        entries = [
            entry(1, 0.00, 0.70, "\u6bd5\u7adf\u6751\u957f\u513f\u5b50", "narration"),
            entry(2, 0.84, 1.24, "\u800c\u4e14", "narration"),
            entry(3, 1.32, 1.90, "\u90fd\u662f\u6df7\u6df7", "narration"),
            entry(4, 3.00, 3.50, "\u53ef\u4e0b\u4e00\u79d2", "narration"),
            entry(5, 3.58, 4.10, "\u4f17\u4eba\u90fd\u6123\u4f4f", "narration"),
        ]
        samples, sample_rate = sine_samples(
            [(0.00, 0.70), (0.84, 1.24), (1.32, 1.90), (3.00, 4.10)],
            duration=4.5,
        )

        context = core.build_speaker_verification_context_window_map(entries, samples, sample_rate)

        self.assertEqual(context[2], (0.84, 1.90))
        self.assertEqual(context[4], (3.00, 4.10))

    @unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for audio pause grouping")
    def test_speaker_verification_context_respects_real_audio_pause(self):
        entries = [
            entry(1, 0.00, 0.56, "\u524d\u4e00\u53e5", "narration"),
            entry(2, 0.88, 1.40, "\u540e\u4e00\u53e5", "narration"),
        ]
        samples, sample_rate = sine_samples([(0.00, 0.56), (0.88, 1.40)], duration=1.8)

        context = core.build_speaker_verification_context_window_map(entries, samples, sample_rate)

        self.assertEqual(context, {})

    @unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for audio pause grouping")
    def test_speaker_verification_context_does_not_mix_preliminary_types(self):
        entries = [
            entry(47, 71.36, 73.20, "\u73b0\u5728\u5230\u4e86\u4f60\u8fd9\u4e00\u8f88\u53c8\u7ecf\u5e38\u6b3a\u8d1f\u6211\u513f\u5b50", "dialogue"),
            entry(48, 73.44, 74.40, "\u4f60\u4eec\u7237\u4fe9\u592a\u8fc7\u5206\u4e86", "dialogue"),
            entry(49, 74.50, 75.94, "\u8001\u674e\u8bf4\u7740\u5c31\u60f3\u8981\u628a\u4ed6\u8d76\u51fa\u53bb", "narration"),
        ]
        samples, sample_rate = sine_samples([(71.36, 75.94)], duration=76.5)

        context = core.build_speaker_verification_context_window_map(entries, samples, sample_rate)

        self.assertNotIn(48, context)
        self.assertNotIn(49, context)

    def test_legacy_non_narrator_voice_is_not_a_protected_override(self):
        self.assertFalse(
            core.audio_override_is_protected(
                {
                    "type": "dialogue",
                    "confidence": 0.92,
                    "source": "audio_speaker_non_narrator_voice",
                },
                "dialogue",
            )
        )
        self.assertFalse(
            core.audio_override_has_speaker_evidence(
                {
                    "type": "dialogue",
                    "confidence": 0.92,
                    "source": "audio_speaker_non_narrator_voice",
                }
            )
        )

    def test_legacy_voice_lock_is_not_credible_speaker_evidence(self):
        legacy_override = {
            "type": "narration",
            "confidence": 0.96,
            "source": "audio_speaker_voice_lock",
        }

        self.assertFalse(core.audio_override_is_protected(legacy_override, "narration"))
        self.assertFalse(core.audio_override_has_speaker_evidence(legacy_override))
        self.assertFalse(
            core.audio_override_has_credible_speaker_evidence(
                legacy_override,
                "narration",
                min_confidence=0.62,
            )
        )

    def test_model_dialogue_protection_threshold_matches_override_threshold(self):
        self.assertTrue(
            core.audio_override_is_protected(
                {
                    "type": "dialogue",
                    "confidence": 0.76,
                    "source": "audio_speaker_espnet_wavlm",
                },
                "dialogue",
            )
        )

    def test_gray_forty_four_second_sentence_gets_no_local_continuity_override(self):
        entries = [
            entry(1, 42.80, 43.60, "\u968f\u540e\u4f17\u4eba\u62e6\u5728\u8def\u4e2d\u95f4", "narration"),
            entry(2, 43.92, 44.68, "\u8bf4\u4eca\u5929\u8fd9\u4e2a\u8def\u4f60\u4eec\u522b\u60f3\u8fc7\u4e86", "original_subtitle"),
            entry(3, 44.84, 45.60, "\u674e\u53d4\u53ea\u80fd\u505c\u4e0b\u811a\u6b65", "narration"),
        ]

        strengthened, changed = core.strengthen_audio_overrides_by_continuity(entries, {})
        repaired = core.repair_final_classification_boundaries(entries, override_meta=strengthened)

        self.assertEqual(changed, 0)
        self.assertEqual(strengthened, {})
        self.assertEqual(repaired[1].entry_type, "original_subtitle")

    def test_short_dialogue_overrides_survive_disabled_local_strengthening(self):
        dialogue_texts = [
            "\u6162\u7740",
            "\u522b\u52a8",
            "\u4f60\u5e72\u561b",
            "\u4e0d\u8f6c",
        ]
        for text in dialogue_texts:
            with self.subTest(text=text):
                entries = [
                    entry(1, 0.00, 0.90, "\u7537\u4eba\u8d76\u5230\u95e8\u53e3", "narration"),
                    entry(2, 1.00, 1.42, text, "dialogue"),
                    entry(3, 1.52, 2.20, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
                ]
                overrides = {
                    2: {
                        "type": "dialogue",
                        "confidence": 0.76,
                        "source": "audio_speaker_espnet_wavlm",
                    }
                }

                strengthened, changed = core.strengthen_audio_overrides_by_continuity(entries, overrides)
                repaired = core.repair_final_classification_boundaries(
                    core.apply_audio_classification_overrides(entries, strengthened),
                    override_meta=strengthened,
                )

                self.assertEqual(changed, 0)
                self.assertEqual(strengthened, overrides)
                self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_ambiguous_model_output_uses_audio_gray_fallback(self):
        original_extract = core.extract_reference_audio_for_classification
        original_clean_extract = core.extract_reference_audio_for_uvr_pause_analysis
        original_build_similarity = core.build_speechbrain_similarity_map
        original_detect_ai = core.detect_ai_audio_seed_labels
        original_supplement = core.supplement_audio_seed_labels_locally
        original_has_audio = core.VideoProcessor.has_audio_stream
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_root = Path(temp_dir)
                wav_path = temp_root / "speaker.wav"
                fake_video = temp_root / "reference.mp4"
                fake_video.write_bytes(b"fake")
                write_sine_wav(wav_path, [(0.0, 3.0)], duration=3.2)
                entries = [
                    entry(1, 0.00, 1.00, "\u7537\u4eba\u8bf4\u6211\u4e00\u5b9a\u8981\u8ba9\u4f60\u4eec\u540e\u6094", "dialogue"),
                    entry(2, 1.10, 2.00, "\u53ef\u4e0b\u4e00\u79d2\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
                ]

                core.VideoProcessor.has_audio_stream = lambda *args, **kwargs: True
                core.extract_reference_audio_for_classification = lambda *args, **kwargs: wav_path
                core.extract_reference_audio_for_uvr_pause_analysis = lambda *args, **kwargs: None
                core.detect_ai_audio_seed_labels = lambda *args, **kwargs: {
                    2: {"label": "narration_seed", "confidence": 0.95}
                }
                core.supplement_audio_seed_labels_locally = lambda _entries, _hint_map, _profiles, seed_map, **kwargs: dict(seed_map)
                core.build_speechbrain_similarity_map = lambda *args, **kwargs: {
                    1: {
                        "speaker_engine": "unispeech_sat_large_sv",
                        "narration_similarity": 0.90,
                        "dialogue_similarity": 0.0,
                    },
                    2: {
                        "speaker_engine": "unispeech_sat_large_sv",
                        "narration_similarity": 0.91,
                        "dialogue_similarity": 0.0,
                    },
                }

                overrides = core.build_audio_classification_overrides(entries, fake_video)
        finally:
            core.extract_reference_audio_for_classification = original_extract
            core.extract_reference_audio_for_uvr_pause_analysis = original_clean_extract
            core.build_speechbrain_similarity_map = original_build_similarity
            core.detect_ai_audio_seed_labels = original_detect_ai
            core.supplement_audio_seed_labels_locally = original_supplement
            core.VideoProcessor.has_audio_stream = original_has_audio

        self.assertEqual(sorted(overrides), [1, 2])
        self.assertTrue(overrides[1].get("gray_fallback"))
        self.assertTrue(overrides[2].get("gray_fallback"))
        self.assertEqual(overrides[1]["type"], "narration")
        self.assertEqual(overrides[2]["type"], "narration")

    def test_espnet_wavlm_review_does_not_replace_decisive_primary_voice(self):
        merged = core.merge_espnet_wavlm_similarity_review(
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.88,
                    "dialogue_similarity": 0.28,
                }
            },
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.20,
                    "dialogue_similarity": 0.48,
                }
            },
        )

        self.assertEqual(merged[1]["narration_similarity"], 0.88)

    def test_espnet_wavlm_review_replaces_low_confidence_primary_voice(self):
        merged = core.merge_espnet_wavlm_similarity_review(
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.58,
                    "dialogue_similarity": 0.62,
                }
            },
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.91,
                    "dialogue_similarity": 0.24,
                }
            },
        )

        self.assertEqual(merged[1]["narration_similarity"], 0.91)

    def test_espnet_wavlm_review_fills_missing_dialogue_voice(self):
        merged = core.merge_espnet_wavlm_similarity_review(
            {},
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.18,
                    "dialogue_similarity": 0.48,
                }
            },
        )

        self.assertEqual(merged[1]["dialogue_similarity"], 0.48)

    def test_espnet_wavlm_review_ignores_ambiguous_fallback_voice(self):
        merged = core.merge_espnet_wavlm_similarity_review(
            {},
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.57,
                }
            },
        )

        self.assertEqual(merged, {})

    def test_espnet_wavlm_primary_keeps_new_decisive_voice_over_legacy(self):
        merged = core.merge_espnet_wavlm_primary_with_legacy_fallback(
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.08,
                    "dialogue_similarity": 0.52,
                }
            },
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.86,
                    "dialogue_similarity": 0.24,
                }
            },
        )

        self.assertEqual(merged[1]["dialogue_similarity"], 0.52)

    def test_espnet_wavlm_primary_keeps_ambiguous_new_voice_over_legacy(self):
        merged = core.merge_espnet_wavlm_primary_with_legacy_fallback(
            {
                1: {
                    "narration_similarity": 0.37,
                    "dialogue_similarity": 0.40,
                    "speaker_engine": "espnet_wavlm",
                }
            },
            {
                1: {
                    "narration_similarity": 0.16,
                    "dialogue_similarity": 0.53,
                }
            },
        )

        self.assertEqual(merged[1]["dialogue_similarity"], 0.40)

    def test_espnet_wavlm_strong_dialogue_becomes_hard_override(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.12,
                "dialogue_similarity": 0.55,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")
        self.assertEqual(override["source"], "audio_speaker_espnet_wavlm")

    def test_espnet_wavlm_strong_narration_becomes_hard_override(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.88,
                "dialogue_similarity": 0.30,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "narration")
        self.assertEqual(override["source"], "audio_speaker_espnet_wavlm")

    def test_espnet_wavlm_ambiguous_voice_does_not_become_hard_override(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.62,
                "dialogue_similarity": 0.57,
            }
        )

        self.assertIsNone(override)

    def test_unispeech_sat_strong_dialogue_becomes_hard_override(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": 0.84,
                "dialogue_similarity": 0.97,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")
        self.assertEqual(override["source"], "audio_speaker_unispeech_sat")

    def test_unispeech_sat_strong_narration_becomes_hard_override(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": 0.96,
                "dialogue_similarity": 0.83,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "narration")
        self.assertEqual(override["source"], "audio_speaker_unispeech_sat")

    def test_unispeech_sat_ambiguous_voice_does_not_become_hard_override(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": 0.90,
                "dialogue_similarity": 0.87,
            }
        )

        self.assertIsNone(override)

    def test_anonymous_similarity_cannot_create_speaker_override(self):
        override = core.speaker_similarity_override_from_item(
            {
                "narration_similarity": 0.98,
                "dialogue_similarity": 0.10,
            }
        )

        self.assertIsNone(override)

    def test_unispeech_sat_dialogue_centroid_does_not_override_narrator_verification(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": 0.955,
                "dialogue_similarity": 0.940,
            }
        )

        self.assertIsNone(override)

    def test_unispeech_sat_without_dialogue_seed_can_verify_narrator(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": 0.966,
                "dialogue_similarity": 0.0,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "narration")

    def test_unispeech_sat_without_dialogue_seed_keeps_mid_similarity_ambiguous(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": 0.936,
                "dialogue_similarity": 0.0,
            }
        )

        self.assertIsNone(override)

    def test_unispeech_sat_without_dialogue_seed_rejects_non_narrator(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": 0.78,
                "dialogue_similarity": 0.0,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")

    def test_espnet_wavlm_rejects_narrator_instead_of_matching_dialogue_class(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.35,
                "dialogue_similarity": 0.46,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")

    def test_espnet_wavlm_without_dialogue_seed_keeps_mid_similarity_ambiguous(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.74,
                "dialogue_similarity": 0.0,
            }
        )

        self.assertIsNone(override)

    def test_espnet_wavlm_without_dialogue_seed_rejects_non_narrator(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.40,
                "dialogue_similarity": 0.0,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")

    def test_narrator_only_gray_short_entry_can_follow_neighbor_voice(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.74,
                "dialogue_similarity": 0.0,
                "entry_duration": 0.42,
                "has_context_window": 0.0,
                "previous_narration_similarity": 0.91,
                "next_narration_similarity": 0.90,
                "previous_similarity": 0.68,
                "next_similarity": 0.66,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "narration")

    def test_narrator_only_gray_short_entry_rejects_when_neighbor_voice_differs(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.55,
                "dialogue_similarity": 0.0,
                "entry_duration": 0.42,
                "has_context_window": 0.0,
                "previous_narration_similarity": 0.91,
                "next_narration_similarity": 0.90,
                "previous_similarity": 0.31,
                "next_similarity": 0.34,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")

    @unittest.skipUnless(core.NUMPY_AVAILABLE, "numpy is required for audio pause grouping")
    def test_speaker_context_window_does_not_expand_by_local_entry_type_when_audio_available(self):
        entries = [
            entry(1, 0.00, 0.30, "他", entry_type="narration"),
            entry(2, 0.52, 0.76, "走到", entry_type="narration"),
            entry(3, 0.98, 1.26, "婚车旁", entry_type="narration"),
        ]
        samples, sample_rate = sine_samples([(0.00, 0.30), (0.52, 0.76), (0.98, 1.26)], duration=1.6)

        context_map = core.build_speaker_verification_context_window_map(entries, samples, sample_rate)

        self.assertEqual(context_map, {})

    def test_ai_audio_seed_detection_ignores_dialogue_seeds(self):
        class SeedAI:
            api_key = "test-key"
            system_prompt = ""

            def request_json_object(self, **kwargs):
                self.system_prompt = str(kwargs.get("system_prompt") or "")
                return {
                    "entries": [
                        {"index": 1, "label": "narration_seed", "confidence": 0.93},
                        {"index": 2, "label": "dialogue_seed", "confidence": 0.99},
                    ]
                }

            def note_ai_issue(self, *args, **kwargs):
                pass

        entries = [
            entry(1, 0.0, 1.0, "男人这才意识到事情不对"),
            entry(2, 1.2, 1.8, "你到底想干什么", entry_type="dialogue"),
        ]
        profiles = {item.index: audio_profile_for(item) for item in entries}
        original_needs_ai = core.audio_seed_detection_needs_ai
        original_select_candidates = core.select_audio_ai_seed_candidates
        try:
            core.audio_seed_detection_needs_ai = lambda *args, **kwargs: True
            core.select_audio_ai_seed_candidates = lambda *args, **kwargs: entries
            ai = SeedAI()
            seed_map = core.detect_ai_audio_seed_labels(
                ai,
                entries,
                {},
                profiles,
            )
        finally:
            core.audio_seed_detection_needs_ai = original_needs_ai
            core.select_audio_ai_seed_candidates = original_select_candidates

        self.assertIn(1, seed_map)
        self.assertNotIn(2, seed_map)
        self.assertNotIn("dialogue_seed", ai.system_prompt)

    def test_narrator_only_similarity_calibration_adds_per_video_thresholds(self):
        similarity_map = {
            index + 1: {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": value,
                "dialogue_similarity": 0.0,
            }
            for index, value in enumerate([0.22, 0.31, 0.40, 0.58, 0.70, 0.81, 0.86, 0.89, 0.91, 0.93])
        }

        calibrated = core.calibrate_narrator_only_similarity_map(similarity_map)

        self.assertLessEqual(calibrated[1]["narrator_verify_min"], core.ESPNET_WAVLM_NARRATOR_ONLY_VERIFY_MIN)
        self.assertGreaterEqual(
            calibrated[1]["narrator_verify_min"],
            core.ESPNET_WAVLM_NARRATOR_ONLY_RELAXED_VERIFY_MIN,
        )
        self.assertLessEqual(calibrated[1]["narrator_reject_max"], core.ESPNET_WAVLM_NARRATOR_ONLY_REJECT_MAX)
        self.assertEqual(core.espnet_wavlm_similarity_label(calibrated[1]), "dialogue")
        self.assertEqual(core.espnet_wavlm_similarity_label(calibrated[10]), "narration")
        self.assertEqual(core.espnet_wavlm_similarity_label(calibrated[5]), "")

    def test_narrator_only_calibration_still_keeps_low_mid_voice_ambiguous(self):
        similarity_map = {
            index + 1: {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": value,
                "dialogue_similarity": 0.0,
            }
            for index, value in enumerate([0.22, 0.31, 0.40, 0.58, 0.70, 0.81, 0.86, 0.89, 0.91, 0.93])
        }

        calibrated = core.calibrate_narrator_only_similarity_map(similarity_map)

        self.assertFalse(core.primary_speaker_similarity_is_decisive(calibrated[4]))
        self.assertFalse(core.primary_speaker_similarity_is_decisive(calibrated[5]))
        self.assertTrue(core.primary_speaker_similarity_is_decisive(calibrated[9]))

    def test_narrator_only_calibration_relaxes_espnet_verify_threshold(self):
        scores = [0.18, 0.31, 0.42, 0.52, 0.61, 0.70, 0.76, 0.78, 0.81, 0.84, 0.88, 0.91]
        similarity_map = {
            index + 1: {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": value,
                "dialogue_similarity": 0.0,
            }
            for index, value in enumerate(scores)
        }

        calibrated = core.calibrate_narrator_only_similarity_map(similarity_map)

        self.assertLess(calibrated[1]["narrator_verify_min"], core.ESPNET_WAVLM_NARRATOR_ONLY_VERIFY_MIN)
        self.assertGreaterEqual(
            calibrated[1]["narrator_verify_min"],
            core.ESPNET_WAVLM_NARRATOR_ONLY_RELAXED_VERIFY_MIN,
        )
        self.assertEqual(core.espnet_wavlm_similarity_label(calibrated[12]), "narration")
        self.assertEqual(core.espnet_wavlm_similarity_label(calibrated[11]), "")
        self.assertEqual(core.espnet_wavlm_similarity_label(calibrated[5]), "")

    def test_narrator_only_calibration_relaxes_unispeech_high_tail_only(self):
        scores = [0.72, 0.79, 0.82, 0.86, 0.89, 0.91, 0.93, 0.94, 0.946, 0.950, 0.960, 0.972]
        similarity_map = {
            index + 1: {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": value,
                "dialogue_similarity": 0.0,
            }
            for index, value in enumerate(scores)
        }

        calibrated = core.calibrate_narrator_only_similarity_map(similarity_map)

        self.assertLess(calibrated[1]["narrator_verify_min"], core.UNISPEECH_SAT_NARRATOR_ONLY_VERIFY_MIN)
        self.assertGreaterEqual(
            calibrated[1]["narrator_verify_min"],
            core.UNISPEECH_SAT_NARRATOR_ONLY_RELAXED_VERIFY_MIN,
        )
        self.assertEqual(core.unispeech_sat_similarity_label(calibrated[10]), "narration")
        self.assertEqual(core.unispeech_sat_similarity_label(calibrated[7]), "")

    def test_speaker_thresholds_are_not_shared_between_models(self):
        espnet_high = {
            "speaker_engine": "espnet_wavlm",
            "narration_similarity": 0.93,
            "dialogue_similarity": 0.0,
        }
        unispeech_same_score = {
            "speaker_engine": "unispeech_sat_large_sv",
            "narration_similarity": 0.93,
            "dialogue_similarity": 0.0,
        }
        unispeech_low = {
            "speaker_engine": "unispeech_sat_large_sv",
            "narration_similarity": 0.76,
            "dialogue_similarity": 0.0,
        }
        espnet_same_low_score = {
            "speaker_engine": "espnet_wavlm",
            "narration_similarity": 0.76,
            "dialogue_similarity": 0.0,
        }

        self.assertEqual(core.espnet_wavlm_similarity_label(espnet_high), "narration")
        self.assertEqual(core.unispeech_sat_similarity_label(unispeech_same_score), "")
        self.assertEqual(core.unispeech_sat_similarity_label(unispeech_low), "dialogue")
        self.assertEqual(core.espnet_wavlm_similarity_label(espnet_same_low_score), "")
        direct_overrides = core.direct_speaker_similarity_overrides(
            {
                1: espnet_high,
                2: unispeech_same_score,
                3: unispeech_low,
                4: espnet_same_low_score,
            }
        )

        self.assertEqual(sorted(direct_overrides), [1, 2, 3, 4])
        self.assertEqual(direct_overrides[1]["type"], "narration")
        self.assertEqual(direct_overrides[1]["source"], "audio_speaker_espnet_wavlm")
        self.assertEqual(direct_overrides[2]["type"], "narration")
        self.assertTrue(direct_overrides[2].get("gray_fallback"))
        self.assertEqual(direct_overrides[3]["type"], "dialogue")
        self.assertEqual(direct_overrides[3]["source"], "audio_speaker_unispeech_sat")
        self.assertEqual(direct_overrides[4]["type"], "narration")
        self.assertTrue(direct_overrides[4].get("gray_fallback"))

    def test_gray_fallback_uses_closer_similarity_then_narration_default(self):
        direct_overrides = core.direct_speaker_similarity_overrides(
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.68,
                },
                2: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.60,
                },
            }
        )

        self.assertEqual(direct_overrides[1]["type"], "dialogue")
        self.assertTrue(direct_overrides[1].get("gray_fallback"))
        self.assertEqual(direct_overrides[2]["type"], "narration")
        self.assertTrue(direct_overrides[2].get("gray_fallback"))

    def test_gray_fallback_can_inherit_hard_neighbor_voice_type(self):
        direct_overrides = core.direct_speaker_similarity_overrides(
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.18,
                    "dialogue_similarity": 0.0,
                    "next_index": 2,
                    "next_similarity": 0.82,
                    "next_narration_similarity": 0.62,
                    "next_dialogue_similarity": 0.0,
                },
                2: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.0,
                    "previous_index": 1,
                    "previous_similarity": 0.82,
                    "previous_narration_similarity": 0.18,
                    "previous_dialogue_similarity": 0.0,
                },
            }
        )

        self.assertEqual(direct_overrides[1]["type"], "dialogue")
        self.assertFalse(direct_overrides[1].get("gray_neighbor_fallback"))
        self.assertEqual(direct_overrides[2]["type"], "dialogue")
        self.assertTrue(direct_overrides[2].get("gray_neighbor_fallback"))

    def test_gray_fallback_does_not_inherit_gray_or_conflicting_neighbors(self):
        direct_overrides = core.direct_speaker_similarity_overrides(
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.0,
                    "next_index": 2,
                    "next_similarity": 0.90,
                    "next_narration_similarity": 0.62,
                    "next_dialogue_similarity": 0.0,
                },
                2: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.0,
                    "previous_index": 1,
                    "previous_similarity": 0.90,
                    "next_index": 3,
                    "next_similarity": 0.88,
                    "previous_narration_similarity": 0.62,
                    "previous_dialogue_similarity": 0.0,
                    "next_narration_similarity": 0.93,
                    "next_dialogue_similarity": 0.0,
                },
                3: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.93,
                    "dialogue_similarity": 0.0,
                    "previous_index": 2,
                    "previous_similarity": 0.88,
                    "previous_narration_similarity": 0.62,
                    "previous_dialogue_similarity": 0.0,
                },
                4: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.0,
                    "previous_index": 5,
                    "previous_similarity": 0.84,
                    "next_index": 6,
                    "next_similarity": 0.82,
                    "previous_narration_similarity": 0.18,
                    "previous_dialogue_similarity": 0.0,
                    "next_narration_similarity": 0.93,
                    "next_dialogue_similarity": 0.0,
                },
                5: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.18,
                    "dialogue_similarity": 0.0,
                    "next_index": 4,
                    "next_similarity": 0.84,
                    "next_narration_similarity": 0.62,
                    "next_dialogue_similarity": 0.0,
                },
                6: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.93,
                    "dialogue_similarity": 0.0,
                    "previous_index": 4,
                    "previous_similarity": 0.82,
                    "previous_narration_similarity": 0.62,
                    "previous_dialogue_similarity": 0.0,
                },
            }
        )

        self.assertEqual(direct_overrides[2]["type"], "narration")
        self.assertTrue(direct_overrides[2].get("gray_neighbor_fallback"))
        self.assertEqual(direct_overrides[4]["type"], "narration")
        self.assertFalse(direct_overrides[4].get("gray_neighbor_fallback"))

    def test_gray_neighbor_fallback_can_propagate_to_adjacent_gray_entry(self):
        direct_overrides = core.direct_speaker_similarity_overrides(
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.18,
                    "dialogue_similarity": 0.0,
                    "next_index": 2,
                    "next_similarity": 0.82,
                    "next_narration_similarity": 0.62,
                    "next_dialogue_similarity": 0.0,
                },
                3: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.0,
                    "previous_index": 2,
                    "previous_similarity": 0.84,
                    "previous_narration_similarity": 0.62,
                    "previous_dialogue_similarity": 0.0,
                },
                2: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.62,
                    "dialogue_similarity": 0.0,
                    "previous_index": 1,
                    "previous_similarity": 0.82,
                    "next_index": 3,
                    "next_similarity": 0.84,
                    "previous_narration_similarity": 0.18,
                    "previous_dialogue_similarity": 0.0,
                    "next_narration_similarity": 0.62,
                    "next_dialogue_similarity": 0.0,
                },
            }
        )

        self.assertEqual(direct_overrides[2]["type"], "dialogue")
        self.assertTrue(direct_overrides[2].get("gray_neighbor_fallback"))
        self.assertEqual(direct_overrides[3]["type"], "dialogue")
        self.assertTrue(direct_overrides[3].get("gray_neighbor_fallback"))

    def test_speaker_override_dispatch_keeps_model_sources_separate(self):
        espnet_override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.93,
                "dialogue_similarity": 0.0,
            }
        )
        unispeech_override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "unispeech_sat_large_sv",
                "narration_similarity": 0.76,
                "dialogue_similarity": 0.0,
            }
        )

        self.assertIsNotNone(espnet_override)
        self.assertEqual(espnet_override["type"], "narration")
        self.assertEqual(espnet_override["source"], "audio_speaker_espnet_wavlm")
        self.assertIsNotNone(unispeech_override)
        self.assertEqual(unispeech_override["type"], "dialogue")
        self.assertEqual(unispeech_override["source"], "audio_speaker_unispeech_sat")

    def test_espnet_isolated_narrator_like_sentence_can_override_dialogue_gray_zone(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.87,
                "dialogue_similarity": 0.0,
                "previous_narration_similarity": 0.68,
                "next_narration_similarity": 0.35,
                "previous_similarity": 0.67,
                "next_similarity": 0.35,
                "entry_duration": 2.10,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "narration")
        self.assertEqual(override["source"], "audio_speaker_espnet_wavlm")

    def test_espnet_isolated_narration_guard_keeps_lower_gray_score_ambiguous(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.84,
                "dialogue_similarity": 0.0,
                "previous_narration_similarity": 0.68,
                "next_narration_similarity": 0.35,
                "previous_similarity": 0.67,
                "next_similarity": 0.35,
                "entry_duration": 2.10,
            }
        )

        self.assertIsNone(override)

    def test_espnet_isolated_narration_guard_requires_voice_separation(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.87,
                "dialogue_similarity": 0.0,
                "previous_narration_similarity": 0.68,
                "next_narration_similarity": 0.35,
                "previous_similarity": 0.82,
                "next_similarity": 0.35,
                "entry_duration": 2.10,
            }
        )

        self.assertIsNone(override)

    def test_ambiguous_narrator_only_voice_is_not_hard_narration(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.80,
                "dialogue_similarity": 0.0,
                "narrator_verify_min": 0.86,
                "narrator_reject_max": 0.56,
            }
        )

        self.assertIsNone(override)

    def test_short_uncontexted_narrator_only_mid_score_is_not_hard_dialogue(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.48,
                "dialogue_similarity": 0.0,
                "narrator_verify_min": 0.86,
                "narrator_reject_max": 0.56,
                "entry_duration": 0.40,
                "has_context_window": 0.0,
            }
        )

        self.assertIsNone(override)

    def test_short_uncontexted_clear_non_narrator_still_becomes_dialogue(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.20,
                "dialogue_similarity": 0.0,
                "narrator_verify_min": 0.86,
                "narrator_reject_max": 0.56,
                "entry_duration": 0.40,
                "has_context_window": 0.0,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")

    def test_short_uncontexted_mid_voice_uses_neighbor_contrast_as_dialogue(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.48,
                "dialogue_similarity": 0.0,
                "narrator_verify_min": 0.86,
                "narrator_reject_max": 0.56,
                "entry_duration": 0.40,
                "has_context_window": 0.0,
                "previous_narration_similarity": 0.90,
                "next_narration_similarity": 0.91,
                "previous_similarity": 0.30,
                "next_similarity": 0.32,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")

    def test_short_uncontexted_mid_voice_without_neighbor_contrast_stays_ambiguous(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.48,
                "dialogue_similarity": 0.0,
                "narrator_verify_min": 0.86,
                "narrator_reject_max": 0.56,
                "entry_duration": 0.40,
                "has_context_window": 0.0,
                "previous_narration_similarity": 0.90,
                "next_narration_similarity": 0.91,
                "previous_similarity": 0.68,
                "next_similarity": 0.32,
            }
        )

        self.assertIsNone(override)

    def test_gray_short_entry_can_follow_mixed_narration_neighbor(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.74,
                "dialogue_similarity": 0.0,
                "entry_duration": 0.42,
                "has_context_window": 0.0,
                "previous_narration_similarity": 0.93,
                "next_narration_similarity": 0.28,
                "previous_similarity": 0.70,
                "next_similarity": 0.30,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "narration")

    def test_gray_short_entry_can_follow_mixed_dialogue_neighbor(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.62,
                "dialogue_similarity": 0.0,
                "entry_duration": 0.42,
                "has_context_window": 0.0,
                "previous_narration_similarity": 0.93,
                "next_narration_similarity": 0.24,
                "previous_similarity": 0.29,
                "next_similarity": 0.68,
            }
        )

        self.assertIsNotNone(override)
        self.assertEqual(override["type"], "dialogue")

    def test_gray_short_entry_ignores_gray_neighbors(self):
        override = core.speaker_similarity_override_from_item(
            {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.62,
                "dialogue_similarity": 0.0,
                "entry_duration": 0.42,
                "has_context_window": 0.0,
                "previous_narration_similarity": 0.75,
                "next_narration_similarity": 0.77,
                "previous_similarity": 0.70,
                "next_similarity": 0.69,
            }
        )

        self.assertIsNone(override)

    def test_speaker_classification_uses_original_audio_not_uvr_denoised_audio(self):
        if not core.NUMPY_AVAILABLE:
            self.skipTest("numpy unavailable")

        original_extract = core.extract_reference_audio_for_classification
        original_uvr_extract = core.extract_reference_audio_for_uvr_pause_analysis
        original_load = core.load_wav_mono_samples
        original_build_similarity = core.build_speechbrain_similarity_map
        captured = {}

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_root = Path(temp_dir)
                reference_video = temp_root / "reference.mp4"
                reference_video.write_bytes(b"video")
                raw_audio = temp_root / "raw.wav"
                raw_audio.write_bytes(b"raw")
                denoised_audio = temp_root / "denoised.wav"
                denoised_audio.write_bytes(b"denoised")

                def fake_extract(_reference_video, _processor, log_func=None):
                    return raw_audio

                def fake_uvr_extract(*args, **kwargs):
                    raise AssertionError("speaker classification should not request UVR denoised audio")

                def fake_load(path):
                    self.assertEqual(Path(path), raw_audio)
                    return core.np.zeros(32000, dtype=core.np.float32), 16000

                def fake_similarity(audio_path, entries, ai_seed_map, **kwargs):
                    captured["audio_path"] = Path(audio_path)
                    captured["speaker_samples"] = kwargs.get("speaker_samples")
                    return {
                        1: {
                            "speaker_engine": "espnet_wavlm",
                            "narration_similarity": 0.94,
                            "dialogue_similarity": 0.0,
                        }
                    }

                core.extract_reference_audio_for_classification = fake_extract
                core.extract_reference_audio_for_uvr_pause_analysis = fake_uvr_extract
                core.load_wav_mono_samples = fake_load
                core.build_speechbrain_similarity_map = fake_similarity

                overrides = core.build_audio_classification_overrides(
                    [entry(1, 0.00, 0.80, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "dialogue")],
                    reference_video,
                )
        finally:
            core.extract_reference_audio_for_classification = original_extract
            core.extract_reference_audio_for_uvr_pause_analysis = original_uvr_extract
            core.load_wav_mono_samples = original_load
            core.build_speechbrain_similarity_map = original_build_similarity

        self.assertEqual(captured["audio_path"], raw_audio)
        self.assertIsNotNone(captured["speaker_samples"])
        self.assertEqual(overrides[1]["type"], "narration")

    def test_multi_character_dialogue_does_not_need_dialogue_centroid_match(self):
        overrides = {
            1: core.speaker_similarity_override_from_item(
                {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.40,
                    "dialogue_similarity": 0.0,
                    "narrator_verify_min": 0.86,
                    "narrator_reject_max": 0.56,
                }
            ),
            2: core.speaker_similarity_override_from_item(
                {
                    "speaker_engine": "unispeech_sat_large_sv",
                    "narration_similarity": 0.76,
                    "dialogue_similarity": 0.0,
                    "narrator_verify_min": 0.955,
                    "narrator_reject_max": 0.80,
                }
            ),
        }

        self.assertEqual(overrides[1]["type"], "dialogue")
        self.assertEqual(overrides[2]["type"], "dialogue")

    def test_unispeech_sat_clear_decision_survives_legacy_review(self):
        merged = core.merge_unispeech_sat_clear_with_legacy_review(
            {
                1: {
                    "speaker_engine": "unispeech_sat_large_sv",
                    "narration_similarity": 0.86,
                    "dialogue_similarity": 0.97,
                }
            },
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.88,
                    "dialogue_similarity": 0.30,
                }
            },
        )

        self.assertEqual(merged[1]["speaker_engine"], "unispeech_sat_large_sv")
        self.assertEqual(merged[1]["dialogue_similarity"], 0.97)

    def test_unispeech_sat_ambiguous_entry_allows_espnet_review(self):
        merged = core.merge_unispeech_sat_clear_with_legacy_review(
            {
                1: {
                    "speaker_engine": "unispeech_sat_large_sv",
                    "narration_similarity": 0.89,
                    "dialogue_similarity": 0.87,
                }
            },
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.12,
                    "dialogue_similarity": 0.55,
                }
            },
        )

        self.assertEqual(merged[1]["speaker_engine"], "espnet_wavlm")
        self.assertEqual(merged[1]["dialogue_similarity"], 0.55)

    def test_speechbrain_map_skips_unispeech_review_when_espnet_is_clear(self):
        original_unispeech = core.build_unispeech_sat_similarity_map
        original_espnet = core.build_espnet_wavlm_similarity_map
        calls = {"unispeech": 0, "espnet": 0}

        def fake_unispeech(*args, **kwargs):
            calls["unispeech"] += 1
            return {
                1: {
                    "speaker_engine": "unispeech_sat_large_sv",
                    "narration_similarity": 0.86,
                    "dialogue_similarity": 0.97,
                }
            }

        def fake_espnet(*args, **kwargs):
            calls["espnet"] += 1
            return {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.88,
                    "dialogue_similarity": 0.30,
                }
            }

        try:
            core.build_unispeech_sat_similarity_map = fake_unispeech
            core.build_espnet_wavlm_similarity_map = fake_espnet
            with tempfile.TemporaryDirectory() as temp_dir:
                audio_path = Path(temp_dir) / "audio.wav"
                write_sine_wav(audio_path, [(0.0, 0.5)], duration=0.8)
                result = core.build_speechbrain_similarity_map(
                    audio_path,
                    [entry(1, 0.00, 0.50, "\u6162\u7740", "dialogue")],
                    {1: {"label": "dialogue_seed", "confidence": 0.95}},
                    allow_seed_expansion=False,
                )
        finally:
            core.build_unispeech_sat_similarity_map = original_unispeech
            core.build_espnet_wavlm_similarity_map = original_espnet

        self.assertEqual(result[1]["speaker_engine"], "espnet_wavlm")
        self.assertEqual(calls, {"unispeech": 0, "espnet": 1})

    def test_speechbrain_map_reviews_only_unispeech_ambiguous_entries(self):
        original_unispeech = core.build_unispeech_sat_similarity_map
        original_espnet = core.build_espnet_wavlm_similarity_map
        calls = {"espnet": 0}

        def fake_unispeech(*args, **kwargs):
            return {
                1: {
                    "speaker_engine": "unispeech_sat_large_sv",
                    "narration_similarity": 0.89,
                    "dialogue_similarity": 0.87,
                }
            }

        def fake_espnet(*args, **kwargs):
            calls["espnet"] += 1
            return {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.12,
                    "dialogue_similarity": 0.55,
                }
            }

        try:
            core.build_unispeech_sat_similarity_map = fake_unispeech
            core.build_espnet_wavlm_similarity_map = fake_espnet
            with tempfile.TemporaryDirectory() as temp_dir:
                audio_path = Path(temp_dir) / "audio.wav"
                write_sine_wav(audio_path, [(0.0, 0.5)], duration=0.8)
                result = core.build_speechbrain_similarity_map(
                    audio_path,
                    [entry(1, 0.00, 0.50, "\u6162\u7740", "dialogue")],
                    {1: {"label": "dialogue_seed", "confidence": 0.95}},
                    allow_seed_expansion=False,
                )
        finally:
            core.build_unispeech_sat_similarity_map = original_unispeech
            core.build_espnet_wavlm_similarity_map = original_espnet

        self.assertEqual(result[1]["speaker_engine"], "espnet_wavlm")
        self.assertEqual(calls, {"espnet": 1})

    def test_speechbrain_map_falls_back_to_espnet_when_unispeech_unavailable(self):
        original_unispeech = core.build_unispeech_sat_similarity_map
        original_espnet = core.build_espnet_wavlm_similarity_map

        def fake_unispeech(*args, **kwargs):
            return {}

        def fake_espnet(*args, **kwargs):
            return {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.12,
                    "dialogue_similarity": 0.55,
                }
            }

        try:
            core.build_unispeech_sat_similarity_map = fake_unispeech
            core.build_espnet_wavlm_similarity_map = fake_espnet
            with tempfile.TemporaryDirectory() as temp_dir:
                audio_path = Path(temp_dir) / "audio.wav"
                write_sine_wav(audio_path, [(0.0, 0.5)], duration=0.8)
                result = core.build_speechbrain_similarity_map(
                    audio_path,
                    [entry(1, 0.00, 0.50, "\u6162\u7740", "dialogue")],
                    {1: {"label": "dialogue_seed", "confidence": 0.95}},
                    allow_seed_expansion=False,
                )
        finally:
            core.build_unispeech_sat_similarity_map = original_unispeech
            core.build_espnet_wavlm_similarity_map = original_espnet

        self.assertEqual(result[1]["speaker_engine"], "espnet_wavlm")

    def test_hard_narration_results_expand_secondary_seeds(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u7ad9\u5728\u95e8\u53e3"),
            entry(2, 1.10, 2.10, "\u6ca1\u60f3\u5230\u4e0b\u4e00\u79d2"),
            entry(3, 2.20, 3.20, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86"),
            entry(4, 3.30, 4.30, "\u4f60\u7ed9\u6211\u7b49\u7740", "dialogue"),
        ]
        base_seed_map = {1: {"label": "narration_seed", "confidence": 0.96}}
        similarity_map = {
            1: {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.93,
                "dialogue_similarity": 0.0,
            },
            2: {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.94,
                "dialogue_similarity": 0.0,
            },
            3: {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.92,
                "dialogue_similarity": 0.0,
            },
            4: {
                "speaker_engine": "espnet_wavlm",
                "narration_similarity": 0.18,
                "dialogue_similarity": 0.0,
            },
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

    def test_hard_narration_seed_expansion_reruns_once(self):
        original_unispeech = core.build_unispeech_sat_similarity_map
        original_espnet = core.build_espnet_wavlm_similarity_map
        calls = {"espnet": 0}
        seen_seed_counts = []

        def fake_unispeech(*args, **kwargs):
            return {}

        def fake_espnet(_audio_path, _entries, seed_map, *args, **kwargs):
            calls["espnet"] += 1
            seen_seed_counts.append(sum(1 for item in seed_map.values() if item.get("label") == "narration_seed"))
            if calls["espnet"] == 1:
                return {
                    1: {"speaker_engine": "espnet_wavlm", "narration_similarity": 0.93, "dialogue_similarity": 0.0},
                    2: {"speaker_engine": "espnet_wavlm", "narration_similarity": 0.94, "dialogue_similarity": 0.0},
                    3: {"speaker_engine": "espnet_wavlm", "narration_similarity": 0.92, "dialogue_similarity": 0.0},
                }
            return {
                1: {"speaker_engine": "espnet_wavlm", "narration_similarity": 0.95, "dialogue_similarity": 0.0},
                2: {"speaker_engine": "espnet_wavlm", "narration_similarity": 0.95, "dialogue_similarity": 0.0},
                3: {"speaker_engine": "espnet_wavlm", "narration_similarity": 0.95, "dialogue_similarity": 0.0},
            }

        try:
            core.build_unispeech_sat_similarity_map = fake_unispeech
            core.build_espnet_wavlm_similarity_map = fake_espnet
            with tempfile.TemporaryDirectory() as temp_dir:
                audio_path = Path(temp_dir) / "audio.wav"
                write_sine_wav(audio_path, [(0.0, 3.0)], duration=3.2)
                result = core.build_speechbrain_similarity_map(
                    audio_path,
                    [
                        entry(1, 0.00, 1.00, "\u7537\u4eba\u7ad9\u5728\u95e8\u53e3"),
                        entry(2, 1.10, 2.10, "\u6ca1\u60f3\u5230\u4e0b\u4e00\u79d2"),
                        entry(3, 2.20, 3.20, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86"),
                    ],
                    {1: {"label": "narration_seed", "confidence": 0.96}},
                )
        finally:
            core.build_unispeech_sat_similarity_map = original_unispeech
            core.build_espnet_wavlm_similarity_map = original_espnet

        self.assertEqual(calls["espnet"], 2)
        self.assertEqual(seen_seed_counts, [1, 3])
        self.assertEqual(result[2]["narration_similarity"], 0.95)

    def test_unispeech_sat_override_is_protected_speaker_evidence(self):
        self.assertTrue(
            core.audio_override_is_protected(
                {
                    "type": "dialogue",
                    "confidence": 0.80,
                    "source": "audio_speaker_unispeech_sat",
                },
                "dialogue",
            )
        )

    def test_espnet_wavlm_strong_dialogue_survives_short_island_stabilization(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u521a\u8d70\u8fdb\u9662\u5b50", "narration"),
            entry(2, 1.04, 1.46, "\u6162\u7740", "narration"),
            entry(3, 1.50, 2.30, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        repaired = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "dialogue",
                    "confidence": 0.71,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_espnet_wavlm_strong_narration_survives_dialogue_context_recovery(self):
        entries = [
            entry(1, 0.00, 0.70, "\u4f60\u522b\u540e\u6094", "dialogue"),
            entry(2, 0.74, 1.18, "\u53ef\u4e0b\u4e00\u79d2", "dialogue"),
            entry(3, 1.22, 1.90, "\u6162\u7740", "dialogue"),
        ]

        repaired = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "narration",
                    "confidence": 0.73,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "narration")

    def test_espnet_wavlm_strong_dialogue_long_local_text_not_demoted(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u7acb\u523b\u51b2\u4e86\u8fc7\u6765", "narration"),
            entry(
                2,
                1.20,
                7.80,
                "\u4f60\u4eec\u8fd9\u7fa4\u4eba\u7ed9\u6211\u542c\u597d\uff0c\u4eca\u5929\u8c01\u6562\u52a8\u5979\uff0c\u6211\u5c31\u8ddf\u8c01\u62fc\u547d",
                "dialogue",
            ),
            entry(3, 8.00, 9.00, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        repaired = core.repair_final_classification_boundaries(
            entries,
            override_meta={
                2: {
                    "type": "dialogue",
                    "confidence": 0.71,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_long_local_dialogue_without_speaker_evidence_is_not_text_demoted(self):
        entries = [
            entry(1, 0.00, 1.00, "\u4f60\u7ed9\u6211\u7b49\u7740", "dialogue"),
            entry(
                2,
                1.20,
                7.80,
                "\u7136\u800c\u8fd8\u6ca1\u7b49\u4ed6\u8fc7\u53bb\uff0c\u5973\u4eba\u53c8\u62ff\u51fa\u4e00\u628a\u5200\uff0c\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86",
                "dialogue",
            ),
            entry(3, 8.00, 9.00, "\u968f\u540e\u4f17\u4eba\u90fd\u5413\u574f\u4e86", "narration"),
        ]

        repaired = core.repair_final_classification_boundaries(entries, override_meta={})

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_first_person_narration_requires_acoustic_narrator_evidence(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u7acb\u523b\u51b2\u4e86\u8fc7\u6765", "narration"),
            entry(2, 1.20, 2.80, "\u6211\u4ece\u5c0f\u5c31\u88ab\u738b\u5bb6\u6b3a\u8d1f", "dialogue"),
            entry(3, 3.00, 4.00, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        without_audio = core.repair_final_classification_boundaries(entries, override_meta={})
        with_audio = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "narration",
                    "confidence": 0.82,
                    "source": "audio_speaker_unispeech_sat",
                }
            },
        )

        self.assertEqual(without_audio[1].entry_type, "dialogue")
        self.assertEqual(with_audio[1].entry_type, "narration")

    def test_first_person_character_dialogue_is_kept_when_acoustically_non_narrator(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u7acb\u523b\u51b2\u4e86\u8fc7\u6765", "narration"),
            entry(2, 1.20, 2.80, "\u6211\u6253\u6b7b\u53c8\u80fd\u600e\u4e48\u6837", "narration"),
            entry(3, 3.00, 4.00, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        repaired = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "dialogue",
                    "confidence": 0.78,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_short_local_dialogue_without_speaker_evidence_is_kept(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u7acb\u523b\u51b2\u4e86\u8fc7\u6765", "narration"),
            entry(2, 1.20, 2.50, "\u6211\u6253\u6b7b\u53c8\u80fd\u600e\u4e48\u6837", "dialogue"),
            entry(3, 2.70, 3.60, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        repaired = core.repair_final_classification_boundaries(entries, override_meta={})

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_speaker_backed_long_dialogue_is_not_demoted(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u7acb\u523b\u51b2\u4e86\u8fc7\u6765", "narration"),
            entry(
                2,
                1.20,
                7.80,
                "\u4f60\u4eec\u8fd9\u7fa4\u4eba\u7ed9\u6211\u542c\u597d\uff0c\u4eca\u5929\u8c01\u6562\u52a8\u5979\uff0c\u6211\u5c31\u8ddf\u8c01\u62fc\u547d",
                "dialogue",
            ),
            entry(3, 8.00, 9.00, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        repaired = core.repair_final_classification_boundaries(
            entries,
            override_meta={
                2: {
                    "type": "dialogue",
                    "confidence": 0.76,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_audio_speaker_dialogue_override_survives_apply_stabilization(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u521a\u8d70\u8fdb\u5927\u5385", "narration"),
            entry(2, 1.04, 1.42, "\u6162\u7740", "narration"),
            entry(3, 1.46, 2.40, "\u4f17\u4eba\u90fd\u5012\u5438\u4e00\u53e3\u51c9\u6c14", "narration"),
        ]

        repaired = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "dialogue",
                    "confidence": 0.76,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_audio_speaker_narration_override_survives_apply_stabilization(self):
        entries = [
            entry(1, 0.00, 0.70, "\u4f60\u522b\u540e\u6094", "dialogue"),
            entry(2, 0.72, 1.10, "\u53ef\u4e0b\u4e00\u79d2", "dialogue"),
            entry(3, 1.12, 1.80, "\u6162\u7740", "dialogue"),
        ]

        repaired = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "narration",
                    "confidence": 0.80,
                    "source": "audio_speaker_unispeech_sat",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "narration")

    def test_audio_speaker_dialogue_can_correct_local_narration(self):
        entries = [
            entry(1, 15.36, 17.20, "\u8001\u674e\u8981\u8ddf\u6211\u95f9\u4e86\u4e00\u8f88\u5b50", "dialogue"),
            entry(2, 17.44, 18.56, "\u73b0\u5728\u5a36\u4e86\u4e2a\u65b0\u5ab3\u5987", "narration"),
            entry(3, 19.20, 20.50, "\u8fd8\u8981\u88ab\u4ed6\u4eec\u738b\u5bb6\u6b3a\u8d1f", "narration"),
        ]

        repaired = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "dialogue",
                    "confidence": 0.76,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )
        final = core.repair_final_classification_boundaries(repaired, override_meta={
            2: {
                "type": "dialogue",
                "confidence": 0.76,
                "source": "audio_speaker_espnet_wavlm",
            }
        })

        self.assertEqual(final[1].entry_type, "dialogue")

    def test_audio_speaker_narration_can_correct_local_dialogue(self):
        entries = [
            entry(1, 70.00, 70.88, "\u4ece\u5c0f\u4f60\u7239\u5c31\u4e00\u76f4\u6b3a\u8d1f\u6211", "dialogue"),
            entry(2, 71.36, 73.20, "\u73b0\u5728\u5230\u4e86\u4f60\u8fd9\u4e00\u8f88\u53c8\u7ecf\u5e38\u6b3a\u8d1f\u6211\u513f\u5b50", "dialogue"),
            entry(3, 73.44, 74.40, "\u4f60\u4eec\u7237\u4fe9\u592a\u8fc7\u5206\u4e86", "dialogue"),
        ]

        repaired = core.apply_audio_classification_overrides(
            entries,
            {
                2: {
                    "type": "narration",
                    "confidence": 0.80,
                    "source": "audio_speaker_unispeech_sat",
                }
            },
        )
        final = core.repair_final_classification_boundaries(repaired, override_meta={
            2: {
                "type": "narration",
                "confidence": 0.80,
                "source": "audio_speaker_unispeech_sat",
            }
        })

        self.assertEqual(final[1].entry_type, "narration")

    def test_speaker_locked_dialogue_does_not_spread_to_following_narration(self):
        entries = [
            entry(1, 0.00, 0.90, "\u7537\u4eba\u521a\u5230\u95e8\u53e3", "narration"),
            entry(2, 1.00, 1.42, "\u6162\u7740", "narration"),
            entry(3, 1.50, 2.20, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]
        overrides = {
            2: {
                "type": "dialogue",
                "confidence": 0.74,
                "source": "audio_speaker_espnet_wavlm",
            }
        }

        repaired = core.apply_audio_classification_overrides(entries, overrides)
        final = core.repair_final_classification_boundaries(repaired, override_meta=overrides)

        self.assertEqual(final[1].entry_type, "dialogue")
        self.assertEqual(final[2].entry_type, "narration")

    def test_neighbor_speaker_locks_do_not_relabel_middle_without_own_evidence(self):
        entries = [
            entry(1, 0.00, 0.90, "\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00", "narration"),
            entry(2, 1.00, 1.42, "\u4f17\u4eba\u90fd\u6123\u4f4f", "dialogue"),
            entry(3, 1.50, 2.20, "\u6162\u7740", "dialogue"),
        ]
        overrides = {
            1: {
                "type": "narration",
                "confidence": 0.80,
                "source": "audio_speaker_unispeech_sat",
            },
            3: {
                "type": "dialogue",
                "confidence": 0.76,
                "source": "audio_speaker_espnet_wavlm",
            },
        }

        repaired = core.apply_audio_classification_overrides(entries, overrides)

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_processed_subtitles_respect_speaker_override_over_local_type(self):
        entries = [
            entry(1, 0.00, 0.90, "\u7537\u4eba\u521a\u8d70\u5230\u95e8\u53e3"),
            entry(2, 1.00, 1.48, "\u6162\u7740"),
            entry(3, 1.60, 2.40, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86"),
            entry(4, 2.60, 3.30, "\u4f60\u4eec\u7237\u4fe9\u592a\u8fc7\u5206\u4e86"),
        ]
        settings = core.CloneSettings(
            reference_video=Path("reference.mp4"),
            source_dir=Path("."),
            output_dir=Path("."),
            subtitle_entries=entries,
            prefer_funasr_audio_subtitles=True,
            disable_ai_subtitle_review=True,
            disable_ai_narration_rewrite=True,
            prefer_funasr_sentence_pauses=True,
        )

        original_build_overrides = core.build_audio_classification_overrides
        original_run_funasr = core.run_funasr_reference_transcription
        try:
            core.run_funasr_reference_transcription = lambda *args, **kwargs: []

            def fake_build_overrides(processed_entries, *args, **kwargs):
                overrides = {}
                for item in processed_entries:
                    if item.text == "\u6162\u7740":
                        overrides[item.index] = {
                            "type": "dialogue",
                            "confidence": 0.72,
                            "source": "audio_speaker_espnet_wavlm",
                        }
                    if item.text == "\u4f60\u4eec\u7237\u4fe9\u592a\u8fc7\u5206\u4e86":
                        overrides[item.index] = {
                            "type": "narration",
                            "confidence": 0.82,
                            "source": "audio_speaker_unispeech_sat",
                        }
                return overrides

            core.build_audio_classification_overrides = fake_build_overrides
            bundle = core.build_processed_subtitles(
                entries,
                "",
                DummyAINarrationGenerator(),
                reference_video=Path("reference.mp4"),
                video_processor=object(),
                settings=settings,
            )
        finally:
            core.build_audio_classification_overrides = original_build_overrides
            core.run_funasr_reference_transcription = original_run_funasr

        types_by_text = {item.text: item.entry_type for item in bundle.all_entries}
        self.assertEqual(types_by_text["\u6162\u7740"], "dialogue")
        self.assertEqual(types_by_text["\u4f60\u4eec\u7237\u4fe9\u592a\u8fc7\u5206\u4e86"], "narration")

    def test_original_subtitle_run_keeps_speaker_backed_type(self):
        entries = [
            entry(1, 0.00, 0.90, "\u7537\u4eba\u521a\u5230\u95e8\u53e3", "narration"),
            entry(2, 0.95, 1.35, "\u6162\u7740", "original_subtitle"),
            entry(3, 1.40, 2.20, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        recovered = core.recover_narration_fragment_runs(
            entries,
            override_meta={
                2: {
                    "type": "dialogue",
                    "confidence": 0.75,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )
        applied = core.apply_audio_classification_overrides(
            recovered,
            {
                2: {
                    "type": "dialogue",
                    "confidence": 0.75,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )
        final = core.repair_final_classification_boundaries(
            applied,
            override_meta={
                2: {
                    "type": "dialogue",
                    "confidence": 0.75,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(recovered[1].entry_type, "original_subtitle")
        self.assertEqual(final[1].entry_type, "dialogue")

    def test_local_text_reclassification_helpers_are_neutralized(self):
        original_run = [
            entry(1, 0.00, 0.90, "\u7537\u4eba\u521a\u5230\u95e8\u53e3", "narration"),
            entry(2, 0.95, 1.35, "\u6162\u7740", "original_subtitle"),
            entry(3, 1.40, 2.20, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]
        dialogue_island = [
            entry(1, 0.00, 0.70, "\u4f60\u522b\u540e\u6094", "dialogue"),
            entry(2, 0.74, 1.18, "\u6162\u7740", "narration"),
            entry(3, 1.22, 1.90, "\u4f60\u7ed9\u6211\u7b49\u7740", "dialogue"),
        ]
        forced_original_text = [
            entry(1, 0.00, 0.80, "\u7b2c3\u96c6", "narration"),
        ]

        self.assertEqual(
            [item.entry_type for item in core.recover_narration_fragment_runs(original_run)],
            ["narration", "original_subtitle", "narration"],
        )
        self.assertEqual(
            [item.entry_type for item in core.strengthen_classification(dialogue_island)],
            ["dialogue", "narration", "dialogue"],
        )
        self.assertEqual(
            [item.entry_type for item in core.refine_classified_entries(forced_original_text)],
            ["narration"],
        )

    def test_text_only_dialogue_guard_no_longer_demotes_without_speaker_evidence(self):
        entries = [
            entry(1, 0.00, 0.90, "\u7537\u4eba\u521a\u5230\u95e8\u53e3", "narration"),
            entry(2, 0.95, 1.35, "\u6162\u7740", "dialogue"),
            entry(3, 1.40, 2.20, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        guarded = core.remove_text_only_dialogue_labels_after_speaker_review(
            entries,
            override_meta={
                1: {
                    "type": "narration",
                    "confidence": 0.82,
                    "source": "audio_speaker_unispeech_sat",
                }
            },
        )

        self.assertEqual(
            [item.entry_type for item in guarded],
            ["narration", "dialogue", "narration"],
        )

    def test_whole_text_context_classifier_is_neutralized(self):
        entries = [
            entry(1, 0.00, 0.80, "\u7537\u4eba\u8bf4\u8bdd", "narration"),
            entry(2, 0.85, 1.15, "\u6162\u7740", "dialogue"),
            entry(3, 1.20, 2.10, "\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "original_subtitle"),
        ]

        classified = core.classify_entries_with_whole_text_context(entries, trust_existing_type=True)

        self.assertEqual([item.entry_type for item in classified], [item.entry_type for item in entries])

    def test_local_seed_cleanup_helpers_are_neutralized(self):
        entries = [
            entry(1, 0.00, 0.80, "\u7537\u4eba\u8bf4\u8bdd", "narration"),
            entry(2, 0.85, 1.15, "\u6162\u7740", "dialogue"),
        ]
        profiles = {item.index: audio_profile_for(item) for item in entries}
        seed_map = {
            1: {"label": "narration_seed", "confidence": 0.91},
            2: {"label": "dialogue_seed", "confidence": 0.92},
        }

        supplemented = core.supplement_audio_seed_labels_locally(entries, {}, profiles, seed_map)
        filtered = core.filter_audio_seed_labels_by_voice_consistency(profiles, seed_map)

        self.assertEqual(supplemented, seed_map)
        self.assertEqual(filtered, seed_map)

    def test_credible_dialogue_speaker_override_survives_short_island_repair(self):
        entries = [
            entry(1, 0.00, 1.00, "\u6ca1\u60f3\u5230\u5973\u4eba\u8d76\u5230", "narration"),
            entry(2, 1.04, 1.52, "\u6162\u7740", "dialogue"),
            entry(3, 1.56, 2.40, "\u968f\u540e\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86", "narration"),
        ]

        repaired = core.repair_final_classification_boundaries(
            entries,
            override_meta={
                2: {
                    "type": "dialogue",
                    "confidence": 0.76,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_credible_narrator_speaker_override_survives_dialogue_tail_repair(self):
        entries = [
            entry(1, 0.00, 0.72, "\u4f60\u522b\u540e\u6094", "dialogue"),
            entry(2, 0.74, 1.16, "\u968f\u540e", "narration"),
            entry(3, 1.18, 2.00, "\u7537\u4eba\u8f6c\u8eab\u79bb\u5f00", "narration"),
        ]

        repaired = core.repair_final_classification_boundaries(
            entries,
            override_meta={
                2: {
                    "type": "narration",
                    "confidence": 0.80,
                    "source": "audio_speaker_unispeech_sat",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "narration")

    def test_weak_speaker_evidence_does_not_enable_text_narration_fallback(self):
        entries = [
            entry(1, 0.00, 1.00, "\u4f60\u7ed9\u6211\u7b49\u7740", "dialogue"),
            entry(
                2,
                1.20,
                7.80,
                "\u7136\u800c\u8fd8\u6ca1\u7b49\u4ed6\u8fc7\u53bb\uff0c\u5973\u4eba\u53c8\u62ff\u51fa\u4e00\u628a\u5200\uff0c\u4f17\u4eba\u90fd\u6123\u4f4f\u4e86",
                "dialogue",
            ),
            entry(3, 8.00, 9.00, "\u968f\u540e\u4f17\u4eba\u90fd\u5413\u574f\u4e86", "narration"),
        ]

        repaired = core.repair_final_classification_boundaries(
            entries,
            override_meta={
                2: {
                    "type": "dialogue",
                    "confidence": 0.30,
                    "source": "audio_speaker_espnet_wavlm",
                }
            },
        )

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_short_dialogue_without_speaker_evidence_is_not_swallowed_by_narration_bridge(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u521a\u8d70\u8fdb\u5927\u5385", "narration"),
            entry(2, 1.04, 1.42, "\u6162\u7740", "dialogue"),
            entry(3, 1.46, 2.40, "\u4f17\u4eba\u90fd\u5012\u5438\u4e00\u53e3\u51c9\u6c14", "narration"),
        ]

        repaired = core.repair_final_classification_boundaries(entries, override_meta={})

        self.assertEqual(repaired[1].entry_type, "dialogue")

    def test_audio_cluster_seed_map_provides_narrator_seed_without_dialogue_centroid(self):
        entries = [
            entry(1, 0.00, 1.00, "\u7537\u4eba\u521a\u8d70\u8fdb\u5927\u5385", "narration"),
            entry(2, 1.10, 2.10, "\u6162\u7740", "dialogue"),
        ]
        narration_profile = core.AudioSegmentProfile(
            10,
            0.00,
            1.00,
            1.00,
            0.95,
            0.90,
            -20.0,
            0.10,
            180.0,
            0.92,
            (1.0, 0.0, 0.0),
        )
        dialogue_profile = core.AudioSegmentProfile(
            20,
            1.10,
            2.10,
            1.00,
            0.95,
            0.90,
            -20.0,
            0.10,
            220.0,
            0.92,
            (0.0, 1.0, 0.0),
        )

        seed_map = core.build_audio_cluster_speaker_seed_map(
            entries,
            {
                1: [(narration_profile, 1.00)],
                2: [(dialogue_profile, 1.00)],
            },
            {
                10: 0,
                20: 1,
            },
            {0},
        )

        self.assertEqual(seed_map[1]["label"], "narration_seed")
        self.assertNotIn(2, seed_map)

    def test_cluster_similarity_hard_decision_can_replace_weak_existing_similarity(self):
        merged = core.merge_speaker_similarity_maps_prefer_hard_audio(
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.10,
                    "dialogue_similarity": 0.52,
                }
            },
            {
                1: {
                    "speaker_engine": "espnet_wavlm",
                    "narration_similarity": 0.60,
                    "dialogue_similarity": 0.58,
                }
            },
        )

        self.assertEqual(merged[1]["dialogue_similarity"], 0.52)

    def test_local_audio_override_allows_speaker_evidence_to_cross_local_type(self):
        self.assertTrue(
            core.local_audio_override_can_confirm_entry_type(
                entry(1, 0.00, 1.00, "\u6162\u7740", "dialogue"),
                "dialogue",
            )
        )
        self.assertTrue(
            core.local_audio_override_can_confirm_entry_type(
                entry(2, 1.10, 2.10, "\u6162\u7740", "narration"),
                "dialogue",
            )
        )
        self.assertTrue(
            core.local_audio_override_can_confirm_entry_type(
                entry(3, 2.20, 3.20, "\u7537\u4eba\u8fde\u5fd9\u540e\u9000", "dialogue"),
                "narration",
            )
        )
        self.assertFalse(
            core.local_audio_override_can_confirm_entry_type(
                entry(4, 3.30, 4.30, "HY44", "watermark"),
                "narration",
            )
        )
        self.assertFalse(
            core.local_audio_override_can_confirm_entry_type(
                entry(5, 4.40, 5.40, "", "narration"),
                "dialogue",
            )
        )


class DialogueMatchRegressionTests(unittest.TestCase):
    def sample(self, timestamp, video_path="7.mp4", global_index=0):
        return core.FrameSample(
            video_path=video_path,
            video_name=Path(video_path).name,
            video_order=7,
            local_index=int(round(timestamp * 2)),
            global_index=global_index,
            timestamp=float(timestamp),
            signature=(),
        )

    def candidate(self, timestamp, audio_similarity=0.0):
        return {
            "sample": self.sample(timestamp, global_index=int(round(timestamp * 2))),
            "audio_similarity": float(audio_similarity),
            "visual": 0.75,
            "score": 0.75,
        }

    def test_short_dialogue_far_relocation_requires_text_confirmation(self):
        frames = [
            core.ReferenceFrame(0, 131.5, ()),
            core.ReferenceFrame(1, 132.0, ()),
        ]
        current = [self.candidate(134.5, 0.71), self.candidate(135.0, 0.71)]
        proposed = [self.candidate(66.5, 0.82), self.candidate(67.0, 0.82)]
        self.assertTrue(
            core.dialogue_short_relocation_needs_text_confirmation(
                frames,
                [0, 1],
                current,
                proposed,
                0.5,
                current_discontinuities=0,
                proposed_discontinuities=0,
                current_avg_audio=0.71,
                proposed_avg_audio=0.82,
            )
        )

    def test_short_dialogue_local_adjustment_does_not_require_text_confirmation(self):
        frames = [
            core.ReferenceFrame(0, 131.5, ()),
            core.ReferenceFrame(1, 132.0, ()),
        ]
        current = [self.candidate(134.5, 0.70), self.candidate(135.0, 0.70)]
        proposed = [self.candidate(135.0, 0.82), self.candidate(135.5, 0.82)]
        self.assertFalse(
            core.dialogue_short_relocation_needs_text_confirmation(
                frames,
                [0, 1],
                current,
                proposed,
                0.5,
                current_discontinuities=0,
                proposed_discontinuities=0,
                current_avg_audio=0.70,
                proposed_avg_audio=0.82,
            )
        )

    def test_very_strong_audio_can_still_repair_short_dialogue(self):
        frames = [
            core.ReferenceFrame(0, 131.5, ()),
            core.ReferenceFrame(1, 132.0, ()),
        ]
        current = [self.candidate(134.5, 0.66), self.candidate(135.0, 0.66)]
        proposed = [self.candidate(66.5, 0.92), self.candidate(67.0, 0.92)]
        self.assertFalse(
            core.dialogue_short_relocation_needs_text_confirmation(
                frames,
                [0, 1],
                current,
                proposed,
                0.5,
                current_discontinuities=0,
                proposed_discontinuities=0,
                current_avg_audio=0.66,
                proposed_avg_audio=0.92,
            )
        )

    def test_short_dialogue_cross_source_relocation_requires_text_confirmation(self):
        frames = [
            core.ReferenceFrame(0, 40.0, ()),
            core.ReferenceFrame(1, 40.5, ()),
        ]
        current = [
            self.candidate(12.0, 0.72),
            self.candidate(12.5, 0.72),
        ]
        proposed = [
            {
                **self.candidate(88.0, 0.82),
                "sample": self.sample(88.0, video_path="8.mp4", global_index=176),
            },
            {
                **self.candidate(88.5, 0.82),
                "sample": self.sample(88.5, video_path="8.mp4", global_index=177),
            },
        ]
        self.assertTrue(
            core.dialogue_short_relocation_needs_text_confirmation(
                frames,
                [0, 1],
                current,
                proposed,
                0.5,
                current_discontinuities=0,
                proposed_discontinuities=0,
                current_avg_audio=0.72,
                proposed_avg_audio=0.82,
            )
        )

    def test_long_dialogue_relocation_uses_existing_audio_continuity_gate(self):
        frames = [
            core.ReferenceFrame(0, 40.0, ()),
            core.ReferenceFrame(1, 40.5, ()),
            core.ReferenceFrame(2, 41.0, ()),
            core.ReferenceFrame(3, 41.5, ()),
        ]
        current = [self.candidate(12.0 + idx * 0.5, 0.72) for idx in range(4)]
        proposed = [self.candidate(88.0 + idx * 0.5, 0.82) for idx in range(4)]
        self.assertFalse(
            core.dialogue_short_relocation_needs_text_confirmation(
                frames,
                [0, 1, 2, 3],
                current,
                proposed,
                0.5,
                current_discontinuities=0,
                proposed_discontinuities=0,
                current_avg_audio=0.72,
                proposed_avg_audio=0.82,
            )
        )

    def test_bad_current_short_dialogue_can_still_use_audio_repair(self):
        frames = [
            core.ReferenceFrame(0, 131.5, ()),
            core.ReferenceFrame(1, 132.0, ()),
        ]
        current = [self.candidate(134.5, 0.48), self.candidate(135.0, 0.48)]
        proposed = [self.candidate(66.5, 0.72), self.candidate(67.0, 0.72)]
        self.assertFalse(
            core.dialogue_short_relocation_needs_text_confirmation(
                frames,
                [0, 1],
                current,
                proposed,
                0.5,
                current_discontinuities=0,
                proposed_discontinuities=0,
                current_avg_audio=0.48,
                proposed_avg_audio=0.72,
            )
        )

    def test_discontinuous_short_dialogue_window_is_left_to_continuity_logic(self):
        frames = [
            core.ReferenceFrame(0, 131.5, ()),
            core.ReferenceFrame(1, 132.0, ()),
        ]
        current = [self.candidate(134.5, 0.71), self.candidate(140.0, 0.71)]
        proposed = [self.candidate(66.5, 0.82), self.candidate(67.0, 0.82)]
        self.assertFalse(
            core.dialogue_short_relocation_needs_text_confirmation(
                frames,
                [0, 1],
                current,
                proposed,
                0.5,
                current_discontinuities=1,
                proposed_discontinuities=0,
                current_avg_audio=0.71,
                proposed_avg_audio=0.82,
            )
        )

    def test_same_source_far_short_dialogue_jump_requires_text_confirmation(self):
        frames = [
            core.ReferenceFrame(0, 75.0, ()),
            core.ReferenceFrame(1, 75.5, ()),
        ]
        current = [self.candidate(40.0, 0.73), self.candidate(40.5, 0.73)]
        proposed = [self.candidate(52.0, 0.82), self.candidate(52.5, 0.82)]
        self.assertTrue(
            core.dialogue_short_relocation_needs_text_confirmation(
                frames,
                [0, 1],
                current,
                proposed,
                0.5,
                current_discontinuities=0,
                proposed_discontinuities=0,
                current_avg_audio=0.73,
                proposed_avg_audio=0.82,
            )
        )

    def test_final_dialogue_audio_rerank_skips_stable_matching_audio(self):
        path = [self.candidate(40.0, 0.80), self.candidate(40.5, 0.79)]
        self.assertFalse(
            core.dialogue_window_warrants_final_audio_rerank(
                path,
                discontinuities=0,
                avg_visual=0.86,
                avg_audio=0.79,
                audio_count=2,
            )
        )

    def test_final_dialogue_audio_rerank_checks_visually_plausible_wrong_audio(self):
        path = [self.candidate(40.0, 0.56), self.candidate(40.5, 0.57)]
        self.assertTrue(
            core.dialogue_window_warrants_final_audio_rerank(
                path,
                discontinuities=0,
                avg_visual=0.74,
                avg_audio=0.57,
                audio_count=2,
            )
        )

    def test_final_dialogue_audio_rerank_checks_discontinuous_window(self):
        path = [self.candidate(40.0, 0.70), self.candidate(88.0, 0.71)]
        self.assertTrue(
            core.dialogue_window_warrants_final_audio_rerank(
                path,
                discontinuities=1,
                avg_visual=0.82,
                avg_audio=0.70,
                audio_count=2,
            )
        )

    def test_final_dialogue_audio_rerank_requires_enough_audio_evidence(self):
        path = [
            self.candidate(40.0, 0.48),
            self.candidate(40.5, 0.0),
            self.candidate(41.0, 0.0),
            self.candidate(41.5, 0.0),
        ]
        self.assertFalse(
            core.dialogue_window_warrants_final_audio_rerank(
                path,
                discontinuities=1,
                avg_visual=0.52,
                avg_audio=0.48,
                audio_count=1,
            )
        )

    def test_noncut_continuity_repair_skips_dialogue_window(self):
        matches = [
            {"ref_time": 14.5, "source_video": "1.mp4", "source_start": 55.0, "duration": 0.5},
            {"ref_time": 15.0, "source_video": "1.mp4", "source_start": 33.0, "duration": 0.5},
            {"ref_time": 15.5, "source_video": "1.mp4", "source_start": 33.5, "duration": 0.5},
        ]
        frames = [
            core.ReferenceFrame(0, 14.5, ()),
            core.ReferenceFrame(1, 15.0, ()),
            core.ReferenceFrame(2, 15.5, ()),
        ]
        repaired, count = core.repair_final_same_video_noncut_jumps(
            matches,
            0.5,
            reference_frames=frames,
            dialogue_intervals=[(14.88, 17.2)],
        )
        self.assertEqual(count, 0)
        self.assertAlmostEqual(float(repaired[1]["source_start"]), 33.0)
        self.assertAlmostEqual(float(repaired[2]["source_start"]), 33.5)

    def test_noncut_continuity_repair_still_repairs_narration_window(self):
        matches = [
            {"ref_time": 10.0, "source_video": "1.mp4", "source_start": 20.0, "duration": 0.5},
            {"ref_time": 10.5, "source_video": "1.mp4", "source_start": 30.0, "duration": 0.5},
        ]
        frames = [
            core.ReferenceFrame(0, 10.0, ()),
            core.ReferenceFrame(1, 10.5, ()),
        ]
        repaired, count = core.repair_final_same_video_noncut_jumps(
            matches,
            0.5,
            reference_frames=frames,
            dialogue_intervals=[(14.88, 17.2)],
        )
        self.assertEqual(count, 1)
        self.assertAlmostEqual(float(repaired[1]["source_start"]), 20.5)

    def test_order_backtrack_repair_skips_dialogue_window(self):
        matches = [
            {"ref_time": 14.5, "source_video": "1.mp4", "source_start": 20.0, "duration": 0.5},
            {"ref_time": 15.0, "source_video": "1.mp4", "source_start": 20.4, "duration": 0.5},
            {"ref_time": 15.5, "source_video": "1.mp4", "source_start": 20.3, "duration": 0.5},
            {"ref_time": 16.0, "source_video": "1.mp4", "source_start": 20.8, "duration": 0.5},
        ]
        frames = [core.ReferenceFrame(index, 14.5 + index * 0.5, ()) for index in range(4)]
        repaired, count = core.repair_final_same_video_match_order_backtracks(
            matches,
            0.5,
            reference_frames=frames,
            dialogue_intervals=[(14.8, 16.2)],
        )

        self.assertEqual(count, 0)
        self.assertEqual([float(item["source_start"]) for item in repaired], [20.0, 20.4, 20.3, 20.8])

    def test_order_backtrack_repair_still_repairs_narration_window(self):
        matches = [
            {"ref_time": 10.0, "source_video": "1.mp4", "source_start": 20.0, "duration": 0.5},
            {"ref_time": 10.5, "source_video": "1.mp4", "source_start": 20.5, "duration": 0.5},
            {"ref_time": 11.0, "source_video": "1.mp4", "source_start": 20.45, "duration": 0.5},
            {"ref_time": 11.5, "source_video": "1.mp4", "source_start": 20.95, "duration": 0.5},
        ]
        frames = [core.ReferenceFrame(index, 10.0 + index * 0.5, ()) for index in range(4)]
        repaired, count = core.repair_final_same_video_match_order_backtracks(
            matches,
            0.5,
            reference_frames=frames,
            dialogue_intervals=[(14.8, 16.2)],
        )

        self.assertGreaterEqual(count, 1)
        self.assertGreaterEqual(float(repaired[2]["source_start"]), float(repaired[1]["source_start"]))


class OutputFrameSizeRegressionTests(unittest.TestCase):
    class FakeVideoProcessor:
        def __init__(self, profiles):
            self.profiles = profiles

        def probe_video(self, path):
            return self.profiles[str(path)]

    def test_output_frame_size_prefers_matched_source_material(self):
        jobs = [
            core.SegmentJob("episode_01.mp4", 0.0, 4.0),
            core.SegmentJob("episode_02.mp4", 10.0, 6.0),
            core.SegmentJob("reference_cache.mp4", 0.0, 1.0),
        ]
        processor = self.FakeVideoProcessor(
            {
                "episode_01.mp4": {
                    "width": "1080",
                    "height": "1920",
                    "duration": "60",
                    "video_duration": "60",
                    "fps": "50/1",
                },
                "episode_02.mp4": {
                    "width": "1080",
                    "height": "1920",
                    "duration": "60",
                    "video_duration": "60",
                    "fps": "50/1",
                },
                "reference_cache.mp4": {
                    "width": "720",
                    "height": "1280",
                    "duration": "60",
                    "video_duration": "60",
                    "fps": "30/1",
                },
            }
        )
        width, height, fps, reason = core.choose_target_video_profile_from_source_jobs(jobs, processor, 720, 1280)
        self.assertEqual((width, height), (1080, 1920))
        self.assertEqual(fps, 50.0)
        self.assertIn("source jobs", reason)

    def test_output_frame_size_falls_back_when_source_probe_is_unusable(self):
        jobs = [core.SegmentJob("tiny.mp4", 0.0, 4.0)]
        processor = self.FakeVideoProcessor(
            {"tiny.mp4": {"width": "320", "height": "240", "duration": "60", "video_duration": "60"}}
        )
        width, height, reason = core.choose_target_frame_size_from_source_jobs(jobs, processor, 720, 1280)
        self.assertEqual((width, height), (720, 1280))
        self.assertEqual(reason, "reference fallback")


if __name__ == "__main__":
    unittest.main()
