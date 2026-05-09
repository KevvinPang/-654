import copy
import unittest

import control_center


class ControlCenterLatestLogicTests(unittest.TestCase):
    def test_default_workspace_uses_audio_primary_with_visual_assist(self):
        task = control_center.default_workspace_task("demo")
        task["auto_clip"] = [
            {
                "reference_video_glob": "downloads/douyin/*.mp4",
                "reference_subtitle_glob": "subtitles/visual/*.srt",
                "source_dir": "downloads/baidu",
            }
        ]
        task["visual_subtitle_extract"] = [
            {
                "input_glob": "downloads/douyin/*.mp4",
                "output_subdir": "subtitles/visual",
                "skip_existing": True,
            }
        ]

        normalized = control_center.apply_workspace_task_defaults(task, "demo")

        self.assertTrue(normalized["settings"]["prefer_funasr_audio_subtitles"])
        self.assertEqual(normalized["auto_clip"][0]["reference_subtitle_glob"], "subtitles/audio/*.srt")
        self.assertEqual(
            normalized["auto_clip"][0]["reference_visual_subtitle_glob"],
            "subtitles/visual/*.srt",
        )
        self.assertFalse(normalized["visual_subtitle_extract"][0]["skip_existing"])

    def test_legacy_workspace_is_migrated_to_latest_dual_srt_logic(self):
        legacy = {
            control_center.TASK_SCHEMA_VERSION_KEY: 0,
            "settings": {"prefer_funasr_audio_subtitles": False},
            "auto_clip": [
                {
                    "reference_video_glob": "downloads/douyin/*.mp4",
                    "reference_subtitle_glob": "subtitles/visual/*.srt",
                    "source_dir": "downloads/baidu",
                    "skip_existing": True,
                }
            ],
            "visual_subtitle_extract": [
                {
                    "input_glob": "downloads/douyin/*.mp4",
                    "output_subdir": "subtitles/visual",
                    "skip_existing": True,
                }
            ],
        }

        migrated, changed = control_center.migrate_legacy_workspace_task(copy.deepcopy(legacy))

        self.assertTrue(changed)
        self.assertTrue(migrated["settings"]["prefer_funasr_audio_subtitles"])
        self.assertEqual(migrated["auto_clip"][0]["reference_subtitle_glob"], "subtitles/audio/*.srt")
        self.assertEqual(
            migrated["auto_clip"][0]["reference_visual_subtitle_glob"],
            "subtitles/visual/*.srt",
        )
        self.assertFalse(migrated["auto_clip"][0]["skip_existing"])
        self.assertFalse(migrated["visual_subtitle_extract"][0]["skip_existing"])


if __name__ == "__main__":
    unittest.main()
