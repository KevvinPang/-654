import json
import logging
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import batch_runner


class BatchRunnerMultiReferenceTests(unittest.TestCase):
    def build_workspace(self, root: Path) -> batch_runner.WorkspaceContext:
        return batch_runner.WorkspaceContext(
            name="demo",
            root=root,
            config_path=root / "task.json",
            config={
                "settings": {
                    "prefer_funasr_audio_subtitles": True,
                    "prefer_funasr_sentence_pauses": True,
                    "bgm_source_mode": "none",
                },
                "auto_clip": [
                    {
                        "reference_video_glob": "downloads/douyin/*.mp4",
                        "reference_subtitle_glob": "subtitles/audio/*.srt",
                        "reference_visual_subtitle_glob": "subtitles/visual/*.srt",
                        "source_dir": "downloads/baidu",
                        "bgm_source_mode": "none",
                    }
                ],
            },
            logger=logging.getLogger("test_batch_runner_multi_reference"),
        )

    def test_visual_srt_is_not_reused_across_multiple_reference_videos(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "downloads" / "douyin").mkdir(parents=True)
            (root / "downloads" / "baidu").mkdir(parents=True)
            (root / "subtitles" / "audio").mkdir(parents=True)
            (root / "subtitles" / "visual").mkdir(parents=True)

            for name in ("ref_a.mp4", "ref_b.mp4"):
                (root / "downloads" / "douyin" / name).write_bytes(b"x")
                stem = Path(name).stem
                (root / "subtitles" / "audio" / f"downloads__douyin__{stem}.srt").write_text(
                    "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
                    encoding="utf-8",
                )

            (root / "subtitles" / "visual" / "downloads__douyin__ref_a.srt").write_text(
                "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
                encoding="utf-8",
            )
            (root / "downloads" / "baidu" / "1.mp4").write_bytes(b"x")

            workspace = self.build_workspace(root)

            specs = batch_runner.build_auto_clip_specs(workspace)

            self.assertEqual(len(specs), 2)
            job_payloads = {
                path.stem: json.loads(path.read_text(encoding="utf-8"))
                for path in (root / "temp" / "auto_clip" / "jobs").glob("*.json")
            }
            self.assertIn("demo_ref_a", job_payloads)
            self.assertIn("demo_ref_b", job_payloads)
            self.assertEqual(
                Path(job_payloads["demo_ref_a"]["reference_visual_subtitle"]).name,
                "downloads__douyin__ref_a.srt",
            )
            self.assertNotIn("reference_visual_subtitle", job_payloads["demo_ref_b"])

    def test_audio_srt_is_not_reused_across_multiple_reference_videos(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "downloads" / "douyin").mkdir(parents=True)
            (root / "downloads" / "baidu").mkdir(parents=True)
            (root / "subtitles" / "audio").mkdir(parents=True)
            (root / "subtitles" / "visual").mkdir(parents=True)

            for name in ("ref_a.mp4", "ref_b.mp4"):
                (root / "downloads" / "douyin" / name).write_bytes(b"x")
                stem = Path(name).stem
                (root / "subtitles" / "visual" / f"downloads__douyin__{stem}.srt").write_text(
                    "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
                    encoding="utf-8",
                )
            (root / "subtitles" / "audio" / "downloads__douyin__ref_a.srt").write_text(
                "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
                encoding="utf-8",
            )
            (root / "downloads" / "baidu" / "1.mp4").write_bytes(b"x")

            specs = batch_runner.build_auto_clip_specs(self.build_workspace(root))

            self.assertEqual(len(specs), 2)
            self.assertEqual(specs[0].skip_reason, "")
            self.assertEqual(specs[1].skip_reason, "no matching subtitle for reference video")
            job_payloads = {
                path.stem: json.loads(path.read_text(encoding="utf-8"))
                for path in (root / "temp" / "auto_clip" / "jobs").glob("*.json")
            }
            self.assertEqual(list(job_payloads), ["demo_ref_a"])

    def test_single_reference_task_matches_its_own_audio_srt(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "downloads" / "douyin").mkdir(parents=True)
            (root / "downloads" / "baidu").mkdir(parents=True)
            (root / "subtitles" / "audio").mkdir(parents=True)
            (root / "subtitles" / "visual").mkdir(parents=True)

            for name in ("ref_01_video.mp4", "ref_02_video.mp4"):
                (root / "downloads" / "douyin" / name).write_bytes(b"x")
                stem = Path(name).stem
                (root / "subtitles" / "audio" / f"downloads__douyin__{stem}.srt").write_text(
                    "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
                    encoding="utf-8",
                )
                (root / "subtitles" / "visual" / f"downloads__douyin__{stem}.srt").write_text(
                    "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
                    encoding="utf-8",
                )
            (root / "downloads" / "baidu" / "1.mp4").write_bytes(b"x")

            workspace = batch_runner.WorkspaceContext(
                name="demo",
                root=root,
                config_path=root / "task.json",
                config={
                    "settings": {
                        "prefer_funasr_audio_subtitles": True,
                        "bgm_source_mode": "none",
                    },
                    "auto_clip": [
                        {
                            "reference_video_glob": "downloads/douyin/ref_02_*",
                            "reference_subtitle_glob": "subtitles/audio/*.srt",
                            "reference_visual_subtitle_glob": "subtitles/visual/*.srt",
                            "source_dir": "downloads/baidu",
                            "match_all_references": False,
                            "bgm_source_mode": "none",
                        }
                    ],
                },
                logger=logging.getLogger("test_batch_runner_single_reference_match"),
            )

            specs = batch_runner.build_auto_clip_specs(workspace)

            self.assertEqual(len(specs), 1)
            self.assertEqual(specs[0].skip_reason, "")
            job_payload = json.loads(next((root / "temp" / "auto_clip" / "jobs").glob("*.json")).read_text(encoding="utf-8"))
            self.assertEqual(Path(job_payload["reference_subtitle"]).name, "downloads__douyin__ref_02_video.srt")
            self.assertEqual(Path(job_payload["reference_visual_subtitle"]).name, "downloads__douyin__ref_02_video.srt")

    def test_official_client_download_defaults_to_invoker_handoff(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workspace = batch_runner.WorkspaceContext(
                name="demo",
                root=root,
                config_path=root / "task.json",
                config={
                    "settings": {},
                    "baidu_share": [
                        {
                            "share_url": "https://pan.baidu.com/s/abc?pwd=1234",
                            "target_filename": "1.mp4",
                            "target_path": "/share/root/1.mp4",
                            "target_fsid": "100",
                            "download_mode": "official_client",
                            "output_subdir": "downloads/baidu",
                            "skip_existing": False,
                        }
                    ],
                },
                logger=logging.getLogger("test_batch_runner_official_client"),
            )

            specs = batch_runner.build_baidu_specs(workspace)

            self.assertEqual(len(specs), 1)
            self.assertIn("--handoff-mode", specs[0].command)
            mode_index = specs[0].command.index("--handoff-mode") + 1
            self.assertEqual(specs[0].command[mode_index], "invoker")

    def test_auto_clip_source_dir_follows_single_baidu_child_folder(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "downloads" / "douyin").mkdir(parents=True)
            (root / "downloads" / "baidu" / "show 1-40").mkdir(parents=True)
            (root / "subtitles" / "audio").mkdir(parents=True)
            (root / "subtitles" / "visual").mkdir(parents=True)
            (root / "downloads" / "douyin" / "ref_a.mp4").write_bytes(b"x")
            (root / "downloads" / "baidu" / "show 1-40" / "1.mp4").write_bytes(b"x")
            (root / "subtitles" / "audio" / "downloads__douyin__ref_a.srt").write_text(
                "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
                encoding="utf-8",
            )

            workspace = batch_runner.WorkspaceContext(
                name="demo",
                root=root,
                config_path=root / "task.json",
                config={
                    "settings": {
                        "prefer_funasr_audio_subtitles": True,
                        "bgm_source_mode": "none",
                    },
                    "baidu_share": [
                        {
                            "share_url": "https://pan.baidu.com/s/abc?pwd=1234",
                            "target_filename": "1.mp4",
                            "target_path": "/share/root/show 1-40/1.mp4",
                            "target_fsid": "100",
                            "target_size": 1,
                            "download_mode": "official_client",
                            "output_subdir": "downloads/baidu",
                            "skip_existing": True,
                        }
                    ],
                    "auto_clip": [
                        {
                            "reference_video_glob": "downloads/douyin/*.mp4",
                            "reference_subtitle_glob": "subtitles/audio/*.srt",
                            "source_dir": "downloads/baidu",
                            "output_subdir": "clips",
                            "temp_subdir": "temp/auto_clip",
                            "bgm_source_mode": "none",
                        }
                    ],
                },
                logger=logging.getLogger("test_batch_runner_baidu_child_source"),
            )

            specs = batch_runner.build_auto_clip_specs(workspace)

            self.assertEqual(len(specs), 1)
            self.assertEqual(specs[0].skip_reason, "")
            job_payload = json.loads(next((root / "temp" / "auto_clip" / "jobs").glob("*.json")).read_text(encoding="utf-8"))
            self.assertEqual(Path(job_payload["source_dir"]).name, "show 1-40")


if __name__ == "__main__":
    unittest.main()
