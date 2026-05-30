import re
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = PROJECT_ROOT / "modules" / "auto_clip_engine"
for path in (PROJECT_ROOT, ENGINE_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import drama_clone_core


class OutputPackagingGradientTests(unittest.TestCase):
    def filter_alphas(self, filters):
        alphas = []
        for item in filters:
            match = re.search(r"black@([0-9.]+)", item)
            if match:
                alphas.append(float(match.group(1)))
        return alphas

    def test_packaging_gradient_tapers_to_transparent_without_hard_cap(self):
        filters = drama_clone_core.build_output_packaging_box_filters(
            1080,
            1920,
            {
                "top_height_ratio": 0.25,
                "bottom_height_ratio": 0.25,
                "top_edge_height_ratio": 0.04,
                "bottom_edge_height_ratio": 0.04,
                "top_edge_alpha": 0.90,
                "bottom_edge_alpha": 0.90,
                "top_gradient_alpha": 0.70,
                "bottom_gradient_alpha": 0.70,
                "gradient_steps": 128,
                "top_gradient_curve": 0.9,
                "bottom_gradient_curve": 0.9,
            },
        )

        top_filters = [item for item in filters if re.search(r"y=(\d+):", item) and int(re.search(r"y=(\d+):", item).group(1)) < 480]
        bottom_filters = [
            item
            for item in filters
            if re.search(r"y=(\d+):", item) and int(re.search(r"y=(\d+):", item).group(1)) >= 1440
        ]
        top_alphas = self.filter_alphas(top_filters)
        bottom_alphas = self.filter_alphas(bottom_filters)

        self.assertGreater(len(top_alphas), 80)
        self.assertGreater(len(bottom_alphas), 80)
        self.assertGreater(top_alphas[0], top_alphas[-1])
        self.assertGreater(bottom_alphas[-1], bottom_alphas[0])
        self.assertLess(top_alphas[-1], 0.02)
        self.assertLess(bottom_alphas[0], 0.02)
        self.assertLess(max(abs(a - b) for a, b in zip(top_alphas, top_alphas[1:])), 0.05)
        self.assertLess(max(abs(a - b) for a, b in zip(bottom_alphas, bottom_alphas[1:])), 0.05)


if __name__ == "__main__":
    unittest.main()
