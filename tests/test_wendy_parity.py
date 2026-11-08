"""
Our scripts must reproduce Wendy's run BYTE FOR BYTE from her own raw inputs.

Runs clean -> merge -> match on her Kalshi JSON and on-chain OrderFilled CSV,
once per window folder she produced (1, 5 and 10 minutes), and requires every
one of the five per-window CSVs to be byte-identical to hers. See
arbpipe.verify.parity for the conversion and the file list.

Skipped when her folder is not present in this clone.

    python -m unittest tests.test_wendy_parity
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from arbpipe.verify.parity import compare_scripts  # noqa: E402

HER = ROOT / "wendysResults" / "mamdani-dem-nomination"
EVENT = "mamdani-dem-nomination"


@unittest.skipUnless((HER / "4_arbitrage").is_dir(), f"Wendy's results not present: {HER}")
class TestWendyParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.results = compare_scripts(EVENT, HER)

    def test_every_window_and_file_was_compared(self):
        windows = {w for w, _ in self.results}
        self.assertEqual(windows, {"1min", "5min", "10min"})
        self.assertEqual(len(self.results), 15)

    def test_all_outputs_byte_identical(self):
        for (window, name), diff in sorted(self.results.items()):
            with self.subTest(window=window, file=name):
                self.assertIsNone(diff, diff)


if __name__ == "__main__":
    unittest.main()
