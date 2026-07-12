import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from portfolio import MarketPosition, Portfolio


class TestMarketPosition(unittest.TestCase):
    def test_buy_yes_then_buy_no_nets_down_z(self):
        """A NO buy in the same market reduces z — no separate NO holding."""
        m = MarketPosition()
        m.buy_yes(10, 0.40)
        m.buy_no(4, 0.55)
        self.assertEqual(m.z, 6.0)
        # y: -10*0.40 + 4*(1-0.55) = -4.0 + 1.8
        self.assertAlmostEqual(m.y, -2.2)
        # the model has exactly one position accumulator
        self.assertEqual(set(vars(m)), {"y", "z"})

    def test_short_yes_marked_at_moved_price_goes_negative(self):
        """A short marked at a worse price is a negative value, not an error."""
        m = MarketPosition()
        m.buy_no(10, 0.50)  # YES short: y=+5, z=-10
        self.assertEqual(m.z, -10.0)
        self.assertAlmostEqual(m.y, 5.0)
        self.assertAlmostEqual(m.value(0.90), -4.0)

    def test_value_uses_current_price_not_execution_price(self):
        m = MarketPosition()
        m.buy_yes(100, 0.40)
        self.assertAlmostEqual(m.value(0.40), 0.0)   # flat at execution price
        self.assertAlmostEqual(m.value(0.75), 35.0)  # marked at current price


class TestPortfolio(unittest.TestCase):
    def _arb(self):
        """Classic arb: YES cheap on poly, NO cheap on kalshi."""
        pf = Portfolio()
        pf.poly.buy_yes(100, 0.40)
        pf.kalshi.buy_no(100, 0.55)
        return pf

    def test_locked_in_value_is_price_invariant_and_nonnegative(self):
        pf = self._arb()
        self.assertAlmostEqual(pf.locked_in_value, 5.0)
        self.assertGreaterEqual(pf.locked_in_value, 0.0)
        for p in (0.0, 0.3, 0.5, 0.9, 1.0):
            self.assertAlmostEqual(pf.net_value(p, p), 5.0)

    def test_hedge_check_matched(self):
        pf = self._arb()
        self.assertEqual(pf.hedge_delta, 0.0)
        self.assertTrue(pf.is_hedged)

    def test_hedge_check_flags_unmatched_exposure(self):
        pf = self._arb()
        pf.poly.buy_yes(7, 0.42)  # unhedged add-on
        self.assertAlmostEqual(pf.hedge_delta, 7.0)
        self.assertFalse(pf.is_hedged)

    def test_settlement_matches_old_four_variable_model(self):
        """y + z*settle equals (winning tokens - cash deployed) of the old model."""
        pf = Portfolio()
        trades = [("YES", 50, 0.40), ("NO", 30, 0.55), ("YES", 20, 0.45)]
        yes_tok = no_tok = cash = 0.0
        for side, qty, price in trades:
            if side == "YES":
                pf.poly.buy_yes(qty, price)
                yes_tok += qty
            else:
                pf.poly.buy_no(qty, price)
                no_tok += qty
            cash += qty * price
        self.assertAlmostEqual(pf.poly.value(1.0), yes_tok - cash)  # resolves YES
        self.assertAlmostEqual(pf.poly.value(0.0), no_tok - cash)   # resolves NO


if __name__ == "__main__":
    unittest.main()
