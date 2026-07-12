"""
Two-variable cash+position portfolio model.

Each market (Polymarket, Kalshi) is a single (y, z) pair:
  y — running cash balance for the market
  z — net signed YES-equivalent contracts

Every trade folds into the SAME pair; a NO purchase is booked at ingestion as
a YES short at the implied YES price (1 - no_price), never as a separate NO
holding.

Mark-to-market uses the market's current YES price (live/last-trade or the
settlement price), never a stored execution price:
  market_value = y + z * current_yes_price
"""


class MarketPosition:
    """Cash + net signed YES-equivalent position for one market."""

    def __init__(self):
        self.y = 0.0  # cash balance
        self.z = 0.0  # net YES-equivalent contracts (short => negative)

    def buy_yes(self, qty: float, yes_price: float) -> None:
        self.y -= qty * yes_price
        self.z += qty

    def buy_no(self, qty: float, no_price: float) -> None:
        # a NO purchase is a YES short credited at the implied YES price
        self.y += qty * (1.0 - no_price)
        self.z -= qty

    def value(self, current_yes_price: float) -> float:
        return self.y + self.z * current_yes_price


class Portfolio:
    """Poly + Kalshi positions in one market pair, valued together."""

    HEDGE_TOL = 1e-9

    def __init__(self):
        self.poly = MarketPosition()
        self.kalshi = MarketPosition()

    def net_value(self, poly_yes_price: float, kalshi_yes_price: float) -> float:
        return self.poly.value(poly_yes_price) + self.kalshi.value(kalshi_yes_price)

    @property
    def locked_in_value(self) -> float:
        """y_poly + y_kalshi — price-invariant; >= 0 for a genuine arbitrage."""
        return self.poly.y + self.kalshi.y

    @property
    def hedge_delta(self) -> float:
        """abs(z_poly) - abs(z_kalshi); nonzero flags unmatched exposure."""
        return abs(self.poly.z) - abs(self.kalshi.z)

    @property
    def is_hedged(self) -> bool:
        return abs(self.hedge_delta) <= self.HEDGE_TOL
