from dataclasses import dataclass, fields, asdict
from pathlib import Path
from typing import Optional, List
import csv

from indicators import MondayAnchor, Volatility
import calendar_calcs
from strategy_reports2 import Reporter, DumpFormat

#DIRECTORY_NAME = '/Users/jcarter/hannibal/dev/simply_lex/adjusted_risk'
DIRECTORY_NAME = '/Users/jcarter/hannibal/dev/simply_lex/expiry6_d3/'
HOLIDAYS = '/Users/jcarter/hannibal/dev/us_market_holidays.csv'

@dataclass
class OHLCBar:
    """A single daily price bar.

    Example CSV rows:
        Date,Open,High,Low,Close,Adj Close,Volume
        2000-01-03,148.25,148.25,143.875,145.4375,96.55506,8164300.0
    """

    Date: str = ''
    Open: float = 0.0
    High: float = 0.0
    Low: float = 0.0
    Close: float = 0.0
    Adj_Close: float = 0.0
    Volume: int = 0

    # ── factory ────────────────────────────────────────────────────────
    @classmethod
    def from_dict(cls, row: dict) -> 'OHLCBar':
        """Build an OHLCBar from a csv.DictReader row.

        Column names with spaces are normalised to underscores so that
        'Adj Close' maps to the Adj_Close field automatically.
        """
        clean = {k.replace(' ', '_'): v for k, v in row.items()}
        new_bar = cls(
            Date=clean.get('Date', ''),
            Open=float(clean.get('Open', 0)),
            High=float(clean.get('High', 0)),
            Low=float(clean.get('Low', 0)),
            Close=float(clean.get('Close', 0)),
            Adj_Close=float(clean.get('Adj_Close', 0)),
            Volume=int(float(clean.get('Volume', 0))),
        )

        # re-map prices relative to Adj_Close
        new_bar.adjust_prices()
        return new_bar

    def adjust_prices(self) -> None:
        # adjust prices relative to Adjusted Close                                                                                                                  
        delta = self.Adj_Close / self.Close                                                                                                                      
                                                                                                                                                                               
        # Apply the adjustment to the 'Open', 'High', and 'Low' columns                                                                                                        
        self.Open = round( self.Open *delta, 4)                                                                                                                             
        self.High = round( self.High *delta, 4)                                                                                                                           
        self.Low = round( self.Low *delta, 4)                                                                                                                               
                                                                                                                                                                               
        # Set the 'Close' column to the 'Adj Close' values                                                                                                                     
        self.Close = self.Adj_Close                                                                                                                                          

    def to_dict(self) -> dict:
        """Return a plain dict keyed by field name."""
        return asdict(self)


@dataclass
class Trade:
    """All the info describing the evolution of a trade.

    High / Low track the highest bar High and lowest bar Low observed
    while the trade is open.
    """

    Entry_Date: str = ''
    Entry_Label: str = ''
    Entry: float = 0.0
    Amount: int = 0
    Exit: float = 0.0
    Exit_Label: str = ''
    Exit_Date: str = ''
    High: float = 0.0
    Low: float = float('inf')
    Duration: int = 0
    Max_Offset: int = 0
    Min_Offset: int = 0
    Max_Date: str = ''
    Min_Date: str = ''
    Drawdown: float = 0.0
    PnL: float = 0.0

    # ── trade‑life helpers ─────────────────────────────────────────────
    def update_extremes(self, bar: OHLCBar) -> None:
        """Update High/Low and their offset/date metadata from a new bar."""
        if bar.High > self.High:
            self.High = bar.High
            self.Max_Offset = self.Duration
            self.Max_Date = bar.Date
        if bar.Low < self.Low:
            self.Low = bar.Low
            self.Min_Offset = self.Duration
            self.Min_Date = bar.Date

    def calc_drawdown(self) -> None:
        """Drawdown as a percentage drop from High to Low."""
        if self.High != 0:
            self.Drawdown = (self.High - self.Low) / self.High

    def calc_pnl(self, exit_price: float, amount: int) -> float:
        """Dollar P&L for *amount* shares exited at *exit_price*."""
        return (exit_price - self.Entry) * amount

    # ── serialisation ──────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return asdict(self)

    def copy(self) -> 'Trade':
        """Deep copy via the dataclass fields — no mutable containers to worry about."""
        return Trade(**{f.name: getattr(self, f.name) for f in fields(self)})


@dataclass
class DailyRecord:
    """Daily snapshot of account activity.

    Equity = running total of realised P&L + current trade MTM.
    """

    Date: str = ''
    Entry_Label: str = ''
    Entry_Price: Optional[float] = None
    Position: Optional[int] = None
    Exit_Label: str = ''
    Exit_Price: Optional[float] = None 
    MTM_Price: Optional[float] = None 
    MTM: Optional[float] = None 
    Equity: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


class SimplyLex:

    def __init__(self, data_source: str, capital: float):
        self.data = self.load_data(data_source)
        self.anchor = MondayAnchor(derived_len=20)
        self.volatility = Volatility(derived_len=20)
        self.holidays = calendar_calcs.load_holidays(HOLIDAYS)
        self.current_trade: Optional[Trade] = None

        # starting capital for trading
        self.capital = capital

        # list of executed trades (Trade objects)
        self.trades: list[Trade] = []
        # daily account snapshots (DailyRecord objects)
        self.test_ledger: list[DailyRecord] = []

        # rest flag for stopped out trades
        self.cooldown = 0

    # ── data loading ───────────────────────────────────────────────────
    @staticmethod
    def load_data(filepath: str) -> list[OHLCBar]:
        """Load a CSV file into a list of OHLCBar objects."""
        bars: list[OHLCBar] = []
        path = Path(filepath)
        with path.open(newline='') as fh:
            for row in csv.DictReader(fh):
                bars.append(OHLCBar.from_dict(row))
        return bars


    # ── order helpers ──────────────────────────────────────────────────
    def buy(self, bar: OHLCBar, amount: int, price: float, label: str) -> None:
        """Open (or add to) a long position."""
        self.current_trade = Trade(
            Entry_Date=bar.Date,
            Entry=price,
            Entry_Label=label,
            Amount=amount,
            High=price,
            Low=price,
            Max_Date=bar.Date,
            Min_Date=bar.Date
        )

        """Handle opening trade"""
        if price == bar.Open:
            self.current_trade.update_extremes(bar)


    def sell(self, bar: OHLCBar, amount: int, price: float, label: str) -> None:
        """Close (or reduce) the current position by *amount* shares.

        Realised P&L is added to self.capital.  If the full position is
        closed, self.current_trade is set to None.
        """
        trade = self.current_trade
        pnl = trade.calc_pnl(price, amount)
        self.capital += pnl

        # snapshot the trade at exit
        trade.Exit = price
        trade.Exit_Date = bar.Date 
        trade.Exit_Label = label
        trade.PnL = pnl
        trade.calc_drawdown()
        trade.Amount -= amount

        closed_trade = trade.copy()
        closed_trade.Amount = amount

        self.trades.append(closed_trade)


    # ── position sizing ────────────────────────────────────────────────

    def calculate_trade_amount(self, bar, wiggle=0.0):
        #standard weights .35, FLOOR = 0.15, CEILING= 0.60
        DEPLOY_FRACTION = 0.35
        TARGET_VOL = 0.40  # median UPRO 20d vol
        FLOOR = 0.15
        CEILING = 0.60

        frac = DEPLOY_FRACTION
        if self.volatility.count() > 0:
            current_vol = self.volatility.valueAt(0)
            if current_vol > 0:
                frac = DEPLOY_FRACTION * (TARGET_VOL / current_vol)
                frac = max(FLOOR, min(CEILING, frac))
        deployed = self.capital * frac

        return int(deployed / ((1 + wiggle) * bar.Open))

    def fractional_trade_amount(self, bar, frac=0.5, wiggle=0.0):
        deployed = self.capital * frac
        return int(deployed / ((1 + wiggle) * bar.Open))

    # ── mark‑to‑market ─────────────────────────────────────────────────
    def mtm(self, bar: OHLCBar, closed: int) -> None:
        """Record a daily mark‑to‑market snapshot."""
        record = DailyRecord(Date=bar.Date)
        unrealised = 0.0

        if self.current_trade is not None:
            trade = self.current_trade
            trade.update_extremes(bar)
            record.Position = trade.Amount
            if trade.Amount != 0:
                unrealised = (bar.Adj_Close - trade.Entry) * trade.Amount
                record.MTM_Price = bar.Adj_Close
                record.MTM = unrealised
            else:
                record.MTM_Price = trade.Exit
                record.MTM = trade.PnL
                record.Position = closed

            if trade.Entry_Date == bar.Date:
                record.Entry_Label = trade.Entry_Label
                record.Entry_Price = trade.Entry

            if trade.Exit_Date == bar.Date:
                record.Exit_Label = trade.Exit_Label
                record.Exit_Price = trade.Exit


        record.Equity = self.capital + unrealised
        self.test_ledger.append(record)

        if self.current_trade is not None and self.current_trade.Amount == 0:
            self.current_trade = None

    # ── position query ─────────────────────────────────────────────────
    @property
    def open_position(self) -> bool:
        return self.current_trade is not None and self.current_trade.Amount > 0

    # ── per‑bar strategy logic ─────────────────────────────────────────
    def execute(self, bar: OHLCBar) -> None:

        cur_dt = calendar_calcs.cvt_date(bar.Date)
        self.anchor.push((cur_dt, bar.to_dict()))
        self.volatility.push(bar.Adj_Close)
        end_of_week = calendar_calcs.is_end_of_week(cur_dt, self.holidays)

        if self.cooldown > 0:
            self.cooldown -= 1

        if all([self.open_position == False, self.anchor.count() > 1, self.cooldown == 0]):
            anchor_bar, bkout = self.anchor.valueAt(1)
            if bkout < 0 and not end_of_week:
                #shares = self.calculate_trade_amount(bar)
                shares = self.fractional_trade_amount(bar, frac=1.0)
                self.buy(bar, shares, bar.Open, 'Lex')

        # ── exit logic ─────────────────────────────────────────────────
        closed = None
        if self.open_position:
            #closed = self.standard_exit(bar, 6)
            closed = self.scaled_exit(bar, exit_points=[3,6])

        self.mtm(bar, closed)


    # ── exit strategies ────────────────────────────────────────────────────────
    def standard_exit(self, bar: OHLCBar, expiry_days: int) -> None:
        closed = self.current_trade.Amount
        self.current_trade.Duration += 1
        if bar.Adj_Close > self.current_trade.Entry:
            self.sell(bar, closed, bar.Adj_Close, 'PnL')
        else:
            if self.current_trade.Duration >= expiry_days: 
                self.sell(bar, closed, bar.Adj_Close, 'Exp')
        return closed

    def scaled_exit(self, bar: OHLCBar, exit_points: List[int]) -> None:
        closed = self.current_trade.Amount
        self.current_trade.Duration += 1
        if bar.Adj_Close > self.current_trade.Entry:
            self.sell(bar, closed, bar.Adj_Close, 'PnL')
            return closed

        # equal division of exits up to expiration
        divisor = len(exit_points)

        if self.current_trade.Duration in exit_points[:-1]: 
            closed = self.current_trade.Amount // divisor
            self.sell(bar, closed, bar.Adj_Close, f'D{self.current_trade.Duration}')
            return closed

        if self.current_trade.Duration >= exit_points[-1]:
            self.sell(bar, closed, bar.Adj_Close, 'Exp')
            return closed


    # ── runners ────────────────────────────────────────────────────────
    def run(self) -> None:
        for bar in self.data:
            self.execute(bar)

    def run_and_report(self) -> None:
        self.run()

        reporter = Reporter(self.trades, self.test_ledger)
        reporter.set_output_directory(DIRECTORY_NAME)

        reporter.generate_metrics()
        reporter.dump_equity_curve(
            formats=[DumpFormat.STDOUT, DumpFormat.CSV, DumpFormat.HTML])
        reporter.dump_trades(
            formats=[DumpFormat.CSV, DumpFormat.HTML])
        reporter.dump_metrics(
            transpose=True,
            formats=[DumpFormat.STDOUT, DumpFormat.JSON, DumpFormat.HTML])



if __name__ == '__main__':
    strategy = SimplyLex('/Users/jcarter/hannibal/dev/simply_lex/UPRO.csv', capital=10000)
    strategy.run_and_report()
