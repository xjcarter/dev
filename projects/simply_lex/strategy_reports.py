import json
import math
from datetime import date, datetime
from enum import Enum
import os

import pandas
from prettytable import PrettyTable
from df_html_fancy import basic_table_to_html


class DumpFormat(str, Enum):
    STDOUT = 'STDOUT'
    TEXT = 'TEXT'
    HTML = 'HTML'
    JSON = 'JSON'
    CSV = 'CSV'


def convert_to_df(items) -> pandas.DataFrame:
    """Convert a list of dataclass objects to a DataFrame via their to_dict()."""
    return pandas.DataFrame([x.to_dict() for x in items])


class Reporter:

    def __init__(self, trades, test_ledger):
        """
        trades:       list of Trade objects
        test_ledger:  list of DailyRecord objects
        """
        self.trades = trades
        self.test_ledger = test_ledger
        self.metrics: dict = {}
        self.output_directory: str | None = None

    def set_output_directory(self, filepath: str) -> None:
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        self.output_directory = filepath

    # ── header / footer for ledger printout ────────────────────────────
    def get_test_header(self) -> str:
        n_trades = len(self.trades)
        start = self.test_ledger[0].Date if self.test_ledger else '?'
        end = self.test_ledger[-1].Date if self.test_ledger else '?'
        return (
            f"\n{'=' * 80}\n"
            f"  Backtest Ledger  |  {start}  →  {end}  |  {n_trades} trades\n"
            f"{'=' * 80}"
        )

    def get_test_footer(self) -> str:
        if self.test_ledger:
            final_equity = self.test_ledger[-1].Equity
            return (
                f"{'=' * 80}\n"
                f"  Final Equity: {final_equity:,.2f}\n"
                f"{'=' * 80}\n"
            )
        return ''

    # ── metrics ────────────────────────────────────────────────────────
    def generate_metrics(self) -> bool:

        if not self.trades:
            return False

        trades_df = convert_to_df(self.trades)
        pnl_series = convert_to_df(self.test_ledger)

        trade_count = len(trades_df)
        wins = trades_df[trades_df['PnL'] > 0]
        win_pct = len(wins) / trade_count

        returns = pnl_series['Equity'].pct_change().dropna()

        trade_returns = (trades_df['Exit'] / trades_df['Entry']) - 1

        trade_wins = trade_returns[trade_returns >= 0]
        trade_losses = trade_returns[trade_returns < 0]

        loss_count = len(trade_losses)
        loss_sum = trade_losses.sum()
        profit_factor = trade_wins.sum() / (-loss_sum) if loss_sum != 0 else float('inf')
        avg_win = trade_wins.mean() if len(trade_wins) else 0.0
        avg_loss = trade_losses.mean() if loss_count else 0.0

        std_win = trade_wins.std() if len(trade_wins) > 1 else 0.0
        std_losses = trade_losses.std() if loss_count > 1 else 0.0

        # vectorised drawdown
        rolling_max = pnl_series['Equity'].cummax()
        drawdown_series = (pnl_series['Equity'] / rolling_max) - 1
        dd = drawdown_series.min()
        drops = drawdown_series[drawdown_series < 0]
        mdd = drops.mean() if len(drops) else 0.0

        years = returns.shape[0] / 252.0

        start_equity = pnl_series.iloc[0]['Equity']
        end_equity = pnl_series.iloc[-1]['Equity']

        total_rtn = (end_equity / start_equity) - 1
        cagr = ((end_equity / start_equity) ** (1.0 / years)) - 1 if years > 0 else 0.0

        annual_vol = returns.std() * math.sqrt(252)
        sharpe = cagr / annual_vol if annual_vol != 0 else 0.0

        self.metrics = dict(
            Sharpe=sharpe,
            CAGR=cagr,
            MaxDD=dd,
            AvgDD=mdd,
            Trades=trade_count,
            WinPct=win_pct,
            PFactor=profit_factor,
            AvgWin=avg_win,
            StdWin=std_win,
            AvgLoss=avg_loss,
            StdLoss=std_losses,
            LossCount=loss_count,
            Years=years,
            TotalRtn=total_rtn,
        )
        return True

    # ── formatting helpers ─────────────────────────────────────────────
    def format_df(self, records) -> pandas.DataFrame:
        """Pretty‑format a list of dicts (or dataclass objects) for display."""
        FLOAT_FIELDS = set('Entry Entry_Price Exit Exit_Price MTM '
                           'Drawdown Equity PNL High Low'.split())
        INT_FIELDS = set('Position Duration Max_Offset Min_Offset Amount'.split())

        def _fmt(value, precision=3):
            if value is None:
                return ''
            r3 = round(value, 3)
            r2 = round(value, 2)
            return str(r2) if r3 == r2 else str(r3)

        raw = [r.to_dict() if hasattr(r, 'to_dict') else dict(r) for r in records]
        for d in raw:
            for k, v in d.items():
                if k in FLOAT_FIELDS:
                    d[k] = _fmt(v)
                elif k in INT_FIELDS:
                    d[k] = '' if v is None else str(v)

        df = pandas.DataFrame(raw).fillna('')
        return df

    def format_table(self, pretty_table: PrettyTable) -> PrettyTable:
        COLUMNS_TO_CENTER = set('Date Entry_Date Exit_Date Entry_Label Exit_Label'.split())
        MONEY_COLUMNS = set('MTM PNL Equity'.split())

        new_table = pretty_table.copy()
        for col in pretty_table.field_names:
            new_table.float_format[col] = '.3'
            if col in MONEY_COLUMNS:
                new_table.float_format[col] = '.2'
            new_table.align[col] = 'r'
            if col in COLUMNS_TO_CENTER:
                new_table.align[col] = 'c'
            if col == 'Position':
                new_table.float_format[col] = '.0'

        return new_table

    # ── dump methods ───────────────────────────────────────────────────
    def dump_trades(self, formats=None) -> None:
        if formats is None:
            formats = [DumpFormat.CSV]

        trades_df = convert_to_df(self.trades)

        if DumpFormat.CSV in formats:
            trades_df.round(4).to_csv(
                f'{self.output_directory}/trades.csv', index=False)

        if DumpFormat.STDOUT in formats:
            display_df = trades_df.fillna('')
            col_list = display_df.columns.tolist()
            table = self.format_table(PrettyTable(col_list))
            for _, row in display_df.iterrows():
                table.add_row(row.tolist())
            print(table)

        if DumpFormat.HTML in formats:
            html_df = self.format_df(self.trades)
            html = basic_table_to_html(html_df, 'BackTest Trades')
            with open(f'{self.output_directory}/trades.html', 'w') as f:
                f.write(html + '\n')

    def dump_equity_curve(self, formats=None) -> None:
        if formats is None:
            formats = [DumpFormat.STDOUT, DumpFormat.TEXT]

        test_ledger_df = convert_to_df(self.test_ledger).round(4)
        pnl_series_df = test_ledger_df[['Date', 'Equity']]

        if DumpFormat.CSV in formats:
            test_ledger_df.to_csv(
                f'{self.output_directory}/test_ledger.csv', index=False)
            pnl_series_df.to_csv(
                f'{self.output_directory}/pnl_series.csv', index=False)

        needs_table = {DumpFormat.STDOUT, DumpFormat.TEXT} & set(formats)
        if needs_table:
            display_df = test_ledger_df.fillna('')
            display_df['MTM'] = display_df['MTM'].replace(0, '')
            col_list = display_df.columns.tolist()
            table = self.format_table(PrettyTable(col_list))
            for _, row in display_df.iterrows():
                table.add_row(row.tolist())

            if DumpFormat.STDOUT in formats:
                print(self.get_test_header())
                print(table)
                print(self.get_test_footer())

            if DumpFormat.TEXT in formats:
                with open(f'{self.output_directory}/test_ledger.txt', 'w') as f:
                    f.write(self.get_test_header() + '\n')
                    f.write(table.get_string() + '\n')
                    f.write(self.get_test_footer() + '\n')

        if DumpFormat.HTML in formats:
            html_df = self.format_df(self.test_ledger)
            html = basic_table_to_html(html_df, 'Backtest Series')
            with open(f'{self.output_directory}/trades_series.html', 'w') as f:
                f.write(html + '\n')

    def dump_metrics(self, formats=None, transpose=False,
                     value_list=None, title=None) -> None:
        if formats is None:
            formats = [DumpFormat.STDOUT, DumpFormat.JSON]

        metrics_df = pandas.DataFrame([self.metrics])
        if value_list:
            metrics_df = metrics_df[value_list]
        metrics_df = metrics_df.round(3)

        ttl = title or ' '

        if transpose:
            metrics_df = metrics_df.T
            metrics_df.reset_index(inplace=True)
            metrics_df.rename(columns={'index': 'Metric', 0: 'Value'}, inplace=True)

        if DumpFormat.STDOUT in formats:
            table = PrettyTable(metrics_df.columns.tolist())
            table.align['Metric'] = 'l'
            table.align['Value'] = 'r'
            table.float_format['Value'] = '.3'
            for _, row in metrics_df.iterrows():
                table.add_row(row.tolist())
            print(table)

        if DumpFormat.HTML in formats:
            html = basic_table_to_html(metrics_df, ttl)
            with open(f'{self.output_directory}/metrics.html', 'w') as f:
                f.write(html + '\n')

        if DumpFormat.JSON in formats:
            formatted_metrics = {k: round(v, 4) for k, v in self.metrics.items()}
            metrics_json = json.dumps(formatted_metrics, indent=4)
            with open(f'{self.output_directory}/metrics.json', 'w') as f:
                f.write(metrics_json + '\n')
