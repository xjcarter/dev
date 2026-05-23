#!/usr/bin/env python3
"""
Trading Framework - Standalone Report Generator
No external dependencies - uses only Python standard library + built-in JSON/CSV
Perfect for initial testing before full stack deployment.

Usage:
    python trading_reporter_standalone.py \
        --trades trades.json \
        --alloc alloc.json \
        --pnl pnl.csv \
        --output report.html
"""

import json
import csv
from datetime import datetime
from pathlib import Path
import sys
import argparse
from html import escape


class SimpleReportGenerator:
    """Generate professional trading reports without external dependencies."""
    
    def __init__(self, trades_file, alloc_file, pnl_file, config=None):
        self.trades_file = trades_file
        self.alloc_file = alloc_file
        self.pnl_file = pnl_file
        self.config = config or self._default_config()
        
        self.trades = []
        self.alloc_data = {}
        self.pnl_data = []
        self.metrics = {}
    
    def _default_config(self):
        return {
            "title": "Trading Performance Report",
            "company_name": "Trading Strategy",
            "color_primary": "#1a5490",
            "color_accent": "#2ecc71"
        }
    
    def load_data(self):
        """Load all data files."""
        print("[1/4] Loading data files...")
        
        # Load trades
        try:
            with open(self.trades_file, 'r') as f:
                trades_data = json.load(f)
            self.trades = trades_data.get('trades', [])
            print(f"  ✓ Loaded {len(self.trades)} trades")
        except Exception as e:
            print(f"  ✗ Error loading trades: {e}")
            return False
        
        # Load allocations
        try:
            with open(self.alloc_file, 'r') as f:
                self.alloc_data = json.load(f)
            print(f"  ✓ Loaded {len(self.alloc_data)} account allocations")
        except Exception as e:
            print(f"  ✗ Error loading allocations: {e}")
            return False
        
        # Load P&L
        try:
            with open(self.pnl_file, 'r') as f:
                reader = csv.DictReader(f)
                self.pnl_data = list(reader)
            print(f"  ✓ Loaded {len(self.pnl_data)} P&L records")
        except Exception as e:
            print(f"  ✗ Error loading P&L: {e}")
            return False
        
        return True
    
    def calculate_metrics(self):
        """Calculate trading metrics from P&L data."""
        print("[2/4] Calculating metrics...")
        
        if not self.pnl_data:
            print("  ✗ No P&L data to analyze")
            return False
        
        # Parse P&L values
        pnl_values = []
        for record in self.pnl_data:
            try:
                realized = float(record.get('realized_pnl', 0))
                pnl_values.append(realized)
            except ValueError:
                continue
        
        if not pnl_values:
            print("  ✗ No valid P&L values")
            return False
        
        # Calculate metrics
        total_pnl = sum(pnl_values)
        winning_pnls = [p for p in pnl_values if p > 0]
        losing_pnls = [p for p in pnl_values if p < 0]
        
        total_trades = len(pnl_values)
        winning_trades = len(winning_pnls)
        losing_trades = len(losing_pnls)
        
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        avg_win = (sum(winning_pnls) / len(winning_pnls)) if winning_pnls else 0
        avg_loss = (sum(losing_pnls) / len(losing_pnls)) if losing_pnls else 0
        
        largest_win = max(pnl_values) if pnl_values else 0
        largest_loss = min(pnl_values) if pnl_values else 0
        
        total_wins = sum(winning_pnls) if winning_pnls else 0
        total_losses = abs(sum(losing_pnls)) if losing_pnls else 0
        
        profit_factor = (total_wins / total_losses) if total_losses > 0 else 0
        
        self.metrics = {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'largest_win': largest_win,
            'largest_loss': largest_loss,
            'profit_factor': profit_factor,
            'total_wins': total_wins,
            'total_losses': total_losses
        }
        
        print(f"  ✓ Total trades: {total_trades}")
        print(f"  ✓ Win rate: {win_rate:.1f}%")
        print(f"  ✓ Total P&L: ${total_pnl:,.2f}")
        
        return True
    
    def create_daily_pnl_chart(self):
        """Create ASCII chart of daily P&L (for reference)."""
        daily_data = {}
        
        for record in self.pnl_data:
            date = record.get('timestamp', '').split('-')[0]
            realized = float(record.get('realized_pnl', 0))
            
            if date not in daily_data:
                daily_data[date] = 0
            daily_data[date] += realized
        
        # Create HTML bar chart using CSS
        html_bars = []
        max_val = max(abs(v) for v in daily_data.values()) if daily_data else 1
        
        for date in sorted(daily_data.keys()):
            value = daily_data[date]
            pct = (abs(value) / max_val * 100) if max_val > 0 else 0
            color = '#27ae60' if value > 0 else '#e74c3c'
            
            html_bars.append(f"""
            <div style="margin-bottom: 10px;">
                <div style="font-size: 0.9em; margin-bottom: 3px;">
                    <strong>{date}</strong>: <span style="color: {color}; font-weight: bold;">${value:,.2f}</span>
                </div>
                <div style="background: #eee; height: 20px; border-radius: 3px; overflow: hidden;">
                    <div style="background: {color}; height: 100%; width: {pct}%; transition: width 0.3s;"></div>
                </div>
            </div>
            """)
        
        return "\n".join(html_bars)
    
    def create_trade_rows(self):
        """Create HTML table rows for trades."""
        rows = []
        
        trades_by_date = {}
        for trade in self.trades:
            date = trade.get('timestamp', '')[:8]
            if date not in trades_by_date:
                trades_by_date[date] = []
            trades_by_date[date].append(trade)
        
        # Get P&L for each trade
        pnl_by_trade = {}
        for record in self.pnl_data:
            trade_id = record.get('trade_id', '')
            if trade_id:
                pnl_by_trade[trade_id] = float(record.get('realized_pnl', 0))
        
        # Create rows (reverse order - newest first)
        for trade in sorted(self.trades, key=lambda x: x.get('timestamp', ''), reverse=True)[:50]:
            trade_id = trade.get('trade_id', 'N/A')
            pnl = pnl_by_trade.get(trade_id, 0)
            
            pnl_class = 'positive' if pnl > 0 else 'negative'
            
            row = f"""
            <tr>
                <td>{trade.get('timestamp', 'N/A')[:16]}</td>
                <td><strong>{escape(str(trade.get('asset', 'N/A')))}</strong></td>
                <td>{trade.get('side', 'N/A')}</td>
                <td style="text-align: right;">{trade.get('units', 0):,.0f}</td>
                <td style="text-align: right;">${float(trade.get('price', 0)):,.2f}</td>
                <td style="text-align: right;" class="{pnl_class}">${pnl:,.2f}</td>
            </tr>
            """
            rows.append(row)
        
        return "\n".join(rows)
    
    def generate_html(self):
        """Generate complete HTML report."""
        print("[3/4] Generating HTML report...")
        
        daily_pnl_chart = self.create_daily_pnl_chart()
        trade_rows = self.create_trade_rows()
        
        total_alloc = sum(
            acc.get('availableEquity', 0)
            for acc in self.alloc_data.values()
        ) if self.alloc_data else 0
        
        # HTML template
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escape(self.config['title'])}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        html, body {{
            width: 100%;
            height: 100%;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }}
        
        .header {{
            background: linear-gradient(135deg, {self.config['color_primary']} 0%, #2c5aa0 100%);
            color: white;
            padding: 50px 20px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 600;
            letter-spacing: -0.5px;
        }}
        
        .header .subtitle {{
            font-size: 1.1em;
            opacity: 0.95;
            margin: 5px 0;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 30px 20px;
        }}
        
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px;
            margin: 40px 0;
        }}
        
        .metric-card {{
            background: white;
            border-radius: 8px;
            padding: 25px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            border-left: 4px solid {self.config['color_accent']};
            transition: transform 0.2s;
        }}
        
        .metric-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
        }}
        
        .metric-card h3 {{
            font-size: 0.85em;
            color: #999;
            text-transform: uppercase;
            margin-bottom: 12px;
            font-weight: 600;
            letter-spacing: 0.5px;
        }}
        
        .metric-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: {self.config['color_primary']};
            word-break: break-word;
        }}
        
        .metric-card.positive .value {{
            color: #27ae60;
        }}
        
        .metric-card.negative .value {{
            color: #e74c3c;
        }}
        
        .section {{
            background: white;
            border-radius: 8px;
            padding: 30px;
            margin: 30px 0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        
        .section h2 {{
            font-size: 1.5em;
            margin-bottom: 25px;
            border-bottom: 2px solid {self.config['color_accent']};
            padding-bottom: 15px;
            color: {self.config['color_primary']};
        }}
        
        .summary-text {{
            background: #f0f4f8;
            padding: 20px;
            border-left: 4px solid {self.config['color_accent']};
            margin: 20px 0;
            border-radius: 4px;
            color: #555;
            line-height: 1.8;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        
        thead {{
            background: #f8f9fa;
            border-bottom: 2px solid {self.config['color_primary']};
        }}
        
        th {{
            padding: 15px;
            text-align: left;
            font-weight: 600;
            color: {self.config['color_primary']};
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #eee;
        }}
        
        tbody tr:hover {{
            background: #f8f9fa;
        }}
        
        .positive {{
            color: #27ae60;
            font-weight: 600;
        }}
        
        .negative {{
            color: #e74c3c;
            font-weight: 600;
        }}
        
        .footer {{
            background: #f8f9fa;
            border-top: 1px solid #eee;
            padding: 30px;
            text-align: center;
            color: #999;
            font-size: 0.9em;
            margin-top: 50px;
            border-radius: 8px;
        }}
        
        @media print {{
            body {{ background: white; }}
            .section {{ box-shadow: none; border: 1px solid #eee; }}
            .metric-card {{ page-break-inside: avoid; }}
        }}
        
        @media (max-width: 768px) {{
            .header h1 {{ font-size: 1.8em; }}
            .metrics-grid {{ grid-template-columns: 1fr; }}
            .container {{ padding: 15px; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{escape(self.config['company_name'])}</h1>
        <div class="subtitle">{escape(self.config['title'])}</div>
        <div class="subtitle">{datetime.now().strftime('%B %d, %Y')}</div>
    </div>
    
    <div class="container">
        <!-- Key Metrics -->
        <div class="metrics-grid">
            <div class="metric-card {'positive' if self.metrics['total_pnl'] > 0 else 'negative'}">
                <h3>Total P&L</h3>
                <div class="value">${self.metrics['total_pnl']:,.2f}</div>
            </div>
            <div class="metric-card">
                <h3>Win Rate</h3>
                <div class="value">{self.metrics['win_rate']:.1f}%</div>
            </div>
            <div class="metric-card">
                <h3>Total Trades</h3>
                <div class="value">{self.metrics['total_trades']}</div>
            </div>
            <div class="metric-card">
                <h3>Avg Win</h3>
                <div class="value">${self.metrics['avg_win']:,.0f}</div>
            </div>
            <div class="metric-card">
                <h3>Avg Loss</h3>
                <div class="value">${self.metrics['avg_loss']:,.0f}</div>
            </div>
            <div class="metric-card">
                <h3>Profit Factor</h3>
                <div class="value">{self.metrics['profit_factor']:.2f}</div>
            </div>
        </div>
        
        <!-- Performance Analysis -->
        <div class="section">
            <h2>Performance Analysis</h2>
            
            <div class="summary-text">
                <strong>Summary:</strong> This strategy executed {self.metrics['total_trades']} trades over the reporting period, 
                with a win rate of {self.metrics['win_rate']:.1f}%. Total realized P&L was 
                <span class="{'positive' if self.metrics['total_pnl'] > 0 else 'negative'}">
                    ${self.metrics['total_pnl']:,.2f}
                </span>, with an average winning trade of 
                <span class="positive">${self.metrics['avg_win']:,.2f}</span> 
                and average losing trade of 
                <span class="negative">${self.metrics['avg_loss']:,.2f}</span>.
            </div>
            
            <p><strong>Capital Allocated:</strong> ${total_alloc:,.2f} across {len(self.alloc_data)} accounts</p>
        </div>
        
        <!-- Daily P&L -->
        <div class="section">
            <h2>Daily P&L Breakdown</h2>
            {daily_pnl_chart}
        </div>
        
        <!-- Trade Details -->
        <div class="section">
            <h2>Recent Trades</h2>
            <table>
                <thead>
                    <tr>
                        <th>Date & Time</th>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Units</th>
                        <th>Price</th>
                        <th>P&L</th>
                    </tr>
                </thead>
                <tbody>
                    {trade_rows}
                </tbody>
            </table>
        </div>
        
        <!-- Footer -->
        <div class="footer">
            <p><strong>{escape(self.config['company_name'])}</strong></p>
            <p>Report generated: {datetime.now().strftime('%B %d, %Y at %H:%M:%S')}</p>
            <p style="margin-top: 15px; font-size: 0.85em; color: #ccc;">
                This report is confidential and provided for informational purposes only.
            </p>
        </div>
    </div>
</body>
</html>
"""
        
        return html
    
    def save(self, output_file):
        """Save report to file."""
        print("[4/4] Saving report...")
        
        html = self.generate_html()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"  ✓ Report saved: {output_file}")
        print("\n✓ SUCCESS! Report generation complete!")
        print(f"\n  View report: file://{Path(output_file).absolute()}")


def main():
    parser = argparse.ArgumentParser(
        description='Professional Trading Report Generator (No Dependencies)',
        epilog="""
Example:
    python trading_reporter_standalone.py \\
        --trades trades.json \\
        --alloc alloc.json \\
        --pnl pnl.csv \\
        --output report.html \\
        --title "Monthly Report" \\
        --company "Your Firm"
        """
    )
    
    parser.add_argument('--trades', required=True, help='Trades JSON file')
    parser.add_argument('--alloc', required=True, help='Allocation JSON file')
    parser.add_argument('--pnl', required=True, help='P&L CSV file')
    parser.add_argument('--output', default='report.html', help='Output HTML file')
    parser.add_argument('--title', default='Trading Performance Report', help='Report title')
    parser.add_argument('--company', default='Trading Strategy', help='Company name')
    parser.add_argument('--color', default='#1a5490', help='Primary color')
    parser.add_argument('--accent', default='#2ecc71', help='Accent color')
    
    args = parser.parse_args()
    
    # Validate files
    for f in [args.trades, args.alloc, args.pnl]:
        if not Path(f).exists():
            print(f"ERROR: File not found: {f}")
            sys.exit(1)
    
    config = {
        'title': args.title,
        'company_name': args.company,
        'color_primary': args.color,
        'color_accent': args.accent
    }
    
    print("=" * 70)
    print("TRADING FRAMEWORK - PROFESSIONAL REPORT GENERATOR")
    print("=" * 70)
    print()
    
    gen = SimpleReportGenerator(args.trades, args.alloc, args.pnl, config)
    
    if not gen.load_data():
        sys.exit(1)
    
    if not gen.calculate_metrics():
        sys.exit(1)
    
    gen.save(args.output)
    
    print()
    print("=" * 70)


if __name__ == '__main__':
    main()
