import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# Read the data
df = pd.read_csv('pnl_series.csv')
df['Date'] = pd.to_datetime(df['Date'])

# Calculate returns and drawdown
df['Returns'] = df['Equity'].pct_change() * 100
df['Cumulative_Returns'] = (1 + df['Returns']/100).cumprod() * 100
df['Running_Max'] = df['Equity'].cummax()
df['Drawdown'] = (df['Equity'] - df['Running_Max']) / df['Running_Max'] * 100

# Calculate key statistics
initial_equity = df['Equity'].iloc[0]
final_equity = df['Equity'].iloc[-1]
total_return = (final_equity - initial_equity) / initial_equity * 100
max_equity = df['Equity'].max()
min_equity = df['Equity'].min()
max_drawdown = df['Drawdown'].min()
annualized_return = total_return / ((df['Date'].iloc[-1] - df['Date'].iloc[0]).days / 365.25)

# Count positive and negative days
positive_days = (df['Returns'] > 0).sum()
negative_days = (df['Returns'] < 0).sum()
total_days = len(df)

# Create subplot figure
fig = make_subplots(
    rows=3, cols=1,
    subplot_titles=('Equity Curve', 'Daily Returns (%)', 'Drawdown (%)'),
    vertical_spacing=0.1,
    row_heights=[0.5, 0.25, 0.25]
)

# Add equity curve trace
fig.add_trace(
    go.Scatter(
        x=df['Date'],
        y=df['Equity'],
        mode='lines',
        name='Equity',
        line=dict(color='#2E86AB', width=2),
        fill='tozeroy',
        fillcolor='rgba(46, 134, 171, 0.1)'
    ),
    row=1, col=1
)

# Add daily returns trace (colored bars)
colors = ['#2ECC71' if r >= 0 else '#E74C3C' for r in df['Returns']]
fig.add_trace(
    go.Bar(
        x=df['Date'],
        y=df['Returns'],
        name='Daily Returns',
        marker_color=colors,
        opacity=0.7
    ),
    row=2, col=1
)

# Add drawdown trace
fig.add_trace(
    go.Scatter(
        x=df['Date'],
        y=df['Drawdown'],
        mode='lines',
        name='Drawdown',
        line=dict(color='#E74C3C', width=2),
        fill='tozeroy',
        fillcolor='rgba(231, 76, 60, 0.2)'
    ),
    row=3, col=1
)

# Add horizontal line at 0 for drawdown chart
fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5, row=3, col=1)

# Update layout
fig.update_layout(
    title={
        'text': 'Portfolio Equity Analysis',
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 24, 'family': 'Arial Black'}
    },
    height=900,
    showlegend=True,
    hovermode='x unified',
    template='plotly_white'
)

# Update axes labels
fig.update_xaxes(title_text="Date", row=3, col=1)
fig.update_yaxes(title_text="Equity ($)", row=1, col=1)
fig.update_yaxes(title_text="Returns (%)", row=2, col=1)
fig.update_yaxes(title_text="Drawdown (%)", row=3, col=1)

# Add range slider
fig.update_xaxes(rangeslider_visible=True, row=1, col=1)

# Create HTML with statistics panel
html_content = f'''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Equity Curve Analysis</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 20px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .stat-card.positive {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .stat-card.negative {{
            background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
        }}
        .stat-card.neutral {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }}
        .stat-label {{
            font-size: 14px;
            opacity: 0.9;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 28px;
            font-weight: bold;
        }}
        .chart-container {{
            margin-top: 20px;
        }}
        h1 {{
            text-align: center;
            color: #333;
            margin-bottom: 30px;
        }}
        .footer {{
            text-align: center;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #e0e0e0;
            color: #666;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📈 Portfolio Equity Analysis</h1>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Initial Equity</div>
                <div class="stat-value">${initial_equity:,.2f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Final Equity</div>
                <div class="stat-value">${final_equity:,.2f}</div>
            </div>
            <div class="stat-card {'positive' if total_return >= 0 else 'negative'}">
                <div class="stat-label">Total Return</div>
                <div class="stat-value">{total_return:+.2f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Max Equity</div>
                <div class="stat-value">${max_equity:,.2f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Min Equity</div>
                <div class="stat-value">${min_equity:,.2f}</div>
            </div>
            <div class="stat-card negative">
                <div class="stat-label">Max Drawdown</div>
                <div class="stat-value">{max_drawdown:.2f}%</div>
            </div>
            <div class="stat-card neutral">
                <div class="stat-label">Annualized Return</div>
                <div class="stat-value">{annualized_return:.2f}%</div>
            </div>
            <div class="stat-card positive">
                <div class="stat-label">Positive Days</div>
                <div class="stat-value">{positive_days} ({positive_days/total_days*100:.1f}%)</div>
            </div>
            <div class="stat-card negative">
                <div class="stat-label">Negative Days</div>
                <div class="stat-value">{negative_days} ({negative_days/total_days*100:.1f}%)</div>
            </div>
        </div>
        
        <div class="chart-container">
            {fig.to_html(full_html=False, include_plotlyjs='cdn')}
        </div>
        
        <div class="footer">
            <p>Data from {df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')} | 
            Total trading days: {len(df)} | 
            Analysis generated on {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>
'''

# Save to file
with open('equity_analysis.html', 'w', encoding='utf-8') as f:
    f.write(html_content)

print("✅ HTML chart generated successfully!")
print(f"📊 Saved as 'equity_analysis.html'")
print(f"\n📈 Key Statistics:")
print(f"   Initial Equity: ${initial_equity:,.2f}")
print(f"   Final Equity:   ${final_equity:,.2f}")
print(f"   Total Return:   {total_return:+.2f}%")
print(f"   Max Drawdown:   {max_drawdown:.2f}%")
print(f"   Annualized:     {annualized_return:.2f}%")