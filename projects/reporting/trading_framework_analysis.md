# Trading Framework Reporting Architecture Analysis
**Analysis Date:** March 29, 2026

---

## EXECUTIVE SUMMARY

Your current data capture is well-structured and audit-ready. The framework successfully tracks:
- ✅ Individual trades with execution details (price, size, timestamp, exchange info)
- ✅ Account-level allocation and capital distribution
- ✅ Position tracking with historical layers
- ✅ Realized and unrealized P&L attribution
- ✅ Multi-account aggregation

**Key Finding:** Your system has a **strategy_id** in allocations but not explicitly in orders/trades. This is a critical gap for detailed reporting.

---

## SECTION 1: DATA STRUCTURE ASSESSMENT

### Current Strengths
1. **Timestamp precision** - millisecond-level trade execution timing (15:58:05 format)
2. **P&L attribution** - Already splitting realized/unrealized and attributing to account level
3. **Account aggregation** - "aim_test" appears to be portfolio-level aggregate
4. **Allocation targets** - You're tracking both current positions and target allocations
5. **Order status tracking** - Complete order lifecycle (open → filled → closed)

### Critical Data Gaps for Comprehensive Reporting

#### Gap 1: Missing Strategy Attribution in Trade Data
**Current state:** Trades have `order_id`, `side`, `asset`, `units`, `price` but NO `strategy_id`
```json
// Current trade structure:
{
  "trade_id": "238677418-0001",
  "asset": "UPRO",
  "units": 10497.0,
  "price": 90.48,
  // ❌ No strategy_id here
  // This means you cannot separately track "strategy_A" performance 
  // vs "strategy_B" performance for the same account
}
```

**Why it matters:**
- Cannot answer: "What was the P&L for strategy_X within account_DU9085815?"
- Reporting goal #6 explicitly requires this granularity
- Allocations file has `strategy_id`, but trades don't link to it

**Recommendation:** Add `strategy_id` to trades.json and orders.json

---

#### Gap 2: No Benchmark Reference Data
**Current state:** You want "relative performance to S&P 500" but have no SPY data captured

**Missing data points:**
- Daily SPY close price (for calculating drawdown vs benchmark)
- Daily SPY returns
- Daily NAV snapshots for your accounts (needed to calculate daily returns)

**Capture options:**
1. **Option A (Simple):** Pull daily SPY close via API at EOD
   - Yahoo Finance API
   - Alpha Vantage
   - Twelve Data
   - Store in: `/portfolio/benchmarks/spy.daily.YYYYMMDD.json`

2. **Option B (Comprehensive):** Also capture account NAV daily
   - NAV = (Cash + Current Market Value of Positions)
   - Timestamp this at EOD with each strategy
   - Enables Sharpe ratio, max drawdown, daily returns

3. **Option C (Plus Daily Position Values):** 
   - For intraday reporting accuracy, capture position market values throughout day
   - Especially important for mark-to-market on unrealized P&L

---

#### Gap 3: Incomplete Commission/Fee Tracking
**Current state:** 
```json
"commission": null,
"fees": 0.0
```

**Missing breakdown:**
- Broker commissions (per trade or % of notional)
- Exchange fees
- Clearing fees
- Regulatory fees
- Any other cost structure

**Impact on reporting:**
- P&L metrics could be off if commissions aren't accurate
- Can't report "cost per round-trip trade"
- Can't optimize for commission drag

**Recommendation:** Even if fees are zero, explicitly capture the fee structure in metadata

---

#### Gap 4: No Position Entry/Exit Metadata
**Current state:** You track `prev_position` and `current_position` but lose history

**Missing:**
- Entry/exit prices for each position layer
- Hold duration in days
- Whether exit was profit/loss (to calculate win% and avg win/loss)
- Reason for exit (if available: stop loss, target hit, rebalance, etc.)

**Current workaround:** Reconstruct from position_detail, but this gets messy with multiple buys before a sell

---

#### Gap 5: Risk Metrics Not Captured
**Needed for your reporting goals:**
- Daily account NAV (for Sharpe ratio calculation)
- Daily strategy-level NAV
- High water mark (for drawdown calculation)
- Realized volatility (daily returns std dev)

**These must be calculated or captured daily.**

---

## SECTION 2: DATABASE VS. FLAT FILES

### Current Approach: Flat Files
**Pros:**
- Simple, version-controllable, audit trail built-in
- No database infrastructure to manage
- Easy to debug and inspect
- Works well for <1TB data

**Cons:**
- Complex to query across date ranges
- Slow aggregations (especially account-level P&L over 3 months)
- Hard to handle real-time queries
- Position reconciliation requires parsing multiple files
- No built-in constraints or validation

### When to Migrate to Database

**Migrate NOW if:**
- You have >500 trades/day
- You need real-time dashboards
- You're running >10 concurrent strategies
- Query latency matters (report generation >5 seconds)

**Migrate LATER if:**
- Current volume is <100 trades/day
- EOD batch processing is sufficient
- You're still optimizing the system

**HYBRID APPROACH (Recommended):**
Keep JSON files as source of truth (audit trail), but also write to database incrementally.
```
JSON Files (Raw Data) → ETL/Validator → Time-Series Database
     (Archive)                          (Fast Queries)
```

### Recommended Database Schema (if you migrate)

```sql
-- Core tables
CREATE TABLE trades (
  trade_id VARCHAR PRIMARY KEY,
  order_id VARCHAR,
  strategy_id VARCHAR NOT NULL,  -- ← Add this
  account_id VARCHAR NOT NULL,
  symbol VARCHAR NOT NULL,
  side ENUM('BUY', 'SELL'),
  units DECIMAL(15,8),
  execution_price DECIMAL(15,8),
  execution_timestamp TIMESTAMP,
  commission DECIMAL(15,8),
  realized_pnl DECIMAL(15,8),
  INDEX (account_id, strategy_id, execution_timestamp)
);

CREATE TABLE daily_pnl (
  date DATE,
  account_id VARCHAR,
  strategy_id VARCHAR,
  realized_pnl DECIMAL(15,8),
  unrealized_pnl DECIMAL(15,8),
  nav DECIMAL(15,8),
  cash DECIMAL(15,8),
  PRIMARY KEY (date, account_id, strategy_id)
);

CREATE TABLE daily_nav (
  date DATE,
  account_id VARCHAR,
  strategy_id VARCHAR,
  nav DECIMAL(15,8),
  daily_return DECIMAL(15,8),
  high_water_mark DECIMAL(15,8),
  PRIMARY KEY (date, account_id, strategy_id)
);

CREATE TABLE benchmarks (
  date DATE,
  symbol VARCHAR(10),
  close_price DECIMAL(15,8),
  daily_return DECIMAL(15,8),
  PRIMARY KEY (date, symbol)
);
```

**For now:** Start with lightweight time-series DB like **InfluxDB** or **TimescaleDB** (PostgreSQL extension) instead of traditional SQL. They're optimized for financial time-series.

---

## SECTION 3: ADDITIONAL DATA TO CAPTURE

### Priority 1: Essential (For Your Stated Goals)
| Data Point | Frequency | Why | Format |
|------------|-----------|-----|--------|
| Daily NAV per account | Daily EOD | Sharpe ratio, drawdown, returns | JSON or CSV |
| Daily NAV per strategy | Daily EOD | Individual strategy performance | JSON or CSV |
| Daily SPY close | Daily EOD | S&P 500 comparison | JSON |
| High water mark | Daily EOD | Drawdown calculation | Calculated |
| Position cost basis | Per trade | Win/loss metrics | JSON (add to trades) |

### Priority 2: Important (For Professional Reporting)
| Data Point | Frequency | Why | Format |
|------------|-----------|-----|--------|
| Position entry timestamp | Per trade | Hold duration, trade win rate | JSON (add to positions) |
| Intraday position values | Hourly/EOD | For intraday performance curves | JSON |
| Trade reason/signal | Per trade | Trading activity narrative | JSON metadata |
| Correlations by symbol | Daily | Risk analysis | Calculated |
| Factor exposures | Daily | Alpha/beta breakdown | JSON |

### Priority 3: Nice-to-Have (For Advanced Analysis)
| Data Point | Frequency | Why | Format |
|------------|-----------|-----|--------|
| Slippage per trade | Per trade | Execution quality metrics | JSON (add to trades) |
| Sector/industry exposure | Daily | Portfolio heat map | Calculated from holdings |
| VIX snapshot | Daily | Vol regimes | JSON |
| Entry/exit reason codes | Per trade | ML categorization | JSON enum |
| Whale trades (>$X notional) | Per trade | Risk dashboard | Calculated |

---

## SECTION 4: RECOMMENDED ARCHITECTURE

### Option A: Lightweight (Recommended for <$100M AUM)
```
Daily Flow:
1. Trading engine outputs JSON files (current state ✓)
2. Python ETL script (nightly cron):
   - Reads JSON files
   - Calculates missing metrics (NAV, daily returns, drawdown)
   - Validates data integrity
   - Writes to SQLite or DuckDB (single-file database)
3. Reporting engine:
   - Queries database
   - Generates HTML reports (Jinja2 templates)
   - Charts via Plotly or Chart.js
   - Exports to S3/web server

Storage:
└── portfolio/
    ├── raw/              (JSON files - source of truth)
    ├── processed/        (SQLite DB - query layer)
    └── reports/          (HTML output)
```

### Option B: Scalable (For >$100M AUM or High Frequency)
```
Daily Flow:
1. Trading engine outputs JSON (current state ✓)
2. Kafka/message queue (optional)
3. Python ETL → TimescaleDB/PostgreSQL
4. Reporting engine (same as Option A)
5. Real-time dashboard (Grafana or custom React)

Advantages:
- Stream processing for real-time data
- Better horizontal scaling
- More sophisticated alerting
- Live dashboards possible
```

### Option C: Enterprise (For Regulated Environment)
```
Add:
- Event sourcing (immutable audit trail)
- Kafka with retention
- Data warehouse (Snowflake/BigQuery)
- BI layer (Tableau/Looker)
- Compliance tracking
```

---

## SECTION 5: IMPLEMENTING PROFESSIONAL REPORTING

### Reporting Module Architecture

```
ReportGenerator
├── Input: config.json (date range, account, strategy filters)
├── Data Layer:
│   ├── Query trades for period
│   ├── Calculate metrics
│   ├── Fetch benchmarks
│   └── Aggregate by dimension
├── Calculations:
│   ├── Win rate: (# winning trades) / (# total trades)
│   ├── Sharpe: (avg daily return) / (std dev daily returns)
│   ├── Max drawdown: max(high water mark - NAV) / high water mark
│   ├── CAGR: (Ending NAV / Starting NAV)^(1/years) - 1
│   ├── Sortino: (avg daily return) / (std dev negative returns only)
│   └── Information ratio: (strategy return - benchmark return) / tracking error
├── Rendering:
│   ├── Jinja2 templates → HTML
│   ├── Plotly → embedded charts
│   └── Tables → formatted DataFrames
└── Output: report.html (self-contained, portable)
```

### CLI Example
```bash
python generate_report.py \
  --config report_config.json \
  --output reports/

# report_config.json:
{
  "start_date": "2026-01-01",
  "end_date": "2026-03-29",
  "reporting_level": "strategy",  # or "account"
  "account_id": "DU9085815",
  "strategy_id": "aim_test",
  "benchmark": "SPY",
  "include_sections": [
    "trading_activity",
    "performance_metrics",
    "risk_analysis",
    "benchmark_comparison",
    "drawdown_analysis"
  ],
  "theme": "professional",
  "format": "html"
}
```

---

## SECTION 6: KEY QUESTIONS FOR YOU

Before you implement, clarify:

1. **Strategy Attribution:**
   - Does each trade belong to exactly ONE strategy?
   - Or can a single strategy trade multiple symbols simultaneously?
   - How do you allocate shared costs (e.g., exchange fees) across strategies?

2. **Account Hierarchy:**
   - Is there a parent-child relationship? (e.g., parent account = "Client XYZ", child accounts = separate subs?)
   - Or are all account_ids peers?
   - For reporting: should we show "all accounts for client XYZ" aggregated?

3. **Performance Attribution:**
   - Do you want to decompose returns into:
     - Strategy alpha (skill)
     - Market beta (exposure)
     - Timing (when capital was deployed)
   - Or just raw P&L?

4. **Real-Time vs. Batch:**
   - Do clients need intraday reports?
   - Or is EOD sufficient?
   - Dashboard vs. email report?

5. **Regulatory/Audit Trail:**
   - Do you need to comply with regulations (SEC, FINRA, etc.)?
   - Do P&L numbers need to be certified/audited?
   - How long must you retain raw data?

6. **Cost Center:**
   - Should reports include a "cost of capital" or hurdle rate?
   - Fee sharing model between accounts/strategies?

---

## SECTION 7: QUICK START IMPLEMENTATION PLAN

### Week 1: Data Enhancement
- [ ] Add `strategy_id` to trades.json and orders.json
- [ ] Set up daily benchmark capture (SPY EOD close)
- [ ] Calculate and log daily NAV per account/strategy
- [ ] Add commission/fee details to metadata

### Week 2-3: ETL Pipeline
- [ ] Build Python ETL script (parquet or SQLite output)
- [ ] Data validation layer (reconcile JSON against CSV)
- [ ] Cron job for daily processing
- [ ] Test with 3 months of historical data

### Week 4-5: Reporting Engine
- [ ] Build metric calculation library
- [ ] Create Jinja2 HTML templates
- [ ] Implement CLI interface with config file support
- [ ] Test report generation for various date ranges

### Week 6: Professional Polish
- [ ] CSS styling (professional theme, client branding)
- [ ] Chart interactivity (Plotly hover, zoom)
- [ ] Mobile-responsive design
- [ ] Export to PDF option
- [ ] Client demo/feedback

---

## FINAL RECOMMENDATIONS

| Question | Answer |
|----------|--------|
| **Use database?** | Not yet. Stay with files until you hit >$100M AUM or >1000 trades/day. Then migrate to PostgreSQL + TimescaleDB. |
| **What to capture first?** | Daily NAV, benchmark data, strategy_id in trades. These three unlock 80% of your reporting goals. |
| **Architecture?** | Python ETL → DuckDB/SQLite → Jinja2 HTML rendering. Add interactive charts via Plotly. Scale up later if needed. |
| **Cost?** | ~$0 if self-hosted. ~$100-500/month if on cloud with modest infrastructure. |
| **Time to MVP?** | 4-6 weeks for professional-grade reporting covering all your stated goals. |
| **Marketing use?** | Yes. HTML reports are perfect for client dashboards and marketing materials—embed them in a web app. |

---

## Appendix: Metrics Formulas

### Win Rate
```
Win % = (# trades with positive realized_pnl) / (# total closed trades) × 100
```

### Sharpe Ratio
```
Daily Return = (NAV_today - NAV_yesterday) / NAV_yesterday
Sharpe = (mean daily return) / (std dev daily return) × √252
(252 trading days/year)
```

### Max Drawdown
```
Running High Water Mark = max(NAV) to date
Drawdown at time T = (HWM - NAV_T) / HWM × 100
Max Drawdown = max(all Drawdowns)
```

### CAGR (Compound Annual Growth Rate)
```
CAGR = (Ending NAV / Starting NAV)^(1 / years) - 1
```

### Information Ratio
```
Strategy Excess Return = Strategy Return - Benchmark Return
Tracking Error = std dev(Strategy Return - Benchmark Return)
Information Ratio = (Excess Return) / (Tracking Error)
```

---

**Next Step:** Answer the questions in Section 6, and I can provide:
1. SQL schema tailored to your account hierarchy
2. Python ETL script template
3. Jinja2 report template with professional styling
4. CLI argument parser for your config system

