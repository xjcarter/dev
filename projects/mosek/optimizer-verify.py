# 1. Test the imports
try:
    import pypfopt
    from pypfopt import EfficientFrontier, risk_models, expected_returns
    import mosek
    print("✓ All libraries imported successfully.")
except ImportError as e:
    print(f"✗ Import failed: {e}")
    exit()

# 2. Run a quick optimization example with sample data
from pypfopt import EfficientFrontier, risk_models, expected_returns
import pandas as pd

# Create a small sample dataset for testing
# This data simulates price changes for 3 assets over 10 days
data = pd.DataFrame({
    'Asset 1': [100, 101, 102, 103, 104, 105, 104, 103, 102, 101],
    'Asset 2': [100, 99, 98, 97, 98, 99, 100, 101, 102, 103],
    'Asset 3': [100, 102, 104, 106, 105, 104, 103, 102, 101, 100],
})

# Calculate expected returns and the risk (covariance) matrix
mu = expected_returns.mean_historical_return(data)
S = risk_models.sample_cov(data)

# Optimize for the portfolio that maximizes the Sharpe ratio
ef = EfficientFrontier(mu, S)
weights = ef.max_sharpe()

# Display the results
print("\n--- Optimization Successful ---")
print("Optimal Weights:", weights)
print("Portfolio Performance:", ef.portfolio_performance())