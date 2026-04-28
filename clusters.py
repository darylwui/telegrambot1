"""Cluster definitions for concentration analysis.

Tickers not listed default to "Other".
Adjust thresholds at the bottom to change alert behaviour.
"""

CLUSTERS = {
    "AI/Semis": ["NVDA", "TSM", "AVGO", "CRWV"],
    "Hyperscalers": ["GOOG", "GOOGL", "AMZN", "MSFT", "META"],
    "SaaS": ["CRWD", "NOW", "SNOW", "TEAM", "ADBE"],
    "Crypto-adj": ["COIN", "BLSH", "BTGO"],
    "Consumer/Other": ["SE", "RDDT", "NFLX"],
}

# Alert thresholds (% of total portfolio market value)
SINGLE_NAME_THRESHOLD = 25.0
CLUSTER_THRESHOLD = 40.0


def classify(ticker: str) -> str:
    """Return the cluster name for a ticker; 'Other' if unmapped."""
    for name, tickers in CLUSTERS.items():
        if ticker in tickers:
            return name
    return "Other"
