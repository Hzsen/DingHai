from quant_agent.graph.router import route_query

def test_routes_structured_regime_query_to_sql(): assert route_query("Which factor performed best during high-volatility regimes?")[1] == "sql_only"
def test_routes_causal_query_to_hybrid():
    query_type, route = route_query("What caused momentum to underperform in March 2020?"); assert query_type == "hybrid_explanation_query"; assert route == "hybrid_sql_retrieval"
def test_routes_factor_definition(): assert route_query("Explain how the liquidity anomaly factor is calculated.")[1] == "factor_definition"
