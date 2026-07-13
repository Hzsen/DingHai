# Interview Notes

## One-Minute Summary

I built a LangGraph-based financial research agent that combines SQL tools, BM25 keyword search, vector retrieval, and answer verification to answer natural-language questions over market data, factor definitions, backtest results, and research notes.

## Technical Challenge

The main challenge was deciding when a question should be answered by structured data versus unstructured retrieval. Financial questions often require exact metrics, so pure vector search is not reliable.

## Why LangGraph

LangGraph gives explicit control over state, routing, tool execution, and verification. This makes it better than a single black-box RAG chain for multi-step analytical workflows.

## Why Hybrid Retrieval

BM25 helps with exact ticker, factor, date, and event matches. Vector retrieval helps with semantic explanations. Combining them improves recall and reduces irrelevant context.

## Hallucination Control

Numerical answers must come from SQL tools. Causal or explanatory answers must have retrieved evidence. If the system lacks evidence, it refuses or states limitations.

## Failure Cases

- Ambiguous query
- Missing factor name
- Incomplete data
- Poor retrieval match
- Conflicting documents
- Sample data limitation

## Possible Extensions

- Real market data ingestion
- Live factor computation
- More robust evaluation
- Portfolio optimizer
- Streamlit UI
- PostgreSQL migration
- Agent memory
- Observability and tracing
