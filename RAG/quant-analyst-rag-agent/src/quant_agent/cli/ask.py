from __future__ import annotations
import argparse
from quant_agent.graph.workflow import ask

def main() -> None:
    parser = argparse.ArgumentParser(description="Ask the Quant Analyst RAG Agent a question."); parser.add_argument("query"); args = parser.parse_args(); result = ask(args.query); print(result["answer"])
    if result.get("errors"):
        print("\nWarnings:")
        for error in result["errors"]: print(f"- {error}")
if __name__ == "__main__": main()
