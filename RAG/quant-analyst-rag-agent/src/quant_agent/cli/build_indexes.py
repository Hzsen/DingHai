from __future__ import annotations
import argparse
from quant_agent.config import Paths
from quant_agent.database.build_db import build_database
from quant_agent.retrieval.hybrid_retriever import build_retrievers

def main() -> None:
    parser = argparse.ArgumentParser(description="Build SQLite and retrieval indexes for the quant RAG agent."); parser.add_argument("--build-db", action="store_true"); parser.add_argument("--build-bm25", action="store_true"); parser.add_argument("--build-vector", action="store_true"); args = parser.parse_args(); paths = Paths(); paths.ensure_processed_dirs(); build_all = not (args.build_db or args.build_bm25 or args.build_vector)
    if args.build_db or build_all: print(f"Built SQLite database: {build_database(paths.db_path, paths.raw_data_dir)}")
    if args.build_bm25 or args.build_vector or build_all:
        build_retrievers(paths.docs_dir, paths.bm25_index_path, paths.vector_index_path)
        if args.build_bm25 or build_all: print(f"Built BM25 index: {paths.bm25_index_path}")
        if args.build_vector or build_all: print(f"Built vector index: {paths.vector_index_path}")
if __name__ == "__main__": main()
