.PHONY: install build ask eval test clean

install:
	python3 -m pip install -e ".[dev]"

build:
	python3 -m quant_agent.cli.build_indexes --build-db --build-bm25 --build-vector

ask:
	python3 -m quant_agent.cli.ask "Which factor performed best during high-volatility regimes?"

eval:
	python3 -m quant_agent.cli.run_eval --routing --retrieval --grounding

test:
	pytest

clean:
	rm -rf data/processed/*.db data/processed/*.pkl data/processed/vector_index .pytest_cache
	find src tests -type d -name __pycache__ -prune -exec rm -rf {} +
