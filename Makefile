# Convenience targets. `make help` lists them.
.DEFAULT_GOAL := help
PY ?= python

.PHONY: help install smoke lint type test check dataset bars coverage validate status bench ingest compact clean

help:  ## show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## editable install with dev + obs extras
	$(PY) -m pip install -e ".[dev,obs]"

smoke: install  ## 60-second smoke test: install, run the test suite, print the project overview
	$(PY) -m pytest -q
	$(PY) -m scripts.status

lint:  ## ruff
	ruff check .

type:  ## mypy
	mypy cryptodata

test:  ## pytest
	pytest -q

check: lint test  ## lint + test (what CI gates on)

dataset:  ## build the sample dataset end-to-end (real exchange data)
	$(PY) -m scripts.build_dataset

bars:  ## (re)build per-venue 1s bars + the consolidated tape for every raw-trades slice
	$(PY) -m scripts.build_bars_1s --all-present

coverage:  ## rebuild the coverage matrix -> data/meta/coverage.{json,md}
	$(PY) -m scripts.coverage_report

validate:  ## run the data-quality scorecards (non-zero exit on a failing slice)
	$(PY) -m scripts.validate

status:  ## print the ops dashboard
	$(PY) -m scripts.status

bench:  ## run benchmarks -> data/meta/benchmarks.json + docs/BENCHMARKS.md
	$(PY) -m scripts.bench

ingest:  ## start live ingest (runs forever; Ctrl-C to stop)
	$(PY) -m scripts.run_ingest

compact:  ## nightly part-file compaction
	$(PY) -m scripts.compact

clean:  ## remove caches (NOT data/)
	rm -rf .pytest_cache .ruff_cache .mypy_cache **/__pycache__ *.egg-info build dist
