PYTHON ?= python
PYTHONPATH ?= src

.PHONY: pipeline collect-ad collect-prices promote-ad promote-prices replay-ad replay-prices replay-summary-ad replay-summary-prices test clean

pipeline:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence pipeline

collect-ad:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence collect-ad

collect-prices:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence collect-prices

promote-ad:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence promote-ad

promote-prices:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence promote-prices

replay-ad:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence replay-ad

replay-prices:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence replay-prices

replay-summary-ad:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence replay-summary-ad

replay-summary-prices:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m rolex_market_intelligence replay-summary-prices

test:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m unittest discover -s tests -v

clean:
	$(PYTHON) -c "from pathlib import Path; [p.unlink() for p in Path('data/processed').glob('*') if p.is_file()]; [p.unlink() for p in Path('data/quality').glob('*') if p.is_file()]; [p.unlink() for p in Path('reports/figures').glob('*') if p.is_file()]"
