.PHONY: help test test-cov lint format type-check build clean install-dev

help:
	@echo "Available commands:"
	@echo "  make install-dev  - Install development dependencies"
	@echo "  make test         - Run tests"
	@echo "  make test-cov     - Run tests with coverage report"
	@echo "  make lint         - Run linter (ruff)"
	@echo "  make format       - Format code (black)"
	@echo "  make type-check   - Run type checker (mypy)"
	@echo "  make build        - Build package"
	@echo "  make clean        - Clean build artifacts"

install-dev:
	pip install -r requirements-dev.txt

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src/mongo_pipebuilder --cov-report=term-missing --cov-report=html:tests/htmlcov --cov-report=json:coverage.json --cov-report=xml:coverage.xml
	@echo ""
	@echo "Coverage reports generated:"
	@echo "  - Terminal: displayed above"
	@echo "  - HTML: tests/htmlcov/index.html"
	@echo "  - JSON: coverage.json"
	@echo "  - XML: coverage.xml"

lint:
	ruff check src/ tests/

format:
	black src/ tests/ examples/

type-check:
	mypy src/

build:
	python -m build

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf tests/htmlcov/
	rm -f coverage.json coverage.xml .coverage
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

