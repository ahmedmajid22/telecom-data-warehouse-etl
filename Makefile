# Automatically set the Python path to the current directory
export PYTHONPATH := $(shell pwd)

.PHONY: setup test lint run all

setup:
	pip install -r requirements.txt

test:
	pytest tests/

lint:
	# E501: line length (handled by Black)
	# W291: trailing whitespace
	# E226: missing whitespace around arithmetic operator (e.g., i+1)
	# W503: line break before binary operator
	flake8 src/ dags/ --ignore=E501,W291,E226,W503

run:
	python src/main.py

all: lint test