.PHONY: setup clean

setup: requirements.txt
	pip install -r requirements.txt

clean:
	rm -rf src/__pycache__ tests/__pycache__ .pytest_cache