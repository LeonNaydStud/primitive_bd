.PHONY: install run build publish package-install help

help:
	@echo "Available commands:"
	@echo "  make install        - Install dependencies using Poetry"
	@echo "  make run            - Run the application via module"
	@echo "  make build          - Build the package"
	@echo "  make publish        - Dry-run publish to PyPI"
	@echo "  make package-install - Install the built package"

install:
	poetry install

run:
	poetry run project

build:
	poetry build

publish:
	poetry publish --dry-run

package-install: build
	python3 -m pip install dist/*.whl