clean:
	for x in .hypothesis __pycache__ .mypy_cache .pytest_cache outputs *.log; do find . -name $$x -exec rm -rf {} +; done
	@echo "Clean done!"

lint:
	flake8 --max-line-length=120 --import-order-style=edited --application-import-names=ivideon viaduct/ tests/
	@echo "Linting done!"

test:
	pytest .
	@echo "Tests done!"