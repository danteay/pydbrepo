# Analyze the given Python modules and compute Cyclomatic Complexity
cc_json = "$(shell poetry run radon cc --min B src --json)"
# Analyze the given Python modules and compute the Maintainability Index
mi_json = "$(shell poetry run radon mi --min B src --json)"

files = `find ./pydbrepo ./tests -name "*.py"`
files_tests = `find ./tests -name "*.py"`

help: ## Display this help screen.
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

lint: ## Run Pylint checks on the project.
	@poetry run pylint $(files)

fmt: ## Format all project files.
	@poetry run isort pydbrepo tests
	@poetry run yapf pydbrepo tests -r -i -vv

test: ## Run unit testings.
	@poetry run mamba $(files_tests) --format documentation --enable-coverage

cover: test ## Execute coverage analysis for executed tests
	@poetry run coverage report --omit '*virtualenvs*','*tests*'

cover_html: test ## Execute coverage analysis for executed test and show it as HTML
	@poetry run coverage html --omit '*virtualenvs*','*tests*' && firefox htmlcov/index.html

cover_xml: test ## Execute coverage analysis for executed test and create an XML report
	@poetry run coverage xml --omit '*virtualenvs*','*tests*'

docs: ## Compile Sphinx documentation
	@poetry run sphinx-apidoc -f -o ./docs ./pydbrepo

docs_html: docs ## Create HTML docs from Sphinx
	@poetry run make -C docs html

install: ## Install project dependencies.
	@poetry install

venv: ## Create new virtual environment. Run `source venv/bin/activate` after this command to enable it.
	@poetry shell

complexity: ## Run radon complexity checks for maintainability status.
	@echo "Complexity check..."

ifneq ($(cc_json), "{}")
	@echo
	@echo "Complexity issues"
	@echo "-----------------"
	@echo $(cc_json)
endif

ifneq ($(mi_json), "{}")
	@echo
	@echo "Maintainability issues"
	@echo "----------------------"
	@echo $(mi_json)
endif

ifneq ($(cc_json), "{}")
	@echo
	exit 1
else
ifneq ($(mi_json), "{}")
	@echo
	exit 1
endif
endif

	@echo "OK"
.PHONY: complexity