# Minimal makefile for Sphinx documentation
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
.PHONY: help
help:
	@printf "\033[36m%-30s\033[0m %-50s %s\n" "[Sub command]" "[Description]" "[Example]"
	@grep -E '^[/a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | perl -pe 's%^([/a-zA-Z_-]+):.*?(##)%$$1 $$2%' | awk -F " *?## *?" '{printf "\033[36m%-30s\033[0m %-50s %s\n", $$1, $$2, $$3}'

.PHONY: clean
clean: ## clean the build directory ## make clean
	-rm -rf $(BUILDDIR)/*

.PHONY: html
html: clean ## build docs as html ## make html
	sphinx-build $(SOURCEDIR) ./$(BUILDDIR)
