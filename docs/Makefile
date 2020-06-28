# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
# 	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	@echo " == simple make == "
	@echo "type 'make clean docstring html' to build documentation file"
	@echo ""
	@echo " == command references == "
	@echo "clean: clean build directory"
	@echo "html: build html file"

.PHONY: help Makefile


clean:
	-rm -rf $(BUILDDIR)/*

html:
	sphinx-build $(SOURCEDIR) ./$(BUILDDIR)
	@echo "finish"

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
# %: Makefile
# 	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
