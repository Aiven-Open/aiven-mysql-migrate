PYTHON ?= python3
PYTHON_SOURCE_DIRS = aiven_mysql_migrate/ test/
PACKAGE_REQUIRES = "python3-PyMySQL"

generated = aiven_mysql_migrate/version.py


all: $(generated)

aiven_mysql_migrate/version.py:
	echo "__version__ = \"$(shell git describe)\"" > $@

build-dep-fedora:
	sudo dnf -y install --best --allowerasing \
		python3-flake8 \
		python3-isort \
		python3-mypy \
		python3-pylint \
		python3-pytest \
		python3-yapf \
		rpm-build

flake8: $(generated)
	$(PYTHON) -m flake8 $(PYTHON_SOURCE_DIRS)

pylint: $(generated)
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

mypy: $(generated)
	$(PYTHON) -m mypy $(PYTHON_SOURCE_DIRS)

isort: $(generated)
	$(PYTHON) -m isort --recursive $(PYTHON_SOURCE_DIRS)

yapf: $(generated)
	$(PYTHON) -m yapf --parallel --recursive --in-place $(PYTHON_SOURCE_DIRS)

static-checks: flake8 pylint mypy

validate-style:
	$(eval CHANGES_BEFORE := $(shell mktemp))
	git diff > $(CHANGES_BEFORE)
	$(MAKE) isort yapf
	$(eval CHANGES_AFTER := $(shell mktemp))
	git diff > $(CHANGES_AFTER)
	diff $(CHANGES_BEFORE) $(CHANGES_AFTER)
	-rm $(CHANGES_BEFORE) $(CHANGES_AFTER)

.PHONY: test systest
test: $(generated)
	$(PYTHON) -m pytest -v test/unit/

.ONESHELL: systest
systest:
	docker compose -f docker-compose.test.yaml up -d --build && \
	docker compose -f docker-compose.test.yaml run test python -m pytest -v test/sys/; \
	code=$$? && \
	docker compose -f docker-compose.test.yaml down --rmi all --remove-orphans --volumes && \
	exit $$code

clean:
	$(RM) aiven_mysql_migrate/version.py

rpm:
	$(PYTHON) setup.py bdist_rpm --requires $(PACKAGE_REQUIRES) && rm -rf build/
