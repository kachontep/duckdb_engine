#!/bin/bash

if ! grep -E 'version = .*\+thdatalabs.*' pyproject.toml >/dev/null ; then
    sed -i 's/^version = "\(.*\)"/version = "\1+thdatalabs"/' pyproject.toml
fi

if ! grep -E '__version__ = ".*\+thdatalabs\."' duckdb_engine/__init__.py >/dev/null ; then
    sed -i 's/^__version__ = \"\(.*\)"/__version__ = "\1+thdatalbas"/' duckdb_engine/__init__.py
fi
