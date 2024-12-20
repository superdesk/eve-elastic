#!/usr/bin/env sh

rm -rf dist build
python -m build .
twine check dist/* && twine upload --repository eve-elastic dist/*
