#!/usr/bin/env sh

rm -rf dist build
python -m pip build
twine check dist/* && twine upload dist/*
