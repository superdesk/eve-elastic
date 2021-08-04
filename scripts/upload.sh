#!/usr/bin/env sh

rm -rf dist build
python setup.py sdist bdist_wheel
twine check dist/* && twine upload dist/*
