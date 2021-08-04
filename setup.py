#!/usr/bin/env python

from setuptools import setup

with open("README.rst") as f:
    readme = f.read()

with open("CHANGELOG.rst") as f:
    changelog = f.read()

setup(
    name="Eve-Elastic",
    version="7.1.4",
    description="Elasticsearch data layer for eve rest framework",
    long_description=readme + "\n\n" + changelog,
    license="GPLv3+",
    author="Petr Jasek",
    author_email="petr.jasek@sourcefabric.org",
    url="https://github.com/petrjasek/eve-elastic",
    packages=["eve_elastic"],
    test_suite="test.test_elastic",
    tests_require=["nose", "flake8"],
    install_requires=[
        "arrow>=0.4.2",
        "ciso8601>=1.0.2,<2",
        "pytz>=2015.4",
        "elasticsearch>=7.0.0,<7.14.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
    ],
)
