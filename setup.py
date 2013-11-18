#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='Eve-Elastic',
    version='0.1-dev',
    description='Elasticsearch data layer for eve',
    author='Petr Jasek',
    author_email='petr.jasek@sourcefabric.org',
    url='https://github.com/petrjasek/eve-elastic',
    license=open('LICENSE').read(),
    platforms=['any'],
    packages=find_packages(),
    test_suite='elastic.tests',
    install_requires=[
        'pyelasticsearch==0.6.1',
        'arrow==0.4.2',
        'Eve==0.2-dev',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
)
