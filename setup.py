#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


# Work around setuptools bug
# http://article.gmane.org/gmane.comp.python.peak/2509
import multiprocessing


PACKAGE_NAME = 'commoncrawl_cdx'
VERSION = '0.0.1'


settings = dict(
    name=PACKAGE_NAME,
    version=VERSION,
    description='Access to an index of Common Crawl URLs.',
    long_description='Access to an index of Common Crawl URLs',
    author='John Wiseman',
    author_email='jjwiseman@gmail.com',
    url='https://github.com/wiseman/commoncrawl_cdx',
    packages=['commoncrawl_cdx', 'commoncrawl_cdx.cli'],
    test_suite='nose.collector',
    install_requires=[
        'boto==2.28.0',
        'python-gflags',
        'requests==2.6.2'
    ],
    tests_require=[
        'nose'
    ],
    license='MIT',
    classifiers=(
        # 'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ),
    entry_points={
        'console_scripts': [
            'cci_lookup = commoncrawl_cdx.cli.cci_lookup:cli_main',
            'cci_fetch = commoncrawl_cdx.cli.cci_fetch:cli_main'
        ],
    },
)


setup(**settings)
