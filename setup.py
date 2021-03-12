#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='custom',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'iotfunctions',
        'pyrenn==0.1',
        'statsmodels==0.11.1'
    ],
    extras_require = {
        'kafka':  ['confluent-kafka==0.11.5']
    }
)
