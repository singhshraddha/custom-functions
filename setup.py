#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='custom',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        "iotfunctions @ git+https://github.com/ibm-watson-iot/functions.git@dev#egg=iotfunctions"
        "pyrenn==0.1",
    ],
    extras_require = {
        'kafka':  ['confluent-kafka==0.11.5']
    }
)
