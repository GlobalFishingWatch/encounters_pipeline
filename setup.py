#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

setup(
    name='encounters',
    version=__import__('pipeline').__version__,
    packages=find_packages(exclude=['test*.*', 'tests'])
)

