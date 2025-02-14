#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

setup(
    name="encounters",
    version="4.1.3",
    packages=find_packages(exclude=["test*.*", "tests"])
)
