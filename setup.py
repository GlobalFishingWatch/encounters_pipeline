from pipe_tools.beam.requirements import requirements as DATAFLOW_PINNED_DEPENDENCIES
from setuptools import setup, find_packages

import codecs


PROJECT_NAME = 'encounters'
PROJECT_VERSION = '1.0.0'
PROJECT_DESCRIPTION = 'Apache Beam pipeline for computing vessel encounters.'
DEPENDENCIES = [
    "ujson",
    "statistics",
    "more_itertools",
    "s2sphere",
    "pipe-tools==3.1.0",
    "jinja2-cli",
    "pyarrow<0.14.0",
    "six>=1.13"
]

with codecs.open('requirements.txt', encoding='utf-8') as f:
    DEPENDENCY_LINKS=[line for line in f]

setup(
    name=PROJECT_NAME,
    version=PROJECT_VERSION,
    description=PROJECT_DESCRIPTION,
    author="Global Fishing Watch",
    author_email="info@globalfishingwatch.org",
    license="Apache 2",
    packages=find_packages(),
    include_package_data=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    dependency_links=DEPENDENCY_LINKS
)
