from pipe_tools.beam.requirements import requirements as DATAFLOW_PINNED_DEPENDENCIES

from setuptools import setup, find_packages

import codecs


package = __import__('pipeline')

DEPENDENCIES = [
    "clikit",
    "ujson==1.35",
    "statistics",
    "more_itertools",
    "s2sphere",
    "pipe-tools==3.2.1",
    "jinja2-cli",
    "six>=1.13",
    "cython"
]

with codecs.open('requirements.txt', encoding='utf-8') as f:
    DEPENDENCY_LINKS=[line for line in f]

setup(
    name='encounters',
    version=package.__version__,
    description=package.__doc__.strip(),
    author=package.__author__,
    author_email=package.__email__,
    license=package.__license__,
    packages=find_packages(),
    include_package_data=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    dependency_links=DEPENDENCY_LINKS
)
