#!/usr/bin/env bash

PIPELINE='encounters'
PIPELINE_VERSION=$(python -c "import pkg_resources; print pkg_resources.get_distribution('${PIPELINE}').version")