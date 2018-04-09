#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

display_usage() {
	echo -e "\nUsage:\npublish_events SOURCE_TABLE DEST_TABLE \n"
	}


if [[ $# -ne 2  ]]
then
    display_usage
    exit 1
fi

SOURCE_TABLE=$1
DEST_TABLE=$2

SQL=${ASSETS}/encounter_events.sql.j2
SOURCE_TABLE=${SOURCE_TABLE//:/.}

TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SOURCE_TABLE}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )

echo "Publishing encounters events to ${DEST_TABLE}..."
echo "${TABLE_DESC}"

jinja2 ${SQL} -D source=${SOURCE_TABLE} \
     | bq -q query --max_rows=0 --allow_large_results --replace \
      --destination_table ${DEST_TABLE}

echo "Updating table description ${DEST_TABLE}"

bq update --description "${TABLE_DESC}" ${DEST_TABLE}

echo "  ${DEST_TABLE} Done."


