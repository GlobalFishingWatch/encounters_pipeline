#!/bin/bash
set -e

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets

SQL=${ASSETS}/encounter_events.sql.j2
SCHEMA=${ASSETS}/events.schema.json

#bq mk -f \
#  --table \
#  --time_partitioning_field=timestamp \
#  world-fishing-827:scratch_paul_ttl_100.encoutner_events_test2 \
#  ${SCHEMA}

#cat ${SQL} | bq query \
#  --max_rows=0 --allow_large_results --replace \
#  --destination_table=world-fishing-827:scratch_paul_ttl_100.encoutner_events_test2 \
#  --time_partitioning_field=timestamp
