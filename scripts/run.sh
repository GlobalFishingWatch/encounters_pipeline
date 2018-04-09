#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
	echo "Available Commands"
	echo "  create_raw_encounters       create raw encounters dataflow"
	echo "  merge_encounters            merge encounters dataflow"
	echo "  publish_events              publish encounter events"
}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi


case $1 in

  create_raw_encounters)

    python -m pipeline.create_raw_encounters "${@:2}"
    ;;

  merge_encounters)

    python -m pipeline.merge_encounters "${@:2}"
    ;;

  publish_events)
    ${THIS_SCRIPT_DIR}/publish_events.sh "${@:2}"
    ;;


  *)
    display_usage
    exit 0
    ;;
esac
