#!/bin/bash
current_ts=$(date +%Y%m%d%H%M)

# read params as CLI arguments, set default args as stated below only IF not provided via cli
while [[ $# -gt 0 ]]; do
  case $1 in
    --source_table)
      source_table="$2"
      shift 2
      ;;
    --adjacency_table)
      adjacency_table="$2"
      shift 2
      ;;
    --start_date)
      start_date="$2"
      shift 2
      ;;
    --end_date)
      end_date="$2"
      shift 2
      ;;
    --ssvid_filter)
      ssvid_filter="$2"
      shift 2
      ;;
    --runner)
      runner="$2"
      shift 2
      ;;
    *)
      echo "Unknown parameter: $1"
      exit 1
      ;;
  esac
done


# set default args
source_table=${source_table:-world-fishing-827.pipe_ais_v3_internal.messages_positions}
adjacency_table=${adjacency_table:-world-fishing-827.scratch_christian_homberg_ttl120d._encounters_dev_adjacency_$current_ts}
start_date=${start_date:-'2012-01-01'}
end_date=${end_date:-'2012-01-02'}
ssvid_filter=${ssvid_filter:-"ssvid"}
runner=${runner:-"DirectRunner"}

# print all parameters
echo "source_table: $source_table"
echo "adjacency_table: $adjacency_table"
echo "start_date: $start_date"
echo "end_date: $end_date"
echo "ssvid_filter: $ssvid_filter"

docker compose run create_raw_encounters \
    --source_table $source_table \
    --adjacency_table $adjacency_table \
    --start_date "$start_date" \
    --end_date "$end_date" \
    --max_encounter_dist_km 0.5 \
    --min_encounter_time_minutes 120 \
    --ssvid_filter "$ssvid_filter" \
    --max_encounter_dist_km 0.5 \
    --temp_location=gs://scratch_chris/dataflow_temp \
    --staging_location=gs://scratch_chris/dataflow_staging \
    --project world-fishing-827 \
    --runner $runner \
    --region us-central1 \
    --max_num_workers=100 \
    --disk_size_gb=50 \
    --setup_file=./setup.py \
    --requirements_file=requirements-worker.txt \
    --job_name encounters-adjacency-intermediary-table-christian-$current_ts


