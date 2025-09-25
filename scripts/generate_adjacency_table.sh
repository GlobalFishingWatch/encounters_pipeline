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

# heuristic for machine type and number of workers: 2gb per vcpu, n2d by default
# figure out how many CPUs to use based on date range and ssvid filter
# the compute-score is based on `ssvid_filter` X `months of data` X `MAXIMUM year of date range`
# e.g. 6 months of data up till 2024 without ssvid filter:
# 6 x 13 x 10 based on 6 months, 1 + (2024-2012), and 10 for "no ssvid filter"
# ssvid_filter_score is either 1 or 10 depending on whether `ssvid` or not
if [[ "$ssvid_filter" == "ssvid" ]]; then
  ssvid_filter_score=10
else
  ssvid_filter_score=1
fi

month_score=$(( ( $(date -d "$end_date" +%s) - $(date -d "$start_date" +%s) ) / 2592000 + 1 ))

# maximum year score is the year of the end_date - 2011
maximum_year_score=$(( $(date -d "$end_date" +%Y) - 2011 ))

total_score=$(( ssvid_filter_score * month_score * maximum_year_score ))

# the absolute maximum score is ~1560 for 2025 -> divide score by 8 to get about 200 workers
num_workers=$(( total_score / 8 ))

if [[ "$num_workers" -lt 4 ]]; then
  num_workers=4
fi

max_num_workers=$(( num_workers * 16 ))

if [[ "$max_num_workers" -gt 100 ]]; then
  max_num_workers=100
fi

# print all parameters
echo "source_table: $source_table"
echo "adjacency_table: $adjacency_table"
echo "start_date: $start_date"
echo "end_date: $end_date"
echo "ssvid_filter: $ssvid_filter"

docker compose build && docker compose run create_raw_encounters \
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
    --num_workers=$num_workers \
    --max_num_workers=$max_num_workers \
    --disk_size_gb=50 \
    --setup_file=./setup.py \
    --requirements_file=requirements-worker.txt \
    --dataflow_service_options=enable_lineage=true \
    --profile_cpu \
    --profile_location gs://scratch_chris/dataflow_profiles/$current_ts \
    --job_name encounters-adjacency-intermediary-table-christian-$current_ts


