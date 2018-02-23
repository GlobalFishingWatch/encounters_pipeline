# Encounters pipeline 

This repository contains the encounters pipeline, which finds vessel encounters
based on AIS messages.

# Running

## Dependencies

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

## Setup

The pipeline reads it's input from BigQuery, so you need to first authenticate
with your google cloud account inside the docker images. To do that, you need
to run this command and follow the instructions:

TODO: used global GCP volume now: add correct instructions for that.

```
docker-compose run gcloud auth application-default login
```

## Overview

The pipeline takes `start_date` and `end_date`. The pipeline pads `start_date`
by one day to warm up, reads the data from from `source_table` and computes
encounters over the specified window.
In incremental mode, `start_date` and `end_date` would be on the same date.  The results
of this encounter are *appended* to the specified `raw_sink` table. A second pipeline
is then run over this second table, merging encounters that are close in time into
one long encounter and *replacing* the table specified in `sink` with the merged results.

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs. Just run `docker-compose run pipeline --help` and follow the
instructions there.

### Examples:

In incremental mode, the form of the command is

        docker-compose run pipeline \
                --source_table SOURCE_TABLE \
                --start_date DATE \
                --end_date DATE \
                --max_encounter_dist_km DISTANCE \
                --min_encounter_time_minutes TIME \
                --raw_sink RAW_TABLE \
                --sink FINAL_TABLE \
                remote \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-pip \
                --max_num_workers 200

Note that raw_table needs to be persistent since it is appended to with each run.
Here is a concrete example:

        docker-compose run create_raw_encounters \
                --source_table pipeline_classify_p_p516_daily. \
                --start_date 2017-01-01 \
                --end_date 2017-01-01 \
                --max_encounter_dist_km 0.5 \
                --min_encounter_time_minutes 120 \
                --raw_sink world-fishing-827:machine_learning_dev_ttl_30d.raw_encounters_test \
                --sink world-fishing-827:machine_learning_dev_ttl_30d.encounters_test \
                remote \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-test \
                --max_num_workers 200



        docker-compose run create_raw_encounters \
                --source_table pipe_staging_a.position_messages_ \
                --start_date 2017-01-01 \
                --end_date 2017-12-31 \
                --max_encounter_dist_km 0.5 \
                --min_encounter_time_minutes 120 \
                --raw_table world-fishing-827:machine_learning_dev_ttl_30d.raw_encounters_uvi_05km_ \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-create-test \
                --max_num_workers 200 \
                --setup_file ./setup.py \
                --requirements_file requirements.txt \
                --runner DataflowRunner \
                --disk_size_gb 100


        docker-compose run merge_encounters \
                --raw_table world-fishing-827:machine_learning_dev_ttl_30d.raw_encounters_uvi_05km_ \
                --sink_table world-fishing-827:machine_learning_dev_ttl_30d.encounters_uvi_05km \
                --max_encounter_dist_km 0.5 \
                --min_encounter_time_minutes 120 \
                --start_date 2017-01-01 \
                --end_date 2017-12-31 \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-merge-test \
                --max_num_workers 200 \
                --setup_file ./setup.py \
                --requirements_file requirements.txt \
                --runner DataflowRunner \
                --disk_size_gb 100


It's also possible to specify multiple source tables. The tables can be optionally prefixed with `ID_PREFIX::`, which will
be prepended to ids from that source. For example:

        docker-compose run pipeline \
                --source_table ais::pipeline_classify_p_p516_daily. \
                --source_table peru_vms::pipeline_p_p588_peru.classify_ \
                --max_encounter_dist_km 0.5 \
                --min_encounter_time_minutes 120 \
                --start_date 2015-01-01 \
                --end_date 2015-01-01 \
                --raw_sink_table world-fishing-827:machine_learning_dev_ttl_30d.raw_mixed_encounters_test \
                --sink world-fishing-827:machine_learning_dev_ttl_30d.mixed_encounters_test \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name mixed-encounters-test \
                --max_num_workers 200


# License

Copyright 2017 Global Fishing Watch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
