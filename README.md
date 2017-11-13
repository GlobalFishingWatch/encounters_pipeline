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

```
docker-compose run gcloud auth application-default login
```

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs. Just run `docker-compose run pipeline --help` and follow the
instructions there.

### Examples:

        docker-compose run pipeline \
                --source_table pipeline_classify_p_p516_daily \
                --start_date 2017-01-01 \
                --end_date 2017-01-01 \
                --raw_sink world-fishing-827:machine_learning_dev_ttl_30d.raw_encounters_test \
                --sink world-fishing-827:machine_learning_dev_ttl_30d.encounters_test \
                remote \
                --project world-fishing-827 \
                --temp_location gs://world-fishing-827-dev-ttl30d/scratch/encounters \
                --job_name encounters-test \
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
