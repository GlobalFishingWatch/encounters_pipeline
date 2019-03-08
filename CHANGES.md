Changes
=======


0.3.1 2018-10-19
-----------------

* [#36](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/36)
  Update the Distance to Port Mask

0.3.0 - 2018-09-07
------------------

* [#38](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/38)
  * Removes the publication of events from this pipeline, which will be handled on [pipe-events](https://github.com/globalfishingwatch/pipe-events). See [pipe-events#7](https://github.com/GlobalFishingWatch/pipe-events/pull/7).


0.2.1 2018-09-03
-----------------

* [#37](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/37)
  * Bump version of pipe-tools to 0.1.7

0.2.0 2018-05-14
-----------------

* [#32](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/32)
  * Publish standardized encounter events


0.1.19 2018-03-12
-----------------

* [#28](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/28)
  * Refactor airflow


0.1.18 2018-02-08
-----------------

* [#23](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/23)
  * Filter encounters output to the specified date range
* [#25](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/25)
  * Added a backfill flag to disable running the encounters_merge dag task when backfilling


0.1.17 2018-02-05
-----------------

* [#11](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/11)
  * Filter out inland encounters
* [#21](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/21)
  * Combine airflow create_raw_encounters with merge_encoutners for the daily run


0.1.16
------

* [#13](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/13)
  * Break out merge into a separate dag and fix the start date
* [#17](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/17)
  * add --neighbor_table parameter in the airflow configuration so that raw_encounters
  * writes out neighbors to a separate table for use in the features pipeline


0.1.15
------

* [#8](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/8)
  * Reorg and rename parameters to match the latest airflow mini-pipeline architecture
