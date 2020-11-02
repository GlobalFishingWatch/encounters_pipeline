# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v3.0.5 - 2020-11-02

### Changed

* [Data Pipeline/PIPELINE-142](https://globalfishingwatch.atlassian.net/browse/PIPELINE-142): Changes
  * Fix lon averaging across dateline for encounter creation.
  * Fix lon averaging for merging.
  * Fix existing tests and add new tests for creation and merging near dateline.

## v3.0.4 - 2020-09-18

### Changed

* [Data Pipeline/PIPELINE-106](https://globalfishingwatch.atlassian.net/browse/PIPELINE-106): Changes
  * Successive runs of merge_encounters on the same data can give slightly
  * different result.  The culprit turned out to be that sorting the incoming
  * raw_encounters by start_time alone was not completely stable since there
  * could by ties in start time. The solution was to stabilize the search by
  * including the end time and vessel_ids as secondary keys.

## v3.0.3 - 2020-06-11

### Added

* [GlobalFishingWatch/gfw-eng-tasks#111](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/111): Adds
  * Pin to `pipe-tools:v3.1.2`.

## v3.0.2 - 2020-03-18

### Added

* [GlobalFishingWatch/gfw-eng-tasks#37](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/37): Adds
   required position_messages and segments table passed as requiered in create_raw_encounters.

## v3.0.1 - 2020-03-13

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#32](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/32): Changes
   hardcoded `segment` to `legacy_segment_v1_` table.

## v3.0.0 - 2020-03-12

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#31](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/31): Changes
   pin `ujson` lib to `1.35` and `pipe-tools:v3.1.1`.
   Dockerfile update just needed lines.

### Added

* [GlobalFishingWatch/gfw-eng-tasks#29](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/29): Adds
    `dist_to_port_10km.pickle` file having the new raster.


## v2.0.0 - 2020-01-29

### Changed

* [GlobalFishingWatch/GFW-Tasks#1166](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1166): Changes
   * supports [pipe-tools:v3.1.0](https://github.com/GlobalFishingWatch/pipe-tools/releases/tag/v3.1.0)
   * Migrates `Apache Beam` from version 2.1.0 to [2.16.0](https://github.com/apache/beam/tree/v2.16.0)
   * Migrates from pytohn 2 to 3.

## v1.0.0 - 2019-03-27

### Added

* [GlobalFishingWatch/GFW-Tasks#991](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/991)
   * Migrates to use the new [airflow-gfw](https://github.com/GlobalFishingWatch/airflow-gfw) library and use the pipe-tools [v2.0.0](https://github.com/GlobalFishingWatch/pipe-tools/releases/tag/v2.0.0)

## 0.3.3

* [GlobalFishingWatch/GFW-Tasks#990](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/990)
  * Support the Yearly mode
* Remove noise segments using the segment table noise flag.

## 0.3.2

* [GlobalFishingWatch/GFW-Tasks#957](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/957)
  * Increments version of pipe_tools to 0.2.5.
  * Ensures the creation of raw_encounters table before merge_encounters pipeline starts.

## 0.3.1 2018-10-19

* [#36](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/36)
  Update the Distance to Port Mask

## 0.3.0 - 2018-09-07

* [#38](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/38)
  * Removes the publication of events from this pipeline, which will be handled on [pipe-events](https://github.com/globalfishingwatch/pipe-events). See [pipe-events#7](https://github.com/GlobalFishingWatch/pipe-events/pull/7).


## 0.2.1 2018-09-03

* [#37](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/37)
  * Bump version of pipe-tools to 0.1.7

## 0.2.0 2018-05-14

* [#32](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/32)
  * Publish standardized encounter events


## 0.1.19 2018-03-12

* [#28](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/28)
  * Refactor airflow


## 0.1.18 2018-02-08

* [#23](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/23)
  * Filter encounters output to the specified date range
* [#25](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/25)
  * Added a backfill flag to disable running the encounters_merge dag task when backfilling


## 0.1.17 2018-02-05

* [#11](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/11)
  * Filter out inland encounters
* [#21](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/21)
  * Combine airflow create_raw_encounters with merge_encoutners for the daily run


## 0.1.16

* [#13](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/13)
  * Break out merge into a separate dag and fix the start date
* [#17](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/17)
  * add --neighbor_table parameter in the airflow configuration so that raw_encounters
  * writes out neighbors to a separate table for use in the features pipeline


## 0.1.15

* [#8](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/8)
  * Reorg and rename parameters to match the latest airflow mini-pipeline architecture
