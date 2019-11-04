#!/usr/bin/env bash

# assumes this file is in the same directory as the TQL file and data files and is run in that folder.
tql < test_sharding.tql
tsload --source_file ok_unsharded.csv --target_database test_sharding --target_table ok_unsharded
tsload --source_file oversharded.csv --target_database test_sharding --target_table oversharded
tsload --source_file undersharded.csv --target_database test_sharding --target_table undersharded
tsload --source_file unsharded_large.csv --target_database test_sharding --target_table unsharded_large
