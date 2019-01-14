#!/bin/bash
hive
--hiveconf browserTable=pavel.browserdata \
--hiveconf cityTable=pavel.citydata \
-f /Scripts/query.sql

exit $?