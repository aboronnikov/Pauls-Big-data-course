#!/bin/bash
hive
--hiveconf tableName=pavel.citydata \
--hiveconf pathToData=/user/maria_dev/citydata \
--hiveconf delimiter=\t \
-f /Scripts/create_city_table.sql

exit $?