#!/bin/bash
hive
--hiveconf tableName=pavel.browserdata \
--hiveconf pathToData=/user/maria_dev/data \
--hiveconf delimiter=\t \
-f /Scripts/create_browser_table.sql

exit $?