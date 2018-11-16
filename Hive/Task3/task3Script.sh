#!/bin/bash
hive --hiveconf dbName=pavel_orekhov \
--hiveconf tableName=train \
-f /Task3/task3.hql

exit $?