#!/bin/bash
hive --hiveconf dbName=pavel_orekhov \
--hiveconf tableName=train \
-f /Task1/task1.hql

exit $?