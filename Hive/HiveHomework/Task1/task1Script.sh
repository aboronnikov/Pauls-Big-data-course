#!/bin/bash
hive --hiveconf dbName=pavel_orekhov \
--hiveconf tableName=train \
-f task1.hql

exit $?