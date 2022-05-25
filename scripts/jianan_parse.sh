#!/bin/sh
log=$1


cat $log/output.log | awk '{if (match($0, "DBBENCH_START ycsbwklda")) {start = 1} if (start == 1 && (match($0, "while loop"))) {total_migs += $11}} END {print total_migs}'
