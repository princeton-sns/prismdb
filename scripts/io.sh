#!/bin/sh

io_log=$1
sudo rm -f $io_log

iostat -x -p nvme2n1,nvme1n1 1 >& $io_log

