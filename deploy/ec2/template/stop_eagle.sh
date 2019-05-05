#!/bin/bash
# Stop eagle locally

APPCHK=$(ps aux | grep -v grep | grep -c EagleDaemon)

if [ $APPCHK = '0' ]; then
  echo "Eagle is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep EagleDaemon |grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped Eagle process"
exit 0;
