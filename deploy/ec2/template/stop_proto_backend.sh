#!/bin/bash
# Stop proto backend locally

APPCHK=$(ps aux | grep -v grep | grep -c SimpleBackend)

if [ $APPCHK = '0' ]; then
  echo "ProtoBackend is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep SimpleBackend |grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped ProtoBackend process"
exit 0;

