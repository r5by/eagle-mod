#!/bin/bash
# Start Prototype frontend

LOG=protoFrontend.log

APPCHK=$(ps aux | grep -v grep | grep -c {{frontend_type}})

if [ ! $APPCHK = '0' ]; then
  echo "Frontend already running, cannot start it."
  exit 1;
fi

nohup java -XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCTimeStamps -Xmx2046m -XX:+PrintGCDetails  -cp eagle-1.0-PROTOTYPE.jar ch.epfl.eagle.examples.{{frontend_type}} -c frontend.conf > $LOG 2>&1 &

PID=$!
echo "Logging to $LOG"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Proto frontend failed to start"
  exit 1;
else
  echo "Proto frontend started with pid $PID"
  exit 0;
fi
