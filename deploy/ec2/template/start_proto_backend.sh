#!/bin/bash
# Start Prototype backend

LOG=protoBackend.log

APPCHK=$(ps aux | grep -v grep | grep -c ProtoBackend)

if [ ! $APPCHK = '0' ]; then
  echo "Backend already running, cannot start it."
  exit 1;
fi

# Wait for daemon ready
sleep 15
nohup java -cp eagle-1.0-PROTOTYPE.jar ch.epfl.eagle.examples.SimpleBackend  > $LOG 2>&1 &

PID=$!
echo "Logging to $LOG"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Proto backend failed to start"
  exit 1;
else
  echo "Proto backend started with pid $PID"
  exit 0;
fi
