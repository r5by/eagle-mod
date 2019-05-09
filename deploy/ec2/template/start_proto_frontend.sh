#!/bin/bash
# Start Prototype frontend

LOG=protoFrontend.log

APPCHK=$(ps aux | grep -v grep | grep -c {{frontend_type}})
IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`

ip_there=`cat frontend.conf |grep hostname`
if [ "X$ip_there" == "X" ]; then
  echo "hostname = $IP" >> frontend.conf
fi

if [ ! $APPCHK = '0' ]; then
  echo "Frontend already running, cannot start it."
  exit 1;
fi

scheduler_id_there =`cat frontend.conf |grep scheduler_id`
if [ "X$scheduler_id_there" == "X" ]; then
  echo "scheduler_id = $1" >> frontend.conf
fi

scheduler_size_there =`cat frontend.conf |grep scheduler_size`
if [ "X$scheduler_size_there" == "X" ]; then
  echo "distributed_scheduler_size = $2" >> frontend.conf
fi

# Wait for daemon ready
sleep 15
nohup java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCTimeStamps -Xmx2046m -XX:+PrintGCDetails  -cp eagle-1.0-PROTOTYPE.jar ch.epfl.eagle.examples.{{frontend_type}} -c frontend.conf > $LOG 2>&1 &

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
