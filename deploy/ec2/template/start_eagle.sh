#!/bin/bash
# Start Eagle locally
ulimit -n 16384

LOG=eagleDaemon.log
IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`

ip_there=`cat eagle.conf |grep hostname`
if [ "X$ip_there" == "X" ]; then
  echo "hostname = $IP" >> eagle.conf
fi

# Make sure software firewall is stopped (ec2 firewall subsumes)
/etc/init.d/iptables stop > /dev/null 2>&1

APPCHK=$(ps aux | grep -v grep | grep -c EagleDaemon)

if [ ! $APPCHK = '0' ]; then
  echo "Eagle already running, cannot start it."
  exit 1;
fi

# -XX:MaxGCPauseMillis=3 
# removed nice -n -20
nohup java -XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCTimeStamps -Xmx2046m -XX:+PrintGCDetails -cp eagle-1.0-PROTOTYPE.jar ch.epfl.eagle.daemon.EagleDaemon -c eagle.conf > $LOG 2>&1 &
PID=$!
echo "Logging to $LOG"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Eagle Daemon failed to start"
  exit 1;
else
  echo "Eagle Daemon started with pid $PID"
  exit 0;
fi
