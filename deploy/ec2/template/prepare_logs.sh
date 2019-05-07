#!/bin/bash
PREFIX={{frontend_type}}
PID=`jps | grep "${PREFIX}" | awk '{ print $1}'`

if [ -z "$PID" ]
then
	PREFIX=Backends
fi

IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`

cd /root/
tar cvzf ${PREFIX}_${IP}.tar.gz *.log *.txt
