#!/bin/bash
echo "Remove logs and results"
rm -f /tmp/*.log
rm -f /tmp/*.log.gz
rm -f ~/*.log
rm -f ~/*.log.gz
rm -f /root/requestInfo.txt
