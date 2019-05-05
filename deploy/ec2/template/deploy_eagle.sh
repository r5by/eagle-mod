#!/bin/bash
# Deploy built version of Sparrow on frontends and backends.

FRONTENDS=`cat frontends.txt`
BACKENDS=`cat backends.txt`
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

for fe in $FRONTENDS; do
  rsync -e "ssh $SSH_OPTS" --delete -az ~/ `dig +short $fe`:~/ &
done
wait

for be in $BACKENDS; do
  rsync -e "ssh $SSH_OPTS" --delete -az ~/ `dig +short $be`:~/ &
done
wait
