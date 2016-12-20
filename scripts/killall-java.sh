#!/bin/bash

# usage:
# scripts/killall-java.sh [ N ]
# Script that kills java processes on top N (or all, if no N is given) hosts
# from scripts/nodes/hosts.txt list

HOST_LIST="./nodes/hosts.txt"
USER=mimuw_nebulostore
SSH_OPTIONS="StrictHostKeyChecking=no"
MAX_THREADS=3

if [ $1 ]; then
    HOSTCOUNT=$1
else
    HOSTCOUNT=`wc -l $HOST_LIST | cut -f1 -d" "`
fi

EXEC_DIR=$(pwd)
cd $(dirname $0)

head -$HOSTCOUNT $HOST_LIST | xargs -P $MAX_THREADS -I {} sh -c "ssh -o $SSH_OPTIONS -l $USER {} \"hostname; killall java\" || exit 0"

cd ${EXEC_DIR}
