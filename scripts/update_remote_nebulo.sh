#!/bin/bash
if [ $# -lt 1 ]
then
    echo "Argument needed"
    exit 1
fi

SERVER_NAME=$1
rsync -tvr --progress --exclude="build" --exclude="scripts" --exclude="resources" ../ $SERVER_NAME:~/nebulostore/
