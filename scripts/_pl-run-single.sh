#!/bin/bash

EXEC_DIR=$(pwd)
cd $(dirname $0)
. _constants.sh

echo "   "$1
ssh -o $SSH_OPTIONS -l $USER $1 "cd $REMOTE_DIR; $JAVA_EXEC -jar Nebulostore.jar > logs/stdout.log 2> logs/stderr.log &"

cd ${EXEC_DIR}
