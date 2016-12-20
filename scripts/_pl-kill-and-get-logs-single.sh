#!/bin/bash

EXEC_DIR=$(pwd)
cd $(dirname $0)
. _constants.sh

echo "   "$2
ssh -o $SSH_OPTIONS -l $USER $2 "cd $REMOTE_DIR; killall java &> /dev/null; mv logs logs_$1; tar -zcf logs.tar.gz logs_$1; mv logs_$1 logs"
rsync $USER@$2:~/$REMOTE_DIR/logs.tar.gz $LOG_DIR/logs_${1}_${2}.tar.gz
cd $LOG_DIR
tar -xzf logs_${1}_${2}.tar.gz

cd ${EXEC_DIR}
exit 0

