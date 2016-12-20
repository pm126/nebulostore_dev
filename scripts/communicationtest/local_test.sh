#!/bin/bash

# Automatic local N-peer test.
# Optional parameters:
# Prints "SUCCESS" or "FAILURE"

# Assuming we are in scripts directory.

export TEST_NAME="org.nebulostore.systest.newcommunication.functionaltest.messageexchange.MessageExchangeTestMain"

BOOTSTRAP_DELAY=1
LOG_DIR=logs

if [ $1 ]; then
  PEER_NUM=$1
else
  echo "Provide number of peers to run."
  exit 1
fi

EXEC_DIR=$(pwd)
cd $(dirname $0)

echo "BUILDING ..."
cd ../
./_build-and-deploy.sh -p $(($PEER_NUM + 1)) -m install > /dev/null
cd communicationtest

# Generate and copy config files.
./generate_config_files.sh -n $(($PEER_NUM + 1))

cd ../

for ((i=1; i<=$((PEER_NUM + 1)); i++))
do
    mv ../Peer.xml.$i ../build/jar/$i/resources/conf/Peer.xml
done

# Run test instances
echo "RUNNING ..."
cd ../build/jar

RUN_CLIENT() {
    cd $1
    java -cp 'Nebulostore.jar:lib/*' $TEST_NAME &
    cd ..
}

export -f RUN_CLIENT

EXIT_CODE=0
cd 1
java -cp 'Nebulostore.jar:lib/*' $TEST_NAME &
MAIN_PID=$!
cd ../
cd 2
java -cp 'Nebulostore.jar:lib/*' $TEST_NAME &
cd ../

sleep $BOOTSTRAP_DELAY
seq 3 $(($PEER_NUM + 1)) | xargs -P 15 -n 1 -I {} bash -c 'RUN_CLIENT {}' &

wait $MAIN_PID
if [ $? -ne 0 ]; then
  EXIT_CODE=1
fi

cd ../..

# Copy logs.
rm -rf $LOG_DIR
mkdir $LOG_DIR
echo "COPYING LOGS ..."
for ((i=1; i<=$(($PEER_NUM + 1)); i++))
do
    cp -r build/jar/$i/logs $LOG_DIR/logs_$i
done

# Kill remaining peers.
ps ax | grep Nebulostore | sed 's/^ *//' | cut -d' ' -f1 | xargs kill 2> /dev/null

if [ $EXIT_CODE -eq 0 ]
then
    echo "SUCCESS"
else
    echo "FAILURE"
fi

cd $EXEC_DIR
exit $EXIT_CODE
