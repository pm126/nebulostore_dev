#!/bin/bash

# Automatic planet-lab N-peer test.
#
# Make sure that you have uploaded necessary libraries to the host
# Use scripts/upload-libs-to-planet-lab.sh for that purpose.
#
# Optional parameters: number_of_peers host_list
# Prints "SUCCESS" or "FAILURE"

EXEC_DIR=$(pwd)
cd $(dirname $0)
SCRIPT_DIR=$(pwd)

cd ../
. _constants.sh
cd $SCRIPT_DIR

export TEST_NAME="org.nebulostore.systest.newcommunication.functionaltest.messageexchange.MessageExchangeTestMain"
export HOST_LIST="../nodes/hosts.txt"

LOG_DIR=logs

BOOTSTRAP_DELAY=5
MAX_THREADS=15

if [ $1 ]; then
  PEER_NUM=$1
  HOST_LIST=${2-"../nodes/hosts.txt"}
fi

MAIN_PEER=`head -n 1 $HOST_LIST`
NAMING_PEER=`head -n 2 $HOST_LIST | tail -n 1`

echo "BUILDING ..."
cd ../
./_build-and-deploy.sh -p 1 -m install > /dev/null
cd $SCRIPT_DIR


# Generate and copy config files.
./generate_config_files.sh -n $((PEER_NUM + 1)) -t $MAIN_PEER -k $NAMING_PEER


echo "COPYING ..."
i=1
PAIRS=""
CLIENT_HOSTS=""
for host in `cat $HOST_LIST`
do
    PAIRS+="$i $host "
    if [ $i -ne 1 ]; then CLIENT_HOSTS+="$host "; fi
    if [ $i -eq $((PEER_NUM + 1)) ]; then break; else ((i++)); fi
done
cd ..
echo $PAIRS | xargs -P $MAX_THREADS -n 2 ./_pl-deploy-single.sh
cd $SCRIPT_DIR



echo "RUNNING ..."

run_single() {
    echo "   "$1
    ssh -o $SSH_OPTIONS -l $USER $1 "cd $REMOTE_DIR; $JAVA_EXEC -cp Nebulostore.jar:lib/* $TEST_NAME > logs/stdout.log 2> logs/stderr.log &"
}

export -f run_single

run_clients() {
    sleep $BOOTSTRAP_DELAY
    echo $CLIENT_HOSTS
    echo $CLIENT_HOSTS | xargs -d ' ' -P $MAX_THREADS -n 1 -I {} bash -c 'run_single {}'
    echo
    echo "WAITING FOR TEST TO FINISH ..."
}

cd ../
run_clients &
cd $SCRIPT_DIR
echo "  " $MAIN_PEER
ssh -o $SSH_OPTIONS -l $USER $MAIN_PEER "cd $REMOTE_DIR; $JAVA_EXEC -cp 'Nebulostore.jar:lib/*' $TEST_NAME > logs/stdout.log 2> logs/stderr.log && echo 'OK' > logs/exit_code || echo 'ERROR' > logs/exit_code"

echo "KILLING PEERS AND COLLECTING LOGS ..."
rm -rf ../../$LOG_DIR
mkdir ../../$LOG_DIR

cd ../
echo $PAIRS | xargs -P $MAX_THREADS -n 2 ./_pl-kill-and-get-logs-single.sh

echo -n "TEST RESULT: "
cat ../$LOG_DIR/logs_1/exit_code

cd communicationtest
cd ${EXEC_DIR}
