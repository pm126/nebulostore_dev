#!/bin/bash

# Generate config files for communication tests. First peer is main server.
# Second peer is comm server.
# Optionally, the following parameters might be provided:
#   -n number_of_peers
#   -k remotemap_server_net_address
#   -t main_server_net_address
#   -h host_list

EXEC_DIR=$(pwd)
cd $(dirname $0)

while getopts ":n:k:t:h:" OPTION
do
  case $OPTION in
    n) PEER_NUM=$OPTARG;;
    k) REMOTEMAP_SERVER_NET_ADDRESS=$OPTARG;;
    t) TEST_SERVER_NET_ADDRESS=$OPTARG;;
    h) HOST_LIST=$OPTARG;;
    # DEFAULT
    *)
       ARG=$(($OPTIND-1));
       if [ ${!ARG} = "-d" ]
       then
           DATA_FILE=""
       else
           echo "Unknown option option chosen: ${!ARG}.";
       fi
  esac
done

: ${PEER_NUM=3}
: ${TEST_SERVER_NET_ADDRESS="localhost"}
: ${REMOTEMAP_SERVER_NET_ADDRESS="localhost"}
: ${REMOTEMAP_LOCAL_PORT=1100}

PADDED=`printf "%03d" 2`
BOOTSTRAP_COMM_ADDRESS=00000000-0000-0000-0002-000000000000

CLIENT_HOSTS=""

i=1
if [[ -n $HOST_LIST ]]; then
    for host in `cat $HOST_LIST`
    do
        if [ $i -ne 1 ]; then CLIENT_HOSTS[$i]=$host; fi
        if [ $i -eq $PEER_NUM ]; then break; else ((i++)); fi
    done
fi

# Configure peers.
cd ../
for ((i=1; i<=$PEER_NUM; i++))
do
    # Make first peer bootstrap server, storage holder and test server.
    if [ $i -eq 1 ]
    then
        TEST_MODE="server"
    else
        TEST_MODE="client"
    fi

    if [ $i -eq 2 ]
    then
        REMOTEMAP_MODE="server"
    else
        REMOTEMAP_MODE="client"
    fi

    if [[ -n $CLIENT_HOSTS[$i] ]]; then
        LOCAL_NET_ADDR=${CLIENT_HOSTS[$i]}
    fi

    PADDED=`printf "%03d" $i`
    ../resources/conf/generate_config.py\
         --local-net-address=$LOCAL_NET_ADDR \
         --bootstrap-net-address=$TEST_SERVER_NET_ADDRESS \
         --comm-address=00000000-0000-0000-0$PADDED-000000000000 \
         --bootstrap-comm-address=$BOOTSTRAP_COMM_ADDRESS \
         --bootstrap/mode=$BOOTSTRAP_MODE  \
         --comm-cli-port=11$PADDED \
         --remotemap/mode=$REMOTEMAP_MODE \
         --remotemap/local-port=$REMOTEMAP_LOCAL_PORT \
         --remotemap/server-net-address=$REMOTEMAP_SERVER_NET_ADDRESS \
         --remotemap/server-port=$REMOTEMAP_LOCAL_PORT \
         --messageexchange/mode=$TEST_MODE \
         --messageexchange/server/client-count=$(($PEER_NUM - 1)) \
         --messageexchange/client/server-net-address=$TEST_SERVER_NET_ADDRESS \
          < ../resources/conf/Peer.xml.template > ../Peer.xml.$i
done

cd ${EXEC_DIR}
