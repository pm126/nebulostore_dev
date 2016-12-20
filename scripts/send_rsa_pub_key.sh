#!/bin/bash

if [ $# -ne 1 ]
then
	echo "Incorrect number of arguments"
	exit 1
fi

NUMBER_OF_MACHINES=$1
IP_PREFIX=192.168.122.1
USERNAME=ubuntu
PUBLIC_KEY=$(cat ~/.ssh/id_rsa.pub)

for i in $(eval echo {00..$NUMBER_OF_MACHINES})
do
	ssh $USERNAME@$IP_PREFIX$i "mkdir .ssh; echo $PUBLIC_KEY > ~/.ssh/authorized_keys"
done
