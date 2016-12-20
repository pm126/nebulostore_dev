#!/bin/bash
rm gcout
while [ 1 ]
do
    date >> gcout
    PID=$(jps | grep jar | cut -f 1 --delimiter=" " | head -n 1)
    if [ -z "$PID" ]
    then
	sleep 10
    else
	jstat -gccause -t $PID 1000 >> gcout
    fi
done
