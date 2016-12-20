#!/bin/bash

while [ 1 ]
do
    echo "-----------------"$(date)"---------------------------------" >> thread_dump
    ps aux | grep java | awk {'print $2'} | xargs -I {} jstack -l {} >> thread_dump 2>> thread_dump
    sleep 5
done
