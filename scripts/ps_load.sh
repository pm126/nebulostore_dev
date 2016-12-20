#!/bin/bash

while [ 1 ]
do
    echo "-----------------"$(date)"---------------------------------" >> ps_report
    ps aux -AL >> ps_report
    sleep 5
done
