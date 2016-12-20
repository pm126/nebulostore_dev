#!/bin/bash

for i in {00..49}
do
    echo $i
    qemu-nbd --connect=/dev/nbd0 ~/ubuntu$i/*.img
    e2fsck -y /dev/nbd0p1
    qemu-nbd -d /dev/nbd0
done
