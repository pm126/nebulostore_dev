LABLIST_FILE=bootable_nodes.txt

for host in $(cat $LABLIST_FILE)
do
    ssh -o ConnectTimeout=1 -o StrictHostKeyChecking=no -o PasswordAuthentication=no mimuw_nebulostore@$host "ls" > /dev/null 2> /dev/null
    if [ $? -ne 0 ]
    then
	echo $host
    fi
done

