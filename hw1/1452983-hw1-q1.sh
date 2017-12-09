#!bin/bash
#sh 1452983-hw1-q1.sh
touch 1452983-hw1-q1.log
i=0
while [ "$i" != "111" ]
do
        echo `uptime` >> 1452983-hw1-q1.log
        i=$(($i+1))
        sleep 10
done

exit 0
