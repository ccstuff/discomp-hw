#!bin/sh
#sh 1452983-hw1-q3.sh

mylog=1452983-hw1-q1.log
outFile=1452983-hw1-q3.log
blog=tmp

ssh -i /media/psf/Desktop/2017-2018-1/distributed/sfsfewwfssdds.pem linux1@148.100.4.90 "sh ~/B-hw1-q1.sh && ls -l -f --full-time B-hw1-q1.log | cut -d ' ' -f 6;cat ~/B-hw1-q1.log; exit" > tmp1

bdate=$(head -n 1 tmp1)
mydate=$(ls -l -f --full-time $mylog | cut -d ' ' -f 6)
sed '1d' tmp1 > $blog

exec 3<$blog
exec 4<$mylog
#echo $bdate $mydate
while read blog <&3 && read mylog <&4 
do
	btime=$(echo $blog | cut -d ' ' -f 1)
	btimestamp=$(date -d "$(echo $bdate $btime)" +%s)
	
	mytime=$(echo $mylog | cut -d ' ' -f 1)
	mytimestamp=$(date -d "$(echo $mydate $mytime)" +%s)

	difference=$((btimestamp-mytimestamp))
	#echo $btime $btimestamp  $mytime $mytimestamp $difference
	echo $difference >> $outFile
done

rm tmp1 tmp

