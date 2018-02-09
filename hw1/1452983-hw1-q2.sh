#!bin/sh
#sh 1452983-hw1-q2.sh  1452983-hw1-q1.log

inFile=1452983-hw1-q1.log 
outFile=1452983-hw1-q2.log 
touch $outFile

#a
wc -l $1 | cut -d ' ' -f 1 >> "$outFile"

#b
firstLine=$(date +%s -d $(head -n 1 $inFile | cut -d ' ' -f 1))
lastLine=$(date +%s -d $(tail -n 1 $inFile | cut -d ' ' -f 1))
difference=$((lastLine-firstLine))
echo "$difference" >> "$outFile"

#c
counts=0
array=$(cat $1 | cut -d ',' -f 5)
for i in $(echo $array)
do 
	if [ "$i" > 0 ] 
	then 
		counts=$(($counts+1))
		#echo $counts
	fi
done
echo $counts >> "$outFile"

#d
cat $inFile | awk '!(i=!i)' > tmp1452983
charCounts=0
while read line; do 
	echo $line >> "$outFile"
	lineCounts=$(echo $line | wc -c | cut -d ' ' -f 1)
	charCounts=$(($charCounts+$lineCounts))
done < tmp1452983
echo $charCounts >> "$outFile"
rm tmp1452983




