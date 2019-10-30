#!/bin/bash

IFS=$'\n'
arr=($(ls | grep "wager"))
unset IFS
sum=0
for i in "${arr[@]}"
do
    delta=($( cat $i | awk -F ';' '{print $4}'))
    for j in "${delta[@]}"
    do
        if [ $j -eq 200 ]
        then
            ((sum+=1))
        fi
    done
    echo $sum
done
echo $sum

