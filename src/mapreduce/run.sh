#!/bin/bash

iterator=0
while [ $iterator -lt 100 ]
do 
    go test >> testresult/out${iterator}.txt
    iterator=`expr ${iterator} + 1`  
done

