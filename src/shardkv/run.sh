#!/bin/bash

testAll="TestBasic TestMove TestLimp TestConcurrent TestConcurrentUnreliable"

testOne="TestConcurrent"

function run_test() {
go test -test.run $1 >$1.out 2>&1
}

rm *.out
for t in $testOne
do
    echo "testing $t"
    echo "done"
    for ((i=0; i < 1; i++))
    do
        run_test $t
        if ! (grep -q Passed $t.out)
        then
            echo "$t failed"
            mv $t.out $t.fail
            break
        fi
        echo -e " $i"
    done
done
