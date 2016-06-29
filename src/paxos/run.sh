#!/bin/bash

testAll="TestBasic TestDeaf TestForget TestManyForget TestForgetMem TestOld TestRPCCount TestManyUnreliable TestMany TestLots TestPartition"

testOne="TestRPCCount TestManyUnreliable"

function run_test() {
go test -test.run $1 >$1.out 2>&1
}

rm *.out
for t in $testAll
do
    echo "testing $t"
    echo "done"
    for ((i=0; i < 100; i++))
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
