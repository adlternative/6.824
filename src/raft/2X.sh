##
## ./2X.sh [A|B|C|D] <test> count
##  E.G. ./2X.sh B TestFailNoAgree2B 3 .
##  E.G. ./2X.sh B ALL 50 .
##

if [ "$4" == "GODEBUG" ]; then
	for ((i = 0; i < $3; i++)); do
		GODEBUG=cgocheck=2 GOGC=2 go test -gcflags=-d=checkptr -race -run $2 -o $1.test >$1.err 2>&1

		if test $? -ne 0; then
			echo "batch $i failed"
			# ./debug >$1-format.err
			exit 1
		else
			echo "batch $i ok"
		fi
	done
else
	go test -c -race -gcflags=all="-N -l" -o $1.test
	for ((i = 0; i < $3; i++)); do
		if [ "$2" != "." ]; then
			./$1.test -test.run $2 1>$1.err 2>&1
		else
			./$1.test 1>$1.err 2>&1
		fi

		if test $? -ne 0; then
			echo "batch $i failed"
			# ./debug >$1-format.err
			exit 1
		else
			echo "batch $i ok"
		fi
	done
fi
