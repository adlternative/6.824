##
## ./2X.sh [A|B|C|D] <test> count
##  E.G. ./2X.sh B TestFailNoAgree2B 3
##  E.G. ./2X.sh B ALL 50
##
go test -c -race -gcflags=all="-N -l"
for ((i = 0; i < $3; i++)); do
	if [ "$2" != "." ]; then
		./raft.test -test.run $2  1>$1.err 2>&1
	else
		./raft.test 1>$1.err 2>&1
	fi

	if test $? -ne 0; then
		echo "batch $i failed"
		# ./debug >$1-format.err
		exit 1
	else
		echo "batch $i ok"
	fi
done
