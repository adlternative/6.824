##
## ./2X.sh [A|B|C|D] <test> count
##  E.G. ./2X.sh B TestFailNoAgree2B 3
##  E.G. ./2X.sh B ALL 50
##
for ((i = 0; i < $3; i++)); do
	if [ "$2" != "ALL" ]; then
		go test -run 2$1 -race -test.run $2 -gcflags=all="-N -l" 1>$1.err
	else
		go test -run 2$1 -race -gcflags=all="-N -l" 1>$1.err
	fi

	if test $? -ne 0; then
		echo "batch $i failed"
		# ./debug >$1-format.err
		exit 1
	else
		echo "batch $i ok"
	fi
done
