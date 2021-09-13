##
## ./2X.sh [A|B|C|D] <test>
##
for ((i = 0; i < 50; i++)); do
	if [ "$2" != "" ]; then
		go test -run 2$1 -race -test.run $2 -gcflags=all="-N -l" 1>$1.err
	else
		go test -run 2$1 -race -gcflags=all="-N -l" 1>$1.err
	fi

	if test $? -ne 0; then
		echo "batch $i failed"
		exit 1
	else
		echo "batch $i ok"
	fi
done
