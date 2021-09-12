for ((i = 0; i < 50; i++)); do
	go test -run 2$1 -race -gcflags=all="-N -l" 1>$1.err
	if test $? -ne 0; then
		echo "batch $i failed"
		exit 1
	else
		echo "batch $i ok"
	fi
done
