for ((i = 0; i < 50; i++)); do
	go test -run 2A -race -gcflags=all="-N -l" 1>a.err
	if test $? -ne 0; then
		exit 1
	else
		echo "batch $i ok"
	fi
done
