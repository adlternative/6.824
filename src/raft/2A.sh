for ((i=0;i<100;i++))
do
go test -run 2A -race  -gcflags=all="-N -l"  1>a.err
if test $? -ne 0
then
	exit 0
fi
done