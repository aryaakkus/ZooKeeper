#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

# 	Replace with your server names and client ports.
export ZKSERVER=lab2-10.cs.mcgill.ca:21831,lab2-11.cs.mcgill.ca:21831,lab2-21.cs.mcgill.ca:21831

java -cp $CLASSPATH:../task:.: DistClient "$@"
