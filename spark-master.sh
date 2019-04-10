#!/bin/bash
# Spark
spk="/home/gfi/spark/spark-2.3.2-bin-hadoop2.7"
sudo $spk/sbin/start-master.sh
# EFS
efs="fs-dcbdf53c.efs.us-east-1.amazonaws.com:/"
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport $efs /efs
