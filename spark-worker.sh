#!/bin/bash
spk="/home/gfi/spark/spark-2.3.2-bin-hadoop2.7"
sudo $spk/sbin/start-slave.sh spark://ip-172-31-91-141.ec2.internal:7077 --memory 12g
# EFS
efs="fs-dcbdf53c.efs.us-east-1.amazonaws.com:/"
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport $efs /efs
# Swap
sudo fallocate -l 12G /swapfile; sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
# memory cache
sudo mount -t tmpfs -o size=11G tmpfs /home/gfi/temp
