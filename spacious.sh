#!/bin/bash

# enable swap partition
sudo mkswap /dev/xvdf
sudo swapon /dev/xvdf -p 1000
# point temp dir to memory; remember to set temp dir path beforehand
sudo mount -t tmpfs -o size=11G tmpfs /home/gfi/temp
# enable EFS
efs="fs-dcbdf53c.efs.us-east-1.amazonaws.com:/"
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport $efs /efs

echo 'If you are in an R session, restart it.'
# if execution fails, check EOL; EOL should be LF
# may use Notepad++ to check & convert EOL
# 'sed -i -e 's/\r$//' <filename>' replaces Windows EOL 'CRLF' by 'LF'
