#!/bin/bash

cat <<EOF
$0  <dbhost> <dbuser> <dbname>
Default dbhost=localhost port=5432 dbuser=jianingy dbname=jianingy
EOF

port=5432

if [ $# == 0 ]; then
    echo $EOF
    dbhost=localhost
    dbuser=jianingy
    dbname=jianingy
elif [ $# == 3 ]; then
    dbhost=$1
    dbuser=$2
    dbname=$3
else
    echo $EOF
    echo "END"
    exit
fi

export MINITREE_SERVER="http://127.0.0.1:8000"
export MINITREE_DSN="host=$dbhost port=$port user=$dbuser dbname=$dbname"
echo $MINITREE_DSN

cd $(dirname $0)
cd test
unit2 discover

