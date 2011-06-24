#!/bin/bash
export MINITREE_SERVER="http://127.0.0.1:8000"
export MINITREE_DSN="host=localhost port=5432 user=jianingy dbname=jianingy"

cd $(dirname $0)
python test/select.py
python test/creation.py

