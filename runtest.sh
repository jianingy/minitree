#!/bin/bash
export MINITREE_SERVER="http://127.0.0.1:8000"
export MINITREE_DSN="host=localhost port=5432 user=jianingy"

cd $(dirname $0)
exec python -m unittest test
