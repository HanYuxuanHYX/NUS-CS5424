#!/bin/sh

EXPERIMENT_N=$1
NODE_N=$2

[ ! -d "output" ] && mkdir output

source /home/stuproj/cs4224l/miniconda3/etc/profile.d/conda.sh
conda activate cassandra

case $EXPERIMENT_N in
    1|2)
        for i in 0 1 2 3
        do
            python run_xact_file.py $EXPERIMENT_N $(($NODE_N+$i*5)) &
        done
        ;;
    3|4)
        for i in 0 1 2 3 4 5 6 7
        do
            python run_xact_file.py $EXPERIMENT_N $(($NODE_N+$i*5)) &
        done
        ;;
    *)
esac
