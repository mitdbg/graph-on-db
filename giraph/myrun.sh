#!/bin/bash

BASEDIR=$(dirname $0)


export HADOOP_HOME=/home/alekh/hadoop-1.0.4/
export GIRAPH_HOME=/home/alekh

#$BASEDIR/pagerank.sh
#$BASEDIR/shortestpath.sh 55542254 43359637
$BASEDIR/triangle_global.sh
#$BASEDIR/triangle_pernode.sh
#$BASEDIR/weak_ties.sh 1000
#$BASEDIR/strong_overlap.sh 100
