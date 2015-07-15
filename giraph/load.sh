#! /bin/sh

NODES=$1
EDGES=$2

export HADOOP_HOME=/home/alekh/hadoop-1.0.4/


# PageRank
N_COUNT=`wc -l $NODES | awk '{print $1}'`
cat $NODES | awk '{r=1.0/'$N_COUNT';print $0"\t"r}' > $NODES.tmp
$HADOOP_HOME/bin/hadoop dfs -rmr nodes edges
$HADOOP_HOME/bin/hadoop dfs -copyFromLocal $NODES.tmp nodes
$HADOOP_HOME/bin/hadoop dfs -copyFromLocal $EDGES edges
rm $NODES.tmp

