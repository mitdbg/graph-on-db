#! /bin/bash

THRESHOLD=$1


NODES=nodes
EDGES=edges




$HADOOP_HOME/bin/hadoop dfs -rmr weakties_output
$HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner core.advanced.WeakTiesVertex -vif org.apache.giraph.io.formats.LongDoubleTextVertexValueInputFormat -vip $NODES -eif org.apache.giraph.io.formats.LongFloatTextEdgeInputFormat -eip $EDGES -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op weakties_output -ca giraph.maxNumberOfSupersteps=100,VertexValue.threshold=$THRESHOLD -w 15


