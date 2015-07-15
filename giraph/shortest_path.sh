#!/bin/bash

START=$1
END=$2

NODES=nodes
EDGES=edges


$HADOOP_HOME/bin/hadoop dfs -rmr sp_output

time $HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.SimpleShortestPathsVertex -vif org.apache.giraph.io.formats.LongDoubleTextVertexValueInputFormat -vip $NODES -eif org.apache.giraph.io.formats.LongFloatTextEdgeInputFormat -eip $EDGES -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op sp_output -ca SimpleShortestPathsVertex.sourceId=$START,giraph.maxNumberOfSupersteps=100 -w 15

