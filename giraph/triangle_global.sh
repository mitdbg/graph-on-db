#! /bin/bash

NODES=nodes
EDGES=edges



$HADOOP_HOME/bin/hadoop dfs -rmr triangle_output

$HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner core.advanced.TriangleCountingVertex -vif org.apache.giraph.io.formats.LongDoubleTextVertexValueInputFormat -vip $NODES -eif org.apache.giraph.io.formats.LongFloatTextEdgeInputFormat -eip $EDGES -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op triangle_output -ca giraph.maxNumberOfSupersteps=100 -w 15
