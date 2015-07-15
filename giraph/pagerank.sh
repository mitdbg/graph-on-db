#! /bin/bash

NODES=nodes
EDGES=edges



$HADOOP_HOME/bin/hadoop dfs -rmr pr_output

time $HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples-1.0.0-for-hadoop-0.20.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner core.advanced.SimplePageRankVertex -vif org.apache.giraph.io.formats.LongDoubleTextVertexValueInputFormat -vip $NODES -eif org.apache.giraph.io.formats.LongFloatTextEdgeInputFormat -eip $EDGES -of org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op pr_output -ca giraph.maxNumberOfSupersteps=100 -w 15 -wc core.advanced.SimplePageRankVertex\$SimplePageRankVertexWorkerContext -mc core.advanced.SimplePageRankVertex\$SimplePageRankVertexMasterCompute
