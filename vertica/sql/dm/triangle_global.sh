#! /bin/bash

PREPROCESS_COND=$1

QUERY="SELECT count(*)
	FROM edge_meta e1
	JOIN edge_meta e2 ON e1.to_node=e2.from_node AND e1.from_node < e1.to_node AND e2.from_node < e2.to_node
	JOIN edge_meta e3 ON e2.to_node=e3.from_node AND e3.to_node=e1.from_node 
	WHERE e3.to_node < e3.from_node
	AND e1.$PREPROCESS_COND AND e2.$PREPROCESS_COND AND e3.$PREPROCESS_COND;"


echo -e "\\\timing\n$QUERY" | $VSQL
