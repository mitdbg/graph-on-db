#! /bin/bash


QUERY="SELECT count(*)
	FROM edge e1
	JOIN edge e2 ON e1.to_node=e2.from_node AND e1.from_node < e1.to_node AND e2.from_node < e2.to_node
	JOIN edge e3 ON e2.to_node=e3.from_node AND e3.to_node=e1.from_node 
	WHERE e3.to_node < e3.from_node;"


echo -e "\\\timing\n$QUERY" | $VSQL
