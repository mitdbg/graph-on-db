#! /bin/bash



QUERY="CREATE TABLE Triangle_Counts AS (
	SELECT e1.from_node,count(*)/2 AS triangles
          FROM edge e1
	  JOIN edge e2 ON e1.to_node=e2.from_node 
	  JOIN edge e3 ON e2.to_node=e3.from_node AND e3.to_node=e1.from_node
          GROUP BY e1.from_node
	);"


echo -e "\\\timing\n$QUERY" | $VSQL
