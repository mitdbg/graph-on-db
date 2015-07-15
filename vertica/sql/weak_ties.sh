#! /bin/bash

THRESHOLD=$1


QUERY="CREATE TABLE weakties AS
	(SELECT e1.to_node AS Id, sum(case when e3.to_node is null then 1 else 0 end)/2 AS Count
	  FROM edge e1
	  JOIN edge e2 ON e1.to_node=e2.from_node AND e1.from_node<>e2.to_node
	  LEFT JOIN edge e3 ON e2.to_node=e3.from_node AND e1.from_node=e3.to_node
	  GROUP BY e1.to_node
	  HAVING sum(case when e3.to_node is null then 1 else 0 end)/2 > $THRESHOLD
	);"


echo -e "\\\timing\n$QUERY" | $VSQL
$VSQL -c "drop table weakties;"
