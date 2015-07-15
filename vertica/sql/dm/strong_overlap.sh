#! /bin/bash

THRESHOLD=$1
PREPROCESS_COND=$2

# SELECT add_vertica_options('EE', 'ENABLE_JOIN_SPILL');

QUERY="CREATE TABLE overlap AS
        (SELECT e1.from_node as n1,e2.from_node as n2, count(*)
          FROM edge_meta e1
	  JOIN edge_meta e2 ON e1.to_node=e2.to_node AND e1.from_node<e2.from_node
          WHERE e1.$PREPROCESS_COND AND e2.$PREPROCESS_COND
          GROUP BY e1.from_node,e2.from_node
          HAVING count(*) > $THRESHOLD);"


echo -e "\\\timing\n$QUERY" | $VSQL
$VSQL -c "drop table overlap;"
