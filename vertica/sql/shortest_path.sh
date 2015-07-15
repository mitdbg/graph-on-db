#!/bin/bash

startNode=$1
endNode=$2

EDGE="twitterlarge_edge"
VSQL="/opt/vertica/bin/vsql -d db_alekh2 -w db_alekh -U dbadmin -q"

INIT_QUERY1="UPDATE node SET value=9999999; COMMIT;"
INIT_QUERY2="UPDATE node SET value=0 WHERE id=$startNode; COMMIT;"
INIT_QUERY3="CREATE TABLE node_update (id int, value int); INSERT INTO node_update VALUES($startNode,0); COMMIT;"

SHORTEST_PATHS_QUERY="CREATE TABLE new_node AS
    (SELECT e.to_node AS id, min(n1.value+1) AS value
        FROM node_update AS n1, $EDGE AS e, node AS n2
        WHERE n1.id=e.from_node AND n2.id=e.to_node
        GROUP BY e.to_node
        HAVING min(n1.value+1) < min(n2.value));"
#SELECT count(*) FROM new_node;"

UPDATE_DISTANCES1="CREATE TABLE t AS
  (SELECT node.id,isnull(new_node.value,node.value) as value
  FROM node LEFT JOIN new_node
  ON node.id = new_node.id
  );"

UPDATE_DISTANCES2="UPDATE node SET value=new_node.value FROM new_node WHERE node.id=new_node.id;"



init(){
  $VSQL -c "$INIT_QUERY1"
  ROW_COUNT=`echo "$INIT_QUERY2" | $VSQL -A -t`
  echo "number of estimates updated: $ROW_COUNT"
  if [ "$ROW_COUNT" != "1" ]; then
    exit 0
  fi
  $VSQL -c "$INIT_QUERY3"
}

sp_loop(){
  while true; do
    #$VSQL -c "explain SELECT e.to_node AS id, min(n1.value+1) AS value
    #    FROM node_update AS n1, $EDGE AS e, node AS n2
    #    WHERE n1.id=e.from_node AND n2.id=e.to_node
    #    GROUP BY e.to_node
    #    HAVING min(n1.value+1) < min(n2.value);"

    echo -e "$SHORTEST_PATHS_QUERY" | $VSQL -A -t
    ROW_COUNT=`$VSQL -A -t -c "select count(*) from new_node;"`

    #ROW_COUNT=`echo "$SHORTEST_PATHS_QUERY;commit;" | $VSQL -A -t`
    echo "number of estimates updated: $ROW_COUNT"
    if [ "$ROW_COUNT" = "0" ]; then
      $VSQL -c "DROP TABLE IF EXISTS new_node; DROP TABLE IF EXISTS node_update;"
      break
    elif [ "$ROW_COUNT" -lt "1000" ]; then
      #$VSQL -c "SELECT ANALYZE_STATISTICS ('new_node');"
      echo -e "$UPDATE_DISTANCES2;commit;" | $VSQL
      #$VSQL -c "$UPDATE_DISTANCES2;commit;"
      $VSQL -c "DROP TABLE IF EXISTS node_update; ALTER TABLE new_node RENAME TO node_update;"
    else
      echo -e "$UPDATE_DISTANCES1;" | $VSQL
      #$VSQL -c "$UPDATE_DISTANCES1;"
      $VSQL -c "DROP TABLE IF EXISTS node_update; ALTER TABLE new_node RENAME TO node_update; DROP TABLE IF EXISTS node CASCADE; ALTER TABLE t RENAME TO node;"
    fi
  done
}

#time init
#time sp_loop
init
sp_loop

