#!/bin/bash


INIT_QUERY1="UPDATE node SET value=id; COMMIT;"
INIT_QUERY3="CREATE TABLE node_update AS SELECT * FROM node;"


CC_QUERY_GLOBAL="CREATE TABLE new_node AS
    (SELECT e.to_node AS id, min(n.value) AS value
        FROM $EDGE AS e, node AS n
        WHERE n.id=e.from_node
        GROUP BY e.to_node
	);
SELECT count(*) FROM new_node;"

CC_QUERY_GLOBAL_INC="CREATE TABLE new_node AS
    (SELECT e.to_node AS id, min(n1.value) AS value
        FROM node AS n1, $EDGE AS e, node AS n2
        WHERE n1.id=e.from_node AND n2.id=e.to_node
        GROUP BY e.to_node
        HAVING min(n1.value) < min(n2.value));
SELECT count(*) FROM new_node;"


CC_QUERY_GLOBAL_INC2="CREATE TABLE new_node AS
    (SELECT e.from_node AS id, min(n1.value) AS value
        FROM node AS n1, $EDGE AS e, node AS n2
        WHERE n1.id=e.to_node AND n2.id=e.from_node
        GROUP BY e.from_node
        HAVING min(n1.value) < min(n2.value));
SELECT count(*) FROM new_node;"


CC_QUERY_INC="CREATE TABLE new_node AS
    (SELECT e.to_node AS id, min(n1.value) AS value
        FROM node_update AS n1, $EDGE AS e, node AS n2
        WHERE n1.id=e.from_node AND n2.id=e.to_node
        GROUP BY e.to_node
        HAVING min(n1.value) < min(n2.value));
SELECT count(*) FROM new_node;"

CC_QUERY_INC2="CREATE TABLE new_node AS
    (SELECT e.from_node AS id, min(n1.value) AS value
        FROM node_update AS n1, $EDGE AS e, node AS n2
        WHERE n1.id=e.to_node AND n2.id=e.from_node
        GROUP BY e.from_node
        HAVING min(n1.value) < min(n2.value));
SELECT count(*) FROM new_node;"

#CC_QUERY_INC="CREATE TABLE new_node AS
#    (SELECT e.to_node AS id, min(n1.value) AS value
#        FROM node_update AS n1, $EDGE AS e, node AS n2
#        WHERE (n1.id=e.from_node AND n2.id=e.to_node) OR (n1.id=e.to_node AND n2.id=e.from_node)
#        GROUP BY e.to_node
#        HAVING min(n1.value) < min(n2.value));
#SELECT count(*) FROM new_node;"


UPDATE_DISTANCES1="CREATE TABLE t AS
  (SELECT node.id,isnull(new_node.value,node.value) as value
  FROM node LEFT JOIN new_node
  ON node.id = new_node.id
  );"

UPDATE_DISTANCES2="UPDATE node SET value=new_node.value FROM new_node WHERE node.id=new_node.id;"



init(){
  $VSQL -c "$INIT_QUERY1"
  #ROW_COUNT=`echo "$INIT_QUERY2" | $VSQL -A -t`
  #echo "number of estimates updated: $ROW_COUNT"
  #if [ "$ROW_COUNT" != "1" ]; then
  #  exit 0
  #fi
  $VSQL -c "$INIT_QUERY3"
}

GBL_ITERS=4

cc_loop(){
  iteration=0
  while true; do
    iteration=$((iteration+1))
    if [ "$iteration" -le $GBL_ITERS ]; then
    #if [[ $iteration -le $GBL_ITERS ]]; then
      isEven=$(( $iteration % 2 ))
      if [ "$isEven" -eq "0" ]; then
        #ROW_COUNT=`echo "$CC_QUERY_GLOBAL_INC2;" | $VSQL -A -t`
        echo -e "$CC_QUERY_GLOBAL_INC2;" | $VSQL -A -t
      else
        #ROW_COUNT=`echo "$CC_QUERY_GLOBAL_INC;" | $VSQL -A -t`
        echo -e "$CC_QUERY_GLOBAL_INC;" | $VSQL -A -t
      fi
    elif [[ $iteration -eq $GBL_ITERS ]]; then
      #ROW_COUNT=`echo "$CC_QUERY_GLOBAL_INC;" | $VSQL -A -t`
      echo -e "$CC_QUERY_GLOBAL_INC;" | $VSQL -A -t
    else
      isEven=$(( $iteration % 2 ))
      if [ "$isEven" -eq "0" ]; then
        #ROW_COUNT=`echo "$CC_QUERY_INC;" | $VSQL -A -t`
        echo -e "$CC_QUERY_INC;" | $VSQL -A -t
      else
        #ROW_COUNT=`echo "$CC_QUERY_INC2;" | $VSQL -A -t`
        echo -e "$CC_QUERY_INC2;" | $VSQL -A -t
      fi
    fi

    ROW_COUNT=`echo "SELECT count(*) FROM new_node;" | $VSQL -A -t`

    echo "number of estimates updated: $ROW_COUNT"
    if [ "$ROW_COUNT" = "0" ]; then
      $VSQL -c "DROP TABLE new_node; DROP TABLE node_update;"
      break
    elif [ "$ROW_COUNT" -lt "10000" ]; then
      #$VSQL -c "SELECT ANALYZE_STATISTICS ('new_node');"
      echo -e "$UPDATE_DISTANCES2;commit;" | $VSQL
      #$VSQL -c "$UPDATE_DISTANCES2;commit;"
      $VSQL -c "DROP TABLE node_update; ALTER TABLE new_node RENAME TO node_update;"
    else
      echo -e "$UPDATE_DISTANCES1;" | $VSQL
      #$VSQL -c "$UPDATE_DISTANCES1;"
      $VSQL -c "DROP TABLE IF EXISTS node_update; ALTER TABLE new_node RENAME TO node_update; DROP TABLE node CASCADE; ALTER TABLE t RENAME TO node;"
    fi
  done
}

#time init
#time cc_loop
init
cc_loop

