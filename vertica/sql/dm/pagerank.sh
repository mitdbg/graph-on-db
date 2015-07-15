#! /bin/bash


ITERATIONS=$1
NODE_COUNT=$2

#PREPROCESS_COND=$3
NODE_FILTER_COND="a6=4"
EDGE_FILTER_COND="type='Family'"
EDGE="edge"

VSQL="/opt/vertica/bin/vsql -d db_alekh2 -w db_alekh -U dbadmin -q"


INITIALIZE="UPDATE node SET value=1.0/$NODE_COUNT; COMMIT;"

# /*+ direct */
OUTBOUND_PR="CREATE TABLE node_outbound AS
		SELECT  id, value/Count(to_node) AS new_value 
		FROM $EDGE AS edge,node 
		WHERE edge.from_node=node.id
		GROUP BY id,value;"
#		ORDER BY id
#		SEGMENTED BY HASH(id) ALL NODES OFFSET 0;
#	     SELECT ANALYZE_STATISTICS ('node_outbound');"

INCOMING_PR="CREATE TABLE node_prime AS
		SELECT to_node AS id, 0.15/$NODE_COUNT + 0.85*SUM(new_value) AS value 
		FROM $EDGE AS edge,node_outbound 
		WHERE edge.from_node=node_outbound.id
		GROUP BY to_node;"
#		ORDER BY id
#		SEGMENTED BY HASH(id) ALL NODES OFFSET 0;"
#	     SELECT ANALYZE_STATISTICS ('node_prime');"



init(){
  $VSQL -c "$INITIALIZE"
}


pr_loop(){
  for c in $(seq 1 $ITERATIONS)
  do
    #$VSQL -c "explain SELECT  id, value/Count(to_node) AS new_value 
    #            FROM $EDGE AS edge,node 
    #            WHERE edge.from_node=node.id 
    #            GROUP BY id,value;"
    echo -e "$OUTBOUND_PR" | $VSQL
    #$VSQL -c "explain SELECT to_node AS id, 0.15/$NODE_COUNT + 0.85*SUM(new_value) AS value 
    #            FROM $EDGE AS edge,node_outbound 
    #            WHERE edge.from_node=node_outbound.id 
    #            GROUP BY to_node;"
    echo -e "$INCOMING_PR" | $VSQL
    echo "DROP TABLE node; DROP TABLE node_outbound; ALTER TABLE node_prime RENAME TO node;" | $VSQL

    #$VSQL -c "explain SELECT  id, value/Count(to_node) AS new_value 
    #            FROM $EDGE AS edge,node 
    #            WHERE edge.from_node=node.id 
    #            GROUP BY id,value;"
    #$VSQL -c "$OUTBOUND_PR"
    #$VSQL -c "explain SELECT to_node AS id, 0.15/$NODE_COUNT + 0.85*SUM(new_value) AS value 
    #            FROM $EDGE AS edge,node_outbound 
    #            WHERE edge.from_node=node_outbound.id 
    #            GROUP BY to_node;"
    #$VSQL -c "$INCOMING_PR"
    #$VSQL -c "DROP TABLE node; DROP TABLE node_outbound; ALTER TABLE node_prime RENAME TO node;"
  done
}

#time init
#time pr_loop
#echo $PREPROCESS_COND

pr_loop


