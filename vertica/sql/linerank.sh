#! /bin/bash


ITERATIONS=$1
EDGE_COUNT=$2


INITIALIZE="UPDATE edge SET value=1.0/$EDGE_COUNT; COMMIT;"

OUTBOUND_LR="CREATE TABLE edge_outbound AS
		SELECT e1.from_node, e1.to_node, e1.value/COUNT(*) AS new_value
		  FROM edge AS e1, edge AS e2
		  WHERE e1.to_node=e2.from_node
		  GROUP BY e1.from_node, e1.to_node, e1.value;"

INCOMING_LR="CREATE TABLE edge_prime AS
		SELECT e.from_node, e.to_node, 0.15/68993773+0.85*SUM(e_out.new_value) as value
		  FROM edge AS e, edge_outbound AS e_out
		  WHERE e.to_node=e_out.from_node
		  GROUP BY e.from_node, e.to_node;"

FINAL_NODE_RANK="SELECT node.id, SUM(edge.value)
			FROM edge,node
			  WHERE node.id=edge.to_node
			  GROUP BY node.id"


init(){
  $VSQL -c "$INITIALIZE"
}

lr_loop(){
  for c in $(seq 1 $ITERATIONS)
  do
    echo -e "\\\timing\n$OUTBOUND_LR" | $VSQL
    echo -e "\\\timing\n$INCOMING_LR" | $VSQL
    echo -e "\\\timing\nDROP TABLE edge cascade; DROP TABLE edge_outbound; ALTER TABLE edge_prime RENAME TO edge;" | $VSQL
    #$VSQL -c "$OUTBOUND_LR"
    #$VSQL -c "$INCOMING_LR"
    #$VSQL -c "DROP TABLE edge cascade; DROP TABLE edge_outbound; ALTER TABLE edge_prime RENAME TO edge;"
  done
}

#time init
time lr_loop
