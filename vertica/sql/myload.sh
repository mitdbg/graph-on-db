#! /bin/sh

DATA=$1
NODE_FILE="tmp.nodes"
EDGE_FILE="tmp.edges"

VSQL_CMD="/opt/vertica/bin/vsql -d db_alekh2 -w db_alekh -U dbadmin"
#PWD=`pwd`


#cat ~/graphs/data/$DATA/nodes | awk '{print $0" 0"}' > $NODE_FILE
#cat ~/graphs/data/$DATA/edges | tr '\t' ' ' > $EDGE_FILE

NODE_FILE="/home/alekh/graphs/data/$DATA/nodes"
EDGE_FILE="/home/alekh/graphs/data/$DATA/edges"


#CREATE_NODE="DROP TABLE IF EXISTS "$DATA"_node; CREATE TABLE "$DATA"_node (id int NOT NULL PRIMARY KEY, value int NOT NULL) ORDER BY Id SEGMENTED BY Hash(id) ALL NODES; COPY "$DATA"_node FROM '$PWD/$NODE_FILE' DELIMITER ' ' NULL 'null';"
CREATE_NODE="DROP TABLE IF EXISTS "$DATA"_node; CREATE TABLE "$DATA"_node (id int NOT NULL PRIMARY KEY) ORDER BY Id SEGMENTED BY Hash(id) ALL NODES; COPY "$DATA"_node FROM '$NODE_FILE' DELIMITER ' ' NULL 'null';"
CREATE_EDGE="DROP TABLE IF EXISTS "$DATA"_edge; CREATE TABLE "$DATA"_edge (from_node INT NOT NULL, to_node INT NOT NULL) ORDER BY from_node SEGMENTED BY Hash(from_node) ALL NODES; COPY "$DATA"_edge FROM '$EDGE_FILE' DELIMITER E'\t' NULL 'null';"



$VSQL_CMD -c "$CREATE_NODE"
$VSQL_CMD -c "ALTER TABLE "$DATA"_node ADD COLUMN value INT DEFAULT 0 NOT NULL;"
$VSQL_CMD -c "$CREATE_EDGE"

#rm $NODE_FILE $EDGE_FILE
