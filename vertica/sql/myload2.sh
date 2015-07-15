#! /bin/sh

DATA=$1
NODE_FILE="tmp.nodes"
EDGE_FILE="tmp.edges"

VSQL_CMD="/opt/vertica/bin/vsql -d db_alekh -w db_alekh -U dbadmin"
PWD=`pwd`


cat ~/graphs/"$DATA"_node | tr '\t' ' ' | awk '{print $0" 0"}' > $NODE_FILE
cat ~/graphs/"$DATA"_edge | tr '\t' ' ' > $EDGE_FILE


CREATE_NODE="DROP TABLE IF EXISTS "$DATA"_node_meta; CREATE TABLE "$DATA"_node_meta (id int NOT NULL PRIMARY KEY, weight int not null, value int NOT NULL) ORDER BY Id SEGMENTED BY Hash(id) ALL NODES; COPY "$DATA"_node_meta FROM '$PWD/$NODE_FILE' DELIMITER ' ' NULL 'null';"
CREATE_EDGE="DROP TABLE IF EXISTS "$DATA"_edge_meta; CREATE TABLE "$DATA"_edge_meta (from_node INT NOT NULL, to_node INT NOT NULL, type char(10), weight int) ORDER BY from_node SEGMENTED BY Hash(from_node) ALL NODES; COPY "$DATA"_edge_meta FROM '$PWD/$EDGE_FILE' DELIMITER ' ' NULL 'null';"



$VSQL_CMD -c "$CREATE_NODE"
$VSQL_CMD -c "$CREATE_EDGE"

rm $NODE_FILE $EDGE_FILE
