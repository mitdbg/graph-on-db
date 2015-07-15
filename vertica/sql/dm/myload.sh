#! /bin/sh

DATA=$1
NODE_FILE="tmp.nodes"
EDGE_FILE="tmp.edges"

VSQL_CMD="/opt/vertica/bin/vsql -d db_alekh2 -w db_alekh -U dbadmin"
#PWD=`pwd`


#cat ~/graphs/data/$DATA/nodes | awk '{print $0" 0"}' > $NODE_FILE
#cat ~/graphs/data/$DATA/edges | tr '\t' ' ' > $EDGE_FILE

NODE_FILE="/home/alekh/graphs/data/$DATA/nodes.augmented"
EDGE_FILE="/home/alekh/graphs/data/$DATA/edges.augmented"


#CREATE_NODE="DROP TABLE IF EXISTS "$DATA"_node; CREATE TABLE "$DATA"_node (id int NOT NULL PRIMARY KEY, value int NOT NULL) ORDER BY Id SEGMENTED BY Hash(id) ALL NODES; COPY "$DATA"_node FROM '$PWD/$NODE_FILE' DELIMITER ' ' NULL 'null';"
CREATE_NODE="DROP TABLE IF EXISTS "$DATA"_node; CREATE TABLE "$DATA"_node (Id int NOT NULL PRIMARY KEY, a1 INT, a2 INT, a3 INT, a4 INT, a5 INT, a6 INT, a7 INT, a8 INT, a9 INT, a10 INT, a11 INT, a12 INT, a13 INT, a14 INT, a15 INT, a16 INT, a17 INT, a18 INT, a19 INT, a20 INT, a21 INT, a22 INT, a23 INT, a24 INT, a25 INT, a26 INT, a27 INT, a28 INT, a29 INT, a30 INT, a31 INT, a32 INT, a33 FLOAT, a34 FLOAT, a35 FLOAT, a36 FLOAT, a37 FLOAT, a38 FLOAT, a39 FLOAT, a40 FLOAT, a41 FLOAT, a42 FLOAT, a43 FLOAT, a44 FLOAT, a45 FLOAT, a46 FLOAT, a47 FLOAT, a48 FLOAT, a49 FLOAT, a50 FLOAT, a51 CHAR(1), a52 CHAR(2), a53 CHAR(3), a54 CHAR(4), a55 CHAR(5), a56 CHAR(6), a57 CHAR(7), a58 CHAR(8), a59 CHAR(9), a60 CHAR(10)) ORDER BY Id SEGMENTED BY Hash(Id) ALL NODES; COPY "$DATA"_node FROM '$NODE_FILE' DELIMITER E'\t' NULL 'null';"

CREATE_EDGE="DROP TABLE IF EXISTS "$DATA"_edge; CREATE TABLE "$DATA"_edge (from_node INT, to_node INT, type VARCHAR(10), weight INT, timestamp CHAR(10)) ORDER BY from_node SEGMENTED BY Hash(from_node) ALL NODES; COPY "$DATA"_edge FROM '$EDGE_FILE' DELIMITER E'\t' NULL 'null';"



$VSQL_CMD -c "$CREATE_NODE"
$VSQL_CMD -c "ALTER TABLE "$DATA"_node ADD COLUMN value INT DEFAULT 0 NOT NULL;"
$VSQL_CMD -c "$CREATE_EDGE"

#rm $NODE_FILE $EDGE_FILE
