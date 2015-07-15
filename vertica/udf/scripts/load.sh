#! /bin/sh


NODES=$1
EDGES=$2

BASEDIR=$(dirname $0)
source $BASEDIR/ddl.sh

#process_data "awk '{print \$0\" 0\"}'" $NODES $NODES.proc
#process_data "tr '\\t' ' '" $EDGES $EDGES.proc

create_table V "(id INT) ORDER BY id SEGMENTED BY Hash(id) ALL NODES" $NODES
create_table E "(from_node INT, to_node INT) ORDER BY from_node SEGMENTED BY Hash(from_node) ALL NODES" $EDGES

run_SQL "ALTER TABLE V ADD COLUMN value Float DEFAULT 0;"

run_ddl
clean
#rm $NODES.proc $EDGES.proc
