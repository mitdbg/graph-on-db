#!/bin/sh

BASEDIR=$(dirname $0)
VSQL_CMD="/opt/vertica/bin/vsql -d db_alekh -w db_alekh -U dbadmin"

initialize(){
  NODE_TABLE=$1
  EDGE_TABLE=$2
  #$VSQL_CMD -c "CREATE TABLE node AS SELECT * FROM $NODE_TABLE ORDER BY Id SEGMENTED BY Hash(id) ALL NODES;"
  #$VSQL_CMD -c "CREATE TABLE node AS SELECT * FROM $NODE_TABLE;"
  export EDGE="$EDGE_TABLE"
  #$VSQL_CMD -c "ALTER TABLE $EDGE_TABLE RENAME TO edge;"
  #$VSQL_CMD -c "SELECT ANALYZE_STATISTICS ('');"
  #$VSQL_CMD -c "select clear_caches();"
}

finalize(){
  NODE_TABLE=$1
  EDGE_TABLE=$2
  #$VSQL_CMD -c "DROP TABLE node;"
}

do_run(){
  QUERY=$1
  NODE_TABLE=$2
  EDGE_TABLE=$3
  PARAMS=$4
  export VSQL="$VSQL_CMD -q"
  initialize $NODE_TABLE $EDGE_TABLE
  $BASEDIR/$QUERY $PARAMS
  finalize $NODE_TABLE $EDGE_TABLE
}


do_run pagerank.sh twitter_node twitter_edge "10 41652230"
#do_run shortest_path.sh twitter_node livejournal_edge_segTo_b0 "55542254 43359637"
#do_run triangle_global.sh friendster_node friendster_edge
#do_run triangle_pernode.sh friendster_node friendster_edge_seg_from
#do_run strong_overlap.sh friendster_node friendster_edge 100
#do_run weak_ties.sh friendster_node friendster_edge 1000

#do_run connected_components.sh twitter_node twitter_edge_segTo_b0
#do_run linerank.sh livejournal_node edge "10 1468365182"
