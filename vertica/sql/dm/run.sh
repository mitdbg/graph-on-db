#! /bin/sh

DATA=$1
CMD=$2

BASEDIR=$(dirname $0)
DATA_DIR=/home/alekh/graphs/data/$DATA
VERTICA_SDK="/opt/vertica/sdk"
VSQL_CMD="/opt/vertica/bin/vsql -d db_alekh3 -w db_alekh -U dbadmin"


if [ "$CMD" = "load" ]; then
  export NODES=$BASEDIR/nodes.tmp
  export EDGES=$BASEDIR/edges.tmp
  cat $DATA_DIR/nodes | awk '{print $0" 0"}' > $NODES
  cat $DATA_DIR/edges | tr '\t' ' ' > $EDGES
  g++ -I $VERTICA_SDK/include -I HelperLibraries -g -Wall -Wno-unused-value -shared -fPIC -std=gnu++0x -O3 -o MyFunctions.so MyFunctions.cpp $VERTICA_SDK/include/Vertica.cpp
  $VSQL_CMD -q -f $BASEDIR/load.sql
  rm $NODES $EDGES MyFunctions.so
elif [ "$CMD" = "query" ]; then
  QUERY=$3
  case $QUERY in 
    pagerank) 
	P_iterations=10
	P_numNodes=`wc -l $DATA_DIR/nodes | awk '{print $1}'` ;;
    shortest_path) 
	P_startNode=`sed -n 1p $DATA_DIR/nodes.rand`
	P_endNode=`sed -n 2p $DATA_DIR/nodes.rand` ;;
    strong_overlap) 
	P_threshold=100 ;;
    triangle_global) ;;
    triangle_pernode) ;;
    weak_ties) 
	P_threshold=1000 ;;
    *) 
      echo "Unknown query: $QUERY"
      exit 1
  esac
  PARAMS=`( set -o posix ; set ) | grep "^P_.*" | sed 's/^.*=//' | awk 'BEGIN{params=""}{params=params""$0" "}END{print params}'`
  export VSQL="$VSQL_CMD -q"
  $BASEDIR/$QUERY.sh $PARAMS
else
  echo "Invalid command: $CMD"
fi

