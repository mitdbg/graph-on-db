#! /bin/sh

QUERY=$1
PARAMS=$2

VSQL="/opt/vertica/bin/vsql -d db_alekh1 -w db_alekh -U dbadmin"
GRP_SIZE=16

#PageRank="numNodes=81306,numIterations=3"
#ShortestPath="startNode=18086960,endNode=106822258"
#ConnectedComponent=""

$VSQL -c "DROP TABLE IF EXISTS V2 CASCADE;select clear_caches();"
if [ "$?" = "0" ]; then
  PARAMETERS=""
  if [ "$PARAMS" != "" ]; then
    PARAMETERS="USING PARAMETERS $PARAMS"
  fi

  SUPERSTEP_SQL="SET SESSION RESOURCE POOL allmem; CREATE TABLE V2 AS (
        SELECT pregel_$QUERY(t.id,t.edge,t.value $PARAMETERS) OVER (PARTITION BY (t.id % $GRP_SIZE) ORDER BY t.id)
          FROM (
            SELECT from_node as id,to_node as edge,cast(null as float) as value FROM E
                UNION ALL
            SELECT id,cast(null as int) as edge,99999 as value FROM V
          ) AS t
        );"

  #SUPERSTEP_SQL="SET SESSION RESOURCE POOL allmem; CREATE TABLE V2 AS (
  #      SELECT pregel_$QUERY(t.id,t.edge,t.value $PARAMETERS) OVER (PARTITION BY (t.id % $GRP_SIZE))
  #        FROM (
  #          SELECT from_node as id,to_node as edge,cast(null as float) as value FROM E
  #              UNION ALL
  #          SELECT id,cast(null as int) as edge,99999 as value FROM V
  #        ) AS t
  #      );"


  sudo /home/alekh/dropCaches.sh
  echo -e "\\\timing\n$SUPERSTEP_SQL" | $VSQL
fi

