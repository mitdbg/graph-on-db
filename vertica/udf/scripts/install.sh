#! /bin/sh


PARAMS=$1

SRC="GraphAlgos"
#PARAMS="SORT_BASED|HASH_BASED;RADIX_SORT|QUICK_SORT;DEBUG;SORT_INITIAL|UNSORT_INITIAL"


PWD=`pwd`
VSQL="/opt/vertica/bin/vsql -d db_alekh1 -w db_alekh -U dbadmin"
BASEDIR=$(dirname $0)
FUNC_OBJ="$PWD/$BASEDIR/TransformFunctions.so"


DEFS=`echo $PARAMS | awk '{split($0,arr,",");s="";for(i=1;i<=length(arr);i++){s=s"-D "arr[i]" "};print s}'`

cd $BASEDIR
make ARGS="-D SORT_BASED -D DEBUG -D DISTRIBUTED" SRC="$SRC"

$VSQL -c "DROP transform function IF EXISTS pregel_pr(int,int,float);"
$VSQL -c "DROP transform function IF EXISTS pregel_sp(int,int,float);"
$VSQL -c "DROP transform function IF EXISTS pregel_cc(int,int,float);"
$VSQL -c "DROP LIBRARY IF EXISTS TransformFunctions;"

$VSQL -c "CREATE LIBRARY TransformFunctions AS '$FUNC_OBJ';"

$VSQL -c "CREATE TRANSFORM FUNCTION pregel_pr as language 'C++' NAME 'PregelPageRankFactory' LIBRARY TransformFunctions;"
$VSQL -c "CREATE TRANSFORM FUNCTION pregel_sp as language 'C++' NAME 'PregelShortestPathFactory' LIBRARY TransformFunctions;"
$VSQL -c "CREATE TRANSFORM FUNCTION pregel_cc as language 'C++' NAME 'PregelConnectedComponentFactory' LIBRARY TransformFunctions;"

