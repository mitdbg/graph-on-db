#! /bin/sh

#BASEDIR=$(dirname $0)
DDL_DIR="/home/alekh/graphs/scripts/vertica"
VERTICA="/opt/vertica"
VERTICA_ADMIN="/home/dbadmin"
VSQL="$VERTICA/bin/vsql -d db_alekh1 -w db_alekh -U dbadmin"

declare -a to_delete=()
TMP="tmp"


process_data(){
  CMD=$1
  INPUT=$2
  OUTPUT=$3
  eval "head -10000000000 $INPUT | $CMD > $INPUT.tmp"
  mv $INPUT.tmp $OUTPUT
}

check_table(){
  NAME=$1
  TBL_COUNT=`$VSQL -A -t -c "select count(*) from tables where table_name='$NAME';"`
  echo $TBL_COUNT
}

create_table(){
  NAME=$1
  DEF=$2
  DATA=$3
  echo "DROP TABLE IF EXISTS $NAME CASCADE;" >> $TMP
  echo "CREATE TABLE $NAME $DEF;" >> $TMP
  #echo "COPY $NAME FROM '$DATA' DIRECT NO ESCAPE DELIMITER ' ' NULL 'null';" >> $TMP
  if [ -f $DATA ];
  then
    echo "COPY $NAME FROM '$DATA' DELIMITER E'\t' NULL 'null';" >> $TMP
  fi
}

drop_table(){
  NAME=$1
  echo "DROP TABLE IF EXISTS $NAME CASCADE;" >> $TMP
}

create_projection(){
  NAME=$1
  DEF=$2
  echo "DROP PROJECTION IF EXISTS $NAME;" >> $TMP
  echo "CREATE PROJECTION $NAME AS $DEF;" >> $TMP
  echo "SELECT start_refresh();" >> $TMP
}

create_function(){
  F_NAME=$1
  F_DEF=$2
  F_FACTORY=$3
  L_NAME=$4
  L_FILE=$5

  # HARD CODED
  echo "DROP transform function IF EXISTS pregel(Integer, Integer, Float, Integer, Float, Integer, Integer);" >> $TMP
  echo "DROP transform function IF EXISTS pregel(Integer, Integer, Float, Integer, Float, Integer);" >> $TMP
  echo "DROP transform function IF EXISTS pregel(Integer, Integer, Float, Float, Integer, Integer);" >> $TMP
  echo "DROP transform function IF EXISTS pregel(Integer, Float, Varchar, Float);" >> $TMP

  echo "DROP transform function IF EXISTS $F_NAME$F_DEF;" >> $TMP
  echo "DROP LIBRARY IF EXISTS $L_NAME;" >> $TMP
  echo "CREATE LIBRARY $L_NAME AS '$L_FILE';" >> $TMP
  echo "create transform function $F_NAME as language 'C++' name '$F_FACTORY' library $L_NAME not fenced;" >> $TMP
}

create_procedure(){
  PROC=$1
  SCRIPT=$2
  SCRIPT_NAME=`basename $SCRIPT`
  sudo -u dbadmin $DDL_DIR/install_procedure.sh $SCRIPT $VERTICA_ADMIN
  echo "DROP PROCEDURE IF EXISTS $PROC;" >> $TMP
  echo "CREATE PROCEDURE $PROC AS '$SCRIPT_NAME' LANGUAGE 'external' USER 'dbadmin';" >> $TMP
}


# should ideally go into a new file for DML operations
run_sql(){
  STATEMENT=$1
  echo "$STATEMENT;" >> $TMP
}

run_ddl(){
  echo "SELECT ANALYZE_STATISTICS ('');" >> $TMP
  echo "select clear_caches();" >> $TMP
  $VSQL < $TMP
  rm $TMP
}


# Garbage Collection Utils

garbage(){
  to_delete[$((${#to_delete[@]}+1))]=$1
}

clean(){
  for (( i=1; i<=${#to_delete[@]}; i++ ))
  do
    echo "removing: ${to_delete[$i]}"
    rm ${to_delete[$i]}
  done
}
