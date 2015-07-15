#! /bin/sh

CURRENT_G=$1
NEW_G=$2


VSQL="/opt/vertica/bin/vsql -d db_alekh -w db_alekh -U dbadmin"

do_rename(){
  OLD_NAME=$1
  NEW_NAME=$2
  $VSQL -c "alter table $OLD_NAME rename to $NEW_NAME;"
}

do_rename E E_$CURRENT_G
do_rename V V_$CURRENT_G

do_rename E_$NEW_G E
do_rename V_$NEW_G V
