#!/bin/bash

sync_update=true

if [ ! `which psql` ] ; then

 echo "psql: command not found..."
 echo "Please install PostgreSQL (http://www.postgresql.org/)."

 exit 1

fi

DB_NAME=uniprotkb
DB_USER=$USER

psql -U $DB_USER -l | grep $DB_NAME > /dev/null

if [ $? != 0 ] ; then

 echo "database \"$DB_NAME\" does not exist."

 exit 1

fi

XML_DIR=uniprot_xml

XSD_SCHEMA=uniprot.xsd
DB_SCHEMA=uniprot.schema

java -classpath ../xsd2pgschema.jar xsd2pgschema --xsd $XSD_SCHEMA --ddl $DB_SCHEMA

echo
echo "Do you want to update $DB_NAME? (y [n]) "

read ans

case $ans in
 y*|Y*) ;;
 *) echo stopped.
  exit 1;;
esac

relations=`psql -d $DB_NAME -U $DB_USER -c "\d" | wc -l`

if [ $sync_update != "true" ] || [ $relations = "0" ] ; then
 sync_update=false
 psql -d $DB_NAME -U $DB_USER -f $DB_SCHEMA --quiet
fi

WORK_DIR=pg_work

if [ $sync_update != "true" ] ; then
 CSV_DIR=$WORK_DIR/csv
fi

MD5_DIR=chk_sum_pgsql
ERR_DIR=$WORK_DIR/err

rm -rf $WORK_DIR

mkdir -p $WORK_DIR

if [ $sync_update != "true" ] ; then
 mkdir -p $CSV_DIR
fi

mkdir -p $ERR_DIR

err_file=$ERR_DIR/all_err

if [ $sync_update != "true" ] ; then

 java -classpath ../xsd2pgschema.jar xml2pgcsv --xsd $XSD_SCHEMA --xml $XML_DIR --csv-dir $CSV_DIR --sync $MD5_DIR --db-name $DB_NAME --db-user $DB_USER 2> $err_file

else

 java -classpath ../xsd2pgschema.jar xml2pgsql --xsd $XSD_SCHEMA --xml $XML_DIR --sync $MD5_DIR --db-name $DB_NAME --db-user $DB_USER 2> $err_file

fi

if [ $? = 0 ] && [ ! -s $err_file ] ; then

 rm -f $err_file

 if [ $sync_update != "true" ] ; then
  rm -rf $CSV_DIR
 fi

else

 echo "$0 aborted."
 exit 1

fi

red='\e[0;31m'
normal='\e[0m'

errs=`ls $ERR_DIR/*_err 2> /dev/null | wc -l`

if [ $errs = 0 ] ; then

 rm -rf $WORK_DIR

 echo "Database ($DB_NAME) is update."

else

 echo
 echo -e "${red}$errs errors were detected. Please check the log files for more details.${normal}"
 exit 1

fi

date

