#!/bin/bash

sync_update=true

if [ ! `which psql` ] ; then

 echo "psql: command not found..."
 echo "Please install PostgreSQL (http://www.postgresql.org/)."
 exit 1

fi

DB_NAME=intact
DB_USER=$USER

psql -U $DB_USER -l | grep $DB_NAME > /dev/null || ( echo "database \"$DB_NAME\" does not exist." && exit 1 )

XML_DIR=pmid

XSD_SCHEMA=MIF254.xsd # newer instead of MIF25.xsd

if [ ! -e $XSD_SCHEMA ] ; then

 if [ $XSD_SCHEMA = "MIF25.xsd" ] ; then
  wget http://psidev.sourceforge.net/molecular_interactions/rel25/src/$XSD_SCHEMA
 elif [ $XSD_SCHEMA = "MIF254.xsd" ] ; then
  wget http://psidev.sourceforge.net/mi/rel25/src/$XSD_SCHEMA
 else
  echo XSD_SCHEMA should be either \"MIF25.xsd\" or \"MIF254.xsd\".
  exit 1
 fi

fi

DB_SCHEMA=`basename $XSD_SCHEMA .xsd`.schema

java -classpath ../xsd2pgschema.jar xsd2pgschema --xsd $XSD_SCHEMA --ddl $DB_SCHEMA

echo
echo "Do you want to update $DB_NAME? (y [n]) "

read ans

case $ans in
 y*|Y*) ;;
 *) echo stopped.
  exit 1;;
esac

if [ ! -d $XML_DIR ] ; then

 SRC_DIR=ftp.ebi.ac.uk/pub/databases/intact/current/psi25

 mkdir -p $XML_DIR

 zip_file_list=zip_file_list
 xml_file_list=xml_file_list

 find $SRC_DIR -name '*.zip' > $zip_file_list

 while read zip_file
 do
  xml_file=`basename $zip_file .zip`
  if [ ! -e $XML_DIR/$xml_file.xml ] ; then
   yes | unzip $zip_file -d $XML_DIR
  fi
 done < $zip_file_list

 rm -f $zip_file_list

 echo done.

fi

MD5_DIR=chk_sum_pgsql

relations=`psql -d $DB_NAME -U $DB_USER -c "\d" | wc -l`

if [ $sync_update != "true" ] || [ ! -d $MD5_DIR ] || [ $relations = "0" ] ; then
 sync_udpate=false
 psql -d $DB_NAME -U $DB_USER -f $DB_SCHEMA --quiet
fi

WORK_DIR=pg_work

if [ $sync_update != "true" ] ; then
 DATA_DIR=$WORK_DIR/data
fi

ERR_DIR=$WORK_DIR/err

err_file=$ERR_DIR/all_err

rm -rf $WORK_DIR

mkdir -p $WORK_DIR

if [ $sync_update != "true" ] ; then
 mkdir -p $DATA_DIR
fi

mkdir -p $ERR_DIR

err_file=$ERR_DIR/all_err

if [ $sync_update != "true" ] ; then

 java -classpath ../xsd2pgschema.jar xml2pgtsv --xsd $XSD_SCHEMA --xml $XML_DIR --work-dir $DATA_DIR --sync $MD5_DIR --db-name $DB_NAME --db-user $DB_USER 2> $err_file

else

 java -classpath ../xsd2pgschema.jar xml2pgsql --xsd $XSD_SCHEMA --xml $XML_DIR --sync $MD5_DIR --db-name $DB_NAME --db-user $DB_USER 2> $err_file

fi

if [ $? = 0 ] && [ ! -s $err_file ] ; then

 rm -f $err_file

 if [ $sync_update != "true" ] ; then
  rm -rf $DATA_DIR
 fi

else

 echo $0 aborted.
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

