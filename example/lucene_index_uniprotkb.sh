#!/bin/bash

sync_update=true

XML_DIR=uniprot_xml

XSD_SCHEMA=uniprot.xsd

IDX_DIR=lucene_index

if [ -d $IDX_DIR ] ; then

 echo
 echo "Do you want to update lucene index? (y [n]) "

 read ans

 case $ans in
  y*|Y*) ;;
  *) echo stopped.
   exit 1;;
 esac

 if [ $sync_update != "true" ] ; then
  rm -rf $IDX_DIR
 fi

fi

WORK_DIR=lucene_work
ERR_DIR=$WORK_DIR/err

rm -rf $WORK_DIR

mkdir -p $WORK_DIR
mkdir -p $ERR_DIR

if [ $sync_update = "true" ] ; then
 MD5_DIR=chk_sum_lucene
fi

err_file=$ERR_DIR/all_err

if [ $sync_update != "true" ] ; then

 java -classpath ../xsd2pgschema.jar xml2luceneidx --xsd $XSD_SCHEMA --xml $XML_DIR --idx-dir $IDX_DIR --attr-all --no-rel 2> $err_file

else

 java -classpath ../xsd2pgschema.jar xml2luceneidx --xsd $XSD_SCHEMA --xml $XML_DIR --idx-dir $IDX_DIR --attr-all --no-rel --sync $MD5_DIR 2> $err_file

fi

if [ $? = 0 ] && [ ! -s $err_file ] ; then

 rm -f $err_file

else

 echo "$0 aborted."
 exit 1

fi

red='\e[0;31m'
normal='\e[0m'

errs=`ls $ERR_DIR/*_err 2> /dev/null | wc -l`

if [ $errs = 0 ] ; then

 echo "Lucene index (UniProtKB) is update."

 rm -rf $WORK_DIR

else

 echo
 echo -e "${red}$errs errors were detected. Please check the log files for more details.${normal}"
 exit 1

fi

date

