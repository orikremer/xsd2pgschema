#!/bin/bash

if [ ! `which indexer` ] ; then

 echo "indexer: command not found..."
 echo "Please install Sphinx (http://sphinxsearch.com/)."

 exit 1

fi

XML_DIR=uniprot_xml
SHARD_SIZE=4

XSD_SCHEMA=uniprot.xsd
PREFIX=uniprotkb

IDX_DIR=sphinx_shard
DIC_DIR=sphinx_dic

if [ -d $IDX_DIR ] ; then

 echo
 echo "Do you want to update sphinx index? (y [n]) "

 read ans

 case $ans in
  y*|Y*) ;;
  *) echo stopped.
   exit 1;;
 esac

 rm -rf $IDX_DIR

fi

mkdir -p $IDX_DIR

WORK_DIR=sphinx_work
ERR_DIR=$WORK_DIR/err

DIC_NAMES=("all" "aut" "pol" "org")
ATTRS=("--attr entry.accession --attr entry.name --attr entry.created" "" "" "")
FIELDS=("" "--field personType.name" "--field evidencedStringType.content" "--field organismNameType.content --field dbReferenceType.id")

dic_id=0

for dic_name in ${DIC_NAMES[@]} ; do

 attrs=${ATTRS[dic_id]}
 fields=${FIELDS[dic_id]}

 let dic_id++

 echo $dic_name

 rm -rf $WORK_DIR

 mkdir -p $WORK_DIR
 mkdir -p $ERR_DIR

 err_file=$ERR_DIR/all_err

 java -classpath ../xsd2pgschema.jar xml2sphinxds --xsd $XSD_SCHEMA --xml $XML_DIR --ds-dir $WORK_DIR --ds-name $PREFIX $attrs $fields --shard-size $SHARD_SIZE 2> $err_file

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

  echo

  for proc_id in `seq 1 $SHARD_SIZE` ; do

   _proc_id=`expr $proc_id - 1`

   if [ $dic_name = "all" ] ; then

    mkdir -p $IDX_DIR/"part-"$_proc_id -m 777
    indexer $PREFIX"_p"$_proc_id
    indexer $PREFIX"_p"$_proc_id --buildstops $WORK_DIR/dictionary_p$_proc_id.txt 100000 --buildfreqs

   else

    indexer $PREFIX"_"$dic_name"_p"$_proc_id
    indexer $PREFIX"_"$dic_name"_p"$_proc_id --buildstops $WORK_DIR/dictionary_p$_proc_id.txt 100000 --buildfreqs

   fi

   if [ $? != 0 ] ; then

    echo "$0 aborted."
    exit 1

   fi

  done

  mkdir -p $DIC_DIR -m 777

  DIC_FILES=

  for proc_id in `seq 1 $SHARD_SIZE` ; do

   _proc_id=`expr $proc_id - 1`
    DIC_FILES="$DIC_FILES$WORK_DIR/dictionary_p$_proc_id.txt "

  done

  SRC_DIC_FILES=`echo " "$DIC_FILES | sed -e 's/ / \-\-dic /g'`

  java -classpath ../xsd2pgschema.jar dicmerge4sphinx --ds-dir $WORK_DIR $SRC_DIC_FILES --freq 1

  if [ $dic_name = "all" ] ; then
   indexer $PREFIX"_dic"
  else
   indexer $PREFIX"_dic_"$dic_name
  fi

  if [ $? = 0 ] ; then

   echo "Sphinx index (UniProtKB:"$dic_name") is update."

   rm -rf $WORK_DIR

  fi

 else

  echo
  echo -e "${red}$errs errors were detected. Please check the log files for more details.${normal}"
  exit 1

 fi

done

date

