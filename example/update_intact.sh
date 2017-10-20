#!/bin/bash

DB_NAME=intact

DB_FTP=ftp.ebi.ac.uk/pub/databases/intact/current/psi25
ZIP_FILE=pmidMIF25.zip

WGET_LOG=wget.log

XML_DIR=pmid

wget -c -m http://$DB_FTP/$ZIP_FILE -o $WGET_LOG

if [ $? != 0 ] ; then

 cat $WGET_LOG
 exit 1

fi

grep 'not retrieving' $WGET_LOG > /dev/null

if [ $? = 0 ] && [ -d $XML_DIR ] ; then

 echo $DB_NAME is update.
 exit 0

fi

grep 'No such file' $WGET_LOG > /dev/null

if [ $? = 0 ] ; then

  cat $WGET_LOG
  exit 1

fi

rm -rf $XML_DIR

if [ ! -e $DB_FTP/$ZIP_FILE ] ; then
 echo Not found $DB_FTP/$ZIP_FILE
 exit 1
fi

unzip $DB_FTP/$ZIP_FILE -d $XML_DIR

date

./clone_intact.sh

