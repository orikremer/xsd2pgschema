#!/bin/bash

DB_NAME=uniprot_sprot

DB_FTP=ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete
DB_GZ=$DB_NAME.xml.gz

WGET_LOG=wget.log

wget -c -m ftp://$DB_FTP/$DB_GZ -o $WGET_LOG

if [ $? != 0 ] ; then

 cat $WGET_LOG
 exit 1

fi

grep 'not retrieving' $WGET_LOG > /dev/null

if [ $? = 0 ] ; then

 echo $DB_NAME is update.
 exit 0

fi

XSD_SCHEMA=uniprot.xsd

if [ ! -e $XSD_SCHEMA ] ; then

 wget http://www.uniprot.org/docs/$XSD_SCHEMA

fi

date

./split_uniprotkb.sh

./clone_uniprotkb.sh

./lucene_index_uniprotkb.sh

./lucene_ftxt_uniprotkb.sh

./sphinx_shard_uniprotkb.sh

./eval_xpath_uniprotkb.sh

