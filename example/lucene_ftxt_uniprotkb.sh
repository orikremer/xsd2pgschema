#!/bin/bash

LUCENE_IDX_DIR=lucene_index

if [ ! -d $LUCENE_IDX_DIR ] ; then

 echo $LUCENE_IDX_DIR is not directory.
 exit 1

fi

LUCENE_FTXT_DIR=lucene_ftxt

rm -rf $LUCENE_FTXT_DIR

java -classpath ../xsd2pgschema.jar luceneidx2ftxt --idx-dir $LUCENE_IDX_DIR --ftxt-dir $LUCENE_FTXT_DIR --dic dictionary

java -classpath ../xsd2pgschema.jar luceneidx2ftxt --idx-dir $LUCENE_IDX_DIR --ftxt-dir $LUCENE_FTXT_DIR --dic dictionary.aut\
 --field personType.name

java -classpath ../xsd2pgschema.jar luceneidx2ftxt --idx-dir $LUCENE_IDX_DIR --ftxt-dir $LUCENE_FTXT_DIR --dic dictionary.pol\
 --field evidencedStringType.content

java -classpath ../xsd2pgschema.jar luceneidx2ftxt --idx-dir $LUCENE_IDX_DIR --ftxt-dir $LUCENE_FTXT_DIR --dic dictionary.org\
 --field organismNameType.content --field dbReferenceType.id

