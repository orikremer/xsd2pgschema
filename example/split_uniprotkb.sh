#!/bin/bash

DB_NAME=uniprot_sprot

DB_FTP=ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete
DB_GZ=$DB_NAME.xml.gz

DB_XML=$DB_NAME.xml

if [ ! -e $DB_XML ] ; then

 if [ ! -e $DB_FTP/$DB_GZ ] ; then

  echo Not found $DB_FTP/$DB_GZ
  exit 1

 fi

 cp $DB_FTP/$DB_GZ .
 gunzip $DB_GZ

fi

XML_DIR=uniprot_xml

XSD_SCHEMA=uniprot.xsd

java -classpath ../xsd2pgschema.jar xmlsplitter --xsd $XSD_SCHEMA --xml $DB_XML --xml-dir $XML_DIR --xpath-doc-key /uniprot/entry/accession

