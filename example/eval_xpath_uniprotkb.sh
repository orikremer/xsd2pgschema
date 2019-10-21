#!/bin/bash

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

XSD_SCHEMA=uniprot.xsd

java -classpath ../xsd2pgschema.jar xpath2xml --xsd $XSD_SCHEMA --out result.xml --db-name $DB_NAME --db-user $DB_USER \
--xpath-query "/uniprot[entry/accession/text()='B2ZDY1']"

java -classpath ../xsd2pgschema.jar xpath2json --xsd $XSD_SCHEMA --out result.json --db-name $DB_NAME --db-user $DB_USER \
--xpath-query "/uniprot[entry/accession/text()='B2ZDY1']"

