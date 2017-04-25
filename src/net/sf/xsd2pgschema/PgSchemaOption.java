/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2017 Masashi Yokochi

    https://sourceforge.net/projects/xsd2pgschema/

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package net.sf.xsd2pgschema;

/**
 * PostgreSQL data modeling option.
 *
 * @author yokochi
 */
public class PgSchemaOption {

	/** The relational model extension. */
	boolean rel_model_ext = true;

	/** The relational data extension. */
	boolean rel_data_ext = true;

	/** The wild card extension. */
	public boolean wild_card = true;

	/** Whether add document key in PostgreSQL DDL. */
	public boolean document_key = true;

	/** Whether add serial key in PostgreSQL DDL. */
	public boolean serial_key = false;

	/** Whether add XPath key in PostgreSQL DDL. */
	public boolean xpath_key = false;

	/** Whether retain case sensitive name in PostgreSQL DDL. */
	public boolean case_sense = true;

	/** Whether output PostgreSQL DDL. */
	public boolean ddl_output = false;

	/** Whether retain primary key/foreign key constraint in PostgreSQL DDL. */
	public boolean retain_key = true;

	/** Whether not retrieve field annotation in PostgreSQL DDL. */
	public boolean no_field_anno = true;

	/** Whether execute XML Schema validation. */
	public boolean validate = false;

	/** Whether append to existing data. */
	public boolean append = false;

	/** The name of hash algorithm. */
	public String hash_algorithm = PgSchemaUtil.def_hash_algorithm;

	/** The size of hash key. */
	public PgHashSize hash_size = PgHashSize.defaultSize();

	/** The size of serial key. */
	public PgSerSize ser_size = PgSerSize.defaultSize();

	/**
	 * Instance of PostgreSQL data modeling option.
	 *
	 * @param document_key the document key
	 */
	public PgSchemaOption(boolean document_key) {

		this.document_key = document_key;

	}

	/**
	 * Instance of PgSchemaOption for JSON Schema conversion.
	 *
	 * @param json_type JSON type
	 */
	public PgSchemaOption(JsonType json_type) {

		setDefaultForJsonSchema(json_type);

	}

	/**
	 * Default settings for JSON Schema conversion.
	 *
	 * @param json_type JSON type
	 */
	public void setDefaultForJsonSchema(JsonType json_type) {

		rel_model_ext = !json_type.equals(JsonType.relational);
		cancelRelDataExt();

	}

	/**
	 * Cancel relational model extension in PostgreSQL.
	 */
	public void cancelRelModelExt() {

		rel_model_ext = false;
		cancelRelDataExt();

	}

	/**
	 * Cancel relational data extension.
	 */
	public void cancelRelDataExt() {

		rel_data_ext = document_key = serial_key = xpath_key = retain_key = false;

	}

	/**
	 * Return minimum size of field.
	 *
	 * @return int minimum size of field
	 */
	public int getMinimumSizeOfField() {
		return (rel_model_ext ? 1 : 0) + (document_key ? 1 : 0);
	}

}
