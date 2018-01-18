/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

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
 * Pending model to be included.
 * 
 * @author yokochi
 */
public class PgPendingModel {

	/** The reference to either attribute group or model group. */
	String ref_model;

	/** The table name. */
	String table_name;

	/** The insert position. */
	int insert_position;

	/**
	 * Instantiates a new pg pending model.
	 *
	 * @param ref_model reference model name
	 * @param table_name table name
	 * @param insert_position insert position
	 */
	public PgPendingModel(String ref_model, String table_name, int insert_position) {

		this.ref_model = ref_model;
		this.table_name = table_name;
		this.insert_position = insert_position;

	}

}
