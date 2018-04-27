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
 * Pending group to be included in PostgreSQL data model.
 *
 * @author yokochi
 */
public class PgPendingGroup {

	/** The reference to either attribute group or model group. */
	protected String ref_group;

	/** The PostgreSQL schema name. */
	protected String pg_schema_name;

	/** The group name in PostgreSQL. */
	protected String name;

	/** The insert position in fields. */
	protected int insert_position;

	/**
	 * Instance of PgPendingGroup.
	 *
	 * @param ref_group reference group name
	 * @param pg_schema_name PostgreSQL schema name
	 * @param name table name
	 * @param insert_position insert position in fields
	 */
	public PgPendingGroup(String ref_group, String pg_schema_name, String name, int insert_position) {

		this.ref_group = ref_group;
		this.pg_schema_name = pg_schema_name;
		this.name = name;
		this.insert_position = insert_position;

	}

}
