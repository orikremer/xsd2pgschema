/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018-2020 Masashi Yokochi

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

package net.sf.xsd2pgschema.serverutil;

import java.io.Serializable;

import org.nustaq.serialization.FSTConfiguration;

import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.option.PgSchemaOption;

/**
 * Query object of PgSchema server.
 *
 * @author yokochi
 */
public class PgSchemaServerQuery implements Serializable {

	/** The default serial version ID. */
	private static final long serialVersionUID = 1L;

	/** The version number. */
	public final String version = PgSchemaUtil.version;

	/** The PgSchema server query type. */
	public PgSchemaServerQueryType type;

	/** The default schema location. */
	public String def_schema_location = null;

	/** The PostgreSQL data model option. */
	public PgSchemaOption option = null;

	/** The serialized PostgreSQL data model. */
	public byte[] schema_bytes = null;

	/** The PgSchema client type. */
	public PgSchemaClientType client_type = null;

	/** The original caller class name (optional). */
	public String original_caller = null;

	/**
	 * Instance of PgSchemaServerQuery.
	 *
	 * @param type query type
	 */
	public PgSchemaServerQuery(PgSchemaServerQueryType type) {

		this.type = type;

	}

	/**
	 * Instance of PgSchemaServerQuery as GET/MATCH query.
	 *
	 * @param type query type should be either GET or MATCH
	 * @param option PostgreSQL data model option
	 * @param client_type PgSchema client type
	 */
	public PgSchemaServerQuery(PgSchemaServerQueryType type, PgSchemaOption option, PgSchemaClientType client_type) {

		this.type = type;
		def_schema_location = option.root_schema_location;
		this.option = option;
		this.client_type = client_type;

	}

	/**
	 * Instance of PgSchemaServerQuery as ADD/UPDATE query.
	 *
	 * @param type query type should be either ADD or UPDATE
	 * @param fst_conf FST configuration
	 * @param schema PostgreSQL data model
	 * @param client_type PgSchema client type
	 * @param original_caller original caller class name
	 */
	public PgSchemaServerQuery(PgSchemaServerQueryType type, FSTConfiguration fst_conf, PgSchema schema, PgSchemaClientType client_type, String original_caller) {

		this.type = type;
		def_schema_location = schema.getDefaultSchemaLocation();
		option = schema.option;
		schema_bytes = fst_conf.asByteArray(schema);
		this.client_type = client_type;
		this.original_caller = original_caller;

	}

}
