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
 * Implementation of PgSchema server.
 *
 * @author yokochi
 */
public class PgSchemaServerImpl {

	/** The PostgreSQL data model option. */
	public PgSchemaOption option;

	/** The serialized PostgreSQL data model. */
	public byte[] schema_bytes;

	/** The original caller class name (optional). */
	public String original_caller;

	/** The last access time in milliseconds (internal use only). */
	private long last_access_time_millis;

	/**
	 * Instance of PgSchemaServerImpl.
	 *
	 * @param query PgSchema server query
	 */
	public PgSchemaServerImpl(PgSchemaServerQuery query) {

		option = query.option;
		schema_bytes = query.schema_bytes;
		original_caller = query.original_caller;

		touch();

	}

	/**
	 * Return whether PostgreSQL data model is obsolete.
	 *
	 * @param current_time_millis current time in milliseconds
	 * @param lifetime_millis lifetime of PostgreSQL data model in milliseconds
	 * @return boolean whether PostgreSQL data model is obsolete
	 */
	public boolean isObsolete(long current_time_millis, long lifetime_millis) {
		return current_time_millis - last_access_time_millis > lifetime_millis;
	}

	/**
	 * Return last access time in milliseconds.
	 *
	 * @return long last access time in milliseconds
	 */
	public long getLastAccessTimeMillis() {
		return last_access_time_millis;
	}

	/**
	 * Touch PostgreSQL data model.
	 */
	public void touch() {

		last_access_time_millis = System.currentTimeMillis();

	}

}
