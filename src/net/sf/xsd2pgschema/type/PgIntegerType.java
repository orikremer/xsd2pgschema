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

package net.sf.xsd2pgschema.type;

/**
 * Enumerator of mapping of integer numbers in PostgreSQL.
 *
 * @author yokochi
 */
public enum PgIntegerType {

	/** The signed long 64 bits. (PG data type: BIGINT) */
	signed_long_64,
	/** The signed int 32 bits. (PG data type: INTEGER) */
	signed_int_32,
	/** The BigInteger. (PG data type: DECIMAL with scale=0) */
	big_integer;

	/**
	 * Return default mapping of integer numbers.
	 *
	 * @return PgInteger the default mapping
	 */
	public static PgIntegerType defaultType() {
		return signed_int_32;
	}

	/**
	 * Return mapping name of integer numbers.
	 *
	 * @return String the mapping name
	 */
	public String getName() {

		switch (this) {
		case big_integer:
			return this.name().replace('_', ' ');
		default:
			return this.name().replace('_', ' ') + " bits";
		}

	}

	/**
	 * Return PostgreSQL data type of integer numbers.
	 *
	 * @return String the PostgreSQL data type
	 */
	public String getPgDataType() {

		switch (this) {
		case signed_long_64:
			return "BIGINT";
		case signed_int_32:
			return "INTEGER";
		case big_integer:
			return "DECIMAL";
		}

		return null;
	}

	/**
	 * Return SQL data type of integer numbers.
	 *
	 * @return int the SQL data type
	 */
	public int getSqlDataType() {

		switch (this) {
		case signed_long_64:
			return java.sql.Types.BIGINT;
		case signed_int_32:
			return java.sql.Types.INTEGER;
		case big_integer:
			return java.sql.Types.DECIMAL;
		}

		return -1;
	}

}
