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
 * Enumerator of mapping of decimal numbers in PostgreSQL.
 *
 * @author yokochi
 */
public enum PgDecimalType {

	/** The double precision 64 bit. (PG data type: DOUBLE PRECISION) */
	double_precision_64,
	/** The single precision 32 bit. (PG data type: REAL) */
	single_precision_32,
	/** The BigDecimal. (PG data type: DECIMAL) */
	big_decimal;

	/**
	 * Return default mapping of decimal numbers.
	 *
	 * @return PgDecimal the default mapping
	 */
	public static PgDecimalType defaultType() {
		return big_decimal;
	}

	/**
	 * Return mapping name of decimal numbers.
	 *
	 * @return String the mapping name
	 */
	public String getName() {

		switch (this) {
		case big_decimal:
			return this.name().replace('_', ' ');
		default:
			return this.name().replace('_', ' ') + " bits";
		}

	}

	/**
	 * Return PostgreSQL data type of decimal numbers.
	 *
	 * @return String the PostgreSQL data type
	 */
	public String getPgDataType() {

		switch (this) {
		case double_precision_64:
			return "DOUBLE PRECISION";
		case single_precision_32:
			return "REAL";
		case big_decimal:
			return "DECIMAL";
		}

		return null;
	}

	/**
	 * Return SQL data type of decimal numbers.
	 *
	 * @return int the SQL data type
	 */
	public int getSqlDataType() {

		switch (this) {
		case double_precision_64:
			return java.sql.Types.DOUBLE;
		case single_precision_32:
			return java.sql.Types.FLOAT;
		case big_decimal:
			return java.sql.Types.DECIMAL;
		}

		return -1;
	}

}
