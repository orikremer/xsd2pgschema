/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2019 Masashi Yokochi

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
 * Enumerator of mapping of xs:date in PostgreSQL.
 *
 * @author yokochi
 */
public enum PgDateType {

	/** The timestamp as same as xs:dateTime. (PG data type: TIMESTAMP) */
	timestamp,
	/** The date without time part. (PG data type: DATE) */
	date;

	/**
	 * Return default mapping of xs:date.
	 *
	 * @return PgInteger the default mapping
	 */
	public static PgDateType defaultType() {
		return date;
	}

	/**
	 * Return mapping name of xs:date.
	 *
	 * @return String the mapping name
	 */
	public String getName() {
		return this.name();
	}

	/**
	 * Return PostgreSQL data type of xs:date.
	 *
	 * @param wo_tz with or without timezone
	 * @return String the PostgreSQL data type
	 */
	public String getPgDataType(boolean wo_tz) {

		switch (this) {
		case timestamp:
			return wo_tz ? "TIMESTAMP" : "TIMESTAMP WITH TIME ZONE";
		case date:
			return "DATE";
		}

		return null;
	}

	/**
	 * Return SQL data type of xs:date.
	 *
	 * @param wo_tz with or without timezone
	 * @return int the SQL data type
	 */
	public int getSqlDataType(boolean wo_tz) {

		switch (this) {
		case timestamp:
			return wo_tz ? java.sql.Types.TIMESTAMP : java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
		case date:
			return java.sql.Types.DATE;
		}

		return -1;
	}

}
