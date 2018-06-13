/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2018 Masashi Yokochi

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
 * Enumerator of serial key size.
 *
 * @author yokochi
 */
public enum PgSerSize {

	/** The unsigned int 32 bit. */
	unsigned_int_32,
	/** The unsigned short 16 bit. */
	unsigned_short_16;

	/**
	 * Return default serial key size.
	 *
	 * @return PgSerSize the default serial key size
	 */
	public static PgSerSize defaultSize() {
		return unsigned_int_32;
	}

	/**
	 * Return serial key size.
	 *
	 * @param name name of serial key size
	 * @return PgSerSize matched serial key size
	 */
	public static PgSerSize getSize(String name) {

		name = name.toLowerCase();

		for (PgSerSize ser_size : values()) {

			if (ser_size.name().contains(name))
				return ser_size;

		}

		return defaultSize();
	}

}
