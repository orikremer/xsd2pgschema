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
 * Enumerator of hash key size
 * @author yokochi
 */
public enum PgHashSize {

	unsigned_long_64, // unsigned long 64bit (default)
	unsigned_int_32, // unsigned int 32bit
	native_default, // algorithm dependent
	debug_string; // debug mode (no hash)

	/**
	 * Return default hash size
	 * @return PgHashSize default value
	 */
	public static PgHashSize defaultSize() {
		return unsigned_long_64;
	}

	/**
	 * Return hash size
	 * @param name name of hash size
	 * @return PgHashSize matched hash size
	 */
	public static PgHashSize getPgHashSize(String name) {

		name = name.toLowerCase();

		for (PgHashSize hash_size : values()) {

			if (hash_size.name().contains(name))
				return hash_size;

		}

		return defaultSize();
	}

}
