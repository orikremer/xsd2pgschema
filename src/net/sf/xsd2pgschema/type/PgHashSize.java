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

package net.sf.xsd2pgschema.type;

/**
 * Enumerator of hash key size.
 *
 * @author yokochi
 */
public enum PgHashSize {

	/** The unsigned long 64 bit. */
	unsigned_long_64,
	/** The unsigned int 32 bit. */
	unsigned_int_32,
	/** The native default bit. */
	native_default,
	/** The debug string (no hashing). */
	debug_string;

	/**
	 * Return default hash size.
	 *
	 * @return PgHashSize the default hash size
	 */
	public static PgHashSize defaultSize() {
		return unsigned_long_64;
	}

	/**
	 * Return hash size.
	 *
	 * @param name name of hash size
	 * @return PgHashSize matched hash size
	 */
	public static PgHashSize getSize(String name) {

		name = name.toLowerCase();

		for (PgHashSize hash_size : values()) {

			if (hash_size.name().contains(name))
				return hash_size;

		}

		return defaultSize();
	}

	/**
	 * Return name of hash key size.
	 *
	 * @return String the name of hash key size
	 */
	public String getName() {

		switch (this) {
		case debug_string:
			return this.name().replace('_', ' ');
		default:
			return this.name().replace('_', ' ') + " bits";
		}

	}

}
