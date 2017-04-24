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
 * Enumerator of JSON type.
 *
 * @author yokochi
 */
public enum JsonType {

	/** The column-oriented JSON. (default) */
	column,
	/** The object-oriented JSON. */
	object,
	/** The relational-oriented JSON */
	relational;

	/**
	 * Return default JSON type.
	 *
	 * @return JsonType default JSON type
	 */
	public static JsonType defaultType() {
		return column;
	}

}
