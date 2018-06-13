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
 * Enumerator of JSON Schema version.
 *
 * @author yokochi
 */
public enum JsonSchemaVersion {

	/** The latest version. */
	latest,
	/** The draft version 7. */
	draft_v7,
	/** The draft version 6. */
	draft_v6,
	/** The draft version 4. */
	draft_v4;

	/**
	 * Return default JSON Schema version.
	 *
	 * @return JsonType the default JSON version
	 */
	public static JsonSchemaVersion defaultVersion() {
		return draft_v7;
	}

	/**
	 * Return JSON Schema version.
	 *
	 * @param name version name of JSON Schema
	 * @return JsonSchemaVersion matched JSON Schema version
	 */
	public static JsonSchemaVersion getVersion(String name) {

		name = name.toLowerCase().replace("-0", "_v").replace("-", "_");

		for (JsonSchemaVersion schema_ver : values()) {

			if (schema_ver.name().equals(name))
				return schema_ver;

		}

		return defaultVersion();
	}

	/**
	 * Return namespace URI of JSON Schema.
	 *
	 * @return String namespace URI of JSON Schema
	 */
	public String getNamespaceURI() {

		switch (this) {
		case draft_v4:
			return "http://json-schema.org/draft-04/schema#";
		case draft_v6:
			return "http://json-schema.org/draft-06/schema#";
		case draft_v7:
			return "http://json-schema.org/draft-07/schema#";
		default:
			return "http://json-schema.org/schema#";
		}

	}

	/**
	 * Return whether JSON Schema version is latest.
	 *
	 * @return boolean whether JSON Schema version is latest
	 */
	public boolean isLatest() {
		return this.equals(latest) || this.equals(defaultVersion());
	}

}
