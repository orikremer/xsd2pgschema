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

package net.sf.xsd2pgschema.serverutil;

/**
 * Enumerator of PgSchema client type.
 *
 * @author yokochi
 */
public enum PgSchemaClientType {

	/** The PostgreSQL data migration. */
	pg_data_migration,
	/** The full-text indexing. */
	full_text_indexing,
	/** The JSON conversion. */
	json_conversion,
	/** The XPath parser/evaluation (XML). */
	xpath_evaluation,
	/** The XPath evaluation to JSON. */
	xpath_evaluation_to_json;

}
