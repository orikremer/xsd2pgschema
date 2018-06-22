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
 * Enumerator of node parser type.
 *
 * @author yokochi
 */
public enum PgSchemaNodeParserType {

	/** The parser for PostgreSQL data migration. */
	pg_data_migration,
	/** The parser for full-text indexing. */
	full_text_indexing,
	/** The parser for JSON conversion. */
	json_conversion;

}
