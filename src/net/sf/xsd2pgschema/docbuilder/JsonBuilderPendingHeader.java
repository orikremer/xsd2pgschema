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

package net.sf.xsd2pgschema.docbuilder;

/**
 * Pending table header in JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilderPendingHeader {

	/** The position header starts. */
	int start;

	/** The position header ends. */
	int end;

	/** The current indent level. */
	int indent_level;

	/**
	 * Instance of pending table header.
	 *
	 * @param start position header starts
	 * @param end position header ends
	 * @param indent_level current indent level
	 */
	public JsonBuilderPendingHeader(int start, int end, int indent_level) {

		this.start = start;
		this.end = end;
		this.indent_level = indent_level;

	}

}
