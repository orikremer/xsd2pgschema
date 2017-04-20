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

import org.apache.commons.lang3.StringEscapeUtils;

/**
 * JSON builder option
 * @author yokochi
 */
public class JsonBuilderOption {

	public String attr_prefix = ""; // prefix of JSON item name of xs:attribute
	public String simple_cont_key = PgSchemaUtil.simple_cont_name; // JSON item name of xs:simpleContent
	public String discard_doc_key = ""; // discarded JSON item name stands for document key

	public int indent_spaces = PgSchemaUtil.indent_spaces; // length of white spaces for indent
	public int key_value_spaces = 1; // length of white spaces between key and value

	public boolean no_field_anno = false; // whether retain field annotation or not
	public boolean linefeed = true; // whether use linefeed code in JSON document
	public boolean array_all = false; // use JSON array uniformly for descendant

	/**
	 * Set prefix in JSON document of xs:attribute
	 * @param attr_prefix argument value
	 */
	public void setAttrPrefix(String attr_prefix) {

		if (attr_prefix == null)
			attr_prefix = "";

		this.attr_prefix = StringEscapeUtils.escapeEcmaScript(attr_prefix);

	}

	/**
	 * Set item name in JSON document of xs:simpleContent
	 * @param simple_cont_key argument value
	 */
	public void setSimpleContKey(String simple_cont_key) {

		if (simple_cont_key == null)
			simple_cont_key= PgSchemaUtil.simple_cont_name;

		this.simple_cont_key = simple_cont_key;

	}

	/**
	 * Set discarded item name stands for document key
	 * @param discard_doc_key argument value
	 */
	public void setDiscardDocKey(String discard_doc_key) {

		if (discard_doc_key == null)
			discard_doc_key = "";

		this.discard_doc_key = discard_doc_key;

	}

	/**
	 * Set length of indent spaces
	 * @param indent_spaces argument value
	 */
	public void setIndentSpaces(String indent_spaces) {

		this.indent_spaces = Integer.valueOf(indent_spaces);

		if (this.indent_spaces < 0)
			this.indent_spaces = 0;
		else if (this.indent_spaces > 4)
			this.indent_spaces = 4;

	}

	/**
	 * Set length of key-value spaces
	 * @param key_value_spaces argument value
	 */
	public void setKeyValueSpaces(String key_value_spaces) {

		this.key_value_spaces = Integer.valueOf(key_value_spaces);

		if (this.key_value_spaces < 0)
			this.key_value_spaces = 0;
		else if (this.key_value_spaces > 1)
			this.key_value_spaces = 1;

	}

	/**
	 * Set JSON compact format
	 */
	public void setCompact() {

		indent_spaces = key_value_spaces = 0;
		linefeed = false;

	}

}
