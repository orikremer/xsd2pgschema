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

import java.util.HashSet;

import org.apache.commons.text.StringEscapeUtils;

/**
 * JSON builder option.
 *
 * @author yokochi
 */
public class JsonBuilderOption {

	/** The prefix of JSON item name of xs:attribute. */
	public String attr_prefix = "";

	/** The JSON item name of xs:simpleContent. */
	public String simple_content_key = PgSchemaUtil.simple_content_name;

	/** The JSON item list of discarded document key. */
	public HashSet<String> discarded_document_keys = null;

	/** The length of white spaces for indent. */
	public int indent_spaces = PgSchemaUtil.indent_spaces;

	/** The length of white spaces between key and value. */
	public int key_value_spaces = 1;

	/** Whether retain field annotation or not. */
	public boolean no_field_anno = false;

	/** Whether use line feed code in JSON document. */
	public boolean linefeed = true;

	/** Whether use JSON array uniformly for descendants. */
	public boolean array_all = false;

	/**
	 * Set prefix in JSON document of xs:attribute.
	 *
	 * @param attr_prefix prefix code of xs:attribute in JSON document
	 */
	public void setAttrPrefix(String attr_prefix) {

		if (attr_prefix == null)
			attr_prefix = "";

		this.attr_prefix = StringEscapeUtils.escapeEcmaScript(attr_prefix);

	}

	/**
	 * Set item name in JSON document of xs:simpleContent.
	 *
	 * @param simple_content_key item name of xs:simpleContent in JSON document
	 */
	public void setSimpleContentKey(String simple_content_key) {

		if (simple_content_key == null)
			simple_content_key= PgSchemaUtil.simple_content_name;

		this.simple_content_key = simple_content_key;

	}

	/**
	 * Add discarded item name stands for document key.
	 *
	 * @param discarded_document_key discarded item name of document key in JSON document
	 * @return result of addition
	 */
	public boolean addDiscardDocKey(String discarded_document_key) {

		if (discarded_document_key == null || discarded_document_key.isEmpty())
			return false;

		if (discarded_document_keys == null)
			discarded_document_keys = new HashSet<String>();

		return discarded_document_keys.add(discarded_document_key);
	}

	/**
	 * Set length of indent spaces.
	 *
	 * @param indent_spaces length of indent spaces in JSON document
	 */
	public void setIndentSpaces(String indent_spaces) {

		this.indent_spaces = Integer.valueOf(indent_spaces);

		if (this.indent_spaces < 0)
			this.indent_spaces = 0;
		else if (this.indent_spaces > 4)
			this.indent_spaces = 4;

	}

	/**
	 * Set length of key-value spaces.
	 *
	 * @param key_value_spaces length of key-value spaces in JSON document
	 */
	public void setKeyValueSpaces(String key_value_spaces) {

		this.key_value_spaces = Integer.valueOf(key_value_spaces);

		if (this.key_value_spaces < 0)
			this.key_value_spaces = 0;
		else if (this.key_value_spaces > 1)
			this.key_value_spaces = 1;

	}

	/**
	 * Set JSON compact format.
	 */
	public void setCompact() {

		indent_spaces = key_value_spaces = 0;
		linefeed = false;

	}

}
