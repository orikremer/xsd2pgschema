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
 * JSON builder option.
 *
 * @author yokochi
 */
public class JsonBuilderOption {

	/** The prefix of JSON item name of xs:attribute. */
	public String attr_prefix = "";

	/** The JSON item name of xs:simpleContent. */
	public String simple_content_key = PgSchemaUtil.simple_content_name;

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

		this.attr_prefix = attr_prefix;

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
