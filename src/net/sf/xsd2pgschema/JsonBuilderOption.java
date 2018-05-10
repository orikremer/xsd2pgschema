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

package net.sf.xsd2pgschema;

/**
 * JSON builder option.
 *
 * @author yokochi
 */
public class JsonBuilderOption {

	/** The prefix of JSON item name of xs:attribute. */
	protected String attr_prefix = "";

	/** The JSON item name of xs:simpleContent. */
	protected String simple_content_key = PgSchemaUtil.simple_content_name;

	/** The indent offset. */
	protected int indent_offset = PgSchemaUtil.indent_offset;

	/** The indent offset between key and value. */
	protected int key_value_offset = 1;

	/** The current line feed code. */
	protected String line_feed_code = "\n";

	/** Whether use JSON array uniformly for descendants. */
	public boolean array_all = false;

	/** Whether retain field annotation or not. */
	public boolean no_field_anno = false;

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
	 * Set indent offset.
	 *
	 * @param indent_offset indent offset
	 */
	public void setIndentOffset(String indent_offset) {

		this.indent_offset = Integer.valueOf(indent_offset);

		if (this.indent_offset < 0)
			this.indent_offset = 0;
		else if (this.indent_offset > 4)
			this.indent_offset = 4;

	}

	/**
	 * Set key-value offset.
	 *
	 * @param key_value_offset indent offset between key and value
	 */
	public void setKeyValueOffset(String key_value_offset) {

		this.key_value_offset = Integer.valueOf(key_value_offset);

		if (this.key_value_offset < 0)
			this.key_value_offset = 0;
		else if (this.key_value_offset > 1)
			this.key_value_offset = 1;

	}

	/**
	 * Set JSON compact format.
	 */
	public void setCompact() {

		indent_offset = key_value_offset = 0;
		setLineFeed(false);

	}

	/**
	 * Set line feed code.
	 *
	 * @param line_feed whether use line feed code or not.
	 */
	public void setLineFeed(boolean line_feed) {

		line_feed_code = line_feed ? "\n" : "";

	}

	/**
	 * Return prefix for xs:attribute.
	 *
	 * @return String prefix for attribute
	 */
	public String getAttrPrefix() {
		return attr_prefix;
	}

	/**
	 * Return item name for xs:simpleContent.
	 *
	 * @return String item name for simple content
	 */
	public String getSimpleContentKey() {
		return simple_content_key;
	}

	/**
	 * Return current indent offset.
	 *
	 * @return int indent offset
	 */
	public int getIndentOffset() {
		return indent_offset;
	}

	/**
	 * Return current indent offset between key and value.
	 *
	 * @return int indent offset between key and value
	 */
	public int getKeyValueOffset() {
		return key_value_offset;
	}

}
