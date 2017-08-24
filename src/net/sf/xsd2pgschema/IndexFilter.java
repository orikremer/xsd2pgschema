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

/**
 * Full-text index filter.
 *
 * @author yokochi
 */
public class IndexFilter {

	/** The attributes for filtering index. */
	public HashSet<String> attrs = null;

	/** The fields to be stored. */
	public HashSet<String> fields = null;

	/** The minimum word length for indexing. */
	public int min_word_len = PgSchemaUtil.min_word_len;

	/** Whether string values are stored as attribute. */
	public boolean attr_string = false;

	/** Whether integer values are stored as attribute. */
	public boolean attr_integer = false;

	/** Whether float values are stored as attribute. */
	public boolean attr_float = false;

	/** Whether date values are stored as attribute. */
	public boolean attr_date = false;

	/** Whether time values are stored as attribute. */
	public boolean attr_time = false;

	/** Whether numeric values are stored in Lucene index. */
	public boolean numeric_index = false;

	/**
	 * Instance of index filter.
	 */
	public IndexFilter() {

		attrs = new HashSet<String>();
		fields = new HashSet<String>();

	}

	/**
	 * Add attribute.
	 *
	 * @param attr attribute name
	 * @return boolean result of addition
	 */
	public boolean addAttr(String attr) {

		if (attrs != null)
			return attrs.add(attr);

		else {

			attrs = new HashSet<String>();

			return attrs.add(attr);
		}

	}

	/**
	 * Add field.
	 *
	 * @param field field name
	 * @return boolean result of addition
	 */
	public boolean addField(String field) {
		return fields.add(field);
	}

	/**
	 * Add all attributes.
	 */
	public void setAttrAll() {

		if (attrs != null) {

			attrs.clear();
			attrs = null;

		}

		attr_string = attr_integer = attr_float = attr_date = attr_time = false;

	}

	/**
	 * Add all fields.
	 */
	public void setFiledAll() {

		fields.clear();

	}

	/**
	 * Set minimum word length.
	 *
	 * @param min_word_len argument value
	 */
	public void setMinWordLen(String min_word_len) {

		this.min_word_len = Integer.valueOf(min_word_len);

		if (this.min_word_len <= 0)
			this.min_word_len = PgSchemaUtil.min_word_len;

	}

	/**
	 * Enable indexing for numerical data.
	 */
	public void enableNumericIndex() {

		numeric_index = true;

	}

}
