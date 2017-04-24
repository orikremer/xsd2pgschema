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

import java.util.ArrayList;
import java.util.List;

/**
 * Full-text index filter.
 *
 * @author yokochi
 */
public class IndexFilter {

	/** The attrs. */
	public List<String> attrs = null; // attributes for filtering index
	
	/** The fields. */
	public List<String> fields = null; // fields to be stored

	/** The min word len. */
	public int min_word_len = PgSchemaUtil.min_word_len; // minimum word length for indexing
	
	/** The numeric index. */
	public boolean numeric_index = false; // numeric values are stored in Lucene index

	/**
	 * Instance of index filter.
	 */
	public IndexFilter() {

		attrs = new ArrayList<String>();
		fields = new ArrayList<String>();

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

			attrs = new ArrayList<String>();

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
