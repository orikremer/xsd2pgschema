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

package net.sf.xsd2pgschema.option;

import java.util.HashSet;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;

/**
 * Full-text index filter.
 *
 * @author yokochi
 */
public class IndexFilter {

	/** The attributes for partial index. */
	public HashSet<String> attrs = new HashSet<String>();

	/** The Sphinx multi-valued attribute for partial index. */
	public HashSet<String> sph_mvas = new HashSet<String>();

	/** The fields to be stored. */
	public HashSet<String> fields = new HashSet<String>();

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

	/** Whether all attributes are selected. */
	public boolean attr_all = false;

	/** Whether numeric values are stored in Lucene index. */
	public boolean lucene_numeric_index = false;

	/** The Sphinx maximum field length. (related to max_xmlpipe2_field in sphinx.conf) */
	public int sph_max_field_len = PgSchemaUtil.sph_max_field_len;

	/**
	 * Add an attribute.
	 *
	 * @param attr attribute name
	 * @return boolean result of addition
	 */
	public boolean addAttr(String attr) {

		attr_all = false;

		if (attrs != null)
			return attrs.add(attr);

		else {

			attrs = new HashSet<String>();

			return attrs.add(attr);
		}

	}

	/**
	 * Add a Sphinx multi-valued attribute.
	 *
	 * @param sph_mva multi-valued attribute name
	 * @return boolean result of addition
	 */
	public boolean addSphMVA(String sph_mva) {

		if (!attr_all)
			addAttr(sph_mva);

		if (sph_mvas != null)
			return sph_mvas.add(sph_mva);

		else {

			sph_mvas = new HashSet<String>();

			return sph_mvas.add(sph_mva);
		}

	}

	/**
	 * Add an field.
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
		attr_all = true;

	}

	/**
	 * Add all fields.
	 */
	public void setFieldAll() {

		fields.clear();

	}

	/**
	 * Append attribute by type dependency.
	 *
	 * @param table table
	 * @param field field
	 */
	public void appendAttrByType(PgTable table, PgField field) {

		switch (field.xs_type) {
		// case xs_bigserial:
		// case xs_serial:
		case xs_integer:
		case xs_nonNegativeInteger:
		case xs_nonPositiveInteger:
		case xs_positiveInteger:
		case xs_negativeInteger:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_int:
		case xs_unsignedInt:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (attr_integer)
				addAttr(table.name + "." + field.name);
			break;
		case xs_float:
		case xs_double:
		case xs_decimal:
			if (attr_float)
				addAttr(table.name + "." + field.name);
			break;
		case xs_dateTime:
		case xs_dateTimeStamp:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			if (attr_date)
				addAttr(table.name + "." + field.name);
			break;
		case xs_time:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
			if (attr_time)
				addAttr(table.name + "." + field.name);
			break;
		default: // free text
			if (attr_string)
				addAttr(table.name + "." + field.name);
		}

	}

	/**
	 * Set minimum word length.
	 *
	 * @param min_word_len argument value
	 */
	public void setMinWordLen(String min_word_len) {

		this.min_word_len = Integer.valueOf(min_word_len);

		if (this.min_word_len < PgSchemaUtil.min_word_len) {
			System.err.println("Minumum word length is less than " + PgSchemaUtil.min_word_len + ". Set to the default value.");
			this.min_word_len = PgSchemaUtil.min_word_len;
		}

	}

	/**
	 * Enable Lucene indexing for numerical data.
	 */
	public void enableLuceneNumericIndex() {

		lucene_numeric_index = true;

	}

	/**
	 * Set Sphinx maximum field length.
	 *
	 * @param sph_max_field_len argument value
	 */
	public void setSphMaxFieldLen(String sph_max_field_len) {

		if (sph_max_field_len.endsWith("k") || sph_max_field_len.endsWith("kB"))
			this.sph_max_field_len = (int) (Float.valueOf(sph_max_field_len.substring(0, sph_max_field_len.indexOf('k'))) * 1024);

		else if (sph_max_field_len.endsWith("M") || sph_max_field_len.endsWith("MB"))
			this.sph_max_field_len = (int) (Float.valueOf(sph_max_field_len.substring(0, sph_max_field_len.indexOf('M'))) * 1024 * 1024);

		else if (sph_max_field_len.endsWith("G") || sph_max_field_len.endsWith("GB"))
			this.sph_max_field_len = (int) (Float.valueOf(sph_max_field_len.substring(0, sph_max_field_len.indexOf('G'))) * 1024 * 1024 * 1024);

		else
			this.sph_max_field_len = Integer.valueOf(sph_max_field_len);

		if (this.sph_max_field_len < PgSchemaUtil.sph_max_field_len) {
			System.err.println("Maximum field length is less than " + PgSchemaUtil.sph_max_field_len / 1024 / 1024 + "MB. Set to the default value.");
			this.sph_max_field_len = PgSchemaUtil.sph_max_field_len;
		}

	}

}
