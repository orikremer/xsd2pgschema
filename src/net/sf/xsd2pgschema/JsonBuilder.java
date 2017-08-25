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

import java.util.List;

import org.apache.commons.text.StringEscapeUtils;

/**
 * JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilder {

	/** The JSON buffer. */
	StringBuilder builder = null;

	/** The prefix of JSON item name of xs:attribute. */
	String attr_prefix = "";

	/** The JSON item name of xs:simpleContent. */
	String simple_cont_key = PgSchemaUtil.simple_cont_name;

	/** The discarded JSON item name stands for document key. */
	String discard_doc_key = "";

	/** The unit of indent space. */
	String indent_space = "  ";

	/** The white spaces between JSON item and JSON data. */
	String key_value_space = " ";

	/** The line feed code in JSON document. */
	String linefeed = "\n";

	/** Whether retain field annotation or not. */
	boolean no_field_anno = false;

	/** Use JSON array uniformly for descendants. */
	boolean array_all = false;

	/** Whether discarded document key exists or not. */
	boolean has_discard_doc_key = false;

	/** The length of the key_value_space. */
	int key_value_spaces = key_value_space.length();

	/**
	 * Instance of JSON builder.
	 *
	 * @param option JSON builder option
	 */
	public JsonBuilder(JsonBuilderOption option) {

		this.no_field_anno = option.no_field_anno;
		this.array_all = option.array_all;

		if (indent_space.length() != option.indent_spaces) {

			StringBuilder sb = new StringBuilder();

			for (int l = 0; l < option.indent_spaces; l++)
				sb.append(" ");

			indent_space = sb.toString();

			initIndentSpacesArray(4);

		}

		if (key_value_space.length() != option.key_value_spaces) {

			StringBuilder sb = new StringBuilder();

			for (int l = 0; l < option.key_value_spaces; l++)
				sb.append(" ");

			key_value_space = sb.toString();
			key_value_spaces = sb.length();

		}

		linefeed = option.linefeed ? "\n" : "";

		if ((attr_prefix = option.attr_prefix) == null)
			attr_prefix = "";

		if ((simple_cont_key = option.simple_cont_key) == null)
			simple_cont_key = PgSchemaUtil.simple_cont_name;

		if ((discard_doc_key = option.discard_doc_key) == null)
			discard_doc_key = "";

		has_discard_doc_key = !discard_doc_key.isEmpty();

		clear();

	}

	/**
	 * Clear JSON builder.
	 */
	public void clear() {

		if (builder == null)
			builder = new StringBuilder();

		else if (builder.length() > 0)
			builder.setLength(0);

	}

	/**
	 * Close JSON builder.
	 */
	public void close() {

		if (builder != null) {

			if (builder.length() > 0)
				builder.setLength(0);

			builder = null;

		}

	}

	/**
	 * Decide JSON indent white spaces.
	 *
	 * @param indent_level current indent level
	 * @return String JSON indent white spaces
	 */
	public String getIndentSpaces(int indent_level) {

		if (indent_level < 0)
			indent_level = 0;

		if (indent_spaces_array == null || indent_level >= indent_spaces_array.length)
			initIndentSpacesArray(indent_level + 4);

		return indent_spaces_array[indent_level];
	}

	/** The indent spaces array. */
	private String[] indent_spaces_array = null;

	/**
	 * Initialize JSON indent white space pattern.
	 *
	 * @param indent_level current indent level
	 */
	private void initIndentSpacesArray(int indent_level) {

		if (indent_level < 2)
			indent_level = 2;

		int size = indent_level + 1;

		indent_spaces_array = new String[size];

		indent_spaces_array[0] = "";
		indent_spaces_array[1] = indent_space;

		for (indent_level = 2; indent_level < size; indent_level++)
			indent_spaces_array[indent_level] = indent_spaces_array[indent_level - 1] + indent_space;

	}

	/**
	 * Write schema property of field.
	 *
	 * @param field current field
	 * @param object whether object
	 * @param array whether array
	 * @param indent_level current indent level
	 */
	public void writeSchemaFieldProperty(PgField field, boolean object, boolean array, int indent_level) {

		if (!object && !array)
			return;

		if (object && !array) { // object

			writeSchemaFieldProperty(field, true, indent_level);

			builder.setLength(builder.length() - (linefeed.equals("\n") ? 1 : 0));
			builder.append("," + linefeed);

		}

		else if (!object && array) { // array

			builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"array\"," + linefeed);

			builder.append(getIndentSpaces(indent_level++) + "\"items\":" + key_value_space + "{" + linefeed); // JSON items start

			writeSchemaFieldProperty(field, true, indent_level);

			builder.append(getIndentSpaces(--indent_level) + "}," + linefeed); // JSON items end

		}

		else { // object or array

			builder.append(getIndentSpaces(indent_level++) + "\"oneOf\":" + key_value_space + "[" + linefeed); // JSON oneOf start

			builder.append(getIndentSpaces(indent_level++) + "{" + linefeed);

			writeSchemaFieldProperty(field, true, indent_level);

			builder.append(getIndentSpaces(--indent_level) + "}," + linefeed);

			builder.append(getIndentSpaces(indent_level++) + "{" + linefeed);

			builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"array\"," + linefeed);

			builder.append(getIndentSpaces(indent_level++) + "\"items\":" + key_value_space + "{" + linefeed); // JSON items start

			writeSchemaFieldProperty(field, false, indent_level);

			builder.append(getIndentSpaces(--indent_level) + "}" + linefeed); // JSON items end

			builder.append(getIndentSpaces(--indent_level) + "}" + linefeed);

			builder.append(getIndentSpaces(--indent_level) + "]," + linefeed); // JSON oneOf end

		}

	}

	/**
	 * Write schema property of field.
	 *
	 * @param field current field
	 * @param field_anno field annotation
	 * @param indent_level current indent level
	 */
	private void writeSchemaFieldProperty(PgField field, boolean field_anno, final int indent_level) {

		String schema_type = XsDataType.getJsonSchemaType(field);

		builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + schema_type + "," + linefeed);

		builder.append(getIndentSpaces(indent_level) + "\"$ref\":" + key_value_space + "\"" + XsDataType.getJsonSchemaRef(field) + "\"," + linefeed);

		builder.append(getIndentSpaces(indent_level) + "\"title\":" + key_value_space + "\"" + (field.attribute || field.any_attribute ? attr_prefix : "") + (field.simple_cont ? simple_cont_key : field.xname) + "\"," + linefeed);

		if (field.xs_type.equals(XsDataType.xs_anyURI))
			builder.append(getIndentSpaces(indent_level) + "\"format\":" + key_value_space + "\"uri\"," + linefeed);

		if (field.default_value != null)
			builder.append(getIndentSpaces(indent_level) + "\"default\":" + key_value_space + XsDataType.getJsonSchemaDefaultValue(field) + "," + linefeed);

		if (field.enum_name != null) {

			String enum_array = XsDataType.getJsonSchemaEnumArray(field, key_value_space);

			if (enum_array.length() > 2)
				builder.append(getIndentSpaces(indent_level) + "\"enum\":" + key_value_space + "[" + enum_array.substring(0, enum_array.length() - (key_value_spaces + 1)) + "]," + linefeed);

		}

		if (field.length != null) {

			builder.append(getIndentSpaces(indent_level) + "\"maxLength\":" + key_value_space + field.length + "," + linefeed);
			builder.append(getIndentSpaces(indent_level) + "\"minLength\":" + key_value_space + field.length + "," + linefeed);

		}

		else {

			if (field.max_length != null)
				builder.append(getIndentSpaces(indent_level) + "\"maxLength\":" + key_value_space + field.max_length + "," + linefeed);

			if (field.min_length != null)
				builder.append(getIndentSpaces(indent_level) + "\"minLength\":" + key_value_space + field.min_length + "," + linefeed);

		}

		if (field.pattern != null)
			builder.append(getIndentSpaces(indent_level) + "\"pattern\":" + key_value_space + "\"" + field.pattern + "\"," + linefeed);

		String schema_maximum = XsDataType.getJsonSchemaMaximumValue(field, key_value_space);

		if (schema_maximum != null)
			builder.append(getIndentSpaces(indent_level) + "\"maximum\":" + key_value_space + schema_maximum + "," + linefeed);

		String schema_minimum = XsDataType.getJsonSchemaMinimumValue(field, key_value_space);

		if (schema_minimum != null)
			builder.append(getIndentSpaces(indent_level) + "\"minimum\":" + key_value_space + schema_minimum + "," + linefeed);

		String multiple_of = XsDataType.getJsonSchemaMultipleOfValue(field);

		if (multiple_of != null)
			builder.append(getIndentSpaces(indent_level) + "\"multipleOf\":" + key_value_space + multiple_of + "," + linefeed);

		if (!no_field_anno) {

			if (field_anno && field.anno != null && !field.anno.isEmpty()) {

				String anno = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(field.anno.replaceAll("\n--", "")).replaceAll("\\\\/", "/").replaceAll("\\\\'", "'"));

				if (!anno.startsWith("\""))
					anno = "\"" + anno + "\"";

				builder.append(getIndentSpaces(indent_level) + "\"description\":" + key_value_space + anno + "," + linefeed);

			}

			else if (!field_anno)
				builder.append(getIndentSpaces(indent_level) + "\"description\":" + key_value_space + "\"array of previous object: " + (field.attribute || field.any_attribute ? attr_prefix : "") + (field.simple_cont ? simple_cont_key : field.xname) + "\"" + "," + linefeed);

		}

		builder.setLength(builder.length() - (linefeed.equals("\n") ? 2 : 1));
		builder.append(linefeed);

	}

	/**
	 * Write table header.
	 *
	 * @param table current table
	 * @param object whether object
	 * @param indent_level current indent level
	 * @return int size of last builder
	 */
	public int writeHeader(PgTable table, boolean object, final int indent_level) {

		int jsonb_len = builder.length();
		int jsonb_end = jsonb_len - (linefeed.equals("\n") ? 2 : 1);

		if (jsonb_end > 0) {

			String jsonb_term = builder.substring(jsonb_end);

			if (jsonb_term.equals("[" + linefeed)) {
			}

			else if (jsonb_term.equals("}" + linefeed)) {

				builder.setLength(jsonb_end);
				builder.append("}," + linefeed);

			}

			else if (!jsonb_term.equals("{" + linefeed)) {

				if (linefeed.equals("\n"))
					builder.setLength(jsonb_len - 1);

				builder.append("," + linefeed);

			}

		}

		builder.append(getIndentSpaces(indent_level) + (object ? "\"" + table.name + "\":" + key_value_space : "") + "{" + linefeed); // JSON object start

		return builder.length();
	}

	/**
	 * Write list holder table header.
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 * @return int size of last builder
	 */
	public int writeArrayHeader(PgTable table, final int indent_level) {

		int jsonb_len = builder.length();
		int jsonb_end = jsonb_len - (linefeed.equals("\n") ? 2 : 1);

		if (jsonb_end > 0) {

			String jsonb_term = builder.substring(jsonb_end);

			if (jsonb_term.equals("[" + linefeed)) {
			}

			else if (jsonb_term.equals("}" + linefeed)) {

				builder.setLength(jsonb_end);
				builder.append("}," + linefeed);

			}

			else if (!jsonb_term.equals("{" + linefeed)) {

				if (linefeed.equals("\n"))
					builder.setLength(jsonb_len - 1);

				builder.append("," + linefeed);

			}

		}

		builder.append(getIndentSpaces(indent_level) + "\"" + table.name + "\":" + key_value_space + "[" + linefeed); // JSON array start

		return builder.length();
	}

	/**
	 * Write field content.
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 */
	public void writeContent(final PgTable table, final int indent_level) {

		List<PgField> fields = table.fields;

		boolean array_json = !table.virtual && array_all;
		boolean has_field = false;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.jsonb == null)
				continue;

			if (field.jsonb_col_size > 0 && field.jsonb.length() > 2) {

				boolean array_field = array_json || field.list_holder || field.jsonb_col_size > 1;

				if (has_field)
					builder.append("," + linefeed);

				builder.append(getIndentSpaces(indent_level) + "\"" + (field.attribute || field.any_attribute ? attr_prefix : "") + (field.simple_cont ? simple_cont_key : field.xname) + "\":" + key_value_space + (array_field ? "[" : ""));

				field.jsonb.setLength(field.jsonb.length() - (key_value_spaces + 1));
				builder.append(field.jsonb);

				if (array_field)
					builder.append("]");

				has_field = true;

			}

			if (field.jsonb.length() > 0)
				field.jsonb.setLength(0);

			field.jsonb_col_size = field.jsonb_null_size = 0;

		}

		if (has_field)
			builder.append(linefeed);

	}

	/**
	 * Write table footer.
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 * @param jsonb_header_begin position of header begin
	 * @param jsonb_header_end position of header end
	 */
	public void writeFooter(PgTable table, final int indent_level, final int jsonb_header_begin, final int jsonb_header_end) {

		int jsonb_len = builder.length();

		if (jsonb_len == jsonb_header_end)
			builder.setLength(jsonb_header_begin);

		else if (jsonb_len > 2) {

			String jsonb_end = builder.substring(jsonb_len - 2);

			if (jsonb_end.equals("},")) {

				builder.setLength(jsonb_len - 1);
				builder.append(linefeed);

			}

			else if (jsonb_end.equals("],")) {

				builder.setLength(jsonb_len - 1);
				builder.append(linefeed);

			}

			builder.append(getIndentSpaces(indent_level) + "}" + linefeed); // JSON object end

		}

	}

	/**
	 * Write list holder table footer.
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 * @param jsonb_header_begin position of header begin
	 * @param jsonb_header_end position of header end
	 */
	public void writeArrayFooter(PgTable table, final int indent_level, final int jsonb_header_begin, final int jsonb_header_end) {

		int jsonb_len = builder.length();

		if (jsonb_len == jsonb_header_end)
			builder.setLength(jsonb_header_begin);

		else if (jsonb_len > 2) {

			String jsonb_end = builder.substring(jsonb_len - 2);

			if (jsonb_end.equals("},")) {

				builder.setLength(jsonb_len - 1);
				builder.append(linefeed);

			}

			builder.append(getIndentSpaces(indent_level) + "]" + linefeed); // JSON object end

		}

	}

}
