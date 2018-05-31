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

import java.util.List;

import org.apache.commons.text.StringEscapeUtils;

/**
 * JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilder {

	/** The JSON type. */
	protected JsonType type = JsonType.defaultType();

	/** The JSON buffer. */
	protected StringBuilder builder = null;

	/** The prefix of JSON item name of xs:attribute. */
	protected String attr_prefix = "";

	/** The JSON item name of xs:simpleContent. */
	protected String simple_content_key = PgSchemaUtil.simple_content_name;

	/** The unit of indent space. */
	private String indent_space = "  ";

	/** The white spaces between JSON item and JSON data. */
	protected String key_value_space = " ";

	/** The indent offset between key and value. */
	protected int key_value_offset = key_value_space.length();

	/** The line feed code in JSON document. */
	protected String line_feed_code = "\n";

	/** Whether retain case sensitive name. */
	protected boolean case_sense = true;

	/** Use JSON array uniformly for descendants. */
	protected boolean array_all = false;

	/** Whether retain field annotation or not. */
	private boolean no_field_anno = false;

	/**
	 * Instance of JSON builder.
	 *
	 * @param option JSON builder option
	 */
	public JsonBuilder(JsonBuilderOption option) {

		this.type = option.type;
		this.case_sense = option.case_sense;
		this.array_all = option.array_all;
		this.no_field_anno = option.no_field_anno;

		if (indent_space.length() != option.indent_offset) {

			StringBuilder sb = new StringBuilder();

			for (int l = 0; l < option.indent_offset; l++)
				sb.append(" ");

			indent_space = sb.toString();

			initIndentSpacesArray(4);

		}

		if (key_value_space.length() != option.key_value_offset) {

			StringBuilder sb = new StringBuilder();

			for (int l = 0; l < option.key_value_offset; l++)
				sb.append(" ");

			key_value_space = sb.toString();
			key_value_offset = sb.length();

		}

		line_feed_code = option.line_feed_code;

		if ((attr_prefix = option.attr_prefix) == null)
			attr_prefix = "";

		if ((simple_content_key = option.simple_content_key) == null)
			simple_content_key = PgSchemaUtil.simple_content_name;

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
	 * Return JSON item name of field.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @return String JSON item name of field
	 */
	public String getItemTitle(PgField field, boolean as_attr) {
		return (field.attribute || field.simple_attribute || (field.simple_attr_cond && as_attr) || field.any_attribute ? attr_prefix : "")
				+ (field.simple_content ? (field.simple_attribute || (field.simple_attr_cond && as_attr) ? (case_sense ? field.foreign_table_xname : field.foreign_table_xname.toLowerCase()) : simple_content_key) : (case_sense ? field.xname : field.xname.toLowerCase()));
	}

	/**
	 * Write schema property of field.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @param object whether object
	 * @param array whether array
	 * @param indent_level current indent level
	 */
	public void writeSchemaFieldProperty(PgField field, boolean as_attr, boolean object, boolean array, int indent_level) {

		if (!object && !array)
			return;

		if (object && !array) { // object

			writeSchemaFieldProperty(field, as_attr, true, indent_level);

			builder.setLength(builder.length() - (line_feed_code.equals("\n") ? 1 : 0));
			builder.append("," + line_feed_code);

		}

		else if (!object && array) { // array

			builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"array\"," + line_feed_code);

			builder.append(getIndentSpaces(indent_level++) + "\"items\":" + key_value_space + "{" + line_feed_code); // JSON items start

			writeSchemaFieldProperty(field, as_attr, true, indent_level);

			builder.append(getIndentSpaces(--indent_level) + "}," + line_feed_code); // JSON items end

		}

		else { // object or array

			builder.append(getIndentSpaces(indent_level++) + "\"oneOf\":" + key_value_space + "[" + line_feed_code); // JSON oneOf start

			builder.append(getIndentSpaces(indent_level++) + "{" + line_feed_code);

			writeSchemaFieldProperty(field, as_attr, true, indent_level);

			builder.append(getIndentSpaces(--indent_level) + "}," + line_feed_code);

			builder.append(getIndentSpaces(indent_level++) + "{" + line_feed_code);

			builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"array\"," + line_feed_code);

			builder.append(getIndentSpaces(indent_level++) + "\"items\":" + key_value_space + "{" + line_feed_code); // JSON items start

			writeSchemaFieldProperty(field, as_attr, false, indent_level);

			builder.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // JSON items end

			builder.append(getIndentSpaces(--indent_level) + "}" + line_feed_code);

			builder.append(getIndentSpaces(--indent_level) + "]," + line_feed_code); // JSON oneOf end

		}

	}

	/**
	 * Write schema property of field.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @param field_anno field annotation
	 * @param indent_level current indent level
	 */
	private void writeSchemaFieldProperty(PgField field, boolean as_attr, boolean field_anno, final int indent_level) {

		String schema_type = field.xs_type.getJsonSchemaType();

		builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + schema_type + "," + line_feed_code);

		builder.append(getIndentSpaces(indent_level) + "\"$ref\":" + key_value_space + "\"" + field.xs_type.getJsonSchemaRef() + "\"," + line_feed_code);

		builder.append(getIndentSpaces(indent_level) + "\"title\":" + key_value_space + "\"" + getItemTitle(field, as_attr) + "\"," + line_feed_code);

		if (field.xs_type.equals(XsDataType.xs_anyURI))
			builder.append(getIndentSpaces(indent_level) + "\"format\":" + key_value_space + "\"uri\"," + line_feed_code);

		if (field.default_value != null)
			builder.append(getIndentSpaces(indent_level) + "\"default\":" + key_value_space + field.getJsonSchemaDefaultValue() + "," + line_feed_code);

		if (field.enum_name != null) {

			String enum_array = field.getJsonSchemaEnumArray(key_value_space);

			if (enum_array.length() > 2)
				builder.append(getIndentSpaces(indent_level) + "\"enum\":" + key_value_space + "[" + enum_array.substring(0, enum_array.length() - (key_value_offset + 1)) + "]," + line_feed_code);

		}

		if (field.length != null) {

			builder.append(getIndentSpaces(indent_level) + "\"maxLength\":" + key_value_space + field.length + "," + line_feed_code);
			builder.append(getIndentSpaces(indent_level) + "\"minLength\":" + key_value_space + field.length + "," + line_feed_code);

		}

		else {

			if (field.max_length != null)
				builder.append(getIndentSpaces(indent_level) + "\"maxLength\":" + key_value_space + field.max_length + "," + line_feed_code);

			if (field.min_length != null)
				builder.append(getIndentSpaces(indent_level) + "\"minLength\":" + key_value_space + field.min_length + "," + line_feed_code);

		}

		if (field.pattern != null)
			builder.append(getIndentSpaces(indent_level) + "\"pattern\":" + key_value_space + "\"" + field.pattern + "\"," + line_feed_code);

		String schema_maximum = field.getJsonSchemaMaximumValue(key_value_space);

		if (schema_maximum != null)
			builder.append(getIndentSpaces(indent_level) + "\"maximum\":" + key_value_space + schema_maximum + "," + line_feed_code);

		String schema_minimum = field.getJsonSchemaMinimumValue(key_value_space);

		if (schema_minimum != null)
			builder.append(getIndentSpaces(indent_level) + "\"minimum\":" + key_value_space + schema_minimum + "," + line_feed_code);

		String multiple_of = field.getJsonSchemaMultipleOfValue();

		if (multiple_of != null)
			builder.append(getIndentSpaces(indent_level) + "\"multipleOf\":" + key_value_space + multiple_of + "," + line_feed_code);

		if (!no_field_anno) {

			if (field_anno && field.anno != null && !field.anno.isEmpty()) {

				String anno = escapeAnnotation(field.anno, true);

				if (!anno.startsWith("\""))
					anno = "\"" + anno + "\"";

				builder.append(getIndentSpaces(indent_level) + "\"description\":" + key_value_space + anno + "," + line_feed_code);

			}

			else if (!field_anno)
				builder.append(getIndentSpaces(indent_level) + "\"description\":" + key_value_space + "\"array of previous object: " + getItemTitle(field, as_attr) + "\"" + "," + line_feed_code);

		}

		builder.setLength(builder.length() - (line_feed_code.equals("\n") ? 2 : 1));
		builder.append(line_feed_code);

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
		int jsonb_end = jsonb_len - (line_feed_code.equals("\n") ? 2 : 1);

		if (jsonb_end > 0) {

			String jsonb_term = builder.substring(jsonb_end);

			if (jsonb_term.equals("[" + line_feed_code)) {
			}

			else if (jsonb_term.equals("}" + line_feed_code)) {

				builder.setLength(jsonb_end);
				builder.append("}," + line_feed_code);

			}

			else if (!jsonb_term.equals("{" + line_feed_code)) {

				if (line_feed_code.equals("\n"))
					builder.setLength(jsonb_len - 1);

				builder.append("," + line_feed_code);

			}

		}

		builder.append(getIndentSpaces(indent_level) + (object ? "\"" + (case_sense ? table.name : table.name.toLowerCase()) + "\":" + key_value_space : "") + "{" + line_feed_code); // JSON object start

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
		int jsonb_end = jsonb_len - (line_feed_code.equals("\n") ? 2 : 1);

		if (jsonb_end > 0) {

			String jsonb_term = builder.substring(jsonb_end);

			if (jsonb_term.equals("[" + line_feed_code)) {
			}

			else if (jsonb_term.equals("}" + line_feed_code)) {

				builder.setLength(jsonb_end);
				builder.append("}," + line_feed_code);

			}

			else if (!jsonb_term.equals("{" + line_feed_code)) {

				if (line_feed_code.equals("\n"))
					builder.setLength(jsonb_len - 1);

				builder.append("," + line_feed_code);

			}

		}

		builder.append(getIndentSpaces(indent_level) + "\"" + (case_sense ? table.name : table.name.toLowerCase()) + "\":" + key_value_space + "[" + line_feed_code); // JSON array start

		return builder.length();
	}

	/**
	 * Write field content.
	 *
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param indent_level current indent level
	 */
	public void writeContent(final PgTable table, final boolean as_attr, final int indent_level) {

		List<PgField> fields = table.fields;

		boolean array_json = !table.virtual && array_all;
		boolean has_field = false;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.jsonb == null)
				continue;

			if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0 && field.jsonb.length() > 2) {

				boolean array_field = array_json || field.list_holder || field.jsonb_col_size > 1;

				if (has_field)
					builder.append("," + line_feed_code);

				builder.append(getIndentSpaces(indent_level) + "\"" + getItemTitle(field, as_attr) + "\":" + key_value_space + (array_field ? "[" : ""));

				field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));
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
			builder.append(line_feed_code);

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
				builder.append(line_feed_code);

			}

			else if (jsonb_end.equals("],")) {

				builder.setLength(jsonb_len - 1);
				builder.append(line_feed_code);

			}

			builder.append(getIndentSpaces(indent_level) + "}" + line_feed_code); // JSON object end

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
				builder.append(line_feed_code);

			}

			builder.append(getIndentSpaces(indent_level) + "]" + line_feed_code); // JSON object end

		}

	}

	/**
	 * Return escaped one-liner annotation.
	 *
	 * @param annotation annotation
	 * @param is_table whether table annotation or not
	 * @return String escaped annotation
	 */
	public String escapeAnnotation(String annotation, boolean is_table) {
		return is_table ? StringEscapeUtils.escapeCsv(annotation.replace("\n--", "")) : StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(annotation).replace("\\/", "/").replace("\\'", "'"));
	}

}
