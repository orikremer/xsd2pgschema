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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;

/**
 * JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilder {

	/** The JSON Schema version. */
	public JsonSchemaVersion schema_ver = JsonSchemaVersion.defaultVersion();

	/** The JSON type. */
	protected JsonType type = JsonType.defaultType();

	/** The JSON buffer. */
	protected StringBuilder builder = new StringBuilder();

	/** The prefix of JSON item name of xs:attribute. */
	protected String attr_prefix = "";

	/** The JSON item name of xs:simpleContent. */
	protected String simple_content_name = PgSchemaUtil.simple_content_name;

	/** The unit of indent space. */
	private String indent_space = "  ";

	/** The white spaces between JSON item and JSON data. */
	protected String key_value_space = " ";

	/** The indent offset between key and value. */
	protected int key_value_offset = key_value_space.length();

	/** The line feed code in JSON document. */
	protected String line_feed_code = "\n";

	/** Whether use line feed code or not. */
	protected boolean line_feed = true;

	/** Whether retain case sensitive name. */
	protected boolean case_sense = true;

	/** Use JSON array uniformly for descendants. */
	protected boolean array_all = false;

	/** Whether retain field annotation or not. */
	private boolean no_field_anno = false;

	/** Whether insert document key. */
	protected boolean insert_doc_key = false;

	/** The pending element. */
	protected LinkedList<PgPendingElem> pending_elem = new LinkedList<PgPendingElem>();

	/** The pending simple content. */
	protected StringBuilder pending_simple_cont = new StringBuilder();

	/**
	 * Instance of JSON builder.
	 *
	 * @param option JSON builder option
	 */
	public JsonBuilder(JsonBuilderOption option) {

		this.schema_ver = option.schema_ver;
		this.type = option.type;
		this.case_sense = option.case_sense;
		this.array_all = option.array_all;
		this.no_field_anno = option.no_field_anno;
		this.insert_doc_key = option.insert_doc_key;

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

		line_feed = line_feed_code.equals("\n");

		if ((attr_prefix = option.attr_prefix) == null)
			attr_prefix = "";

		if ((simple_content_name = option.simple_content_name) == null)
			simple_content_name = PgSchemaUtil.simple_content_name;

	}

	/**
	 * Clear JSON builder.
	 *
	 * @param clear_buffer whether clear JSON buffer
	 */
	public void clear(boolean clear_buffer) {

		if (clear_buffer)
			builder.setLength(0);

		pending_elem.clear();
		pending_simple_cont.setLength(0);

	}

	// indent white space

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

	// JSON Schema

	/**
	 * Write JSON Schema header.
	 *
	 * @param def_namespaces default namespaces
	 * @param def_anno_appinfo top level xs:annotation/xs:appinfo
	 * @param def_anno_doc top level xs:annotation/xs:documentation
	 */
	public void writeSchemaHeader(HashMap<String, String> def_namespaces, String def_anno_appinfo, String def_anno_doc) {

		builder.append("{" + line_feed_code); // JSON document start

		builder.append(getIndentSpaces(1) + "\"$schema\":" + key_value_space + "\"" + schema_ver.getNamespaceURI() + "\"," + line_feed_code); // declare JSON Schema

		if (def_namespaces != null) {

			String def_namespace = def_namespaces.get("");

			if (def_namespace != null)
				builder.append(getIndentSpaces(1) + "\"" + (schema_ver.equals(JsonSchemaVersion.draft_v4) ? "" : "$") + "id\":" + key_value_space + "\"" + def_namespace + "\"," + line_feed_code); // declare unique identifier

		}

		if (def_anno_appinfo != null) {

			String _def_anno_appinfo = escapeAnnotation(def_anno_appinfo, false);

			if (!_def_anno_appinfo.startsWith("\""))
				_def_anno_appinfo = "\"" + _def_anno_appinfo + "\"";

			builder.append(getIndentSpaces(1) + "\"title\":" + key_value_space + _def_anno_appinfo + "," + line_feed_code);

		}

		if (def_anno_doc != null) {

			String _def_anno_doc = escapeAnnotation(def_anno_doc, false);

			if (!_def_anno_doc.startsWith("\""))
				_def_anno_doc = "\"" + _def_anno_doc + "\"";

			builder.append(getIndentSpaces(1) + "\"description\":" + key_value_space + _def_anno_doc + "," + line_feed_code);

		}

		builder.append(getIndentSpaces(1) + "\"type\":" + key_value_space + "\"object\"," + line_feed_code);

		builder.append(getIndentSpaces(1) + "\"properties\":" + key_value_space + "{" + line_feed_code); // start root properties

	}

	/**
	 * Write JSON Schema footer.
	 */
	public void writeSchemaFooter() {

		trimComma();

		builder.append(getIndentSpaces(1) + "}" + line_feed_code); // end root properties

		builder.append("}" + line_feed_code); // JSON document end

	}

	/**
	 * Write table header of JSON Schema.
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 */
	public void writeSchemaTableHeader(PgTable table, int indent_level) {

		builder.append(getIndentSpaces(indent_level++) + "\"" + (case_sense ? table.name : table.name.toLowerCase()) + "\":" + key_value_space + "{" + line_feed_code);  // start table

		if (table.anno != null && !table.anno.isEmpty()) {

			String table_anno = escapeAnnotation(table.anno, true);

			if (!table_anno.startsWith("\""))
				table_anno = "\"" + table_anno + "\"";

			builder.append(getIndentSpaces(indent_level) + "\"description\":" + key_value_space + table_anno + "," + line_feed_code);

		}

		builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"object\"," + line_feed_code);

		builder.append(getIndentSpaces(indent_level++) + "\"properties\":" + key_value_space + "{" + line_feed_code); // start field

	}

	/**
	 * Write table footer of JSON Schema.
	 *
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param indent_level current indent level
	 */
	public void writeSchemaTableFooter(PgTable table, boolean as_attr, int indent_level) {

		trimComma();

		boolean has_required = table.fields.stream().anyMatch(field -> field.required && field.jsonable);

		builder.append(getIndentSpaces(indent_level + 1) + "}" + (has_required ? "," : "") + line_feed_code); // end field

		if (has_required) {

			builder.append(getIndentSpaces(indent_level + 1) + "\"required\":" + key_value_space + "[");

			table.fields.stream().filter(field -> field.required && field.jsonable).forEach(field -> builder.append("\"" + getItemTitle(field, as_attr) + "\"," + key_value_space));

			builder.setLength(builder.length() - (key_value_offset + 1));

			builder.append("]" + line_feed_code);

		}

		builder.append(getIndentSpaces(indent_level) + "}," + line_feed_code); // end table

	}

	/**
	 * Write field property of JSON Schema.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @param object whether field as JSON object
	 * @param array whether field as JSON array
	 * @param indent_level current indent level
	 */
	@SuppressWarnings("deprecation")
	public void writeSchemaFieldProperty(PgField field, boolean as_attr, boolean object, boolean array, int indent_level) {

		if (!object && !array)
			return;

		String schema_type = field.xs_type.getJsonSchemaType();

		builder.append(getIndentSpaces(indent_level++) + "\"" + getItemTitle(field, as_attr) + "\":" + key_value_space + "{" + line_feed_code); // start field

		String format = field.getJsonSchemaFormat(schema_ver);
		String _format = field.getJsonSchemaFormat(JsonSchemaVersion.latest);

		// object

		if (object && !array) {}

		// array

		else if (!object && array) {

			builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"array\"," + line_feed_code);
			builder.append(getIndentSpaces(indent_level++) + "\"items\":" + key_value_space + "{" + line_feed_code); // start items

			if (!field.required && format != null) {

				builder.append(getIndentSpaces(indent_level++) + "\"oneOf\":" + key_value_space + "[" + line_feed_code); // start oneOf to allow empty string
				builder.append(getIndentSpaces(indent_level++) + "{" + line_feed_code); // start original array part

			}

		}

		// object or array

		else {

			builder.append(getIndentSpaces(indent_level++) + "\"oneOf\":" + key_value_space + "[" + line_feed_code); // start oneOf
			builder.append(getIndentSpaces(indent_level++) + "{" + line_feed_code); // start object part

		}

		builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + schema_type + "," + line_feed_code);

		if (format != null)
			builder.append(getIndentSpaces(indent_level) + "\"format\":" + key_value_space + "\"" + format + "\"," + line_feed_code);
		else if (_format == null)
			builder.append(getIndentSpaces(indent_level) + "\"$ref\":" + key_value_space + "\"" + field.xs_type.getJsonSchemaRef() + "\"," + line_feed_code);

		if (!no_field_anno && field.anno != null && !field.anno.isEmpty()) {

			String anno = escapeAnnotation(field.anno, true);

			if (!anno.startsWith("\""))
				anno = "\"" + anno + "\"";

			builder.append(getIndentSpaces(indent_level) + "\"description\":" + key_value_space + anno + "," + line_feed_code);

		}

		if (field.default_value != null)
			builder.append(getIndentSpaces(indent_level) + "\"default\":" + key_value_space + field.getJsonSchemaDefaultValue() + "," + line_feed_code);

		String enum_array = null;

		if (field.enum_name != null) {

			enum_array = field.getJsonSchemaEnumArray(key_value_space);

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
			builder.append(getIndentSpaces(indent_level) + "\"pattern\":" + key_value_space + "\"" + StringEscapeUtils.escapeEcmaScript(field.pattern) + "\"," + line_feed_code);

		String schema_maximum = null;
		String schema_minimum = null;

		switch (schema_ver) {
		case draft_v4:
			schema_maximum = field.getJsonSchemaMaximumValueDraftV4(key_value_space);

			if (schema_maximum != null)
				builder.append(getIndentSpaces(indent_level) + "\"maximum\":" + key_value_space + schema_maximum + "," + line_feed_code);

			schema_minimum = field.getJsonSchemaMinimumValueDraftV4(key_value_space);

			if (schema_minimum != null)
				builder.append(getIndentSpaces(indent_level) + "\"minimum\":" + key_value_space + schema_minimum + "," + line_feed_code);
			break;
		case draft_v6:
		case draft_v7:
		case latest:
			schema_maximum = field.getJsonSchemaMaximumValue(key_value_space);

			if (schema_maximum != null)
				builder.append(getIndentSpaces(indent_level) + schema_maximum + "," + line_feed_code);

			schema_minimum = field.getJsonSchemaMinimumValue(key_value_space);

			if (schema_minimum != null)
				builder.append(getIndentSpaces(indent_level) + schema_minimum + "," + line_feed_code);
		}

		String multiple_of = field.getJsonSchemaMultipleOfValue();

		if (multiple_of != null)
			builder.append(getIndentSpaces(indent_level) + "\"multipleOf\":" + key_value_space + multiple_of + "," + line_feed_code);

		trimComma();

		// object

		if (object && !array) {}

		// array

		else if (!object && array) {

			if (!field.required && format != null) {

				builder.append(getIndentSpaces(--indent_level) + "}," + line_feed_code); // end original array part

				builder.append(getIndentSpaces(indent_level++) + "{" + line_feed_code); // start empty string part

				builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"string\"," + line_feed_code);
				builder.append(getIndentSpaces(indent_level) + "\"maxLength\":" + key_value_space + "0" + line_feed_code);

				builder.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end empty string part

				builder.append(getIndentSpaces(--indent_level) + "]" + line_feed_code); // end oneOf to allow empty string

			}

			builder.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end items

		}

		// object or array

		else {

			trimComma();

			builder.append(getIndentSpaces(--indent_level) + "}," + line_feed_code); // end object part

			builder.append(getIndentSpaces(indent_level++) + "{" + line_feed_code); // start array part

			builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"array\"," + line_feed_code);
			builder.append(getIndentSpaces(indent_level++) + "\"items\":" + key_value_space + "{" + line_feed_code); // start items

			if (!field.required && format != null) {

				builder.append(getIndentSpaces(indent_level++) + "\"oneOf\":" + key_value_space + "[" + line_feed_code); // start oneOf to allow empty string
				builder.append(getIndentSpaces(indent_level++) + "{" + line_feed_code); // start original array part

			}

			builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + schema_type + "," + line_feed_code);

			if (format != null)
				builder.append(getIndentSpaces(indent_level) + "\"format\":" + key_value_space + "\"" + format + "\"," + line_feed_code);
			else if (_format == null)
				builder.append(getIndentSpaces(indent_level) + "\"$ref\":" + key_value_space + "\"" + field.xs_type.getJsonSchemaRef() + "\"," + line_feed_code);

			if (!no_field_anno)
				builder.append(getIndentSpaces(indent_level) + "\"description\":" + key_value_space + "\"array of previous object: " + getItemTitle(field, as_attr) + "\"" + "," + line_feed_code);

			if (field.default_value != null)
				builder.append(getIndentSpaces(indent_level) + "\"default\":" + key_value_space + field.getJsonSchemaDefaultValue() + "," + line_feed_code);

			if (field.enum_name != null && enum_array.length() > 2)
				builder.append(getIndentSpaces(indent_level) + "\"enum\":" + key_value_space + "[" + enum_array.substring(0, enum_array.length() - (key_value_offset + 1)) + "]," + line_feed_code);

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
				builder.append(getIndentSpaces(indent_level) + "\"pattern\":" + key_value_space + "\"" + StringEscapeUtils.escapeEcmaScript(field.pattern) + "\"," + line_feed_code);

			switch (schema_ver) {
			case draft_v4:
				if (schema_maximum != null)
					builder.append(getIndentSpaces(indent_level) + "\"maximum\":" + key_value_space + schema_maximum + "," + line_feed_code);

				if (schema_minimum != null)
					builder.append(getIndentSpaces(indent_level) + "\"minimum\":" + key_value_space + schema_minimum + "," + line_feed_code);
				break;
			case draft_v6:
			case draft_v7:
			case latest:
				if (schema_maximum != null)
					builder.append(getIndentSpaces(indent_level) + schema_maximum + "," + line_feed_code);

				if (schema_minimum != null)
					builder.append(getIndentSpaces(indent_level) + schema_minimum + "," + line_feed_code);
			}

			if (multiple_of != null)
				builder.append(getIndentSpaces(indent_level) + "\"multipleOf\":" + key_value_space + multiple_of + "," + line_feed_code);

			trimComma();

			if (!field.required && format != null) {

				builder.append(getIndentSpaces(--indent_level) + "}," + line_feed_code); // end original array part

				builder.append(getIndentSpaces(indent_level++) + "{" + line_feed_code); // start empty string part

				builder.append(getIndentSpaces(indent_level) + "\"type\":" + key_value_space + "\"string\"," + line_feed_code);
				builder.append(getIndentSpaces(indent_level) + "\"maxLength\":" + key_value_space + "0" + line_feed_code);

				builder.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end empty string part

				builder.append(getIndentSpaces(--indent_level) + "]" + line_feed_code); // end oneOf to allow empty string

			}

			builder.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end items

			builder.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end array part
			builder.append(getIndentSpaces(--indent_level) + "]" + line_feed_code); // end oneOf

		}

		builder.append(getIndentSpaces(--indent_level) + "}," + line_feed_code); // end field

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
				+ (field.simple_content ? (field.simple_attribute || (field.simple_attr_cond && as_attr) ? (case_sense ? field.foreign_table_xname : field.foreign_table_xname.toLowerCase()) : simple_content_name) : (case_sense ? field.xname : field.xname.toLowerCase()));
	}

	/**
	 * Return escaped one-liner annotation.
	 *
	 * @param annotation annotation
	 * @param is_table whether table annotation or not
	 * @return String escaped annotation
	 */
	private String escapeAnnotation(String annotation, boolean is_table) {
		return is_table ? StringEscapeUtils.escapeCsv(annotation.replace("\n--", "")) : StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(annotation).replace("\\/", "/").replace("\\'", "'"));
	}

	/**
	 * Trim a comma character from the last object.
	 */
	public void trimComma() {

		int position = builder.length() - (line_feed ? 2 : 1);

		if (position < 0 || builder.charAt(position) != ',')
			return;

		builder.setLength(position);
		builder.append(line_feed_code);

	}

	/**
	 * Append a comma character to the last object.
	 */
	public void appendComma() {

		int position = builder.length() - (line_feed ? 2 : 1);

		if (position < 0 || builder.charAt(position) == ',')
			return;

		builder.setLength(position + 1);
		builder.append("," + line_feed_code);

	}

	// JSON document

	/**
	 * Write table header.
	 *
	 * @param table current table
	 * @param object whether object or array
	 * @param indent_level current indent level
	 * @return int size of last builder
	 */
	public int writeTableHeader(PgTable table, boolean object, final int indent_level) {

		int jsonb_len = builder.length();
		int jsonb_end = jsonb_len - (line_feed ? 2 : 1);

		if (jsonb_end > 0) {

			String jsonb_term = builder.substring(jsonb_end);

			if (jsonb_term.equals("}" + line_feed_code)) {

				builder.setLength(jsonb_end);
				builder.append("}," + line_feed_code);

			}

			else if (!jsonb_term.equals("{" + line_feed_code) && !jsonb_term.equals("," + line_feed_code)) {

				if (line_feed)
					builder.setLength(jsonb_len - 1);

				builder.append("," + line_feed_code);

			}

		}

		builder.append(getIndentSpaces(indent_level) + (object ? "\"" + (case_sense ? table.name : table.name.toLowerCase()) + "\":" + key_value_space : "") + "{" + line_feed_code); // JSON object start

		return builder.length();
	}

	/**
	 * Write table footer.
	 *
	 * @param indent_level current indent level
	 * @param jsonb_header_begin position of header begin
	 * @param jsonb_header_end position of header end
	 */
	public void writeTableFooter(final int indent_level, final int jsonb_header_begin, final int jsonb_header_end) {

		int jsonb_len = builder.length();

		if (jsonb_len == jsonb_header_end)
			builder.setLength(jsonb_header_begin);

		else if (jsonb_len > 1) {

			String jsonb_end = builder.substring(jsonb_len - 2);

			if (jsonb_end.equals("},")) {

				builder.setLength(jsonb_len - 1);
				builder.append(line_feed_code);

			}

			else if (jsonb_end.equals("],")) {

				builder.setLength(jsonb_len - 1);
				builder.append(line_feed_code);

			}

			int _jsonb_end = jsonb_len - (line_feed ? 2 : 1);

			String jsonb_term = builder.substring(_jsonb_end);

			if (jsonb_term.equals("," + line_feed_code)) {

				if (line_feed)
					builder.setLength(_jsonb_end);

				builder.append(line_feed_code);

			}

			builder.append(getIndentSpaces(indent_level) + "}" + line_feed_code); // JSON object end

		}

	}

	/**
	 * Merge fields' JSON buffer of current table to the mainline's JSON buffer.
	 *
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param indent_level current indent level
	 */
	public void mergeTableBuffer(PgTable table, boolean as_attr, final int indent_level) {

		List<PgField> fields = table.fields;

		boolean array_json = !table.virtual && array_all;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.jsonb == null)
				continue;

			if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0 && field.jsonb.length() > 2) {

				if (field.jsonb_null_size == 1 && field.getJsonSchemaFormat(schema_ver) != null)
					continue;

				boolean array_field = array_json || field.jsonb_col_size > 1;

				builder.append(getIndentSpaces(indent_level) + "\"" + getItemTitle(field, as_attr) + "\":" + key_value_space + (array_field ? "[" : ""));

				field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));
				builder.append(field.jsonb);

				if (array_field)
					builder.append("]");

				builder.append("," + line_feed_code);

			}

			field.jsonb.setLength(0);

			field.jsonb_col_size = field.jsonb_null_size = 0;

		}

	}

	// XPath evaluation over PostgreSQL (fragment)

	/**
	 * Write field content as JSON fragment.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @param content content
	 */
	public void writeFieldFrag(PgField field, boolean as_attr, String content) {

		if (array_all)
			field.writeValue2JsonBuf(schema_ver, content, key_value_space);
		else
			builder.append("{" + line_feed_code + getIndentSpaces(1) + "\"" + getItemTitle(field, as_attr) + "\":" + key_value_space + field.normalizeAsJson(schema_ver, content) + line_feed_code + "}," + line_feed_code);

	}

	/**
	 * Merge field's JSON buffer to the mainline's JSON buffer.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 */
	public void mergeFieldFragBuffer(PgField field, boolean as_attr) {

		if (field.jsonb == null)
			return;

		if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0 && field.jsonb.length() > 2) {

			builder.append("{" + line_feed_code + getIndentSpaces(1) + "\"" + getItemTitle(field, as_attr) + "\":" + key_value_space + "[");

			field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));
			builder.append(field.jsonb);

			builder.append("]" + line_feed_code + "}," + line_feed_code);

			field.jsonb.setLength(0);

			field.jsonb_col_size = field.jsonb_null_size = 0;

		}

	}

	// XPath evaluation over PostgreSQL

	/**
	 * Write pending elements.
	 *
	 * @param attr_only whether element has attribute only
	 */
	public void writePendingElems(boolean attr_only) {

		PgPendingElem elem;

		while ((elem = pending_elem.pollLast()) != null) {

			int size = pending_elem.size();

			elem.attr_only = size > 0 ? false : attr_only;

			elem.write(this);

		}

	}

	/**
	 * Append simple content.
	 *
	 * @param content simple content
	 */
	public void appendSimpleCont(String content) {

		pending_simple_cont.append(content);

	}

	/**
	 * Write pending simple content.
	 */
	public void writePendingSimpleCont() {

		if (pending_simple_cont.length() == 0)
			return;

		builder.append(pending_simple_cont.toString());

		pending_simple_cont.setLength(0);

	}

	/**
	 * Write field's content
	 *
	 * @param table current table
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @param content content
	 * @param indent_level current indent level
	 */
	public void writeField(PgTable table, PgField field, boolean as_attr, String content, final int indent_level) {

		boolean array_field = table != null && !table.virtual && !table.list_holder && !table.bridge && array_all;

		builder.append(getIndentSpaces(indent_level) + "\"" + getItemTitle(field, as_attr) + "\":" + key_value_space + (array_field ? "[" : "") + field.normalizeAsJson(schema_ver, content) + (array_field ? "]" : "") + "," + line_feed_code);

	}

	/**
	 * Write any attribute's content
	 *
	 * @param attr_name attribute name
	 * @param content content
	 * @param indent_level current indent level
	 */
	public void writeAnyAttr(String attr_name, String content, final int indent_level) {

		attr_name = attr_prefix + attr_name;

		builder.append(getIndentSpaces(indent_level) + "\"" + (case_sense ? attr_name : attr_name.toLowerCase()) + "\":" + key_value_space + "\"" + content + "\"," + line_feed_code);

	}

}
