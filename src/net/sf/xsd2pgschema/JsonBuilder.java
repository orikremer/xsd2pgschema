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

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;

/**
 * JSON buffer.
 *
 * @author yokochi
 */
public class JsonBuilder {

	/** The JSON Schema version. */
	public JsonSchemaVersion schema_ver = JsonSchemaVersion.defaultVersion();

	/** The JSON type. */
	protected JsonType type = JsonType.defaultType();

	/** The JSON buffer. */
	protected StringBuilder buffer = new StringBuilder();

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

	/** The pending table header. */
	protected LinkedList<JsonBuilderPendingHeader> pending_header = new LinkedList<JsonBuilderPendingHeader>();

	/** The pending element. */
	protected LinkedList<JsonBuilderPendingElem> pending_elem = new LinkedList<JsonBuilderPendingElem>();

	/** The pending simple content. */
	protected StringBuilder pending_simple_cont = new StringBuilder();

	/**
	 * Instance of JSON buffer.
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
	 * Clear JSON buffer.
	 *
	 * @param clear_buffer whether clear JSON buffer
	 */
	public void clear(boolean clear_buffer) {

		if (clear_buffer)
			buffer.setLength(0);

		pending_header.clear();

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
	private String getIndentSpaces(int indent_level) {

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
	 * Write a start object.
	 *
	 * @param root whether initialize JSON buffer or not
	 */
	public void writeStartDocument(boolean root) {

		if (root) {

			if (buffer.length() > 0)
				buffer.setLength(0);

		}

		else {

			int position = buffer.length() - (line_feed ? 2 : 1);

			if (position > 0 && buffer.charAt(position) != ',') {

				buffer.setLength(position + 1);
				buffer.append("," + line_feed_code);

			}

		}

		buffer.append("{" + line_feed_code);

	}

	/**
	 * Write a end object.
	 */
	public void writeEndDocument() {

		trimComma();

		buffer.append("}" + line_feed_code);

	}

	/**
	 * Return JSON item name of field.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @return String JSON item name of field
	 */
	private String getItemName(PgField field, boolean as_attr) {
		return (field.attribute || field.simple_attribute || (field.simple_attr_cond && as_attr) || field.any_attribute ? attr_prefix : "")
				+ (field.simple_content ? (field.simple_attribute || (field.simple_attr_cond && as_attr) ? (case_sense ? field.foreign_table_xname : field.foreign_table_xname.toLowerCase()) : simple_content_name) : (case_sense ? field.xname : field.xname.toLowerCase()));
	}

	// JSON Schema

	/**
	 * Write JSON Schema header.
	 *
	 * @param def_namespaces default namespaces
	 * @param def_anno_appinfo top level xs:annotation/xs:appinfo
	 * @param def_anno_doc top level xs:annotation/xs:documentation
	 */
	public void writeStartSchema(HashMap<String, String> def_namespaces, String def_anno_appinfo, String def_anno_doc) {

		String indent_space = getIndentSpaces(1);

		buffer.append(indent_space + "\"$schema\":" + key_value_space + "\"" + schema_ver.getNamespaceURI() + "\"," + line_feed_code); // declare JSON Schema

		if (def_namespaces != null) {

			String def_namespace = def_namespaces.get("");

			if (def_namespace != null)
				buffer.append(indent_space + "\"" + (schema_ver.equals(JsonSchemaVersion.draft_v4) ? "" : "$") + "id\":" + key_value_space + "\"" + def_namespace + "\"," + line_feed_code); // declare unique identifier

		}

		if (def_anno_appinfo != null) {

			String _def_anno_appinfo = getOneLinerAnnotation(def_anno_appinfo, false);

			if (!_def_anno_appinfo.startsWith("\""))
				_def_anno_appinfo = "\"" + _def_anno_appinfo + "\"";

			buffer.append(indent_space + "\"title\":" + key_value_space + _def_anno_appinfo + "," + line_feed_code);

		}

		if (def_anno_doc != null) {

			String _def_anno_doc = getOneLinerAnnotation(def_anno_doc, false);

			if (!_def_anno_doc.startsWith("\""))
				_def_anno_doc = "\"" + _def_anno_doc + "\"";

			buffer.append(indent_space + "\"description\":" + key_value_space + _def_anno_doc + "," + line_feed_code);

		}

		buffer.append(indent_space + "\"type\":" + key_value_space + "\"object\"," + line_feed_code);

		buffer.append(indent_space + "\"properties\":" + key_value_space + "{" + line_feed_code); // start root properties

	}

	/**
	 * Write JSON Schema footer.
	 */
	public void writeEndSchema() {

		trimComma();

		buffer.append(getIndentSpaces(1) + "}" + line_feed_code); // end root properties

	}

	/**
	 * Write table header of JSON Schema.
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 */
	public void writeStartSchemaTable(PgTable table, final int indent_level) {

		int header_start = buffer.length();

		buffer.append(getIndentSpaces(indent_level) + "\"" + (case_sense ? table.name : table.name.toLowerCase()) + "\":" + key_value_space + "{" + line_feed_code);  // start table

		String _indent_space = getIndentSpaces(indent_level + 1);

		if (table.anno != null && !table.anno.isEmpty()) {

			String table_anno = getOneLinerAnnotation(table.anno, true);

			if (!table_anno.startsWith("\""))
				table_anno = "\"" + table_anno + "\"";

			buffer.append(_indent_space + "\"description\":" + key_value_space + table_anno + "," + line_feed_code);

		}

		buffer.append(_indent_space + "\"type\":" + key_value_space + "\"object\"," + line_feed_code);

		buffer.append(_indent_space + "\"properties\":" + key_value_space + "{" + line_feed_code); // start field

		pending_header.push(new JsonBuilderPendingHeader(header_start, buffer.length(), indent_level));

	}

	/**
	 * Write table footer of JSON Schema.
	 *
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 */
	public void writeEndSchemaTable(PgTable table, boolean as_attr) {

		int len = buffer.length();

		JsonBuilderPendingHeader header = pending_header.poll();

		if (len == header.end) { // no content

			buffer.setLength(header.start);

			return;
		}

		trimComma();

		boolean has_required = table.fields.stream().anyMatch(field -> field.required && field.jsonable);

		String _indent_space = getIndentSpaces(header.indent_level + 1);

		buffer.append(_indent_space + "}" + (has_required ? "," : "") + line_feed_code); // end field

		if (has_required) {

			buffer.append(_indent_space + "\"required\":" + key_value_space + "[");

			table.fields.stream().filter(field -> field.required && field.jsonable).forEach(field -> buffer.append("\"" + getItemName(field, as_attr) + "\"," + key_value_space));

			buffer.setLength(buffer.length() - (key_value_offset + 1));

			buffer.append("]" + line_feed_code);

		}

		buffer.append(getIndentSpaces(header.indent_level) + "}," + line_feed_code); // end table

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
	public void writeSchemaField(PgField field, boolean as_attr, boolean object, boolean array, int indent_level) {

		if (!object && !array)
			return;

		String schema_type = field.xs_type.getJsonSchemaType();

		buffer.append(getIndentSpaces(indent_level++) + "\"" + getItemName(field, as_attr) + "\":" + key_value_space + "{" + line_feed_code); // start field

		String format = field.getJsonSchemaFormat(schema_ver);
		String _format = field.getJsonSchemaFormat(JsonSchemaVersion.latest);
		String _indent_space;

		// object

		if (object && !array) {}

		// array

		else if (!object && array) {

			_indent_space = getIndentSpaces(indent_level++);

			buffer.append(_indent_space + "\"type\":" + key_value_space + "\"array\"," + line_feed_code);
			buffer.append(_indent_space + "\"items\":" + key_value_space + "{" + line_feed_code); // start items

			if (!field.required && format != null) {

				buffer.append(getIndentSpaces(indent_level++) + "\"oneOf\":" + key_value_space + "[" + line_feed_code); // start oneOf to allow empty string
				buffer.append(getIndentSpaces(indent_level++) + "{" + line_feed_code); // start original array part

			}

		}

		// object or array

		else {

			buffer.append(getIndentSpaces(indent_level++) + "\"oneOf\":" + key_value_space + "[" + line_feed_code); // start oneOf
			buffer.append(getIndentSpaces(indent_level++) + "{" + line_feed_code); // start object part

		}

		_indent_space = getIndentSpaces(indent_level);

		buffer.append(_indent_space + "\"type\":" + key_value_space + schema_type + "," + line_feed_code);

		if (format != null)
			buffer.append(_indent_space + "\"format\":" + key_value_space + "\"" + format + "\"," + line_feed_code);
		else if (_format == null)
			buffer.append(_indent_space + "\"$ref\":" + key_value_space + "\"" + field.xs_type.getJsonSchemaRef() + "\"," + line_feed_code);

		if (!no_field_anno && field.anno != null && !field.anno.isEmpty()) {

			String anno = getOneLinerAnnotation(field.anno, true);

			if (!anno.startsWith("\""))
				anno = "\"" + anno + "\"";

			buffer.append(_indent_space + "\"description\":" + key_value_space + anno + "," + line_feed_code);

		}

		if (field.default_value != null)
			buffer.append(_indent_space + "\"default\":" + key_value_space + field.getJsonSchemaDefaultValue() + "," + line_feed_code);

		String enum_array = null;

		if (field.enum_name != null) {

			enum_array = field.getJsonSchemaEnumArray(key_value_space);

			if (enum_array.length() > 2)
				buffer.append(_indent_space + "\"enum\":" + key_value_space + "[" + enum_array.substring(0, enum_array.length() - (key_value_offset + 1)) + "]," + line_feed_code);

		}

		if (field.length != null) {

			buffer.append(_indent_space + "\"maxLength\":" + key_value_space + field.length + "," + line_feed_code);
			buffer.append(_indent_space + "\"minLength\":" + key_value_space + field.length + "," + line_feed_code);

		}

		else {

			if (field.max_length != null)
				buffer.append(_indent_space + "\"maxLength\":" + key_value_space + field.max_length + "," + line_feed_code);

			if (field.min_length != null)
				buffer.append(_indent_space + "\"minLength\":" + key_value_space + field.min_length + "," + line_feed_code);

		}

		if (field.pattern != null)
			buffer.append(_indent_space + "\"pattern\":" + key_value_space + "\"" + StringEscapeUtils.escapeEcmaScript(field.pattern) + "\"," + line_feed_code);

		String schema_maximum = null;
		String schema_minimum = null;

		switch (schema_ver) {
		case draft_v4:
			schema_maximum = field.getJsonSchemaMaximumValueDraftV4(key_value_space);

			if (schema_maximum != null)
				buffer.append(_indent_space + "\"maximum\":" + key_value_space + schema_maximum + "," + line_feed_code);

			schema_minimum = field.getJsonSchemaMinimumValueDraftV4(key_value_space);

			if (schema_minimum != null)
				buffer.append(_indent_space + "\"minimum\":" + key_value_space + schema_minimum + "," + line_feed_code);
			break;
		case draft_v6:
		case draft_v7:
		case latest:
			schema_maximum = field.getJsonSchemaMaximumValue(key_value_space);

			if (schema_maximum != null)
				buffer.append(_indent_space + schema_maximum + "," + line_feed_code);

			schema_minimum = field.getJsonSchemaMinimumValue(key_value_space);

			if (schema_minimum != null)
				buffer.append(_indent_space + schema_minimum + "," + line_feed_code);
		}

		String multiple_of = field.getJsonSchemaMultipleOfValue();

		if (multiple_of != null)
			buffer.append(_indent_space + "\"multipleOf\":" + key_value_space + multiple_of + "," + line_feed_code);

		trimComma();

		// object

		if (object && !array) {}

		// array

		else if (!object && array) {

			if (!field.required && format != null) {

				_indent_space = getIndentSpaces(--indent_level);

				buffer.append(_indent_space + "}," + line_feed_code); // end original array part
				buffer.append(_indent_space + "{" + line_feed_code); // start empty string part

				_indent_space = getIndentSpaces(++indent_level);

				buffer.append(_indent_space + "\"type\":" + key_value_space + "\"string\"," + line_feed_code);
				buffer.append(_indent_space + "\"maxLength\":" + key_value_space + "0" + line_feed_code);

				buffer.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end empty string part

				buffer.append(getIndentSpaces(--indent_level) + "]" + line_feed_code); // end oneOf to allow empty string

			}

			buffer.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end items

		}

		// object or array

		else {

			_indent_space = getIndentSpaces(--indent_level);

			buffer.append(_indent_space + "}," + line_feed_code); // end object part
			buffer.append(_indent_space + "{" + line_feed_code); // start array part

			_indent_space = getIndentSpaces(++indent_level);

			buffer.append(_indent_space + "\"type\":" + key_value_space + "\"array\"," + line_feed_code);
			buffer.append(_indent_space + "\"items\":" + key_value_space + "{" + line_feed_code); // start items

			if (!field.required && format != null) {

				buffer.append(getIndentSpaces(++indent_level) + "\"oneOf\":" + key_value_space + "[" + line_feed_code); // start oneOf to allow empty string
				buffer.append(getIndentSpaces(++indent_level) + "{" + line_feed_code); // start original array part

			}

			_indent_space = getIndentSpaces(++indent_level);

			buffer.append(_indent_space + "\"type\":" + key_value_space + schema_type + "," + line_feed_code);

			if (format != null)
				buffer.append(_indent_space + "\"format\":" + key_value_space + "\"" + format + "\"," + line_feed_code);
			else if (_format == null)
				buffer.append(_indent_space + "\"$ref\":" + key_value_space + "\"" + field.xs_type.getJsonSchemaRef() + "\"," + line_feed_code);

			if (!no_field_anno)
				buffer.append(_indent_space + "\"description\":" + key_value_space + "\"array of previous object: " + getItemName(field, as_attr) + "\"" + "," + line_feed_code);

			if (field.default_value != null)
				buffer.append(_indent_space + "\"default\":" + key_value_space + field.getJsonSchemaDefaultValue() + "," + line_feed_code);

			if (field.enum_name != null && enum_array.length() > 2)
				buffer.append(_indent_space + "\"enum\":" + key_value_space + "[" + enum_array.substring(0, enum_array.length() - (key_value_offset + 1)) + "]," + line_feed_code);

			if (field.length != null) {

				buffer.append(_indent_space + "\"maxLength\":" + key_value_space + field.length + "," + line_feed_code);
				buffer.append(_indent_space + "\"minLength\":" + key_value_space + field.length + "," + line_feed_code);

			}

			else {

				if (field.max_length != null)
					buffer.append(_indent_space + "\"maxLength\":" + key_value_space + field.max_length + "," + line_feed_code);

				if (field.min_length != null)
					buffer.append(_indent_space + "\"minLength\":" + key_value_space + field.min_length + "," + line_feed_code);

			}

			if (field.pattern != null)
				buffer.append(_indent_space + "\"pattern\":" + key_value_space + "\"" + StringEscapeUtils.escapeEcmaScript(field.pattern) + "\"," + line_feed_code);

			switch (schema_ver) {
			case draft_v4:
				if (schema_maximum != null)
					buffer.append(_indent_space + "\"maximum\":" + key_value_space + schema_maximum + "," + line_feed_code);

				if (schema_minimum != null)
					buffer.append(_indent_space + "\"minimum\":" + key_value_space + schema_minimum + "," + line_feed_code);
				break;
			case draft_v6:
			case draft_v7:
			case latest:
				if (schema_maximum != null)
					buffer.append(_indent_space + schema_maximum + "," + line_feed_code);

				if (schema_minimum != null)
					buffer.append(_indent_space + schema_minimum + "," + line_feed_code);
			}

			if (multiple_of != null)
				buffer.append(_indent_space + "\"multipleOf\":" + key_value_space + multiple_of + "," + line_feed_code);

			trimComma();

			if (!field.required && format != null) {

				_indent_space = getIndentSpaces(--indent_level);

				buffer.append(_indent_space + "}," + line_feed_code); // end original array part
				buffer.append(_indent_space + "{" + line_feed_code); // start empty string part

				_indent_space = getIndentSpaces(++indent_level);

				buffer.append(_indent_space + "\"type\":" + key_value_space + "\"string\"," + line_feed_code);
				buffer.append(_indent_space + "\"maxLength\":" + key_value_space + "0" + line_feed_code);

				buffer.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end empty string part

				buffer.append(getIndentSpaces(--indent_level) + "]" + line_feed_code); // end oneOf to allow empty string

			}

			buffer.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end items

			buffer.append(getIndentSpaces(--indent_level) + "}" + line_feed_code); // end array part
			buffer.append(getIndentSpaces(--indent_level) + "]" + line_feed_code); // end oneOf

		}

		buffer.append(getIndentSpaces(--indent_level) + "}," + line_feed_code); // end field

	}

	/**
	 * Return escaped one-liner annotation.
	 *
	 * @param annotation annotation
	 * @param is_table whether table annotation or not
	 * @return String escaped one-liner annotation
	 */
	private String getOneLinerAnnotation(String annotation, boolean is_table) {
		return is_table ? StringEscapeUtils.escapeCsv(annotation.replace("\n--", "")) : StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(annotation).replace("\\/", "/").replace("\\'", "'"));
	}

	/**
	 * Trim a comma character from the last object.
	 */
	private void trimComma() {

		int position = buffer.length() - (line_feed ? 2 : 1);

		if (position < 0 || buffer.charAt(position) != ',')
			return;

		buffer.setLength(position);
		buffer.append(line_feed_code);

	}

	/**
	 * Print buffer.
	 */
	public void print() {

		System.out.print(buffer.toString());

		buffer.setLength(0);

	}

	// JSON document

	/**
	 * Write table header.
	 *
	 * @param table current table
	 * @param object whether object or array
	 * @param indent_level current indent level
	 */
	public void writeStartTable(PgTable table, boolean object, final int indent_level) {

		int header_start = buffer.length();

		buffer.append(getIndentSpaces(indent_level) + (object ? "\"" + (case_sense ? table.name : table.name.toLowerCase()) + "\":" + key_value_space : "") + "{" + line_feed_code);

		pending_header.push(new JsonBuilderPendingHeader(header_start, buffer.length(), indent_level));

	}

	/**
	 * Write table footer.
	 */
	public void writeEndTable() {

		int len = buffer.length();

		JsonBuilderPendingHeader header = pending_header.poll();

		if (len == header.end) { // no content

			buffer.setLength(header.start);

			return;
		}

		trimComma();

		buffer.append(getIndentSpaces(header.indent_level) + "}," + line_feed_code);

	}

	/**
	 * Merge fields' JSON buffer of current table to the mainline's JSON buffer.
	 *
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param indent_level current indent level
	 */
	public void writeFields(PgTable table, boolean as_attr, final int indent_level) {

		boolean array_json = !table.virtual && array_all;

		String indent_space = getIndentSpaces(indent_level);

		List<PgField> fields = table.fields;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.jsonb == null)
				continue;

			if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0 && field.jsonb.length() > 2) {

				if (field.jsonb_null_size == 1 && field.getJsonSchemaFormat(schema_ver) != null)
					continue;

				boolean array_field = array_json || field.jsonb_col_size > 1;

				buffer.append(indent_space + "\"" + getItemName(field, as_attr) + "\":" + key_value_space + (array_field ? "[" : ""));

				field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));

				buffer.append(field.jsonb);
				buffer.append((array_field ? "]" : "") + "," + line_feed_code);

			}

			field.jsonb.setLength(0);

			field.jsonb_col_size = field.jsonb_null_size = 0;

		}

	}

	/**
	 * Merge fields' JSON buffer of current table to the mainline's JSON buffer (Relational-oriented JSON).
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 */
	public void writeFields(PgTable table, final int indent_level) {

		boolean unique_table = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child);

		String indent_space = getIndentSpaces(indent_level);

		List<PgField> fields = table.fields;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.jsonb == null)
				continue;

			if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0 && field.jsonb.length() > 2) {

				boolean array_field = array_all || (!unique_table && field.jsonb_col_size > 1);

				buffer.append(indent_space + "\"" + getItemName(field, false) + "\":" + key_value_space + (array_field ? "[" : ""));

				field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));

				buffer.append(field.jsonb.toString());
				buffer.append((array_field ? "]" : "") + "," + line_feed_code);

			}

			field.jsonb.setLength(0);

			field.jsonb_col_size = field.jsonb_null_size = 0;

		}

	}

	/**
	 * Write buffer to BufferedWriter.
	 *
	 * @param buffw buffered writer
	 * @throws PgSchemaException the pg schema exception
	 */
	public void write(BufferedWriter buffw) throws PgSchemaException {

		try {
			buffw.write(buffer.toString());
		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

		buffer.setLength(0);

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
			buffer.append(getIndentSpaces(1) + "\"" + getItemName(field, as_attr) + "\":" + key_value_space + field.normalizeAsJson(schema_ver, content) + line_feed_code + ",");

	}

	/**
	 * Merge field's JSON buffer to the mainline's JSON buffer.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 */
	public void writeFieldFrag(PgField field, boolean as_attr) {

		if (field.jsonb == null)
			return;

		if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0 && field.jsonb.length() > 2) {

			buffer.append(getIndentSpaces(1) + "\"" + getItemName(field, as_attr) + "\":" + key_value_space + "[");

			field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));
			buffer.append(field.jsonb);

			buffer.append("]," + line_feed_code);

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

		JsonBuilderPendingElem elem;

		while ((elem = pending_elem.pollLast()) != null)
			elem.write(this);

	}

	/**
	 * Write pending simple content.
	 */
	public void writePendingSimpleCont() {

		if (pending_simple_cont.length() == 0)
			return;

		buffer.append(pending_simple_cont.toString());

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

		buffer.append(getIndentSpaces(indent_level) + "\"" + getItemName(field, as_attr) + "\":" + key_value_space + (array_field ? "[" : "") + field.normalizeAsJson(schema_ver, content) + (array_field ? "]" : "") + "," + line_feed_code);

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

		buffer.append(getIndentSpaces(indent_level) + "\"" + (case_sense ? attr_name : attr_name.toLowerCase()) + "\":" + key_value_space + "\"" + content + "\"," + line_feed_code);

	}

	/**
	 * Write buffer to BufferredOutputStream.
	 *
	 * @param bout buffered output stream
	 * @throws PgSchemaException the pg schema exception
	 */
	public void write(BufferedOutputStream bout) throws PgSchemaException {

		try {
			bout.write(buffer.toString().getBytes());
		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

		buffer.setLength(0);

	}

}
