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

package net.sf.xsd2pgschema.docbuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;
import net.sf.xsd2pgschema.type.XsTableType;

/**
 * JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilder extends CommonBuilder {

	/** The JSON Schema version. */
	public JsonSchemaVersion schema_ver;

	/** The JSON type. */
	public JsonType type;

	/** The JSON buffer. */
	public StringBuilder buffer = new StringBuilder();

	/** The prefix of JSON key of xs:attribute. */
	protected String attr_prefix;

	/** The JSON key of xs:simpleContent. */
	protected String simple_content_name;

	/** The unit of indent space. */
	private String indent_space = " ";

	/** The JSON key value space with concatenation. */
	public String concat_value_space;

	/** The line feed code with concatenation. */
	public String concat_line_feed;

	/** The indent offset between key and value. */
	protected int key_value_offset = 1;

	/** Whether to retain case sensitive name. */
	public boolean case_sense;

	/** Use JSON array uniformly for descendants. */
	public boolean array_all;

	/** Whether to retain field annotation. */
	private boolean no_field_anno;

	/** The pending table header. */
	protected LinkedList<JsonBuilderPendingHeader> pending_header = new LinkedList<JsonBuilderPendingHeader>();

	/** The pending element. */
	public LinkedList<JsonBuilderPendingElem> pending_elem = new LinkedList<JsonBuilderPendingElem>();

	/** The suffix for JSON key declaration (internal use only). */
	private String key_decl_suffix_code;

	/** The code to start JSON object (internal use only). */
	private String start_object_code;

	/** The code to end JSON object (internal use only). */
	private String end_object_code;

	/** The code to end JSON object with concatenation (internal use only). */
	private String end_object_concat_code;

	/** The code to end JSON array (internal use only). */
	private String end_array_code;

	/** The code to end JSON array with concatenation (internal use only). */
	private String end_array_concat_code;

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

		StringBuilder sb = new StringBuilder();

		for (int l = 0; l < (key_value_offset = option.key_value_offset); l++)
			sb.append(" ");

		String key_value_space = sb.toString();

		sb.setLength(0);

		line_feed_code = option.line_feed_code;

		line_feed = line_feed_code.equals("\n");

		concat_value_space = "," + key_value_space;
		concat_line_feed = "," + line_feed_code;
		key_decl_suffix_code = "\":" + key_value_space;
		start_object_code = "{" + line_feed_code;
		end_object_code = "}" + line_feed_code;
		end_object_concat_code = "}" + concat_line_feed;
		end_array_code = "]" + line_feed_code;
		end_array_concat_code = "]" + concat_line_feed;

		if ((attr_prefix = option.attr_prefix) == null)
			attr_prefix = "";

		if ((simple_content_name = option.simple_content_name) == null)
			simple_content_name = PgSchemaUtil.simple_content_name;

	}

	/**
	 * Clear JSON builder.
	 *
	 * @param clear_buffer whether to clear JSON buffer
	 */
	public void clear(boolean clear_buffer) {

		super.clear();

		if (clear_buffer)
			buffer.setLength(0);

		pending_header.clear();

		pending_elem.clear();

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
	 * @param clear_buffer whether to clear JSON buffer
	 */
	public void writeStartDocument(boolean clear_buffer) {

		if (clear_buffer)
			buffer.setLength(0);

		else {

			int position = buffer.length() - (line_feed ? 2 : 1);

			if (position > 0 && buffer.charAt(position) != ',') {

				buffer.setLength(position + 1);
				buffer.append(concat_line_feed);

			}

		}

		buffer.append(start_object_code);

	}

	/**
	 * Write a end object.
	 */
	public void writeEndDocument() {

		trimComma();

		buffer.append(end_object_code);

	}

	/**
	 * Return JSON key of field.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @return String JSON key of field
	 */
	private String getKey(PgField field, boolean as_attr) {
		return (field.attribute || field.simple_attribute || (field.simple_attr_cond && as_attr) ? attr_prefix : "")
				+ (field.simple_content ? (field.simple_attribute || (field.simple_attr_cond && as_attr) ? (case_sense ? field.foreign_table_xname : field.foreign_table_xname.toLowerCase()) : simple_content_name) : (case_sense ? field.xname : field.xname.toLowerCase()));
	}

	/**
	 * Return JSON key declaration of field.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @return String JSON key declaration of field
	 */
	private String getKeyDecl(PgField field, boolean as_attr) {
		return "\"" + getKey(field, as_attr) + key_decl_suffix_code;
	}

	/**
	 * Return JSON key declaration.
	 *
	 * @param local_name local name as JSON key
	 * @param as_attr whether parent node as attribute
	 * @return String JSON key declaration
	 */
	private String getKeyDecl(String local_name, boolean as_attr) {
		return "\"" + (as_attr ? attr_prefix : "") + (case_sense ? local_name : local_name.toLowerCase()) + key_decl_suffix_code;
	}

	/**
	 * Return canonical JSON key declaration.
	 *
	 * @param canonical_key canonical JSON key
	 * @return String JSON key declaration
	 */
	public String getCanKeyDecl(String canonical_key) {
		return "\"" + canonical_key + key_decl_suffix_code;
	}

	/**
	 * Return canonical JSON key-value pair declaration with quotation of value.
	 *
	 * @param canonical_key canonical JSON key
	 * @param value JSON value
	 * @return String JSON key-value pair declaration
	 */
	private String getCanKeyValuePairDecl(String canonical_key, String value) {
		return getCanKeyDecl(canonical_key) + "\"" + value + "\"" + concat_line_feed;
	}

	/**
	 * Return canonical JSON key-value pair declaration without quotation of value.
	 *
	 * @param canonical_key canonical JSON key
	 * @param value JSON value
	 * @return String JSON key-value pair declaration
	 */
	private String getCanKeyValuePairDeclNoQuote(String canonical_key, String value) {
		return getCanKeyDecl(canonical_key) + value + concat_line_feed;
	}

	/**
	 * Return canonical JSON key declaration with starting object.
	 *
	 * @param canonical_key canonical JSON key
	 * @return String JSON key declaration with starting object.
	 */
	private String getCanKeyDeclStartObj(String canonical_key) {
		return getCanKeyDecl(canonical_key) + start_object_code;
	}

	/**
	 * Return canonical JSON key declaration with starting array.
	 *
	 * @param canonical_key canonical JSON key
	 * @param inline whether in-line declaration of array
	 * @return String JSON key declaration with starting array.
	 */
	private String getCanKeyDeclStartArray(String canonical_key, boolean inline) {
		return getCanKeyDecl(canonical_key) + "[" + (inline ? "" : line_feed_code);
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

		String indent_spaces = getIndentSpaces(1);

		buffer.append(indent_spaces + getCanKeyValuePairDecl("$schema", schema_ver.getNamespaceURI())); // declare JSON Schema

		if (def_namespaces != null) {

			String def_namespace = getSchemaAnnotation(def_namespaces.get(""));

			if (def_namespace != null)
				buffer.append(indent_spaces + getCanKeyValuePairDecl((schema_ver.equals(JsonSchemaVersion.draft_v4) ? "" : "$") + "id", def_namespace)); // declare unique identifier

		}

		if (def_anno_appinfo != null) {

			String _def_anno_appinfo = getSchemaAnnotation(def_anno_appinfo);

			if (!_def_anno_appinfo.startsWith("\""))
				_def_anno_appinfo = "\"" + _def_anno_appinfo + "\"";

			buffer.append(indent_spaces + getCanKeyValuePairDeclNoQuote("title", _def_anno_appinfo));

		}

		if (def_anno_doc != null) {

			String _def_anno_doc = getSchemaAnnotation(def_anno_doc);

			if (!_def_anno_doc.startsWith("\""))
				_def_anno_doc = "\"" + _def_anno_doc + "\"";

			buffer.append(indent_spaces + getCanKeyValuePairDeclNoQuote("description", _def_anno_doc));

		}

		buffer.append(indent_spaces + getCanKeyValuePairDecl("type", "object"));

		buffer.append(indent_spaces + getCanKeyDeclStartObj("properties")); // start root properties

	}

	/**
	 * Write JSON Schema footer.
	 */
	public void writeEndSchema() {

		trimComma();

		buffer.append(getIndentSpaces(1) + end_object_code); // end root properties

	}

	/**
	 * Write table header of JSON Schema.
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 */
	public void writeStartSchemaTable(PgTable table, final int indent_level) {

		int header_start = buffer.length();

		buffer.append(getIndentSpaces(indent_level) + getKeyDecl(table.name, false) + start_object_code); // start table

		String _indent_spaces = getIndentSpaces(indent_level + 1);

		if (table.anno != null && !table.anno.isEmpty()) {

			String table_anno = getOneLinerAnnotation(table.anno);

			if (!table_anno.startsWith("\""))
				table_anno = "\"" + table_anno + "\"";

			buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("description", table_anno));

		}

		buffer.append(_indent_spaces + getCanKeyValuePairDecl("type", "object"));

		buffer.append(_indent_spaces + getCanKeyDeclStartObj("properties")); // start field

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

		boolean has_required = table.fields.parallelStream().anyMatch(field -> field.required && field.jsonable);

		String _indent_spaces = getIndentSpaces(header.indent_level + 1);

		buffer.append(_indent_spaces + (has_required ? end_object_concat_code : end_object_code)); // end field

		if (has_required) {

			buffer.append(_indent_spaces + getCanKeyDeclStartArray("required", true));

			table.fields.stream().filter(field -> field.required && field.jsonable).forEach(field -> buffer.append("\"" + getKey(field, as_attr) + "\"" + concat_value_space));

			buffer.setLength(buffer.length() - (key_value_offset + 1));

			buffer.append(end_array_code);

		}

		buffer.append(getIndentSpaces(header.indent_level) + end_object_concat_code); // end table

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

		switch (field.xs_type) {
		case xs_any:
		case xs_anyAttribute:
			return;
		default:
			if (!object && !array)
				return;
		}

		String schema_type = field.xs_type.getJsonSchemaType();

		buffer.append(getIndentSpaces(indent_level++) + getKeyDecl(field, as_attr) + start_object_code); // start field

		String format = field.getJsonSchemaFormat(schema_ver);
		String _format = field.getJsonSchemaFormat(JsonSchemaVersion.latest);
		String ref = null;
		String _indent_spaces;

		// object

		if (object && !array) {}

		// array

		else if (!object && array) {

			_indent_spaces = getIndentSpaces(indent_level++);

			buffer.append(_indent_spaces + getCanKeyValuePairDecl("type", "array"));
			buffer.append(_indent_spaces + getCanKeyDeclStartObj("items")); // start items

			if (!field.required && format != null) {

				buffer.append(getIndentSpaces(indent_level++) + getCanKeyDeclStartArray("oneOf", false)); // start oneOf to allow empty string
				buffer.append(getIndentSpaces(indent_level++) + start_object_code); // start original array part

			}

		}

		// object or array

		else {

			buffer.append(getIndentSpaces(indent_level++) + getCanKeyDeclStartArray("oneOf", false)); // start oneOf
			buffer.append(getIndentSpaces(indent_level++) + start_object_code); // start object part

		}

		_indent_spaces = getIndentSpaces(indent_level);

		buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("type", schema_type));

		if (format != null)
			buffer.append(_indent_spaces + getCanKeyValuePairDecl("format", format));

		else if (_format == null) {

			ref = field.xs_type.getJsonSchemaRef();

			if (ref != null)
				buffer.append(_indent_spaces + getCanKeyValuePairDecl("$ref", field.xs_type.getJsonSchemaRef()));

		}

		if (!no_field_anno && field.anno != null && !field.anno.isEmpty()) {

			String anno = getOneLinerAnnotation(field.anno);

			if (!anno.startsWith("\""))
				anno = "\"" + anno + "\"";

			buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("description", anno));

		}

		if (field.default_value != null)
			buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("default", field.getJsonSchemaDefaultValue()));

		String enum_array = null;

		if (field.enum_name != null) {

			enum_array = field.getJsonSchemaEnumArray(concat_value_space);

			buffer.append(_indent_spaces + getCanKeyDeclStartArray("enum", true) + enum_array.substring(0, enum_array.length() - (key_value_offset + 1)) + end_array_concat_code);

		}

		if (field.length != null) {

			buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("maxLength", field.length));
			buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("minLength", field.length));

		}

		else {

			if (field.max_length != null)
				buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("maxLength", field.max_length));

			if (field.min_length != null)
				buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("minLength", field.min_length));

		}

		if (field.pattern != null)
			buffer.append(_indent_spaces + getCanKeyValuePairDecl("pattern", StringEscapeUtils.escapeEcmaScript(field.pattern)));

		String schema_maximum = null;
		String schema_minimum = null;

		switch (schema_ver) {
		case draft_v4:
			schema_maximum = field.getJsonSchemaMaximumValueDraftV4(this);

			if (schema_maximum != null)
				buffer.append(_indent_spaces + getCanKeyDecl("maximum") + schema_maximum + concat_line_feed);

			schema_minimum = field.getJsonSchemaMinimumValueDraftV4(this);

			if (schema_minimum != null)
				buffer.append(_indent_spaces + getCanKeyDecl("minimum") + schema_minimum + concat_line_feed);
			break;
		case draft_v6:
		case draft_v7:
		case latest:
			schema_maximum = field.getJsonSchemaMaximumValue(this);

			if (schema_maximum != null)
				buffer.append(_indent_spaces + schema_maximum + concat_line_feed);

			schema_minimum = field.getJsonSchemaMinimumValue(this);

			if (schema_minimum != null)
				buffer.append(_indent_spaces + schema_minimum + concat_line_feed);
		}

		String multiple_of = field.getJsonSchemaMultipleOfValue();

		if (multiple_of != null)
			buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("multipleOf", multiple_of));

		trimComma();

		// object

		if (object && !array) {}

		// array

		else if (!object && array) {

			if (!field.required && format != null) {

				_indent_spaces = getIndentSpaces(--indent_level);

				buffer.append(_indent_spaces + end_object_concat_code); // end original array part
				buffer.append(_indent_spaces + start_object_code); // start empty string part

				_indent_spaces = getIndentSpaces(++indent_level);

				buffer.append(_indent_spaces + getCanKeyValuePairDecl("type", "string"));
				buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("maxLength", "0"));

				trimComma();

				buffer.append(getIndentSpaces(--indent_level) + end_object_code); // end empty string part

				buffer.append(getIndentSpaces(--indent_level) + end_array_code); // end oneOf to allow empty string

			}

			buffer.append(getIndentSpaces(--indent_level) + end_object_code); // end items

		}

		// object or array

		else {

			_indent_spaces = getIndentSpaces(--indent_level);

			buffer.append(_indent_spaces + end_object_concat_code); // end object part
			buffer.append(_indent_spaces + start_object_code); // start array part

			_indent_spaces = getIndentSpaces(++indent_level);

			buffer.append(_indent_spaces + getCanKeyValuePairDecl("type", "array"));
			buffer.append(_indent_spaces + getCanKeyDeclStartObj("items")); // start items

			if (!field.required && format != null) {

				buffer.append(getIndentSpaces(++indent_level) + getCanKeyDeclStartArray("oneOf", false)); // start oneOf to allow empty string
				buffer.append(getIndentSpaces(++indent_level) + start_object_code); // start original array part

			}

			_indent_spaces = getIndentSpaces(++indent_level);

			buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("type", schema_type));

			if (format != null)
				buffer.append(_indent_spaces + getCanKeyValuePairDecl("format", format));

			else if (_format == null && ref != null)
				buffer.append(_indent_spaces + getCanKeyValuePairDecl("$ref", ref));

			if (!no_field_anno)
				buffer.append(_indent_spaces + getCanKeyValuePairDecl("description", "array of previous object: " + getKey(field, as_attr)));

			if (field.default_value != null)
				buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("default", field.getJsonSchemaDefaultValue()));

			if (field.enum_name != null)
				buffer.append(_indent_spaces + getCanKeyDeclStartArray("enum", true) + enum_array.substring(0, enum_array.length() - (key_value_offset + 1)) + end_array_concat_code);

			if (field.length != null) {

				buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("maxLength", field.length));
				buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("minLength", field.length));

			}

			else {

				if (field.max_length != null)
					buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("maxLength", field.max_length));

				if (field.min_length != null)
					buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("minLength", field.min_length));

			}

			if (field.pattern != null)
				buffer.append(_indent_spaces + getCanKeyValuePairDecl("pattern", StringEscapeUtils.escapeEcmaScript(field.pattern)));

			switch (schema_ver) {
			case draft_v4:
				if (schema_maximum != null)
					buffer.append(_indent_spaces + getCanKeyDecl("maximum") + schema_maximum + concat_line_feed);

				if (schema_minimum != null)
					buffer.append(_indent_spaces + getCanKeyDecl("minimum") + schema_minimum + concat_line_feed);
				break;
			case draft_v6:
			case draft_v7:
			case latest:
				if (schema_maximum != null)
					buffer.append(_indent_spaces + schema_maximum + concat_line_feed);

				if (schema_minimum != null)
					buffer.append(_indent_spaces + schema_minimum + concat_line_feed);
			}

			if (multiple_of != null)
				buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("multipleOf", multiple_of));

			trimComma();

			if (!field.required && format != null) {

				_indent_spaces = getIndentSpaces(--indent_level);

				buffer.append(_indent_spaces + end_object_concat_code); // end original array part
				buffer.append(_indent_spaces + start_object_code); // start empty string part

				_indent_spaces = getIndentSpaces(++indent_level);

				buffer.append(_indent_spaces + getCanKeyValuePairDecl("type", "string"));
				buffer.append(_indent_spaces + getCanKeyValuePairDeclNoQuote("maxLength", "0"));

				trimComma();

				buffer.append(getIndentSpaces(--indent_level) + end_object_code); // end empty string part

				buffer.append(getIndentSpaces(--indent_level) + end_array_code); // end oneOf to allow empty string

			}

			buffer.append(getIndentSpaces(--indent_level) + end_object_code); // end items

			buffer.append(getIndentSpaces(--indent_level) + end_object_code); // end array part
			buffer.append(getIndentSpaces(--indent_level) + end_array_code); // end oneOf

		}

		buffer.append(getIndentSpaces(--indent_level) + end_object_concat_code); // end field

	}

	/**
	 * Return schema annotation by replacement of XML Schema relating words.
	 *
	 * @param annotation annotation
	 * @return String escaped schema annotation
	 */
	private String getSchemaAnnotation(String annotation) {
		return StringEscapeUtils.escapeEcmaScript(annotation).replace("\\/", "/").replace("\\'", "'").replace(".xsd", ".json").replaceAll(".xml", ".json").replace("XSD", "JSON Schema").replace("Xsd", "Json schema").replace("xsd", "json schema").replace("XML", "JSON").replace("Xml", "Json").replace("xml", "json");
	}

	/**
	 * Return one-liner annotation.
	 *
	 * @param annotation annotation
	 * @return String escaped one-liner annotation
	 */
	private String getOneLinerAnnotation(String annotation) {
		return StringEscapeUtils.escapeCsv(annotation.replace("\n--", ""));
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

		clear(true);

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

		buffer.append(getIndentSpaces(indent_level) + (object ? getKeyDecl(table.name, false) : "") + start_object_code);

		pending_header.push(new JsonBuilderPendingHeader(header_start, buffer.length(), indent_level));

	}

	/**
	 * Write any table header.
	 *
	 * @param local_name local name
	 * @param indent_level current indent level
	 */
	public void writeStartAnyTable(String local_name, final int indent_level) {

		int header_start = buffer.length();

		buffer.append(getIndentSpaces(indent_level) + getKeyDecl(local_name, false) + start_object_code);

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

		buffer.append(getIndentSpaces(header.indent_level) + end_object_concat_code);

	}

	/**
	 * Write fields' JSON buffer of current table to the mainline's JSON buffer.
	 *
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param indent_level current indent level
	 */
	public void writeFields(PgTable table, boolean as_attr, final int indent_level) {

		boolean array_json = !table.virtual && array_all;

		for (PgField field : table.fields) {

			if (field.jsonb == null)
				continue;

			if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0) {

				if (field.jsonb_null_size == 1 && field.getJsonSchemaFormat(schema_ver) != null)
					continue;

				writeField(field, as_attr, array_json || field.jsonb_col_size > 1, indent_level);

			}

			field.jsonb.setLength(0);

			field.jsonb_col_size = field.jsonb_null_size = 0;

		}

	}

	/**
	 * Write field's JSON buffer of current table to the mainline's JSON buffer.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @param array_field whether field as JSON array
	 * @param indent_level current indent level
	 */
	private void writeField(PgField field, boolean as_attr, boolean array_field, final int indent_level) {

		String indent_spaces = getIndentSpaces(indent_level);

		switch (field.xs_type) {
		case xs_any:
		case xs_anyAttribute:
			field.jsonb.setLength(field.jsonb.length() - 1); // remove the last tag code

			String[] fragments = field.jsonb.toString().split("\t", -1);

			ArrayList<String> item_paths = new ArrayList<String>();

			for (String fragment : fragments) {

				for (String item : fragment.split("\n")) {

					String item_path = item.substring(0, item.indexOf(':'));
					String parent_path = getParentPath(item_path);

					if (!parent_path.isEmpty() && !item_paths.contains(parent_path))
						item_paths.add(parent_path);

					if (!item_paths.contains(item_path))
						item_paths.add(item_path);

				}

			}

			writeAnyField(field, array_field, false, false, "", fragments, item_paths, indent_level);

			item_paths.clear();
			break;
		default:
			buffer.append(indent_spaces + getKeyDecl(field, as_attr) + (array_field ? "[" : ""));

			field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));

			buffer.append(field.jsonb);
			buffer.append(array_field ? end_array_concat_code : concat_line_feed);
		}

	}

	/**
	 * Write any field' JSON buffer to the mainline's JSON buffer.
	 *
	 * @param field current field
	 * @param array_field whether field as JSON array
	 * @param attr_only whether to process attribute only
	 * @param elem_only whether to process element only
	 * @param cur_path current path
	 * @param fragments fragmented JSON content
	 * @param item_paths list of item paths
	 * @param indent_level current indent level
	 */
	private void writeAnyField(PgField field, boolean array_field, boolean attr_only, boolean elem_only, String cur_path, String[] fragments, ArrayList<String> item_paths, int indent_level) {

		String indent_spaces = getIndentSpaces(indent_level);

		StringBuilder sb = new StringBuilder();

		if (!elem_only) {

			// attribute

			item_paths.stream().filter(item_path -> getParentPath(item_path).equals(cur_path) && getLastNameOfPath(item_path).startsWith("@")).forEach(item_path -> {

				boolean has_value;

				for (String fragment : fragments) {

					has_value = false;

					for (String item : fragment.split("\n")) {

						if (item.substring(0, item.lastIndexOf(':')).equals(item_path)) {

							sb.append(item.substring(item.indexOf(':') + 1) + concat_value_space);

							has_value = true;

						}

					}

					if (!has_value)
						sb.append("\"\"" + concat_value_space);

				}

				buffer.append(indent_spaces + getKeyDeclOfPath(item_path) + (array_field ? "[" : ""));

				sb.setLength(sb.length() - (key_value_offset + 1));

				buffer.append(sb);
				buffer.append(array_field ? end_array_concat_code : concat_line_feed);	

				sb.setLength(0);

			});

			if (attr_only)
				return;

		}

		// element

		item_paths.stream().filter(item_path -> getParentPath(item_path).equals(cur_path) && !getLastNameOfPath(item_path).startsWith("@")).forEach(item_path -> {

			boolean has_attr = item_paths.parallelStream().anyMatch(_item_path -> getParentPath(_item_path).equals(item_path) && getLastNameOfPath(_item_path).startsWith("@"));
			boolean has_child = item_paths.parallelStream().anyMatch(_item_path -> getParentPath(_item_path).equals(item_path));
			boolean has_content = false;

			for (String fragment : fragments) {

				for (String item : fragment.split("\n")) {

					if (item.substring(0, item.lastIndexOf(':')).equals(item_path)) {

						sb.append(item.substring(item.indexOf(':') + 1) + concat_value_space);

						has_content = true;

					}

				}

			}

			if (has_content) {

				if (has_child) {

					buffer.append(indent_spaces + getKeyDeclOfPath(item_path) + start_object_code);

					if (has_attr)
						writeAnyField(field, array_field, true, false, item_path, fragments, item_paths, indent_level + 1);

					buffer.append(getIndentSpaces(indent_level + 1) + getKeyDeclOfPath(simple_content_name) + (array_field ? "[" : ""));

					sb.setLength(sb.length() - (key_value_offset + 1));

					buffer.append(sb);
					buffer.append(array_field ? end_array_concat_code : concat_line_feed);

					writeAnyField(field, array_field, false, true, item_path, fragments, item_paths, indent_level + 1);

					trimComma();

					buffer.append(indent_spaces + end_object_concat_code);

				}

				else {

					buffer.append(indent_spaces + getKeyDeclOfPath(item_path) + (array_field ? "[" : ""));

					sb.setLength(sb.length() - (key_value_offset + 1));

					buffer.append(sb);
					buffer.append(array_field ? end_array_concat_code : concat_line_feed);	

				}

				sb.setLength(0);

			}

			// blank element

			else {

				buffer.append(indent_spaces + getKeyDeclOfPath(item_path) + start_object_code);

				writeAnyField(field, array_field, false, false, item_path, fragments, item_paths, indent_level + 1);

				trimComma();

				buffer.append(indent_spaces + end_object_concat_code);

			}

		});

	}

	/**
	 * Return JSON key of current path.
	 *
	 * @param path current path
	 * @return String JSON key of current path
	 */
	private String getKeyOfPath(String path) {

		String name = getLastNameOfPath(path);

		if (!attr_prefix.equals("@"))
			name = name.replace("@", attr_prefix);

		return (case_sense ? name : name.toLowerCase());
	}

	/**
	 * Return JSON key declaration of current path.
	 *
	 * @param path current path
	 * @return String JSON key declaration of current path
	 */
	private String getKeyDeclOfPath(String path) {
		return "\"" + getKeyOfPath(path) + key_decl_suffix_code;
	}

	/**
	 * Write fields' JSON buffer of current table to the mainline's JSON buffer (Relational-oriented JSON).
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 */
	public void writeFields(PgTable table, final int indent_level) {

		boolean unique_table = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child);

		for (PgField field : table.fields) {

			if (field.jsonb == null)
				continue;

			if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0)
				writeField(field, false, array_all || (!unique_table && field.jsonb_col_size > 1), indent_level);

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
			field.write(schema_ver, content, false, concat_value_space);
		else
			buffer.append(getIndentSpaces(1) + getKeyDecl(field, as_attr) + field.normalize(schema_ver, content) + line_feed_code + ",");

	}

	/**
	 * Write field's JSON buffer to the mainline's JSON buffer.
	 *
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 */
	public void writeFieldFrag(PgField field, boolean as_attr) {

		if (field.jsonb == null)
			return;

		if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0) {

			buffer.append(getIndentSpaces(1) + getKeyDecl(field, as_attr) + "[");

			field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));
			buffer.append(field.jsonb);

			buffer.append(end_array_concat_code);

			field.jsonb.setLength(0);

			field.jsonb_col_size = field.jsonb_null_size = 0;

		}

	}

	/**
	 * Write any field's content as JSON fragment.
	 *
	 * @param field current field
	 * @param local_name local name
	 * @param as_attr whether parent node as attribute
	 * @param content content
	 * @param indent_level current indent level
	 */
	public void writeAnyFieldFrag(PgField field, String local_name, boolean as_attr, String content, final int indent_level) {

		if (array_all)
			field.write(schema_ver, content, true, concat_value_space);

		else
			writeAnyField(local_name, as_attr, content, indent_level);

	}

	/**
	 * Write any field's JSON buffer to the mainline's JSON buffer.
	 *
	 * @param field current field
	 * @param path current path
	 */
	public void writeAnyFieldFrag(PgField field, String path) {

		if (field.jsonb == null)
			return;

		if ((field.required || field.jsonb_not_empty) && field.jsonb_col_size > 0) {

			buffer.append(getIndentSpaces(1) + getKeyDeclOfPath(path) + "[");

			field.jsonb.setLength(field.jsonb.length() - (key_value_offset + 1));
			buffer.append(field.jsonb);

			buffer.append(end_array_concat_code);

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

		super.clear();

	}

	/**
	 * Write field's content.
	 *
	 * @param table current table
	 * @param field current field
	 * @param as_attr whether parent node as attribute
	 * @param content content
	 * @param indent_level current indent level
	 */
	public void writeField(PgTable table, PgField field, boolean as_attr, String content, final int indent_level) {

		boolean array_field = table != null && !table.virtual && !table.list_holder && !table.bridge && array_all;

		buffer.append(getIndentSpaces(indent_level) + getKeyDecl(field, as_attr) + (array_field ? "[" : "") + field.normalize(schema_ver, content) + (array_field ? end_array_concat_code : concat_line_feed));

	}

	/**
	 * Write any field's content.
	 *
	 * @param local_name local name
	 * @param as_attr whether parent node as attribute
	 * @param content content
	 * @param indent_level current indent level
	 */
	public void writeAnyField(String local_name, boolean as_attr, String content, final int indent_level) {

		buffer.append(getIndentSpaces(indent_level) + getKeyDecl(local_name, as_attr) + "\"" + StringEscapeUtils.escapeEcmaScript(content) + "\"" + concat_line_feed);

	}

	/**
	 * Write any attribute's content.
	 *
	 * @param any_attr pending any attribute
	 */
	public void writeAnyAttr(JsonBuilderPendingAttr any_attr) {

		buffer.append(getIndentSpaces(any_attr.indent_level) + getKeyDecl(any_attr.local_name, true) + "\"" + StringEscapeUtils.escapeEcmaScript(any_attr.content) + "\"" + line_feed_code);

	}

	/**
	 * Write any attributes' content.
	 *
	 * @param any_attrs list of pending any attributes
	 */
	public void writeAnyAttrs(List<JsonBuilderPendingAttr> any_attrs) {

		JsonBuilderPendingAttr first = any_attrs.get(0);

		int indent_level = first.indent_level;

		String indent_spaces = getIndentSpaces(indent_level);

		any_attrs.forEach(any_attr -> buffer.append(indent_spaces + getKeyDecl(any_attr.local_name, true) + "\"" + StringEscapeUtils.escapeEcmaScript(any_attr.content) + "\"" + concat_line_feed));

	}

	/**
	 * Write buffer to OutputStream.
	 *
	 * @param out output stream
	 * @throws PgSchemaException the pg schema exception
	 */
	public void write(OutputStream out) throws PgSchemaException {

		try {
			out.write(buffer.toString().getBytes(PgSchemaUtil.def_charset));
		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

		clear(true);

	}

	// JSON composer over PostgreSQL

	/**
	 * Nest node and compose JSON document.
	 *
	 * @param schema PostgreSQL data model
	 * @param table current table
	 * @param parent_key parent key
	 * @param as_attr whether parent key is simple attribute
	 * @param parent_nest_test nest test result of parent node
	 * @return JsonBuilderNestTester nest test of this node
	 * @throws PgSchemaException the pg schema exception
	 */	
	public JsonBuilderNestTester nestChildNode2Json(final PgSchema schema, final PgTable table, final Object parent_key, final boolean as_attr, JsonBuilderNestTester parent_nest_test) throws PgSchemaException {

		try {

			JsonBuilderNestTester nest_test = new JsonBuilderNestTester(table, parent_nest_test);
			JsonBuilderPendingElem elem;
			JsonBuilderPendingAttr attr;

			boolean not_virtual = !table.virtual && !as_attr;
			boolean not_list_and_bridge = !table.list_holder && table.bridge;
			boolean array_field = not_virtual && !not_list_and_bridge && table.total_nested_fields == 0 && type.equals(JsonType.column);

			boolean category = not_virtual && (not_list_and_bridge || array_field);
			boolean category_item = not_virtual && !(not_list_and_bridge || array_field);

			if (category) {

				pending_elem.push(new JsonBuilderPendingElem(table, nest_test.current_indent_level));

				if (parent_nest_test.has_insert_doc_key)
					parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

			}

			boolean use_doc_key_index = schema.document_id != null && !table.has_unique_primary_key;
			boolean use_primary_key = !use_doc_key_index || table.list_holder || table.virtual || table.has_simple_content || table.total_foreign_fields > 1;
			boolean attr_only;

			PreparedStatement ps = table.ps;

			if (ps == null) {

				String sql = "SELECT * FROM " + table.pgname + " WHERE " + (use_doc_key_index ? table.doc_key_pgname + " = ?" : "") + (use_primary_key ? (use_doc_key_index ? " AND " : "") + table.primary_key_pgname + " = ?" : "");

				ps = table.ps = schema.db_conn.prepareStatement(sql);
				ps.setFetchSize(PgSchemaUtil.pg_min_rows_for_doc_key_index);

				if (use_doc_key_index)
					ps.setString(1, schema.document_id);

			}

			int param_id = use_doc_key_index ? 2 : 1;

			if (use_primary_key) {

				switch (schema.option.hash_size) {
				case native_default:
					ps.setBytes(param_id, (byte[]) parent_key);
					break;
				case unsigned_int_32:
					ps.setInt(param_id, (int) (parent_key));
					break;
				case unsigned_long_64:
					ps.setLong(param_id, (long) parent_key);
					break;
				default:
					throw new PgSchemaException("Not allowed to use string hash key (debug mode) for XPath evaluation.");
				}

			}

			ResultSet rset = ps.executeQuery();

			List<PgField> fields = table.fields;

			String content;

			PgTable nested_table;

			Object key;

			int n;

			while (rset.next()) {

				if (category_item) {

					pending_elem.push(new JsonBuilderPendingElem(table, nest_test.current_indent_level));

					if (parent_nest_test.has_insert_doc_key)
						parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

					if (!table.bridge)
						nest_test.has_child_elem = false;

				}

				// attribute, simple attribute, any_attribute

				if (table.has_attrs) {

					param_id = 1;

					for (PgField field : fields) {

						if (field.attribute) {

							content = field.retrieve(rset, param_id);

							if (content != null) {

								if (array_field)
									field.write(schema_ver, content, false, concat_value_space);

								else {

									attr = new JsonBuilderPendingAttr(field, content, nest_test.child_indent_level);

									elem = pending_elem.peek();

									if (elem != null)
										elem.appendPendingAttr(attr);
									else
										attr.write(this);

								}

								if (!nest_test.has_content)
									nest_test.has_content = true;

							}

						}

						else if ((field.simple_attribute || field.simple_attr_cond) && as_attr) {

							content = field.retrieve(rset, param_id);

							if (content != null) {

								if (array_field)
									field.write(schema_ver, content, false, concat_value_space);

								else {

									attr = new JsonBuilderPendingAttr(field, schema.getForeignTable(field), content, nest_test.child_indent_level - 1); // decreasing indent level means simple attribute or conditional attribute derives from parent table

									elem = pending_elem.peek();

									if (elem != null)
										elem.appendPendingAttr(attr);
									else
										attr.write(this);

								}

								nest_test.has_content = true;

							}

						}

						else if (field.any_attribute) {

							SQLXML xml_object = rset.getSQLXML(param_id);

							if (xml_object != null) {

								InputStream in = xml_object.getBinaryStream();

								if (in != null) {

									JsonBuilderAnyAttrRetriever any_attr = new JsonBuilderAnyAttrRetriever(table.pname, field, nest_test, array_field, this);

									nest_test.any_sax_parser.parse(in, any_attr);

									nest_test.any_sax_parser.reset();

									in.close();

								}

								xml_object.free();

							}

						}

						else if (field.nested_key_as_attr) {

							key = rset.getObject(param_id);

							if (key != null)
								nest_test.merge(nestChildNode2Json(schema, schema.getTable(field.foreign_table_id), key, true, nest_test));

						}

						if (!field.omissible)
							param_id++;

					}

				}

				// simple_content, element, any

				if (table.has_elems) {

					param_id = 1;

					for (PgField field : fields) {

						if (field.simple_content && !field.simple_attribute && !as_attr) {

							content = field.retrieve(rset, param_id);

							if (content != null) {

								if (array_field)
									field.write(schema_ver, content, false, concat_value_space);

								else {

									if (pending_elem.peek() != null)
										writePendingElems(false);

									writePendingSimpleCont();

									writeField(table, field, false, content, nest_test.child_indent_level);

								}

								nest_test.has_simple_content = nest_test.has_open_simple_content = true;

							}

						}

						else if (field.element) {

							content = field.retrieve(rset, param_id);

							if (content != null) {

								if (array_field)
									field.write(schema_ver, content, false, concat_value_space);

								else {

									if (pending_elem.peek() != null)
										writePendingElems(false);

									writePendingSimpleCont();

									writeField(table, field, false, content, nest_test.child_indent_level);

								}

								if (!nest_test.has_child_elem || !nest_test.has_content)
									nest_test.has_child_elem = nest_test.has_content = true;

								if (parent_nest_test.has_insert_doc_key)
									parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

							}

						}

						else if (field.any) {

							SQLXML xml_object = rset.getSQLXML(param_id);

							if (xml_object != null) {

								InputStream in = xml_object.getBinaryStream();

								if (in != null) {

									JsonBuilderAnyRetriever any = new JsonBuilderAnyRetriever(table.pname, field, nest_test, array_field, this);

									nest_test.any_sax_parser.parse(in, any);

									nest_test.any_sax_parser.reset();

									if (parent_nest_test.has_insert_doc_key)
										parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

									in.close();

								}

								xml_object.free();

							}

						}

						if (!field.omissible)
							param_id++;

					}

				}

				// nested key

				if (table.total_nested_fields > 0) {

					param_id = 1;
					n = 0;

					for (PgField field : fields) {

						if (field.nested_key && !field.nested_key_as_attr) {

							key = rset.getObject(param_id);

							if (key != null) {

								nest_test.has_child_elem |= n++ > 0;

								nested_table = schema.getTable(field.foreign_table_id);

								if (nested_table.content_holder || !nested_table.bridge || as_attr)
									nest_test.merge(nestChildNode2Json(schema, nested_table, key, false, nest_test));

								// skip bridge table for acceleration

								else if (nested_table.list_holder)
									nest_test.merge(skipListAndBridgeNode2Json(schema, nested_table, key, nest_test));

								else
									nest_test.merge(skipBridgeNode2Json(schema, nested_table, key, nest_test));

							}

						}

						if (!field.omissible)
							param_id++;

					}

				}

				if (category_item) {

					if (nest_test.has_content || nest_test.has_simple_content) {

						attr_only = false;

						if (pending_elem.peek() != null)
							writePendingElems(attr_only = true);

						writePendingSimpleCont();

						if (!nest_test.has_open_simple_content && !attr_only) { }
						else if (nest_test.has_simple_content)
							nest_test.has_open_simple_content = false;

						writeEndTable();

					}

					else
						pending_elem.poll();

				}

			}

			rset.close();

			if (category) {

				if (nest_test.has_content || nest_test.has_simple_content) {

					attr_only = false;

					if (pending_elem.peek() != null)
						writePendingElems(attr_only = true);

					writePendingSimpleCont();

					if (array_field)
						writeFields(table, false, nest_test.child_indent_level);

					if (!nest_test.has_open_simple_content && !attr_only) { }
					else if (nest_test.has_simple_content)
						nest_test.has_open_simple_content = false;

					writeEndTable();

				}

				else
					pending_elem.poll();

			}

			return nest_test;

		} catch (SQLException | SAXException | IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Skip list holder and bridge node and continue to compose JSON document.
	 *
	 * @param schema PostgreSQL data model
	 * @param table list holder and bridge table
	 * @param parent_key parent key
	 * @param parent_nest_test nest test result of parent node
	 * @return JsonBuilderNestTester nest test of this node
	 * @throws PgSchemaException the pg schema exception
	 */
	public JsonBuilderNestTester skipListAndBridgeNode2Json(final PgSchema schema, final PgTable table, final Object parent_key, JsonBuilderNestTester parent_nest_test) throws PgSchemaException {

		try {

			JsonBuilderNestTester nest_test = new JsonBuilderNestTester(table, parent_nest_test);

			boolean category_item = !table.virtual;

			boolean use_doc_key_index = schema.document_id != null && !table.has_unique_primary_key;
			boolean attr_only;

			PreparedStatement ps = table.ps;

			PgField nested_key = table.nested_fields.stream().filter(field -> !field.nested_key_as_attr).findFirst().get();
			PgTable nested_table = schema.getTable(nested_key.foreign_table_id);

			if (ps == null) {

				String sql = "SELECT " + PgSchemaUtil.avoidPgReservedWords(nested_key.pname) + " FROM " + table.pgname + " WHERE " + (use_doc_key_index ? table.doc_key_pgname + " = ?" : "") + (use_doc_key_index ? " AND " : "") + table.primary_key_pgname + " = ?";

				ps = table.ps = schema.db_conn.prepareStatement(sql);
				ps.setFetchSize(PgSchemaUtil.pg_min_rows_for_doc_key_index);

				if (use_doc_key_index)
					ps.setString(1, schema.document_id);

			}

			int param_id = use_doc_key_index ? 2 : 1;

			switch (schema.option.hash_size) {
			case native_default:
				ps.setBytes(param_id, (byte[]) parent_key);
				break;
			case unsigned_int_32:
				ps.setInt(param_id, (int) (parent_key));
				break;
			case unsigned_long_64:
				ps.setLong(param_id, (long) parent_key);
				break;
			default:
				throw new PgSchemaException("Not allowed to use string hash key (debug mode) for XPath evaluation.");
			}

			ResultSet rset = ps.executeQuery();

			Object key;

			while (rset.next()) {

				if (category_item) {

					pending_elem.push(new JsonBuilderPendingElem(table, nest_test.current_indent_level));

					if (parent_nest_test.has_insert_doc_key)
						parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

					if (!table.bridge)
						nest_test.has_child_elem = false;

				}

				// nested key

				key = rset.getObject(1);

				if (key != null) {

					if (nested_table.content_holder || !nested_table.bridge)
						nest_test.merge(nestChildNode2Json(schema, nested_table, key, false, nest_test));

					// skip bridge table for acceleration

					else if (nested_table.list_holder)
						nest_test.merge(skipListAndBridgeNode2Json(schema, nested_table, key, nest_test));

					else
						nest_test.merge(skipBridgeNode2Json(schema, nested_table, key, nest_test));

				}

				if (category_item) {

					if (nest_test.has_content || nest_test.has_simple_content) {

						attr_only = false;

						if (pending_elem.peek() != null)
							writePendingElems(attr_only = true);

						writePendingSimpleCont();

						if (!nest_test.has_open_simple_content && !attr_only) { }
						else if (nest_test.has_simple_content)
							nest_test.has_open_simple_content = false;

						writeEndTable();

					}

					else
						pending_elem.poll();

				}

			}

			rset.close();

			return nest_test;

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Skip bridge node and continue to compose JSON document.
	 *
	 * @param schema PostgreSQL data model
	 * @param table bridge table
	 * @param parent_key parent key
	 * @param parent_nest_test nest test result of parent node
	 * @return JsonBuilderNestTester nest test of this node
	 * @throws PgSchemaException the pg schema exception
	 */	
	public JsonBuilderNestTester skipBridgeNode2Json(final PgSchema schema, final PgTable table, final Object parent_key, JsonBuilderNestTester parent_nest_test) throws PgSchemaException {

		JsonBuilderNestTester nest_test = new JsonBuilderNestTester(table, parent_nest_test);

		boolean category = !table.virtual;

		if (category) {

			pending_elem.push(new JsonBuilderPendingElem(table, nest_test.current_indent_level));

			if (parent_nest_test.has_insert_doc_key)
				parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

		}

		for (PgField field : table.nested_fields) {

			if (!field.nested_key_as_attr) {

				PgTable nested_table = schema.getTable(field.foreign_table_id);

				if (nested_table.content_holder || !nested_table.bridge)
					nest_test.merge(nestChildNode2Json(schema, nested_table, parent_key, false, nest_test));

				// skip bridge table for acceleration

				else if (nested_table.list_holder)
					nest_test.merge(skipListAndBridgeNode2Json(schema, nested_table, parent_key, nest_test));

				else
					nest_test.merge(skipBridgeNode2Json(schema, nested_table, parent_key, nest_test));

				break;
			}

		}

		if (category) {

			if (nest_test.has_content || nest_test.has_simple_content) {

				boolean attr_only = false;

				if (pending_elem.peek() != null)
					writePendingElems(attr_only = true);

				writePendingSimpleCont();

				if (!nest_test.has_open_simple_content && !attr_only) { }
				else if (nest_test.has_simple_content)
					nest_test.has_open_simple_content = false;

				writeEndTable();

			}

			else
				pending_elem.poll();

		}

		return nest_test;
	}

}
