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

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * PostgreSQL field declaration.
 *
 * @author yokochi
 */
public class PgField {

	/** The target namespace. */
	protected String target_namespace = PgSchemaUtil.xs_namespace_uri;

	/** The prefix of target namespace. */
	protected String prefix = "";

	/** The canonical name in XML Schema. */
	protected String xname = "";

	/** The field name in PostgreSQL. */
	protected String pname = "";

	/** The field name. */
	protected String name = "";

	/** The data type in XML document. */
	protected String xtype = null;

	/** The data type. */
	protected String type = null;

	/** The @substitutionGroup. */
	protected String substitution_group = null;

	/** The @maxOccurs. */
	protected String maxoccurs = "1";

	/** The @minOccurs. */
	protected String minoccurs = "1";

	/** The content of xs:annotation/xs:documentation (as is). */
	protected String xanno_doc = null;

	/** The content of xs:annotation. */
	protected String anno = null;

	/** The XML Schema data type. */
	protected XsDataType xs_type;

	/** Whether target namespace equals URI of XML Schema 1.x. */
	protected boolean is_xs_namespace = true;

	/** Whether xs:element. */
	protected boolean element = false;

	/** Whether xs:attribute. */
	protected boolean attribute = false;

	/** Whether xs:simpleContent. */
	protected boolean simple_content = false;

	/** Whether xs:simpleContent as primitive list. */
	protected boolean simple_primitive_list = false;

	/** Whether xs:simpleContent as attribute. */
	protected boolean simple_attribute = false;

	/** Whether xs:simpleContent as attribute conditionally, which depends on parent node. */
	protected boolean simple_attr_cond = false;

	/** Whether xs:any. */
	protected boolean any = false;

	/** Whether xs:anyAttribute. */
	protected boolean any_attribute = false;

	/** Whether primary key. */
	protected boolean primary_key = false;

	/** Whether unique key. */
	protected boolean unique_key = false;

	/** Whether foreign key. */
	protected boolean foreign_key = false;

	/** Whether nested key. */
	protected boolean nested_key = false;

	/** Whether nested key as attribute. */
	protected boolean nested_key_as_attr = false;

	/** Whether document key. */
	protected boolean document_key = false;

	/** Whether serial key. */
	protected boolean serial_key = false;

	/** Whether XPath key. */
	protected boolean xpath_key = false;

	/** Whether @use="required" | @nillable="false". */
	protected boolean xrequired = false;

	/** Whether @use="required" | @nillalbe="false", but be false in PostgreSQL when name collision occurs. */
	protected boolean required = false;

	/** Whether @use is "prohibited". */
	protected boolean prohibited = false;

	/** Whether @maxOccurs is greater than 1 || @minOccurs is greater than 1. */
	protected boolean list_holder = false;

	/** Whether representative field of substitution group. */
	protected boolean rep_substitution_group = false;

	/** Whether Sphinx multi-valued attribute. */
	protected boolean sph_mva = false;

	/** Whether it is selected as field for partial indexing. */
	protected boolean field_sel = false;

	/** Whether it is selected as attribute for partial indexing. */
	protected boolean attr_sel = false;

	/** Whether it is selected as attribute and ready for partial indexing. */
	protected boolean attr_sel_rdy = true;

	/** The constraint name in PostgreSQL. */
	protected String constraint_name = null;

	/** The foreign table id. */
	protected int foreign_table_id = -1;

	/** The schema name of foreign table in PostgreSQL (default schema name is "public"). */
	protected String foreign_schema = PgSchemaUtil.pg_public_schema_name;

	/** The foreign table name in XML Schema. */
	protected String foreign_table_xname = null;

	/** The foreign table name in PostgreSQL. */
	protected String foreign_table_pname = null;

	/** The foreign field name in PostgreSQL. */
	protected String foreign_field_pname = null;

	/** The ancestor node names. */
	protected String ancestor_node = null;

	/** The parent node names. */
	protected String parent_node = null;

	/** The @fixed. */
	protected String fixed_value = null;

	/** The @default. */
	protected String default_value = null;

	/** The @block. */
	protected String block_value = null;

	/** Whether field has any restriction. */
	protected boolean restriction = false;

	/** The xs:enumeration. */
	protected String enum_name = null;

	/** The array of xs:enumeration in XML document. */
	protected String[] xenumeration = null;

	/** The array of xs:enumeration in PostgreSQL. */
	protected String[] enumeration = null;

	/** The xs:length restriction. */
	protected String length = null;

	/** The xs:minLength restriction. */
	protected String min_length = null;

	/** The xs:maxLength restriction. */
	protected String max_length = null;

	/** The xs:pattern restriction. */
	protected String pattern = null;

	/** The xs:maxInclusive restriction. */
	protected String max_inclusive = null;

	/** The xs:maxExclusive restriction. */
	protected String max_exclusive = null;

	/** The xs:minExclusive restriction. */
	protected String min_exclusive = null;

	/** The xs:minInclusive restriction. */
	protected String min_inclusive = null;

	/** The xs:totalDigits restriction. */
	protected String total_digits = null;

	/** The xs:fractionDigits restriction. */
	protected String fraction_digits = null;

	/** The xs:whiteSpace restriction. */
	protected String white_space = null;

	/** The xs:explicitTimezone restriction. */
	protected String explicit_timezone = null;

	/** The xs:assertions restriction. */
	protected String assertions = null;

	/** Whether xs:list. */
	protected boolean _list = false;

	/** Whether xs:union. */
	protected boolean _union = false;

	/** The --fill-this option. */
	protected boolean fill_this = false;

	/** The filled text used in post XML edition. */
	protected String filled_text = null;

	/** The --filt-out option. */
	protected boolean filt_out = false;

	/** The filter patterns in post XML edition. */
	protected String[] filter_pattern = null;

	/** Whether it has any system's administrative key (primary_key || foreign_key || nested_key). */
	protected boolean system_key = false;

	/** Whether it has any user's discretion key (document_key || serial_key || xpath_key). */
	protected boolean user_key = false;

	/** Whether field is omissible. */
	protected boolean omissible = false;

	/** Whether field is indexable. */
	protected boolean indexable = true;

	/** Whether field is JSON convertible. */
	protected boolean jsonable = true;

	/** Whether JSON buffer is not empty (internal use only). */
	protected boolean jsonb_not_empty = false;

	/** The size of data in JSON buffer (internal use only). */
	protected int jsonb_col_size = 0;

	/** The size of null data in JSON buffer (internal use only). */
	protected int jsonb_null_size = 0;

	/** The JSON buffer (internal use only). */
	protected StringBuilder jsonb = null;

	/**
	 * Extract @type, @itemType, @memberTypes or @base and set type.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 */
	protected void extractType(PgSchemaOption option, Node node) {

		String xs_prefix = option.xs_prefix;
		String xs_prefix_ = option.xs_prefix_;

		type = null;

		try {

			if (node.hasAttributes()) {

				Element e = (Element) node;

				String _type = e.getAttribute("type");

				if (_type != null && !_type.isEmpty()) {
					type = _type;
					return;
				}

				String item_type = e.getAttribute("itemType"); // xs:list

				if (item_type != null && !item_type.isEmpty()) {
					_list = true;
					type = xs_prefix_ + "string";
					return;
				}

				String mem_types = e.getAttribute("memberTypes"); // xs:union

				if (mem_types != null && !mem_types.isEmpty()) {
					_union = true;
					type = mem_types;
					return;
				}

				String _substitution_group = e.getAttribute("substitutionGroup");

				if (_substitution_group != null && !_substitution_group.isEmpty()) {
					substitution_group = _substitution_group;
					return;
				}

			}

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				String child_name = child.getNodeName();

				if (child_name.equals(xs_prefix_ + "annotation"))
					continue;

				else if (child_name.equals(xs_prefix_ + "any") ||
						child_name.equals(xs_prefix_ + "anyAttribute") ||
						child_name.equals(xs_prefix_ + "attribute") ||
						child_name.equals(xs_prefix_ + "attributeGroup") ||
						child_name.equals(xs_prefix_ + "element") ||
						child_name.equals(xs_prefix_ + "group"))
					return;

				else if (child.hasAttributes()) {

					Element e = (Element) child;

					String _type = e.getAttribute("type");

					if (_type != null && !_type.isEmpty()) {
						type = _type;
						return;
					}

					String item_type = e.getAttribute("itemType"); // xs:list

					if (item_type != null && !item_type.isEmpty()) {
						_list = true;
						type = xs_prefix_ + "string";
						return;
					}

					String mem_types = e.getAttribute("memberTypes"); // xs:union

					if (mem_types != null && !mem_types.isEmpty()) {
						_union = true;
						type = mem_types;
						return;
					}

					String _substitution_group = e.getAttribute("substitutionGroup");

					if (_substitution_group != null && !_substitution_group.isEmpty()) {
						substitution_group = _substitution_group;
						return;
					}

					String base = e.getAttribute("base");

					if (base != null && !base.isEmpty()) {
						type = base;
						return;
					}

				}

				if (child.hasChildNodes())
					extractType(option, child);

			}

		} finally {

			if (type == null)
				return;

			type = xtype = type.trim();

			if (!type.contains(" "))
				return;

			String[] types = type.split(" ");

			String[] _type1 = types[0].split(":");

			if (_type1.length != 2 || !_type1[0].equals(xs_prefix)) {
				type = xs_prefix_ + "string";
				return;
			}

			XsDataType xs_type1 = XsDataType.valueOf("xs_" + _type1[1]);

			for (int i = 1; i < types.length; i++) {

				String[] _type2 = types[i].split(":");

				if (_type2.length != 2 || !_type2[0].equals(xs_prefix)) {
					type = xs_prefix_ + "string";
					return;
				}

				XsDataType xs_type2 = XsDataType.valueOf("xs_" + _type2[1]);

				xs_type1 = xs_type1.leastCommonOf(xs_type2);

			}

			type = xs_type1.name().replaceFirst("^xs_", xs_prefix_);

		}

	}

	/**
	 * Extract @targetNamespace of current node.
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 */
	protected void extractNamespace(PgSchema schema, Node node) {

		target_namespace = null;

		if (node.hasAttributes()) {

			Element e = (Element) node;

			String namespace = e.getAttribute("targetNamespace");

			if (namespace != null && !namespace.isEmpty()) {

				target_namespace = namespace;

				return;
			}

		}

		if (type == null)
			return;

		target_namespace = type.contains(":") ? schema.getNamespaceUriForPrefix(type.split(":")[0]) : schema.getNamespaceUriForPrefix("");

	}

	/**
	 * Extract @maxOccurs.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 */
	protected void extractMaxOccurs(PgSchemaOption option, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		if (node.hasAttributes()) {

			Element e = (Element) node;

			String _maxoccurs = e.getAttribute("maxOccurs");

			if (_maxoccurs != null && !_maxoccurs.isEmpty()) {

				maxoccurs = _maxoccurs;

				list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

				return;
			}

		}

		// test parent node of xs:all, xs:choice, xs:sequence

		Node parent_node = node.getParentNode();

		if (parent_node != null) {

			String parent_name = parent_node.getNodeName();

			if (parent_name.equals(xs_prefix_ + "all") ||
					parent_name.equals(xs_prefix_ + "choice") ||
					parent_name.equals(xs_prefix_ + "sequence")) {

				Element e = (Element) parent_node;

				String _maxoccurs = e.getAttribute("maxOccurs");

				if (_maxoccurs != null && !_maxoccurs.isEmpty()) {

					maxoccurs = _maxoccurs;

					list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

					return;
				}

			}

		}

		// test child nodes

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			String child_name = child.getNodeName();

			if (child_name.equals(xs_prefix_ + "annotation"))
				continue;

			else if (child_name.equals(xs_prefix_ + "any") ||
					child_name.equals(xs_prefix_ + "anyAttribute") ||
					child_name.equals(xs_prefix_ + "attribute") ||
					child_name.equals(xs_prefix_ + "attributeGroup") ||
					child_name.equals(xs_prefix_ + "element") ||
					child_name.equals(xs_prefix_ + "group"))
				return;

			else if (child.hasAttributes()) {

				Element e = (Element) child;

				String _maxoccurs = e.getAttribute("maxOccurs");

				if (_maxoccurs != null && !_maxoccurs.isEmpty()) {

					maxoccurs = _maxoccurs;

					list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

					return;
				}

			}

			if (child.hasChildNodes())
				extractMaxOccurs(option, child);

		}

	}

	/**
	 * Extract @minOccurs.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 */
	protected void extractMinOccurs(PgSchemaOption option, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		if (node.hasAttributes()) {

			Element e = (Element) node;

			String _minoccurs = e.getAttribute("minOccurs");

			if (_minoccurs != null && !_minoccurs.isEmpty()) {

				minoccurs = _minoccurs;

				list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

				return;
			}

		}

		// test parent node of xs:all, xs:choice, xs:sequence

		Node parent_node = node.getParentNode();

		if (parent_node != null) {

			String parent_name = parent_node.getNodeName();

			if (parent_name.equals(xs_prefix_ + "all") ||
					parent_name.equals(xs_prefix_ + "choice") ||
					parent_name.equals(xs_prefix_ + "sequence")) {

				Element e = (Element) parent_node;

				String _minoccurs = e.getAttribute("minOccurs");

				if (_minoccurs != null && !_minoccurs.isEmpty()) {

					minoccurs = _minoccurs;

					list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

					return;
				}

			}

		}

		// test child nodes

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			String child_name = child.getNodeName();

			if (child_name.equals(xs_prefix_ + "annotation"))
				continue;

			else if (child_name.equals(xs_prefix_ + "any") ||
					child_name.equals(xs_prefix_ + "anyAttribute") ||
					child_name.equals(xs_prefix_ + "attribute") ||
					child_name.equals(xs_prefix_ + "attributeGroup") ||
					child_name.equals(xs_prefix_ + "element") ||
					child_name.equals(xs_prefix_ + "group"))
				return;

			else if (child.hasAttributes()) {

				Element e = (Element) child;

				String _minoccurs = e.getAttribute("minOccurs");

				if (_minoccurs != null && !_minoccurs.isEmpty()) {

					minoccurs = _minoccurs;

					list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

					return;
				}

			}

			if (child.hasChildNodes())
				extractMinOccurs(option, child);

		}

	}

	/**
	 * Extract @use or @nillable of current node.
	 *
	 * @param node current node
	 */
	protected void extractRequired(Node node) {

		required = false;

		if (node.hasAttributes()) {

			Element e = (Element) node;

			String use = e.getAttribute("use");

			if (use != null && !use.isEmpty()) {

				required = use.equals("required");
				prohibited = use.equals("prohibited");

			}

			String nillable = e.getAttribute("nillable");

			if (nillable != null && !nillable.isEmpty())
				required = nillable.equals("false");

		}

		xrequired = required;

	}

	/**
	 * Extract @fixed of current node.
	 *
	 * @param node current node
	 */
	protected void extractFixedValue(Node node) {

		fixed_value = null;

		if (node.hasAttributes()) {

			Element e = (Element) node;

			String fixed = e.getAttribute("fixed");

			if (fixed != null && !fixed.isEmpty()) {

				required = xrequired = true;
				fixed_value = fixed;

			}

		}

	}

	/**
	 * Extract @default of current node.
	 *
	 * @param node current node
	 */
	protected void extractDefaultValue(Node node) {

		default_value = null;

		if (node.hasAttributes()) {

			Element e = (Element) node;

			String _default = e.getAttribute("default");

			if (_default != null && !_default.isEmpty())
				default_value = _default;

		}

	}

	/**
	 * Extract @block of current node.
	 *
	 * @param node current node
	 */
	protected void extractBlockValue(Node node) {

		block_value = null;

		if (node.hasAttributes()) {

			Element e = (Element) node;

			String _block = e.getAttribute("block");

			if (_block != null && !_block.isEmpty())
				block_value = _block;

		}

	}

	/**
	 * Extract xs:restriction/xs:enumeration@value.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 */
	protected void extractEnumeration(PgSchemaOption option, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		enumeration = xenumeration = null;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			String child_name = child.getNodeName();

			if (child_name.equals(xs_prefix_ + "annotation"))
				continue;

			else if (child_name.equals(xs_prefix_ + "any") ||
					child_name.equals(xs_prefix_ + "anyAttribute") ||
					child_name.equals(xs_prefix_ + "attribute") ||
					child_name.equals(xs_prefix_ + "attributeGroup") ||
					child_name.equals(xs_prefix_ + "element") ||
					child_name.equals(xs_prefix_ + "group"))
				return;

			else if (child_name.equals(xs_prefix_ + "restriction")) {

				int length = 0;

				for (Node enum_node = child.getFirstChild(); enum_node != null; enum_node = enum_node.getNextSibling()) {

					if (enum_node.getNodeName().equals(xs_prefix_ + "enumeration")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty())
							length++;

					}

				}

				if (length == 0)
					return;

				restriction = true;

				xenumeration = new String[length];
				enumeration = new String[length];

				int _length = 0;

				for (Node enum_node = child.getFirstChild(); enum_node != null; enum_node = enum_node.getNextSibling()) {

					if (enum_node.getNodeName().equals(xs_prefix_ + "enumeration")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							String enum_value = value.replace("'", "''");

							xenumeration[_length] = enum_value;
							enumeration[_length] = enum_value.length() <= PgSchemaUtil.max_enum_len ? enum_value : enum_value.substring(0, PgSchemaUtil.max_enum_len);

							_length++;

						}

					}

				}

				boolean duplicated = (_length < length);

				for (int i = 0; i < _length; i++) {

					if (enumeration[i] == null || enumeration[i].isEmpty())
						continue;

					for (int j = 0; j < i; j++) {

						if (enumeration[j] == null || enumeration[j].isEmpty())
							continue;

						if (enumeration[i].equals(enumeration[j])) {

							duplicated = true;

							enumeration[i] = xenumeration[i] = null;

							break;
						}

					}

				}

				if (duplicated) {

					length = 0;

					for (int i = 0; i < _length; i++) {

						if (enumeration[i] != null && !enumeration[i].isEmpty())
							continue;

						length++;
					}

					if (length == 0)
						enumeration = xenumeration = null;

					else {

						String[] _xenumeration = new String[length];
						String[] _enumeration = new String[length];

						length = 0;

						for (int i = 0; i < _length; i++) {

							if (enumeration[i] != null && !enumeration[i].isEmpty())
								continue;

							_xenumeration[length] = xenumeration[i];
							_enumeration[length] = enumeration[i];

							length++;

						}

						xenumeration = _xenumeration;
						enumeration = _enumeration;

					}

				}

				if (type == null) {

					type = xs_prefix_ + "string";
					xs_type = XsDataType.xs_string;

				}

				return;
			}

			if (child.hasChildNodes())
				extractEnumeration(option, child);

		}

	}

	/**
	 * Extract xs:restriction/xs:any@value.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 */
	protected void extractRestriction(PgSchemaOption option, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		length = null; // xs:length
		min_length = null; // xs:minLength
		max_length = null; // xs:maxLength

		pattern = null; // xs:pattern

		max_inclusive = null; // xs:maxInclusive
		max_exclusive = null; // xs:maxExclusive
		min_exclusive = null; // xs:minExclusive
		min_inclusive = null; // xs:minInclusive

		total_digits = null; // xs:totalDigits
		fraction_digits = null; // xs:fractionDigits

		white_space = null;
		explicit_timezone = null;

		assertions = null;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			String child_name = child.getNodeName();

			if (child_name.equals(xs_prefix_ + "annotation"))
				continue;

			else if (child_name.equals(xs_prefix_ + "any") ||
					child_name.equals(xs_prefix_ + "anyAttribute") ||
					child_name.equals(xs_prefix_ + "attribute") ||
					child_name.equals(xs_prefix_ + "attributeGroup") ||
					child_name.equals(xs_prefix_ + "element") ||
					child_name.equals(xs_prefix_ + "group"))
				return;

			else if (child_name.equals(xs_prefix_ + "restriction")) {

				for (Node enum_node = child.getFirstChild(); enum_node != null; enum_node = enum_node.getNextSibling()) {

					if (enum_node.getNodeName().equals(xs_prefix_ + "length")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								int i = Integer.parseInt(value);

								if (i > 0) {

									restriction = true;

									if (length == null)
										length = value;

									else if (i > Integer.valueOf(length))
										length = value;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "minLength")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								int i = Integer.parseInt(value);

								if (i > 0) {

									restriction = true;

									if (min_length == null)
										min_length = value;

									else if (i < Integer.valueOf(min_length))
										min_length = value;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "maxLength")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								int i = Integer.parseInt(value);

								if (i > 0) {

									restriction = true;

									if (max_length == null)
										max_length = value;

									else if (i > Integer.valueOf(max_length))
										max_length = value;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "pattern")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							pattern = value;

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "maxInclusive")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								BigDecimal new_value = new BigDecimal(value);

								restriction = true;

								if (max_inclusive == null)
									max_inclusive = value;

								else {

									BigDecimal old_value = new BigDecimal(max_inclusive);

									if (new_value.compareTo(old_value) > 0)
										max_inclusive = value;

								}

								if (max_exclusive != null) {

									BigDecimal inc_value = new BigDecimal(max_inclusive);
									BigDecimal exc_value = new BigDecimal(max_exclusive);

									if (inc_value.compareTo(exc_value) < 0)
										max_inclusive = null;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "maxExclusive")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								BigDecimal new_value = new BigDecimal(value);

								restriction = true;

								if (max_exclusive == null)
									max_exclusive = value;

								else {

									BigDecimal old_value = new BigDecimal(max_inclusive);

									if (new_value.compareTo(old_value) > 0)
										max_exclusive = value;

								}

								if (max_inclusive != null) {

									BigDecimal inc_value = new BigDecimal(max_inclusive);
									BigDecimal exc_value = new BigDecimal(max_exclusive);

									if (exc_value.compareTo(inc_value) < 0)
										max_exclusive = null;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "minExclusive")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								BigDecimal new_value = new BigDecimal(value);

								restriction = true;

								if (min_exclusive == null)
									min_exclusive = value;

								else {

									BigDecimal old_value = new BigDecimal(max_inclusive);

									if (new_value.compareTo(old_value) < 0)
										min_exclusive = value;

								}

								if (min_inclusive != null) {

									BigDecimal inc_value = new BigDecimal(min_inclusive);
									BigDecimal exc_value = new BigDecimal(min_exclusive);

									if (exc_value.compareTo(inc_value) > 0)
										min_exclusive = null;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "minInclusive")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								BigDecimal new_value = new BigDecimal(value);

								restriction = true;

								if (min_inclusive == null)
									min_inclusive = value;

								else {

									BigDecimal old_value = new BigDecimal(max_inclusive);

									if (new_value.compareTo(old_value) < 0)
										min_inclusive = value;

								}

								if (min_exclusive != null) {

									BigDecimal inc_value = new BigDecimal(min_inclusive);
									BigDecimal exc_value = new BigDecimal(min_exclusive);

									if (inc_value.compareTo(exc_value) > 0)
										min_inclusive = null;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "totalDigits")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								int i = Integer.parseInt(value);

								if (i > 0) {

									restriction = true;

									if (total_digits == null)
										total_digits = value;

									else if (i > Integer.valueOf(total_digits))
										total_digits = value;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "fractionDigits")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							try {

								int i = Integer.parseInt(value);

								if (i >= 0) {

									restriction = true;

									if (fraction_digits == null)
										fraction_digits = value;

									else if (i > Integer.valueOf(fraction_digits))
										fraction_digits = value;

								}

							} catch (NumberFormatException ex) {
							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "whiteSpace")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							if (value.equals("replace") || value.equals("collapse")) {

								restriction = true;
								white_space = value;

							}

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "explicitTimezone")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							explicit_timezone = value;

						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "assertions")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							assertions = value;

						}

					}

				}

				return;
			}

			if (child.hasChildNodes())
				extractRestriction(option, child);

		}

	}

	/**
	 * Set hash key type.
	 *
	 * @param option PostgreSQL data model option
	 */
	protected void setHashKeyType(PgSchemaOption option) {

		String xs_prefix_ = option.xs_prefix_;

		switch (option.hash_size) {
		case native_default:
			type = xs_prefix_ + "hexBinary";
			xs_type = XsDataType.xs_hexBinary;
			break;
		case unsigned_int_32:
			type = xs_prefix_ + "unsignedInt";
			xs_type = XsDataType.xs_unsignedInt;
			break;
		case unsigned_long_64:
			type = xs_prefix_ + "unsignedLong";
			xs_type = XsDataType.xs_unsignedLong;
			break;
		default:
			type = xs_prefix_ + "string";
			xs_type = XsDataType.xs_string;
		}

	}

	/**
	 * Set serial key type.
	 *
	 * @param option PostgreSQL data model option
	 */
	protected void setSerKeyType(PgSchemaOption option) {

		String xs_prefix_ = option.xs_prefix_;

		switch (option.ser_size) {
		case unsigned_int_32:
			type = xs_prefix_ + "unsignedInt";
			xs_type = XsDataType.xs_unsignedInt;
			break;
		case unsigned_short_16:
			type = xs_prefix_ + "unsignedShort";
			xs_type = XsDataType.xs_unsignedShort;
			break;
		}

	}

	/**
	 * Decide whether field is system key.
	 */
	protected void setSystemKey() {

		system_key = primary_key || foreign_key || nested_key;

	}

	/**
	 * Decide whether field is user key.
	 */
	protected void setUserKey() {

		user_key = document_key || serial_key || xpath_key;

	}

	/**
	 * Decide whether field is omissible.
	 *
	 * @param table current table
	 * @param option PostgreSQL data model option
	 */
	protected void setOmissible(PgTable table, PgSchemaOption option) {

		if ((element || attribute) && (option.discarded_document_key_names.contains(name) || option.discarded_document_key_names.contains(table.name + "." + name))) {
			omissible = true;
			return;
		}

		omissible = (!option.document_key && !option.inplace_document_key && document_key) || (!option.serial_key && serial_key) || (!option.xpath_key && xpath_key) || (!option.rel_data_ext && system_key);

	}

	/**
	 * Decide whether field is indexable.
	 *
	 * @param table current table
	 * @param option PostgreSQL data model option
	 */
	protected void setIndexable(PgTable table, PgSchemaOption option) {

		if (system_key || user_key || ((element || attribute) && (option.discarded_document_key_names.contains(name) || option.discarded_document_key_names.contains(table.name + "." + name)))) {
			indexable = false;
			return;
		}

		indexable = !option.field_resolved || (option.field_resolved && field_sel) || (option.attr_resolved && attr_sel);

	}

	/**
	 * Decide whether field is JSON convertible.
	 *
	 * @param table current table
	 * @param option PostgreSQL data model option
	 */
	protected void setJsonable(PgTable table, PgSchemaOption option) {

		if (system_key || user_key || ((element || attribute) && (option.discarded_document_key_names.contains(name) || option.discarded_document_key_names.contains(table.name + "." + name)))) {
			jsonable = false;
			return;
		}

		jsonable = true;

	}

	/**
	 * Return whether content matches enumeration.
	 *
	 * @param content content
	 * @return boolean whether content matches enumeration
	 */
	public boolean matchesEnumeration(String content) {

		if (content == null || content.isEmpty())
			return !required;

		for (String enum_string : enumeration) {

			if (enum_string.equals(content))
				return true;

		}

		return false;
	}

	/**
	 * Return whether content matches enumeration.
	 *
	 * @param content content
	 * @return boolean whether content matches enumeration
	 */
	public boolean matchesXEnumeration(String content) {

		if (content == null || content.isEmpty())
			return !required;

		for (String enum_string : xenumeration) {

			if (enum_string.equals(content))
				return true;

		}

		return false;
	}

	/**
	 * Return whether content matches filter pattern.
	 *
	 * @param content content
	 * @return boolean whether content matches filter pattern
	 */
	public boolean matchesFilterPattern(String content) {

		if (content == null || content.isEmpty())
			return false;

		if (filter_pattern == null)
			return true;

		for (String rex_pattern : filter_pattern) {

			if (content.matches(rex_pattern))
				return true;

		}

		return false;
	}

	/**
	 * Return whether node name matches.
	 *
	 * @param option PostgreSQL data model option
	 * @param node_name node name
	 * @param as_attr whether evaluate this node as attribute
	 * @param wild_card whether wild card follows or not
	 * @return boolean whether node name matches
	 */
	public boolean matchesNodeName(PgSchemaOption option, String node_name, boolean as_attr, boolean wild_card) {

		if ((element || attribute) && option.discarded_document_key_names.contains(name))
			return false;

		String _xname = as_attr ? (simple_attribute || simple_attr_cond ? foreign_table_xname : xname) : xname;

		if (wild_card)
			return _xname.matches(node_name);

		return node_name.equals("*") || _xname.equals(node_name);
	}

	// PostgreSQL schema generation

	/**
	 * Return target namespace.
	 *
	 * @return String target namespace
	 */
	public String getTagetNamespace() {
		return target_namespace;
	}

	/**
	 * Return prefix of target namespace.
	 *
	 * @return String prefix of target namespace
	 */
	public String getPrefix() {
		return prefix;
	}

	/**
	 * Return canonical name in XML Schema.
	 *
	 * @return String canonical name of field
	 */
	public String getCanName() {
		return xname;
	}

	/**
	 * Return field name.
	 *
	 * @return String field name
	 */
	public String getName() {
		return name;
	}

	// PostgreSQL DDL

	/**
	 * Return PostgreSQL DDL type definition.
	 *
	 * @return String PostgreSQL DDL type definition
	 */
	public String getPgDataType() {

		String _name = PgSchemaUtil.avoidPgReservedWords(pname);

		String base;
		StringBuilder check = null;

		try {

			switch (xs_type) {
			case xs_boolean:
				return "BOOLEAN";
			case xs_hexBinary:
			case xs_base64Binary:
				return "BYTEA";
			case xs_bigserial:
				return "BIGSERIAL";
			case xs_serial:
				return "SERIAL";
			case xs_float:
				base = "REAL";

				if (!restriction)
					return base;

				check = new StringBuilder();

				if (min_inclusive != null) {

					try {

						float f = Float.parseFloat(min_inclusive);

						check.append(_name + " >= " + f + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (min_exclusive != null) {

					try {

						float f = Float.parseFloat(min_exclusive);

						check.append(_name + " > " + f + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (max_inclusive != null) {

					try {

						float f = Float.parseFloat(max_inclusive);

						check.append(_name + " <= " + f + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (max_exclusive != null) {

					try {

						float f = Float.parseFloat(max_exclusive);

						check.append(_name + " < " + f + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_double:
				base = "DOUBLE PRECISION";

				if (!restriction)
					return base;

				check = new StringBuilder();

				if (min_inclusive != null) {

					try {

						double d = Double.parseDouble(min_inclusive);

						check.append(_name + " >= " + d + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (min_exclusive != null) {

					try {

						double d = Double.parseDouble(min_exclusive);

						check.append(_name + " > " + d + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (max_inclusive != null) {

					try {

						double d = Double.parseDouble(max_inclusive);

						check.append(_name + " <= " + d + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (max_exclusive != null) {

					try {

						double d = Double.parseDouble(max_exclusive);

						check.append(_name + " < " + d + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_decimal:
				base = "DECIMAL";

				if (!restriction)
					return base;

				check = new StringBuilder();

				if (min_inclusive != null) {

					try {

						BigDecimal bd = new BigDecimal(min_inclusive);

						check.append(_name + " >= " + bd + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (min_exclusive != null) {

					try {

						BigDecimal bd = new BigDecimal(min_exclusive);

						check.append(_name + " > " + bd + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (max_inclusive != null) {

					try {

						BigDecimal bd = new BigDecimal(max_inclusive);

						check.append(_name + " <= " + bd + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (max_exclusive != null) {

					try {

						BigDecimal bd = new BigDecimal(max_exclusive);

						check.append(_name + " < " + bd + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_integer:
			case xs_int:
				base = "INTEGER";

				if (!restriction)
					return base;

				check = new StringBuilder();

				if (min_inclusive != null) {

					try {

						int i = Integer.parseInt(min_inclusive);

						check.append(_name + " >= " + i + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (min_exclusive != null) {

					try {

						int i = Integer.parseInt(min_exclusive);

						check.append(_name + " > " + i + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (max_inclusive != null) {

					try {

						int i = Integer.parseInt(max_inclusive);

						check.append(_name + " <= " + i + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (max_exclusive != null) {

					try {

						int i = Integer.parseInt(max_exclusive);

						check.append(_name + " < " + i + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (total_digits != null) {

					try {

						int i = Integer.parseInt(total_digits);

						if (i > 0)
							check.append(_name + " < " + (int) Math.pow(10, i) + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_long:
			case xs_bigint:
				base = "BIGINT";

				if (!restriction)
					return base;

				check = new StringBuilder();

				if (min_inclusive != null) {

					try {

						long l = Long.parseLong(min_inclusive);

						check.append(_name + " >= " + l + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (min_exclusive != null) {

					try {

						long l = Long.parseLong(min_exclusive);

						check.append(_name + " > " + l + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (max_inclusive != null) {

					try {

						long l = Long.parseLong(max_inclusive);

						check.append(_name + " <= " + l + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (max_exclusive != null) {

					try {

						long l = Long.parseLong(max_exclusive);

						check.append(_name + " < " + l + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (total_digits != null) {

					try {

						int i = Integer.parseInt(total_digits);

						if (i > 0)
							check.append(_name + " < " + (long) Math.pow(10, i) + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_short:
			case xs_byte:
				base = "SMALLINT";

				if (!restriction)
					return base;

				check = new StringBuilder();

				if (min_inclusive != null) {

					try {

						short s = Short.parseShort(min_inclusive);

						check.append(_name + " >= " + s + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (min_exclusive != null) {

					try {

						short s = Short.parseShort(min_exclusive);

						check.append(_name + " > " + s + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (max_inclusive != null) {

					try {

						short s = Short.parseShort(max_inclusive);

						check.append(_name + " <= " + s + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (max_exclusive != null) {

					try {

						short s = Short.parseShort(max_exclusive);

						check.append(_name + " < " + s + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (total_digits != null) {

					try {

						int i = Integer.parseInt(total_digits);

						if (i > 0)
							check.append(_name + " < " + (short) Math.pow(10, i) + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_nonPositiveInteger:
				base = "INTEGER";

				check = new StringBuilder();

				if (!restriction)
					check.append(_name + " <= 0 AND ");

				else {

					if (min_inclusive != null) {

						try {

							int i = Integer.parseInt(min_inclusive);

							check.append(_name + " >= " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (min_exclusive != null) {

						try {

							int i = Integer.parseInt(min_exclusive);

							check.append(_name + " > " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (max_inclusive != null) {

						try {

							int i = Integer.parseInt(max_inclusive);

							if (i < 0)
								check.append(_name + " <= " + i + " AND ");
							else
								check.append(_name + " <= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " <= 0 AND ");
						}

					}

					else if (max_exclusive != null) {

						try {

							int i = Integer.parseInt(max_exclusive);

							if (i < 1)
								check.append(_name + " < " + i + " AND ");
							else
								check.append(_name + " <= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " <= 0 AND ");
						}

					}

					else
						check.append(_name + " <= 0 AND ");

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_negativeInteger:
				base = "INTEGER";

				check = new StringBuilder();

				if (!restriction)
					check.append(_name + " < 0 AND ");

				else {

					if (min_inclusive != null) {

						try {

							int i = Integer.parseInt(min_inclusive);

							check.append(_name + " >= " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (min_exclusive != null) {

						try {

							int i = Integer.parseInt(min_exclusive);

							check.append(_name + " > " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (max_inclusive != null) {

						try {

							int i = Integer.parseInt(max_inclusive);

							if (i < -1)
								check.append(_name + " <= " + i + " AND ");
							else
								check.append(_name + " < 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " < 0 AND ");
						}

					}

					else if (max_exclusive != null) {

						try {

							int i = Integer.parseInt(max_exclusive);

							if (i < 0)
								check.append(_name + " < " + i + " AND ");
							else
								check.append(_name + " < 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " < 0 AND ");
						}

					}

					else
						check.append(_name + " < 0 AND ");

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_nonNegativeInteger:
			case xs_unsignedInt:
				base = "INTEGER";

				check = new StringBuilder();

				if (!restriction)
					check.append(_name + " >= 0 AND ");

				else {

					if (min_inclusive != null) {

						try {

							int i = Integer.parseInt(min_inclusive);

							if (i > 0)
								check.append(_name + " >= " + i + " AND ");
							else
								check.append(_name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " >= 0 AND ");
						}

					}

					else if (min_exclusive != null) {

						try {

							int i = Integer.parseInt(min_exclusive);

							if (i > -1)
								check.append(_name + " > " + i + " AND ");
							else
								check.append(_name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " >= 0 AND ");
						}

					}

					else
						check.append(_name + " >= 0 AND ");

					if (max_inclusive != null) {

						try {

							int i = Integer.parseInt(max_inclusive);

							check.append(_name + " <= " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (max_exclusive != null) {

						try {

							int i = Integer.parseInt(max_exclusive);

							check.append(_name + " < " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (total_digits != null) {

						try {

							int i = Integer.parseInt(total_digits);

							if (i > 0)
								check.append(_name + " < " + (int) Math.pow(10, i) + " AND ");

						} catch (NumberFormatException e) {
						}

					}

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_positiveInteger:
				base = "INTEGER";

				check = new StringBuilder();

				if (!restriction)
					check.append(_name + " > 0 AND ");

				else {

					if (min_inclusive != null) {

						try {

							int i = Integer.parseInt(min_inclusive);

							if (i > 1)
								check.append(_name + " >= " + i + " AND ");
							else
								check.append(_name + " > 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " > 0 AND ");
						}

					}

					else if (min_exclusive != null) {

						try {

							int i = Integer.parseInt(min_exclusive);

							if (i > 0)
								check.append(_name + " > " + i + " AND ");
							else
								check.append(_name + " > 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " > 0 AND ");
						}

					}

					else
						check.append(_name + " > 0 AND ");

					if (max_inclusive != null) {

						try {

							int i = Integer.parseInt(max_inclusive);

							check.append(_name + " <= " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (max_exclusive != null) {

						try {

							int i = Integer.parseInt(max_exclusive);

							check.append(_name + " < " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (total_digits != null) {

						try {

							int i = Integer.parseInt(total_digits);

							if (i > 0)
								check.append(_name + " < " + (int) Math.pow(10, i) + " AND ");

						} catch (NumberFormatException e) {
						}

					}

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_unsignedLong:
				base = "BIGINT";

				check = new StringBuilder();

				if (!restriction)
					check.append(_name + " >= 0 AND ");

				else {

					if (min_inclusive != null) {

						try {

							long l = Long.parseLong(min_inclusive);

							if (l > 0)
								check.append(_name + " >= " + l + " AND ");
							else
								check.append(_name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " >= 0 AND ");
						}

					}

					else if (min_exclusive != null) {

						try {

							long l = Long.parseLong(min_exclusive);

							if (l > -1)
								check.append(_name + " > " + l + " AND ");
							else
								check.append(_name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " >= 0 AND ");
						}

					}

					else
						check.append(_name + " >= 0 AND ");

					if (max_inclusive != null) {

						try {

							long l = Long.parseLong(max_inclusive);

							check.append(_name + " <= " + l + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (max_exclusive != null) {

						try {

							long l = Long.parseLong(max_exclusive);

							check.append(_name + " < " + l + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (total_digits != null) {

						try {

							int i = Integer.parseInt(total_digits);

							if (i > 0)
								check.append(_name + " < " + (long) Math.pow(10, i) + " AND ");

						} catch (NumberFormatException e) {
						}

					}

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_unsignedShort:
			case xs_unsignedByte:
				base = "SMALLINT";

				check = new StringBuilder();

				if (!restriction)
					check.append(_name + " >= 0 AND ");

				else {

					if (min_inclusive != null) {

						try {

							short s = Short.parseShort(min_inclusive);

							if (s > 0)
								check.append(_name + " >= " + s + " AND ");
							else
								check.append(_name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " >= 0 AND ");
						}

					}

					else if (min_exclusive != null) {

						try {

							short s = Short.parseShort(min_exclusive);

							if (s > -1)
								check.append(_name + " > " + s + " AND ");
							else
								check.append(_name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(_name + " >= 0 AND ");
						}

					}

					else
						check.append(_name + " >= 0 AND ");

					if (max_inclusive != null) {

						try {

							short s = Short.parseShort(max_inclusive);

							check.append(_name + " <= " + s + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (max_exclusive != null) {

						try {

							short s = Short.parseShort(max_exclusive);

							check.append(_name + " < " + s + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (total_digits != null) {

						try {

							int i = Integer.parseInt(total_digits);

							if (i > 0)
								check.append(_name + " < " + (short) Math.pow(10, i) + " AND ");

						} catch (NumberFormatException e) {
						}

					}

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_dateTime:
				if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required")))
					return "TIMESTAMP";
				else
					return "TIMESTAMP WITH TIME ZONE";
			case xs_time:
				if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required")))
					return "TIME";
				else
					return "TIME WITH TIME ZONE";
			case xs_date:
			case xs_gYearMonth:
			case xs_gYear:
				return "DATE";
			case xs_gMonthDay:
			case xs_gMonth:
			case xs_gDay:
			case xs_duration:
			case xs_anyType:
			case xs_string:
			case xs_normalizedString:
			case xs_token:
			case xs_NMTOKEN:
			case xs_NMTOKENS:
			case xs_IDREFS:
			case xs_ENTITIES:
			case xs_NOTATION:
			case xs_language:
			case xs_Name:
			case xs_QName:
			case xs_NCName:
			case xs_anyURI:
			case xs_ID:
			case xs_IDREF:
			case xs_ENTITY:
				if (_list) // length restriction of xs:list is not effective
					return "TEXT";

				if (length != null) {

					try {

						int i = Integer.parseInt(length);

						if (i > 0)
							return "VARCHAR(" + i + ")";
						else
							return "TEXT";

					} catch (NumberFormatException e) {
						return "TEXT";
					}

				}

				if (max_length != null) {

					try {

						int i = Integer.parseInt(max_length);

						if (i > 0)
							return "VARCHAR(" + i + ")";
						else
							return "TEXT";

					} catch (NumberFormatException e) {
						return "TEXT";
					}

				}

				return "TEXT";
			case xs_any:
			case xs_anyAttribute:
				return "XML";
			}

		} finally {
			if (check != null) {
				check.setLength(0);
				check = null;
			}
		}

		return null;
	}

	/**
	 * Return java.sql.Types.
	 *
	 * @return int java.sqlTypes
	 */
	public int getSqlDataType() {

		if (enum_name != null)
			return java.sql.Types.VARCHAR;

		switch (xs_type) {
		case xs_boolean:
			return java.sql.Types.BOOLEAN;
		case xs_hexBinary:
		case xs_base64Binary:
			return java.sql.Types.BINARY;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
			return java.sql.Types.BIGINT;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
			return java.sql.Types.INTEGER;
		case xs_float:
			return java.sql.Types.FLOAT;
		case xs_double:
			return java.sql.Types.DOUBLE;
		case xs_decimal:
			return java.sql.Types.DECIMAL;
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return java.sql.Types.SMALLINT;
		case xs_dateTime:
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required")))
				return java.sql.Types.TIMESTAMP;
			else
				return java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
		case xs_time:
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required")))
				return java.sql.Types.TIME;
			else
				return java.sql.Types.TIME_WITH_TIMEZONE;
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			return java.sql.Types.DATE;
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			return java.sql.Types.VARCHAR;
		case xs_any:
		case xs_anyAttribute:
			return java.sql.Types.SQLXML;
		}

		return java.sql.Types.NULL;
	}

	/**
	 * Return SQL predicate of content.
	 *
	 * @param value content
	 * @return String SQL predicate of content
	 */
	public String getSqlPredicate(String value) {

		switch (xs_type) {
		case xs_hexBinary:
			return "E'\\\\x" + value + "'";
		case xs_base64Binary:
			return "decode('" + value + "','base64')";
		case xs_boolean:
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return value;
		case xs_dateTime:
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required")))
				return "TIMESTAMP '" + value + "'";
			else
				return "TIMESTAMP WITH TIME ZONE '" + value + "'";
		case xs_time:
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required")))
				return "TIME '" + value + "'";
			else
				return "TIME WITH TIME ZONE '" + value + "'";
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			return "DATE '" + value + "'";
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			if (enum_name == null)
				return "'" + value.replace("'", "''") + "'";
			else {
				if (value.length() > PgSchemaUtil.max_enum_len)
					value = value.substring(0, PgSchemaUtil.max_enum_len);
				return "'" + value.replace("'", "''") + "'";
			}
		case xs_any:
		case xs_anyAttribute:
			return "XMLPARSE (CONTENT '" + value + "')";
		}

		return null;
	}

	// JSON Schema

	/**
	 * Return JSON Schema default value.
	 *
	 * @return String JSON Schema default value
	 */
	public String getJsonSchemaDefaultValue() {

		switch (xs_type) {
		case xs_boolean:
			return default_value;
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_integer:
		case xs_int:
		case xs_short:
		case xs_byte:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
		case xs_positiveInteger:
		case xs_unsignedLong:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return default_value;
		case xs_hexBinary:
		case xs_base64Binary:
		case xs_dateTime:
		case xs_time:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			return "\"" + StringEscapeUtils.escapeEcmaScript(default_value) + "\"";
		default: // xs_any, xs_anyAttribute
		}

		return "null";
	}

	/**
	 * Return JSON Schema enumeration array.
	 *
	 * @param json_key_value_space the JSON key value space
	 * @return String JSON Schema enumeration array
	 */
	public String getJsonSchemaEnumArray(String json_key_value_space) {

		StringBuilder sb = new StringBuilder();

		try {

			switch (xs_type) {
			case xs_boolean:
			case xs_bigserial:
			case xs_serial:
			case xs_float:
			case xs_double:
			case xs_decimal:
			case xs_long:
			case xs_bigint:
			case xs_integer:
			case xs_int:
			case xs_short:
			case xs_byte:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_unsignedInt:
			case xs_positiveInteger:
			case xs_unsignedLong:
			case xs_unsignedShort:
			case xs_unsignedByte:
				for (String enumeration : xenumeration)
					sb.append(enumeration + "," + json_key_value_space);
				break;
			case xs_hexBinary:
			case xs_base64Binary:
			case xs_dateTime:
			case xs_time:
			case xs_date:
			case xs_gYearMonth:
			case xs_gYear:
			case xs_gMonthDay:
			case xs_gMonth:
			case xs_gDay:
			case xs_duration:
			case xs_anyType:
			case xs_string:
			case xs_normalizedString:
			case xs_token:
			case xs_NMTOKEN:
			case xs_NMTOKENS:
			case xs_IDREFS:
			case xs_ENTITIES:
			case xs_NOTATION:
			case xs_language:
			case xs_Name:
			case xs_QName:
			case xs_NCName:
			case xs_anyURI:
			case xs_ID:
			case xs_IDREF:
			case xs_ENTITY:
				for (String enumeration : xenumeration)
					sb.append("\"" + StringEscapeUtils.escapeEcmaScript(enumeration) + "\"," + json_key_value_space);
				break;
			default: // xs_any, xs_anyAttribute
			}

			return sb.toString();

		} finally {
			sb.setLength(0);
			sb = null;
		}
	}

	/**
	 * Return JSON Schema maximum value (draft-04).
	 *
	 * @param json_key_value_space the JSON key value space
	 * @return String JSON Schema maximum value
	 */
	@Deprecated
	public String getJsonSchemaMaximumValueDraftV4(String json_key_value_space) {

		switch (xs_type) {
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_integer:
		case xs_int:
		case xs_short:
		case xs_byte:
			if (max_inclusive != null)
				return max_inclusive;
			else if (max_exclusive != null)
				return max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
			break;
		case xs_nonPositiveInteger:
			if (!restriction)
				return "0";

			if (max_inclusive != null) {

				try {

					int i = Integer.parseInt(max_inclusive);

					if (i < 0)
						return max_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (max_exclusive != null) {

				try {

					int i = Integer.parseInt(max_exclusive);

					if (i < 1)
						return i + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else
				return "0";
		case xs_negativeInteger:
			if (!restriction)
				return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

			if (max_inclusive != null) {

				try {

					int i = Integer.parseInt(max_inclusive);

					if (i < -1)
						return max_inclusive;
					else
						return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
					return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
				}

			}

			else if (max_exclusive != null) {

				try {

					int i = Integer.parseInt(max_exclusive);

					if (i < 0)
						return i + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
					else
						return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
					return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
				}

			}

			else
				return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Integer.parseInt(max_inclusive);

					return max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Integer.parseInt(max_exclusive);

					return max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					int i = Integer.parseInt(total_digits);

					if (i > 0)
						return String.valueOf((int) Math.pow(10, i)) + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_positiveInteger:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Integer.parseInt(max_inclusive);

					return max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Integer.parseInt(max_exclusive);

					return max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					int i = Integer.parseInt(total_digits);

					if (i > 0)
						return String.valueOf((int) Math.pow(10, i)) + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_unsignedLong:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Long.parseLong(max_inclusive);

					return max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Long.parseLong(max_exclusive);

					return max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					int i = Integer.parseInt(total_digits);

					if (i > 0)
						return String.valueOf((long) Math.pow(10, i)) + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Short.parseShort(max_inclusive);

					return max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Short.parseShort(max_exclusive);

					return max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					int i = Integer.parseInt(total_digits);

					if (i > 0)
						return String.valueOf((short) Math.pow(10, i)) + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		default: // not numeric
			return	null;
		}

		return null;
	}

	/**
	 * Return JSON Schema minimum value (draft-04).
	 *
	 * @param json_key_value_space the JSON key value space
	 * @return String JSON Schema minimum value
	 */
	@Deprecated
	public String getJsonSchemaMinimumValueDraftV4(String json_key_value_space) {

		switch (xs_type) {
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_integer:
		case xs_int:
		case xs_short:
		case xs_byte:
			if (min_inclusive != null)
				return min_inclusive;
			else if (min_exclusive != null)
				return min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
			break;
		case xs_nonPositiveInteger:
			if (!restriction)
				return null;

			if (min_inclusive != null) {

				try {

					Integer.parseInt(min_inclusive);

					return min_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (min_exclusive != null) {

				try {

					Integer.parseInt(min_exclusive);

					return min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_negativeInteger:
			if (!restriction)
				return null;

			if (min_inclusive != null) {

				try {

					Integer.parseInt(min_inclusive);

					return min_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (min_exclusive != null) {

				try {

					Integer.parseInt(min_exclusive);

					return min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			if (!restriction)
				return "0";

			if (min_inclusive != null) {

				try {

					int i = Integer.parseInt(min_inclusive);

					if (i > 0)
						return min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					int i = Integer.parseInt(min_exclusive);

					if (i > -1)
						return min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else
				return "0";
		case xs_positiveInteger:
			if (!restriction)
				return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

			if (min_inclusive != null) {

				try {

					int i = Integer.parseInt(min_inclusive);

					if (i > 1)
						return min_inclusive;
					else
						return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
					return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
				}

			}

			else if (min_exclusive != null) {

				try {

					int i = Integer.parseInt(min_exclusive);

					if (i > 0)
						return min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
					else
						return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
					return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
				}

			}

			else
				return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
		case xs_unsignedLong:
			if (!restriction)
				return "0";

			if (min_inclusive != null) {

				try {

					long l = Long.parseLong(min_inclusive);

					if (l > 0)
						return min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					long l = Long.parseLong(min_exclusive);

					if (l > -1)
						return min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else
				return "0";
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (!restriction)
				return "0";

			if (min_inclusive != null) {

				try {

					short s = Short.parseShort(min_inclusive);

					if (s > 0)
						return min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					short s = Short.parseShort(min_exclusive);

					if (s > -1)
						return min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else
				return "0";
		default: // not numeric
			return	null;
		}

		return null;
	}

	/**
	 * Return JSON Schema maximum value.
	 *
	 * @param json_key_value_space the JSON key value space
	 * @return String JSON Schema maximum value
	 */
	public String getJsonSchemaMaximumValue(String json_key_value_space) {

		switch (xs_type) {
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_integer:
		case xs_int:
		case xs_short:
		case xs_byte:
			if (max_inclusive != null)
				return "\"maximum\":" + json_key_value_space + max_inclusive;
			else if (max_exclusive != null)
				return "\"exclusiveMaximum\":" + json_key_value_space + max_exclusive;
			break;
		case xs_nonPositiveInteger:
			if (!restriction)
				return "\"maximum\":" + json_key_value_space + "0";

			if (max_inclusive != null) {

				try {

					int i = Integer.parseInt(max_inclusive);

					if (i < 0)
						return "\"maximum\":" + json_key_value_space + max_inclusive;
					else
						return "\"maximum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"maximum\":" + json_key_value_space + "0";
				}

			}

			else if (max_exclusive != null) {

				try {

					int i = Integer.parseInt(max_exclusive);

					if (i < 1)
						return "\"exclusiveMaximum\":" + json_key_value_space + i;
					else
						return "\"maximum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"maximum\":" + json_key_value_space + "0";
				}

			}

			else
				return "\"maximum\":" + json_key_value_space + "0";
		case xs_negativeInteger:
			if (!restriction)
				return "\"exclusiveMaximum\":" + json_key_value_space + "0";

			if (max_inclusive != null) {

				try {

					int i = Integer.parseInt(max_inclusive);

					if (i < -1)
						return "\"maximum\":" + json_key_value_space + max_inclusive;
					else
						return "\"exclusiveMaximum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"exclusiveMaximum\":" + json_key_value_space + "0";
				}

			}

			else if (max_exclusive != null) {

				try {

					int i = Integer.parseInt(max_exclusive);

					if (i < 0)
						return "\"exclusiveMaximum\":" + json_key_value_space + i;
					else
						return "\"exclusiveMaximum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"exclusiveMaximum\":" + json_key_value_space + "0";
				}

			}

			else
				return "\"exclusiveMaximum\":" + json_key_value_space + "0";
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Integer.parseInt(max_inclusive);

					return "\"maximum\":" + json_key_value_space + max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Integer.parseInt(max_exclusive);

					return "\"exclusiveMaximum\":" + json_key_value_space + max_exclusive;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					int i = Integer.parseInt(total_digits);

					if (i > 0)
						return "\"exclusiveMaximum\":" + json_key_value_space + String.valueOf((int) Math.pow(10, i));

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_positiveInteger:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Integer.parseInt(max_inclusive);

					return "\"maxinum:\"" + json_key_value_space + max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Integer.parseInt(max_exclusive);

					return "\"exclusiveMaximum\":" + json_key_value_space + max_exclusive;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					int i = Integer.parseInt(total_digits);

					if (i > 0)
						return "\"exclusiveMaximum\":" + json_key_value_space + String.valueOf((int) Math.pow(10, i));

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_unsignedLong:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Long.parseLong(max_inclusive);

					return "\"maximum\":" + json_key_value_space + max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Long.parseLong(max_exclusive);

					return "\"exclusiveMaximum\":" + json_key_value_space + max_exclusive;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					int i = Integer.parseInt(total_digits);

					if (i > 0)
						return "\"exclusiveMaximum\":" + json_key_value_space + String.valueOf((long) Math.pow(10, i));

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Short.parseShort(max_inclusive);

					return "\"maximum:\"" + json_key_value_space + max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Short.parseShort(max_exclusive);

					return "\"exclusiveMaximum\":" + json_key_value_space + max_exclusive;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					int i = Integer.parseInt(total_digits);

					if (i > 0)
						return "\"exclusiveMaximum\":" + json_key_value_space + String.valueOf((short) Math.pow(10, i));

				} catch (NumberFormatException e) {
				}

			}
			break;
		default: // not numeric
			return	null;
		}

		return null;
	}

	/**
	 * Return JSON Schema minimum value.
	 *
	 * @param json_key_value_space the JSON key value space
	 * @return String JSON Schema minimum value
	 */
	public String getJsonSchemaMinimumValue(String json_key_value_space) {

		switch (xs_type) {
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_integer:
		case xs_int:
		case xs_short:
		case xs_byte:
			if (min_inclusive != null)
				return "\"minumum\":" + json_key_value_space + min_inclusive;
			else if (min_exclusive != null)
				return "\"exclusiveMinimum\":" + json_key_value_space + min_exclusive;
			break;
		case xs_nonPositiveInteger:
			if (!restriction)
				return null;

			if (min_inclusive != null) {

				try {

					Integer.parseInt(min_inclusive);

					return "\"minumum\":" + json_key_value_space + min_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (min_exclusive != null) {

				try {

					Integer.parseInt(min_exclusive);

					return "\"exclusiveMinimum\":" + json_key_value_space + min_exclusive;

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_negativeInteger:
			if (!restriction)
				return null;

			if (min_inclusive != null) {

				try {

					Integer.parseInt(min_inclusive);

					return "\"minumum\":" + json_key_value_space + min_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (min_exclusive != null) {

				try {

					Integer.parseInt(min_exclusive);

					return "\"exclusiveMinimum\":" + json_key_value_space + min_exclusive;

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			if (!restriction)
				return "\"minumum\":" + json_key_value_space + "0";

			if (min_inclusive != null) {

				try {

					int i = Integer.parseInt(min_inclusive);

					if (i > 0)
						return "\"minumum\":" + json_key_value_space + min_inclusive;
					else
						return "\"minumum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"minumum\":" + json_key_value_space + "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					int i = Integer.parseInt(min_exclusive);

					if (i > -1)
						return "\"exclusiveMinimum\":" + json_key_value_space + min_exclusive;
					else
						return "\"minumum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"minumum\":" + json_key_value_space + "0";
				}

			}

			else
				return "\"minumum\":" + json_key_value_space + "0";
		case xs_positiveInteger:
			if (!restriction)
				return "\"exclusiveMinimum\":" + json_key_value_space + "0";

			if (min_inclusive != null) {

				try {

					int i = Integer.parseInt(min_inclusive);

					if (i > 1)
						return "\"minumum\":" + json_key_value_space + min_inclusive;
					else
						return "\"exclusiveMinimum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"exclusiveMinimum\":" + json_key_value_space + "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					int i = Integer.parseInt(min_exclusive);

					if (i > 0)
						return "\"exclusiveMinimum\":" + json_key_value_space + min_exclusive;
					else
						return "\"exclusiveMinimum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"exclusiveMinimum\":" + json_key_value_space + "0";
				}

			}

			else
				return "\"exclusiveMinimum\":" + json_key_value_space + "0";
		case xs_unsignedLong:
			if (!restriction)
				return "\"minumum\":" + json_key_value_space + "0";

			if (min_inclusive != null) {

				try {

					long l = Long.parseLong(min_inclusive);

					if (l > 0)
						return "\"minumum\":" + json_key_value_space + min_inclusive;
					else
						return "\"minumum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"minumum\":" + json_key_value_space + "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					long l = Long.parseLong(min_exclusive);

					if (l > -1)
						return "\"exclusiveMinimum\":" + json_key_value_space + min_exclusive;
					else
						return "\"minumum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"minumum\":" + json_key_value_space + "0";
				}

			}

			else
				return "\"minumum\":" + json_key_value_space + "0";
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (!restriction)
				return "\"minumum\":" + json_key_value_space + "0";

			if (min_inclusive != null) {

				try {

					short s = Short.parseShort(min_inclusive);

					if (s > 0)
						return "\"minumum\":" + json_key_value_space + min_inclusive;
					else
						return "\"minumum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"minumum\":" + json_key_value_space + "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					short s = Short.parseShort(min_exclusive);

					if (s > -1)
						return "\"exclusiveMinimum\":" + json_key_value_space + min_exclusive;
					else
						return "\"minumum\":" + json_key_value_space + "0";

				} catch (NumberFormatException e) {
					return "\"minumum\":" + json_key_value_space + "0";
				}

			}

			else
				return "\"minumum\":" + json_key_value_space + "0";
		default: // not numeric
			return	null;
		}

		return null;
	}

	/**
	 * Return JSON Schema multipleOf value.
	 *
	 * @return String JSON Schema multipleOf value
	 */
	public String getJsonSchemaMultipleOfValue() {

		switch (xs_type) {
		case xs_float:
		case xs_double:
		case xs_decimal:
			if (fraction_digits == null)
				return null;

			try {

				int i = Integer.parseInt(total_digits);

				if (i > 0) {

					StringBuilder sb = new StringBuilder();

					try {

						sb.append("0.");

						for (int j = 1; j < i; j++)
							sb.append("0");

						sb.append("1");

						return sb.toString();

					} finally {
						sb.setLength(0);
						sb = null;
					}

				}

			} catch (NumberFormatException e) {
				return null;
			}
			break;
		case xs_bigserial:
		case xs_serial:
		case xs_long:
		case xs_bigint:
		case xs_integer:
		case xs_int:
		case xs_short:
		case xs_byte:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
		case xs_positiveInteger:
		case xs_unsignedLong:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return "1";
		default: // not numeric
			return	null;
		}

		return null;
	}

	/**
	 * Return JSON Schema format definition.
	 *
	 * @param schema_ver JSON Schema version assumed
	 * @return String JSON Schema format definition
	 */
	public String getJsonSchemaFormat(JsonSchemaVersion schema_ver) {

		boolean latest = schema_ver.is_latest();

		switch (xs_type) {
		case xs_dateTime:
			return "date-time";
		case xs_time:
			return latest ? "time" : null;
		case xs_date:
			return latest ? "date" : null;
		case xs_anyURI:
			return "uri";
		case xs_ID:
			return latest ? "iri" : null;
		case xs_IDREF:
		case xs_IDREFS:
			return latest ?"iri-reference" : null;
		default:
			return null;
		}

	}

	// content writer functions

	/**
	 * Validate content.
	 *
	 * @param value content
	 * @return boolean whether content is valid
	 */
	public boolean validate(String value) {

		if (restriction && min_length != null) {

			if (value.length() < Integer.valueOf(min_length))
				return false;

		}

		switch (xs_type) {
		case xs_boolean:
			return true;
		case xs_bigserial:
			try {
				Long l = Long.parseLong(value);
				return (l > 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_serial:
			try {
				Integer i = Integer.parseInt(value);
				return (i > 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_hexBinary:
			try {
				DatatypeConverter.parseHexBinary(value);
			} catch (IllegalArgumentException e) {
				return false;
			}
		case xs_base64Binary:
			try {
				DatatypeConverter.parseBase64Binary(value);
			} catch (IllegalArgumentException e) {
				return false;
			}
		case xs_long:
		case xs_bigint:
			try {
				Long.parseLong(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_float:
			try {
				Float.parseFloat(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_double:
			try {
				Double.parseDouble(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_decimal:
			try {
				new BigDecimal(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_integer:
		case xs_int:
			try {
				Integer.parseInt(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_short:
		case xs_byte:
			try {
				Short.parseShort(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_nonPositiveInteger:
			try {
				int i = Integer.parseInt(value);
				return (i <= 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_negativeInteger:
			try {
				int i = Integer.parseInt(value);
				return (i < 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			try {
				int i = Integer.parseInt(value);
				return (i >= 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_positiveInteger:
			try {
				int i = Integer.parseInt(value);
				return (i > 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_unsignedLong:
			try {
				long l = Long.parseLong(value);
				return (l >= 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_unsignedShort:
		case xs_unsignedByte:
			try {
				short s = Short.parseShort(value);
				return (s >= 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_dateTime:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
			return (PgSchemaUtil.parseDate(value) != null);
		case xs_time:
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required"))) {

				try {
					LocalTime.parse(value);
				} catch (DateTimeParseException e) {

					try {
						OffsetTime.parse(value);
					} catch (DateTimeParseException e2) {
						return false;
					}
				}

			}

			else {

				try {
					OffsetTime.parse(value);
				} catch (DateTimeParseException e) {

					try {
						LocalTime.parse(value);
					} catch (DateTimeParseException e2) {
						return false;
					}

				}

			}
			return true;
		default: // free text
			return true;
		}

	}

	/**
	 * Return normalized content.
	 *
	 * @param value content
	 * @return String normalized content
	 */
	public String normalize(String value) {

		switch (xs_type) {
		case xs_hexBinary:
			return "E'\\\\x" + value + "'";
		case xs_base64Binary:
			return "decode('" + value + "','base64')";
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			Calendar cal = Calendar.getInstance();

			cal.setTime(PgSchemaUtil.parseDate(value));
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			cal.setTimeZone(TimeZone.getTimeZone("UTC"));

			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

			return format.format(cal.getTime());
		case xs_float:
		case xs_double:
		case xs_decimal:
			if (!restriction)
				return value;

			// xs:fractionDigits

			if (fraction_digits != null) {

				Integer i = Integer.parseInt(fraction_digits);

				if (i < 0)
					return value;

				BigDecimal b = new BigDecimal(value);
				b.setScale(i);

				return b.toString();
			}

			return value;
		case xs_any:
		case xs_anyAttribute:
			return "XMLPARSE (CONTENT '" + value + "')";
		default: // free text
			if (!restriction)
				return value;

			// xs:whiteSpace

			if (white_space != null) {

				if (white_space.equals("replace"))
					value = PgSchemaUtil.replaceWhiteSpace(value);
				else if (white_space.equals("collapse"))
					value = PgSchemaUtil.collapseWhiteSpace(value);

			}

			return value;
		}

	}

	// PostgreSQL data migration via prepared statement

	/**
	 * Write value via PreparedStatement.
	 *
	 * @param ps prepared statement
	 * @param par_idx parameter index id
	 * @param value content
	 * @throws SQLException the SQL exception
	 */
	protected void writeValue2PgSql(PreparedStatement ps, int par_idx, String value) throws SQLException {

		if (enum_name != null) {
			ps.setString(par_idx, value);
			return;
		}

		switch (xs_type) {
		case xs_boolean:
			ps.setBoolean(par_idx, Boolean.valueOf(value));
			break;
		case xs_hexBinary:
			ps.setBytes(par_idx, DatatypeConverter.parseHexBinary(value));
			break;
		case xs_base64Binary:
			ps.setBytes(par_idx, DatatypeConverter.parseBase64Binary(value));
			break;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
			try {
				ps.setLong(par_idx, Long.valueOf(value));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			break;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
			ps.setInt(par_idx, Integer.valueOf(value));
			break;
		case xs_float:
			ps.setFloat(par_idx, Float.valueOf(value));
			break;
		case xs_double:
			ps.setDouble(par_idx, Double.valueOf(value));
			break;
		case xs_decimal:
			ps.setBigDecimal(par_idx, new BigDecimal(value));
			break;
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			ps.setInt(par_idx, Integer.valueOf(value));
			break;
		case xs_dateTime:
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required")))
				ps.setTimestamp(par_idx, new java.sql.Timestamp(PgSchemaUtil.parseDate(value).getTime()));
			else
				ps.setTimestamp(par_idx, new java.sql.Timestamp(PgSchemaUtil.parseDate(value).getTime()), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
			break;
		case xs_time:
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required"))) {

				try {
					ps.setTime(par_idx, java.sql.Time.valueOf(LocalTime.parse(value)));
				} catch (DateTimeParseException e) {

					try {
						ps.setTime(par_idx, java.sql.Time.valueOf(OffsetTime.parse(value).toLocalTime()));
					} catch (DateTimeParseException e2) {
					}
				}

			}

			else {

				try {
					ps.setTime(par_idx, java.sql.Time.valueOf(OffsetTime.parse(value).toLocalTime()), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
				} catch (DateTimeParseException e) {

					try {
						ps.setTime(par_idx, java.sql.Time.valueOf(LocalTime.parse(value)), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
					} catch (DateTimeParseException e2) {
					}
				}

			}
			break;
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			Calendar cal = Calendar.getInstance();

			cal.setTime(PgSchemaUtil.parseDate(value));
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			cal.setTimeZone(TimeZone.getTimeZone("UTC"));

			ps.setDate(par_idx, new java.sql.Date(cal.getTimeInMillis()));
			break;
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			ps.setString(par_idx, value);
			break;
		default: // xs_any, xs_anyAttribute
		}

	}

	/**
	 * Write XML object via PreparedStatement.
	 *
	 * @param ps prepared statement
	 * @param par_idx parameter index id
	 * @param value content
	 * @throws SQLException the SQL exception
	 */
	protected void writeValue2PgSql(PreparedStatement ps, int par_idx, SQLXML value) throws SQLException {

		switch (xs_type) {
		case xs_any:
		case xs_anyAttribute:
			ps.setSQLXML(par_idx, value);
		default: // not xml
		}

	}

	// Lucene full-text indexing

	/**
	 * Write value to Lucene document.
	 *
	 * @param lucene_doc Lucene document
	 * @param name field name
	 * @param value content
	 * @param min_word_len_filter whether it exceeds minimum word length
	 * @param numeric_lucidx whether numeric values are stored in index
	 */
	protected void writeValue2LucIdx(org.apache.lucene.document.Document lucene_doc, String name, String value, boolean min_word_len_filter, boolean numeric_lucidx) {

		if (attr_sel_rdy) {

			switch (xs_type) {
			case xs_boolean:
			case xs_hexBinary:
			case xs_base64Binary:
				lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				lucene_doc.add(new LongPoint(name, Long.valueOf(value)));
				if (numeric_lucidx)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
				lucene_doc.add(new IntPoint(name, Integer.valueOf(value)));
				if (numeric_lucidx)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			case xs_float:
				lucene_doc.add(new FloatPoint(name, Float.valueOf(value)));
				if (numeric_lucidx)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			case xs_double:
			case xs_decimal:
				lucene_doc.add(new DoublePoint(name, Double.valueOf(value)));
				if (numeric_lucidx)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				lucene_doc.add(new IntPoint(name, Integer.valueOf(value)));
				if (numeric_lucidx)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			case xs_dateTime:
				java.util.Date util_time = PgSchemaUtil.parseDate(value);
				lucene_doc.add(new StringField(name, DateTools.dateToString(util_time, DateTools.Resolution.SECOND), Field.Store.YES));
				break;
			case xs_date:
				java.util.Date util_date = PgSchemaUtil.parseDate(value);
				lucene_doc.add(new StringField(name, DateTools.dateToString(util_date, DateTools.Resolution.DAY), Field.Store.YES));
				break;
			case xs_gYearMonth:
				java.util.Date util_month = PgSchemaUtil.parseDate(value);
				lucene_doc.add(new StringField(name, DateTools.dateToString(util_month, DateTools.Resolution.MONTH), Field.Store.YES));
				break;
			case xs_gYear:
				java.util.Date util_year = PgSchemaUtil.parseDate(value);
				lucene_doc.add(new StringField(name, DateTools.dateToString(util_year, DateTools.Resolution.YEAR), Field.Store.YES));
				break;
			case xs_time:
			case xs_gMonthDay:
			case xs_gMonth:
			case xs_gDay:
				lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			default: // free text
				lucene_doc.add(new TextField(name, value, Field.Store.YES));
			}

			attr_sel_rdy = false;

		}

		switch (xs_type) {
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			lucene_doc.add(new VecTextField(PgSchemaUtil.simple_content_name, value, Field.Store.NO));
			break;
		default: // not numetic
			if (min_word_len_filter)
				lucene_doc.add(new VecTextField(PgSchemaUtil.simple_content_name, value, Field.Store.NO));
		}

	}

	// Sphinx full-text indexing

	/**
	 * Write value to Sphinx data source.
	 *
	 * @param buffw the buffered writer
	 * @param attr_name Sphinx attribute name
	 * @param value content
	 * @param min_word_len_filter whether it exceeds minimum word length
	 */
	protected void writeValue2SphDs(BufferedWriter buffw, String attr_name, String value, boolean min_word_len_filter) {

		try {

			boolean escaped = false;

			if (attr_sel_rdy || sph_mva) {

				switch (xs_type) {
				case xs_bigserial:
				case xs_long:
				case xs_bigint:
				case xs_unsignedLong:
					buffw.write("<" + attr_name + ">" + Long.valueOf(value) + "</" + attr_name + ">\n");
					break;
				case xs_serial:
				case xs_integer:
				case xs_int:
				case xs_nonPositiveInteger:
				case xs_negativeInteger:
				case xs_nonNegativeInteger:
				case xs_positiveInteger:
				case xs_unsignedInt:
					buffw.write("<" + attr_name + ">" + Integer.valueOf(value) + "</" + attr_name + ">\n");
					break;
				case xs_float:
					buffw.write("<" + attr_name + ">" + Float.valueOf(value) + "</" + attr_name + ">\n");
					break;
				case xs_double:
				case xs_decimal:
					buffw.write("<" + attr_name + ">" + Double.valueOf(value) + "</" + attr_name + ">\n");
					break;
				case xs_short:
				case xs_byte:
				case xs_unsignedShort:
				case xs_unsignedByte:
					buffw.write("<" + attr_name + ">" + Integer.valueOf(value) + "</" + attr_name + ">\n");
					break;
				case xs_dateTime:
				case xs_date:
				case xs_gYearMonth:
				case xs_gYear:
					java.util.Date util_time = PgSchemaUtil.parseDate(value);
					buffw.write("<" + attr_name + ">" + util_time.getTime() / 1000L + "</" + attr_name + ">\n");
					break;
				default: // free text
					value = StringEscapeUtils.escapeXml10(value);
					buffw.write("<" + attr_name + ">" + value + "</" + attr_name + ">\n");
					escaped = true;
				}

				attr_sel_rdy = false;

			}

			switch (xs_type) {
			case xs_bigserial:
			case xs_serial:
			case xs_float:
			case xs_double:
			case xs_decimal:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				buffw.write("<" + PgSchemaUtil.simple_content_name + ">" + value + "</" + PgSchemaUtil.simple_content_name + ">\n");
				break;
			default: // not numeric
				if (min_word_len_filter)
					buffw.write("<" + PgSchemaUtil.simple_content_name + ">" + (escaped ? value : StringEscapeUtils.escapeXml10(value)) + "</" + PgSchemaUtil.simple_content_name + ">\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// JSON buffer

	/**
	 * Write value to JSON buffer.
	 *
	 * @param schema_ver JSON schema version
	 * @param value content
	 * @param json_key_value_space the JSON key value space
	 * @return boolean whether value is successfully set
	 */
	protected boolean writeValue2JsonBuf(JsonSchemaVersion schema_ver, String value, String json_key_value_space) {

		if (jsonb == null)
			return false;

		jsonb_col_size++;

		if (value == null || value.isEmpty()) {

			if (++jsonb_null_size > 100 && jsonb_col_size == jsonb_null_size) {

				jsonb.setLength(0);
				jsonb = null;

				jsonb_col_size = jsonb_null_size = 0;

				return false;
			}

			switch (xs_type) {
			case xs_boolean:
			case xs_bigserial:
			case xs_serial:
			case xs_float:
			case xs_double:
			case xs_decimal:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				jsonb.append("null");
				break;
			default: // string
				jsonb.append(value == null ? "null" : "\"\"");
			}

			jsonb.append("," + json_key_value_space);

			return false;
		}

		if (!jsonb_not_empty)
			jsonb_not_empty = true;

		switch (xs_type) {
		case xs_boolean:
			jsonb.append(value);
			break;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
			jsonb.append(Long.parseLong(value));
			break;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
			jsonb.append(Integer.parseInt(value));
			break;
		case xs_float:
			jsonb.append(Float.parseFloat(value));
			break;
		case xs_double:
			jsonb.append(Double.parseDouble(value));
			break;
		case xs_decimal:
			jsonb.append(new BigDecimal(value));
			break;
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			jsonb.append(Integer.parseInt(value));
			break;
		case xs_date:
			if (value.endsWith("Z") && schema_ver.is_latest())
				value = value.substring(0, value.length() - 1);
		default: // free text
			value = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(value).replace("\\/", "/").replace("\\'", "'"));

			if (value.startsWith("\""))
				jsonb.append(value);
			else
				jsonb.append("\"" + value + "\"");

		}

		jsonb.append("," + json_key_value_space);

		return true;
	}

	// XPath evaluation over PostgreSQL

	/**
	 * Retrieve value from ResultSet.
	 *
	 * @param rset result set
	 * @param par_idx parameter index id
	 * @param fill_default_value whether fill default value in case of empty
	 * @return String retrieved value
	 * @throws SQLException the SQL exception
	 */
	public String retrieveValue(ResultSet rset, int par_idx, boolean fill_default_value) throws SQLException {

		Object obj = rset.getObject(par_idx);

		if (obj == null)
			return fill_default_value ? default_value : null;

		if (enum_name != null) {

			String ret = rset.getString(par_idx);

			if (ret.length() < PgSchemaUtil.max_enum_len)
				return ret;

			for (String enum_string : xenumeration) {

				if (enum_string.startsWith(ret))
					return enum_string;

			}

			return null;
		}

		switch (xs_type) {
		case xs_boolean:
			return DatatypeConverter.printBoolean(rset.getBoolean(par_idx));
		case xs_hexBinary:
			return DatatypeConverter.printHexBinary(rset.getBytes(par_idx));
		case xs_base64Binary:
			return DatatypeConverter.printBase64Binary(rset.getBytes(par_idx));
		case xs_dateTime:
			Timestamp ts = rset.getTimestamp(par_idx);

			if (ts == null)
				return null;

			Calendar dt_cal = Calendar.getInstance();
			dt_cal.setTimeInMillis(ts.getTime());

			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required"))) { }
			else
				dt_cal.setTimeZone(TimeZone.getTimeZone("UTC"));

			return DatatypeConverter.printDateTime(dt_cal);
		case xs_time:
			Time tm = rset.getTime(par_idx);

			if (tm == null)
				return null;

			Calendar t_cal = Calendar.getInstance();
			t_cal.setTimeInMillis(tm.getTime());

			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required"))) { }
			else
				t_cal.setTimeZone(TimeZone.getTimeZone("UTC"));

			return DatatypeConverter.printTime(t_cal);
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			Date d = rset.getDate(par_idx);

			if (d == null)
				return null;

			Calendar d_cal = Calendar.getInstance();
			d_cal.setTime(d);
			d_cal.set(Calendar.HOUR_OF_DAY, 0);
			d_cal.set(Calendar.MINUTE, 0);
			d_cal.set(Calendar.SECOND, 0);
			d_cal.set(Calendar.MILLISECOND, 0);
			d_cal.setTimeZone(TimeZone.getTimeZone("UTC"));

			String ret = DatatypeConverter.printDate(d_cal);

			switch (xs_type) {
			case xs_date:
				return ret.replaceFirst("Z$", "");
			case xs_gYearMonth:
				return ret.substring(0, ret.lastIndexOf('-'));
			default:
				return ret.substring(0, ret.indexOf('-'));
			}
		case xs_float:
		case xs_double:
			switch (xs_type) {
			case xs_float:
				return String.valueOf((float) obj); // rset.getFloat(par_idx));
			default:
				return String.valueOf((double) obj); // rset.getDouble(par_idx));
			}
		case xs_decimal:
			BigDecimal bd = rset.getBigDecimal(par_idx);

			return bd != null ? bd.toString() : null;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			return rset.getString(par_idx);
		default: // xs_any, xs_anyAttribute
		}

		return null;
	}

	/**
	 * Normalize content as JSON value.
	 *
	 * @param schema_ver JSON schema version
	 * @param value content
	 * @return String normalized content
	 */
	public String normalizeAsJson(JsonSchemaVersion schema_ver, String value) {

		if (value == null || value.isEmpty()) {

			switch (xs_type) {
			case xs_boolean:
			case xs_bigserial:
			case xs_serial:
			case xs_float:
			case xs_double:
			case xs_decimal:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return "null";
			default: // string
				return "\"\"";
			}

		}

		switch (xs_type) {
		case xs_boolean:
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return value;
		case xs_date:
			if (value.endsWith("Z") && schema_ver.is_latest())
				value = value.substring(0, value.length() - 1);
		default: // free text
			value = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(value).replace("\\/", "/").replace("\\'", "'"));

			if (value.startsWith("\""))
				return value;
			else
				return "\"" + value + "\"";
		}

	}

}
