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
import java.io.Serializable;
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
import org.nustaq.serialization.annotations.Flat;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import net.sf.xsd2pgschema.docbuilder.JsonBuilder;
import net.sf.xsd2pgschema.docbuilder.JsonSchemaVersion;
import net.sf.xsd2pgschema.luceneutil.VecTextField;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.type.XsFieldType;

/**
 * PostgreSQL field declaration.
 *
 * @author yokochi
 */
public class PgField implements Serializable {

	/** The default serial version ID. */
	private static final long serialVersionUID = 1L;

	/** The target namespace (@targetNamespace). */
	public String target_namespace = PgSchemaUtil.xs_namespace_uri;

	/** The namespace restriction for any content (@namespace). */
	public String namespace = "##any";

	/** The prefix of target namespace. */
	public String prefix = "";

	/** The canonical name in XML Schema. */
	public String xname = "";

	/** The field name in PostgreSQL. */
	public String pname = "";

	/** The field name. */
	public String name = "";

	/** The data type in XML document. */
	protected String xtype = null;

	/** The data type. */
	protected String type = null;

	/** The @maxOccurs. */
	protected String maxoccurs = "1";

	/** The @minOccurs. */
	protected String minoccurs = "1";

	/** The field type. */
	public XsFieldType xs_type;

	/** Whether target namespace equals URI of XML Schema 1.x. */
	public boolean is_xs_namespace = true;

	/** Whether xs:element. */
	public boolean element = false;

	/** Whether xs:attribute. */
	public boolean attribute = false;

	/** Whether xs:simpleContent. */
	public boolean simple_content = false;

	/** Whether xs:simpleContent as primitive list. */
	public boolean simple_primitive_list = false;

	/** Whether xs:simpleContent as attribute. */
	public boolean simple_attribute = false;

	/** Whether xs:simpleContent as attribute conditionally, which depends on parent node. */
	public boolean simple_attr_cond = false;

	/** Whether xs:any. */
	public boolean any = false;

	/** Whether xs:anyAttribute. */
	public boolean any_attribute = false;

	/** Whether primary key. */
	public boolean primary_key = false;

	/** Whether unique key. */
	public boolean unique_key = false;

	/** Whether foreign key. */
	public boolean foreign_key = false;

	/** Whether nested key. */
	public boolean nested_key = false;

	/** Whether nested key as attribute. */
	public boolean nested_key_as_attr = false;

	/** Whether document key. */
	public boolean document_key = false;

	/** Whether serial key. */
	public boolean serial_key = false;

	/** Whether XPath key. */
	public boolean xpath_key = false;

	/** Whether @use="required" | @nillable="false". */
	protected boolean xrequired = false;

	/** Whether @use="required" | @nillalbe="false", but be false in PostgreSQL when name collision occurs. */
	public boolean required = false;

	/** Whether @maxOccurs is greater than 1 || @minOccurs is greater than 1. */
	public boolean list_holder = false;

	/** The foreign table id. */
	public int foreign_table_id = -1;

	/** The schema name of foreign table in PostgreSQL (default schema name is "public"). */
	protected String foreign_schema = PgSchemaUtil.pg_public_schema_name;

	/** The foreign table name in XML Schema. */
	public String foreign_table_xname = null;

	/** The foreign table name in PostgreSQL. */
	protected String foreign_table_pname = null;

	/** The foreign field name in PostgreSQL. */
	public String foreign_field_pname = null;

	/** The concatenated ancestor node names. */
	public String ancestor_node = null;

	/** The concatenated parent node names. */
	public String parent_node = null;

	/** The array of ancestor node name. */
	public String[] ancestor_nodes = null;

	/** The array of parent node name. */
	public String[] parent_nodes = null;

	/** The @fixed. */
	protected String fixed_value = null;

	/** The @default. */
	public String default_value = null;

	/** Whether field has any restriction. */
	protected boolean restriction = false;

	/** The xs:enumeration. */
	public String enum_name = null;

	/** The array of xs:enumeration in XML document. */
	protected String[] xenumeration = null;

	/** The array of xs:enumeration in PostgreSQL. */
	protected String[] enumeration = null;

	/** The xs:length restriction. */
	public String length = null;

	/** The xs:minLength restriction. */
	public String min_length = null;

	/** The xs:maxLength restriction. */
	public String max_length = null;

	/** The xs:pattern restriction. */
	public String pattern = null;

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

	/** Whether xs:list. */
	protected boolean _list = false;

	/** Whether it has any system's administrative key (primary_key || foreign_key || nested_key). */
	public boolean system_key = false;

	/** Whether it has any user's discretion key (document_key || serial_key || xpath_key). */
	protected boolean user_key = false;

	/** Whether field is omissible. */
	public boolean omissible = false;

	/** Whether field is JSON convertible. */
	public boolean jsonable = true;

	/** The content of xs:annotation/xs:documentation (as is). */
	@Flat
	protected String xanno_doc = null;

	/** The content of xs:annotation. */
	@Flat
	public String anno = null;

	/** The constraint name in PostgreSQL. */
	@Flat
	protected String constraint_name = null;

	/** The @substitutionGroup. */
	@Flat
	protected String substitution_group = null;

	/** The @block. */
	@Flat
	protected String block_value = null;

	/** The xs:assertions restriction. */
	@Flat
	protected String assertions = null;

	/** The filled text used in post XML edition. */
	@Flat
	public String filled_text = null;

	/** The filter patterns in post XML edition. */
	@Flat
	protected String[] filter_pattern = null;

	/** Whether @use is "prohibited". */
	@Flat
	protected boolean prohibited = false;

	/** Whether representative field of substitution group. */
	@Flat
	protected boolean rep_substitution_group = false;

	/** Whether xs:union. */
	@Flat
	protected boolean _union = false;

	/** Whether field is indexable. */
	@Flat
	public boolean indexable = true;

	/** Whether Sphinx multi-valued attribute. */
	@Flat
	protected boolean sph_mva = false;

	/** Whether it is selected as field for partial indexing. */
	@Flat
	protected boolean field_sel = false;

	/** Whether it is selected as attribute for partial indexing. */
	@Flat
	protected boolean attr_sel = false;

	/** Whether it is selected as attribute and ready for partial indexing. */
	@Flat
	protected boolean attr_sel_rdy;

	/** The --fill-this option. */
	@Flat
	public boolean fill_this = false;

	/** The --filt-out option. */
	@Flat
	public boolean filt_out = false;

	/** Whether JSON buffer is not empty (internal use only). */
	@Flat
	public boolean jsonb_not_empty = false;

	/** The size of data in JSON buffer (internal use only). */
	@Flat
	public int jsonb_col_size = 0;

	/** The size of null data in JSON buffer (internal use only). */
	@Flat
	public int jsonb_null_size = 0;

	/** The JSON buffer (internal use only). */
	@Flat
	public StringBuilder jsonb = null;

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

				Element elem = (Element) node;

				String _type = elem.getAttribute("type");

				if (_type != null && !_type.isEmpty()) {
					type = _type;
					return;
				}

				String item_type = elem.getAttribute("itemType"); // xs:list

				if (item_type != null && !item_type.isEmpty()) {
					_list = true;
					type = xs_prefix_ + "string";
					return;
				}

				String mem_types = elem.getAttribute("memberTypes"); // xs:union

				if (mem_types != null && !mem_types.isEmpty()) {
					_union = true;
					type = mem_types;
					return;
				}

				String _substitution_group = elem.getAttribute("substitutionGroup");

				if (_substitution_group != null && !_substitution_group.isEmpty()) {
					substitution_group = _substitution_group;
					return;
				}

			}

			Element child_elem;
			String child_name;

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
					continue;

				child_elem = (Element) child;
				child_name = child_elem.getLocalName();

				if (child_name.equals("annotation"))
					continue;

				else if (child_name.equals("any") ||
						child_name.equals("anyAttribute") ||
						child_name.equals("attribute") ||
						child_name.equals("attributeGroup") ||
						child_name.equals("element") ||
						child_name.equals("group"))
					return;

				else if (child.hasAttributes()) {

					String _type = child_elem.getAttribute("type");

					if (_type != null && !_type.isEmpty()) {
						type = _type;
						return;
					}

					String item_type = child_elem.getAttribute("itemType"); // xs:list

					if (item_type != null && !item_type.isEmpty()) {
						_list = true;
						type = xs_prefix_ + "string";
						return;
					}

					String mem_types = child_elem.getAttribute("memberTypes"); // xs:union

					if (mem_types != null && !mem_types.isEmpty()) {
						_union = true;
						type = mem_types;
						return;
					}

					String _substitution_group = child_elem.getAttribute("substitutionGroup");

					if (_substitution_group != null && !_substitution_group.isEmpty()) {
						substitution_group = _substitution_group;
						return;
					}

					String base = child_elem.getAttribute("base");

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

			XsFieldType xs_type1 = XsFieldType.valueOf("xs_" + _type1[1]), xs_type2;
			String[] _type2;

			for (int i = 1; i < types.length; i++) {

				_type2 = types[i].split(":");

				if (_type2.length != 2 || !_type2[0].equals(xs_prefix)) {
					type = xs_prefix_ + "string";
					return;
				}

				xs_type2 = XsFieldType.valueOf("xs_" + _type2[1]);

				xs_type1 = xs_type1.leastCommonOf(xs_type2);

			}

			type = xs_type1.name().replaceFirst("^xs_", xs_prefix_);

		}

	}

	/**
	 * Extract target namespace of current node (@targetNamespace).
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 */
	protected void extractTargetNamespace(PgSchema schema, Node node) {

		target_namespace = null;

		if (node.hasAttributes()) {

			String namespace = ((Element) node).getAttribute("targetNamespace");

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
	 * Extract namespace restriction for any content (@namespace).
	 *
	 * @param node current node
	 */
	protected void extractNamespace(Node node) {

		if (node.hasAttributes()) {

			String namespace = ((Element) node).getAttribute("namespace");

			if (namespace != null && !namespace.isEmpty()) {

				this.namespace = namespace;

				return;
			}

		}

	}

	/**
	 * Extract @maxOccurs.
	 *
	 * @param node current node
	 */
	protected void extractMaxOccurs(Node node) {

		if (node.hasAttributes()) {

			String _maxoccurs = ((Element) node).getAttribute("maxOccurs");

			if (_maxoccurs != null && !_maxoccurs.isEmpty()) {

				maxoccurs = _maxoccurs;

				list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

				return;
			}

		}

		// test parent node of xs:all, xs:choice, xs:sequence

		Node parent_node = node.getParentNode();

		if (parent_node != null) {

			Element parent_elem = (Element) parent_node;
			String parent_name = parent_elem.getLocalName();

			if (parent_name.equals("all") ||
					parent_name.equals("choice") ||
					parent_name.equals("sequence")) {

				String _maxoccurs = parent_elem.getAttribute("maxOccurs");

				if (_maxoccurs != null && !_maxoccurs.isEmpty()) {

					maxoccurs = _maxoccurs;

					list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

					return;
				}

			}

		}

		// test child nodes

		Element child_elem;
		String child_name;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_elem = (Element) child;
			child_name = child_elem.getLocalName();

			if (child_name.equals("annotation"))
				continue;

			else if (child_name.equals("any") ||
					child_name.equals("anyAttribute") ||
					child_name.equals("attribute") ||
					child_name.equals("attributeGroup") ||
					child_name.equals("element") ||
					child_name.equals("group"))
				return;

			else if (child.hasAttributes()) {

				String _maxoccurs = child_elem.getAttribute("maxOccurs");

				if (_maxoccurs != null && !_maxoccurs.isEmpty()) {

					maxoccurs = _maxoccurs;

					list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

					return;
				}

			}

			if (child.hasChildNodes())
				extractMaxOccurs(child);

		}

	}

	/**
	 * Extract @minOccurs.
	 *
	 * @param node current node
	 */
	protected void extractMinOccurs(Node node) {

		if (node.hasAttributes()) {

			String _minoccurs = ((Element) node).getAttribute("minOccurs");

			if (_minoccurs != null && !_minoccurs.isEmpty()) {

				minoccurs = _minoccurs;

				list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

				return;
			}

		}

		// test parent node of xs:all, xs:choice, xs:sequence

		Node parent_node = node.getParentNode();

		if (parent_node != null) {

			Element parent_elem = (Element) parent_node;
			String parent_name = parent_elem.getLocalName();

			if (parent_name.equals("all") ||
					parent_name.equals("choice") ||
					parent_name.equals("sequence")) {

				String _minoccurs = parent_elem.getAttribute("minOccurs");

				if (_minoccurs != null && !_minoccurs.isEmpty()) {

					minoccurs = _minoccurs;

					list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

					return;
				}

			}

		}

		// test child nodes

		Element child_elem;
		String child_name;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_elem = (Element) child;
			child_name = child_elem.getLocalName();

			if (child_name.equals("annotation"))
				continue;

			else if (child_name.equals("any") ||
					child_name.equals("anyAttribute") ||
					child_name.equals("attribute") ||
					child_name.equals("attributeGroup") ||
					child_name.equals("element") ||
					child_name.equals("group"))
				return;

			else if (child.hasAttributes()) {

				String _minoccurs = child_elem.getAttribute("minOccurs");

				if (_minoccurs != null && !_minoccurs.isEmpty()) {

					minoccurs = _minoccurs;

					list_holder = (!maxoccurs.equals("0") && !maxoccurs.equals("1")) || (!minoccurs.equals("0") && !minoccurs.equals("1"));

					return;
				}

			}

			if (child.hasChildNodes())
				extractMinOccurs(child);

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

			Element elem = (Element) node;

			String use = elem.getAttribute("use");

			if (use != null && !use.isEmpty()) {

				required = use.equals("required");
				prohibited = use.equals("prohibited");

			}

			String nillable = elem.getAttribute("nillable");

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

			String fixed = ((Element) node).getAttribute("fixed");

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

			String _default = ((Element) node).getAttribute("default");

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

			String _block = ((Element) node).getAttribute("block");

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

		enumeration = xenumeration = null;

		String child_name;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_name = ((Element) child).getLocalName();

			if (child_name.equals("annotation"))
				continue;

			else if (child_name.equals("any") ||
					child_name.equals("anyAttribute") ||
					child_name.equals("attribute") ||
					child_name.equals("attributeGroup") ||
					child_name.equals("element") ||
					child_name.equals("group"))
				return;

			else if (child_name.equals("restriction")) {

				Element enum_elem;
				String value;

				int length = 0;

				for (Node enum_node = child.getFirstChild(); enum_node != null; enum_node = enum_node.getNextSibling()) {

					if (enum_node.getNodeType() != Node.ELEMENT_NODE || !enum_node.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
						continue;

					enum_elem = (Element) enum_node;

					if (enum_elem.getLocalName().equals("enumeration")) {

						value = enum_elem.getAttribute("value");

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

					if (enum_node.getNodeType() != Node.ELEMENT_NODE || !enum_node.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
						continue;

					enum_elem = (Element) enum_node;

					if (enum_elem.getLocalName().equals("enumeration")) {

						value = enum_elem.getAttribute("value");

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

					type = option.xs_prefix_ + "string";
					xs_type = XsFieldType.xs_string;

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
	 * @param node current node
	 */
	protected void extractRestriction(Node node) {

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

		String child_name;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_name = ((Element) child).getLocalName();

			if (child_name.equals("annotation"))
				continue;

			else if (child_name.equals("any") ||
					child_name.equals("anyAttribute") ||
					child_name.equals("attribute") ||
					child_name.equals("attributeGroup") ||
					child_name.equals("element") ||
					child_name.equals("group"))
				return;

			else if (child_name.equals("restriction")) {

				Element rest_elem;
				String value;

				for (Node rest_node = child.getFirstChild(); rest_node != null; rest_node = rest_node.getNextSibling()) {

					if (rest_node.getNodeType() != Node.ELEMENT_NODE || !rest_node.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
						continue;

					rest_elem = (Element) rest_node;

					value = rest_elem.getAttribute("value");

					if (value == null || value.isEmpty())
						continue;

					if (rest_elem.getLocalName().equals("length")) {

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

					else if (rest_elem.getLocalName().equals("minLength")) {

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

					else if (rest_elem.getLocalName().equals("maxLength")) {

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

					else if (rest_elem.getLocalName().equals("pattern")) {

						restriction = true;
						pattern = value;

					}

					else if (rest_elem.getLocalName().equals("maxInclusive")) {

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

					else if (rest_elem.getLocalName().equals("maxExclusive")) {

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

					else if (rest_elem.getLocalName().equals("minExclusive")) {

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

					else if (rest_elem.getLocalName().equals("minInclusive")) {

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

					else if (rest_elem.getLocalName().equals("totalDigits")) {

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

					else if (rest_elem.getLocalName().equals("fractionDigits")) {

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

					else if (rest_elem.getLocalName().equals("whiteSpace")) {

						if (value.equals("replace") || value.equals("collapse")) {

							restriction = true;
							white_space = value;

						}

					}

					else if (rest_elem.getLocalName().equals("explicitTimezone")) {

						restriction = true;
						explicit_timezone = value;

					}

					else if (rest_elem.getLocalName().equals("assertions")) {

						restriction = true;
						assertions = value;

					}

				}

				return;
			}

			if (child.hasChildNodes())
				extractRestriction(child);

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
			xs_type = XsFieldType.xs_hexBinary;
			break;
		case unsigned_int_32:
			type = xs_prefix_ + "unsignedInt";
			xs_type = XsFieldType.xs_unsignedInt;
			break;
		case unsigned_long_64:
			type = xs_prefix_ + "unsignedLong";
			xs_type = XsFieldType.xs_unsignedLong;
			break;
		default:
			type = xs_prefix_ + "string";
			xs_type = XsFieldType.xs_string;
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
			xs_type = XsFieldType.xs_unsignedInt;
			break;
		case unsigned_short_16:
			type = xs_prefix_ + "unsignedShort";
			xs_type = XsFieldType.xs_unsignedShort;
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

		omissible = (!option.document_key && !option.in_place_document_key && document_key) || (!option.serial_key && serial_key) || (!option.xpath_key && xpath_key) || (!option.rel_data_ext && system_key);

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
	 * Return whether node name contains one of parent node names.
	 *
	 * @param node_name node name
	 * @return boolean whether node name contains one of parent node names
	 */
	public boolean containsParentNodeName(String node_name) {

		if (parent_nodes == null)
			return true;

		for (String parent_node : parent_nodes) {

			if (node_name.contains(parent_node))
				return true;

		}

		return false;
	}

	/**
	 * Return whether node name matches ancestor node names.
	 *
	 * @param node_name node name
	 * @return boolean whether node name matches ancestor node names
	 */
	public boolean matchesAncestorNodeName(String node_name) {

		if (ancestor_nodes == null)
			return true;

		for (String ancestor_node : ancestor_nodes) {

			if (node_name.equals(ancestor_node))
				return true;

		}

		return false;
	}

	/**
	 * Return whether node name matches parent node names.
	 *
	 * @param node_name node name
	 * @return boolean whether node name matches parent node names
	 */
	public boolean matchesParentNodeName(String node_name) {

		if (parent_nodes == null)
			return true;

		for (String parent_node : parent_nodes) {

			if (node_name.equals(parent_node))
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
	 * @param wild_card whether wild card follows
	 * @return boolean whether node name matches
	 */
	public boolean matchesNodeName(PgSchemaOption option, String node_name, boolean as_attr, boolean wild_card) {

		if ((element || attribute) && option.discarded_document_key_names.contains(name))
			return false;

		String _xname = as_attr ? (simple_attribute || simple_attr_cond ? foreign_table_xname : xname) : xname;

		if (node_name.startsWith(prefix + ":"))
			node_name = PgSchemaUtil.getUnqualifiedName(node_name);

		if (wild_card)
			return _xname.matches(node_name);

		return node_name.equals("*") || _xname.equals(node_name);
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
			if (check != null)
				check.setLength(0);
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
			return "'{" + value + "}'";
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
	 * @param concat_value_space the JSON key value space with concatenation
	 * @return String JSON Schema enumeration array
	 */
	public String getJsonSchemaEnumArray(final String concat_value_space) {

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
					sb.append(enumeration + concat_value_space);
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
					sb.append("\"" + StringEscapeUtils.escapeEcmaScript(enumeration) + "\"" + concat_value_space);
				break;
			default: // xs_any, xs_anyAttribute
			}

			return sb.toString();

		} finally {
			sb.setLength(0);
		}

	}

	/**
	 * Return JSON Schema maximum value (draft-04).
	 *
	 * @param jsonb JSON builder
	 * @return String JSON Schema maximum value
	 */
	@Deprecated
	public String getJsonSchemaMaximumValueDraftV4(JsonBuilder jsonb) {

		final String exclusive_maximum = jsonb.concat_value_space + jsonb.getCanKeyDecl("exclusiveMaximum") + "true";

		int i;

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
				return max_exclusive + exclusive_maximum;
			break;
		case xs_nonPositiveInteger:
			if (!restriction)
				return "0";

			if (max_inclusive != null) {

				try {

					if (Integer.parseInt(max_inclusive) < 0)
						return max_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (max_exclusive != null) {

				try {

					if (Integer.parseInt(max_exclusive) < 1)
						return max_exclusive + exclusive_maximum;
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
				return "0" + exclusive_maximum;

			if (max_inclusive != null) {

				try {

					if (Integer.parseInt(max_inclusive) < -1)
						return max_inclusive;
					else
						return "0" + exclusive_maximum;

				} catch (NumberFormatException e) {
					return "0" + exclusive_maximum;
				}

			}

			else if (max_exclusive != null) {

				try {

					if (Integer.parseInt(max_exclusive) < 0)
						return max_exclusive + exclusive_maximum;
					else
						return "0" + exclusive_maximum;

				} catch (NumberFormatException e) {
					return "0" + exclusive_maximum;
				}

			}

			else
				return "0" + exclusive_maximum;
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

					return max_exclusive + exclusive_maximum;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					if ((i = Integer.parseInt(total_digits)) > 0)
						return String.valueOf((int) Math.pow(10, i)) + exclusive_maximum;

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

					return max_exclusive + exclusive_maximum;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					if ((i = Integer.parseInt(total_digits)) > 0)
						return String.valueOf((int) Math.pow(10, i)) + exclusive_maximum;

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

					return max_exclusive + exclusive_maximum;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					if ((i = Integer.parseInt(total_digits)) > 0)
						return String.valueOf((long) Math.pow(10, i)) + exclusive_maximum;

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

					return max_exclusive + exclusive_maximum;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					if ((i = Integer.parseInt(total_digits)) > 0)
						return String.valueOf((short) Math.pow(10, i)) + exclusive_maximum;

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
	 * @param jsonb JSON builder
	 * @return String JSON Schema minimum value
	 */
	@Deprecated
	public String getJsonSchemaMinimumValueDraftV4(JsonBuilder jsonb) {

		final String exclusive_minimum = jsonb.concat_value_space + jsonb.getCanKeyDecl("exclusiveMinimum") + "true";

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
				return min_exclusive + exclusive_minimum;
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

					return min_exclusive + exclusive_minimum;

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

					return min_exclusive + exclusive_minimum;

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

					if (Integer.parseInt(min_inclusive) > 0)
						return min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					if (Integer.parseInt(min_exclusive) > -1)
						return min_exclusive + exclusive_minimum;
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
				return "0" + exclusive_minimum;

			if (min_inclusive != null) {

				try {

					if (Integer.parseInt(min_inclusive) > 1)
						return min_inclusive;
					else
						return "0" + exclusive_minimum;

				} catch (NumberFormatException e) {
					return "0" + exclusive_minimum;
				}

			}

			else if (min_exclusive != null) {

				try {

					if (Integer.parseInt(min_exclusive) > 0)
						return min_exclusive + exclusive_minimum;
					else
						return "0" + exclusive_minimum;

				} catch (NumberFormatException e) {
					return "0" + exclusive_minimum;
				}

			}

			else
				return "0" + exclusive_minimum;
		case xs_unsignedLong:
			if (!restriction)
				return "0";

			if (min_inclusive != null) {

				try {

					if (Long.parseLong(min_inclusive) > 0)
						return min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					if (Long.parseLong(min_exclusive) > -1)
						return min_exclusive + exclusive_minimum;
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

					if (Short.parseShort(min_inclusive) > 0)
						return min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (min_exclusive != null) {

				try {

					if (Short.parseShort(min_exclusive) > -1)
						return min_exclusive + exclusive_minimum;
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
	 * @param jsonb JSON builder
	 * @return String JSON Schema maximum value
	 */
	public String getJsonSchemaMaximumValue(JsonBuilder jsonb) {

		final String maximum_ = jsonb.getCanKeyDecl("maximum");
		final String exclusive_maximum_ = jsonb.getCanKeyDecl("exclusiveMaximum");
		final String maximum_zero = maximum_ + "0";
		final String exclusive_maximum_zero = exclusive_maximum_ + "0";

		int i;

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
				return maximum_ + max_inclusive;
			else if (max_exclusive != null)
				return exclusive_maximum_ + max_exclusive;
			break;
		case xs_nonPositiveInteger:
			if (!restriction)
				return maximum_zero;

			if (max_inclusive != null) {

				try {

					if (Integer.parseInt(max_inclusive) < 0)
						return maximum_ + max_inclusive;
					else
						return maximum_zero;

				} catch (NumberFormatException e) {
					return maximum_zero;
				}

			}

			else if (max_exclusive != null) {

				try {

					if (Integer.parseInt(max_exclusive) < 1)
						return exclusive_maximum_ + max_exclusive;
					else
						return maximum_zero;

				} catch (NumberFormatException e) {
					return maximum_zero;
				}

			}

			else
				return maximum_zero;
		case xs_negativeInteger:
			if (!restriction)
				return exclusive_maximum_zero;

			if (max_inclusive != null) {

				try {

					if (Integer.parseInt(max_inclusive) < -1)
						return maximum_ + max_inclusive;
					else
						return exclusive_maximum_zero;

				} catch (NumberFormatException e) {
					return exclusive_maximum_zero;
				}

			}

			else if (max_exclusive != null) {

				try {

					if (Integer.parseInt(max_exclusive) < 0)
						return exclusive_maximum_ + max_exclusive;
					else
						return exclusive_maximum_zero;

				} catch (NumberFormatException e) {
					return exclusive_maximum_zero;
				}

			}

			else
				return exclusive_maximum_zero;
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			if (!restriction)
				return null;

			if (max_inclusive != null) {

				try {

					Integer.parseInt(max_inclusive);

					return maximum_ + max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Integer.parseInt(max_exclusive);

					return exclusive_maximum_ + max_exclusive;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					if ((i = Integer.parseInt(total_digits)) > 0)
						return exclusive_maximum_ + String.valueOf((int) Math.pow(10, i));

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

					return maximum_ + max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Integer.parseInt(max_exclusive);

					return exclusive_maximum_ + max_exclusive;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					if ((i = Integer.parseInt(total_digits)) > 0)
						return exclusive_maximum_ + String.valueOf((int) Math.pow(10, i));

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

					return maximum_ + max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Long.parseLong(max_exclusive);

					return exclusive_maximum_ + max_exclusive;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					if ((i = Integer.parseInt(total_digits)) > 0)
						return exclusive_maximum_ + String.valueOf((long) Math.pow(10, i));

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

					return maximum_ + max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (max_exclusive != null) {

				try {

					Short.parseShort(max_exclusive);

					return exclusive_maximum_ + max_exclusive;

				} catch (NumberFormatException e) {
				}

			}

			if (total_digits != null) {

				try {

					if ((i = Integer.parseInt(total_digits)) > 0)
						return exclusive_maximum_ + String.valueOf((short) Math.pow(10, i));

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
	 * @param jsonb JSON builder
	 * @return String JSON Schema minimum value
	 */
	public String getJsonSchemaMinimumValue(JsonBuilder jsonb) {

		final String minimum_ = jsonb.getCanKeyDecl("minimum");
		final String exclusive_minimum_ = jsonb.getCanKeyDecl("exclusiveMinimum");
		final String minimum_zero = minimum_ + "0";
		final String exclusive_minimum_zero = exclusive_minimum_ + "0";

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
				return minimum_ + min_inclusive;
			else if (min_exclusive != null)
				return exclusive_minimum_ + min_exclusive;
			break;
		case xs_nonPositiveInteger:
			if (!restriction)
				return null;

			if (min_inclusive != null) {

				try {

					Integer.parseInt(min_inclusive);

					return minimum_ + min_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (min_exclusive != null) {

				try {

					Integer.parseInt(min_exclusive);

					return exclusive_minimum_ + min_exclusive;

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

					return minimum_ + min_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (min_exclusive != null) {

				try {

					Integer.parseInt(min_exclusive);

					return exclusive_minimum_ + min_exclusive;

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			if (!restriction)
				return minimum_zero;

			if (min_inclusive != null) {

				try {

					if (Integer.parseInt(min_inclusive) > 0)
						return minimum_ + min_inclusive;
					else
						return minimum_zero;

				} catch (NumberFormatException e) {
					return minimum_zero;
				}

			}

			else if (min_exclusive != null) {

				try {

					if (Integer.parseInt(min_exclusive) > -1)
						return exclusive_minimum_ + min_exclusive;
					else
						return minimum_zero;

				} catch (NumberFormatException e) {
					return minimum_zero;
				}

			}

			else
				return minimum_zero;
		case xs_positiveInteger:
			if (!restriction)
				return exclusive_minimum_zero;

			if (min_inclusive != null) {

				try {

					if (Integer.parseInt(min_inclusive) > 1)
						return minimum_ + min_inclusive;
					else
						return exclusive_minimum_zero;

				} catch (NumberFormatException e) {
					return exclusive_minimum_zero;
				}

			}

			else if (min_exclusive != null) {

				try {

					if (Integer.parseInt(min_exclusive) > 0)
						return exclusive_minimum_ + min_exclusive;
					else
						return exclusive_minimum_zero;

				} catch (NumberFormatException e) {
					return exclusive_minimum_zero;
				}

			}

			else
				return exclusive_minimum_zero;
		case xs_unsignedLong:
			if (!restriction)
				return minimum_zero;

			if (min_inclusive != null) {

				try {

					if (Long.parseLong(min_inclusive) > 0)
						return minimum_ + min_inclusive;
					else
						return minimum_zero;

				} catch (NumberFormatException e) {
					return minimum_zero;
				}

			}

			else if (min_exclusive != null) {

				try {

					if (Long.parseLong(min_exclusive) > -1)
						return exclusive_minimum_ + min_exclusive;
					else
						return minimum_zero;

				} catch (NumberFormatException e) {
					return minimum_zero;
				}

			}

			else
				return minimum_zero;
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (!restriction)
				return minimum_zero;

			if (min_inclusive != null) {

				try {

					if (Short.parseShort(min_inclusive) > 0)
						return minimum_ + min_inclusive;
					else
						return minimum_zero;

				} catch (NumberFormatException e) {
					return minimum_zero;
				}

			}

			else if (min_exclusive != null) {

				try {

					if (Short.parseShort(min_exclusive) > -1)
						return exclusive_minimum_ + min_exclusive;
					else
						return minimum_zero;

				} catch (NumberFormatException e) {
					return minimum_zero;
				}

			}

			else
				return minimum_zero;
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

		boolean latest = schema_ver.isLatest();

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
			return latest ? "iri-reference" : null;
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
				return Long.parseLong(value) > 0;
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_serial:
			try {
				return Integer.parseInt(value) > 0;
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
				return Integer.parseInt(value) <= 0;
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_negativeInteger:
			try {
				return Integer.parseInt(value) < 0;
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			try {
				return Integer.parseInt(value) >= 0;
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_positiveInteger:
			try {
				return Integer.parseInt(value) > 0;
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_unsignedLong:
			try {
				return Long.parseLong(value) >= 0;
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_unsignedShort:
		case xs_unsignedByte:
			try {
				return Short.parseShort(value) >= 0;
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
			return PgSchemaUtil.parseDate(value) != null;
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
	 * Normalize content as PostgreSQL value.
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
			return "'{" + value + "}'";
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
	 * @param par_idx parameter index
	 * @param ins_idx parameter index for upsert
	 * @param value content
	 * @throws SQLException the SQL exception
	 */
	public void write(PreparedStatement ps, int par_idx, int ins_idx, String value) throws SQLException {

		boolean upsert = ins_idx != -1;

		if (enum_name != null) {
			ps.setString(par_idx, value);
			if (upsert)
				ps.setString(ins_idx, value);
			return;
		}

		switch (xs_type) {
		case xs_boolean:
			boolean boolean_value = Boolean.valueOf(value);
			ps.setBoolean(par_idx, boolean_value);
			if (upsert)
				ps.setBoolean(ins_idx, boolean_value);
			break;
		case xs_hexBinary:
			byte[] hexbin_value = DatatypeConverter.parseHexBinary(value);
			ps.setBytes(par_idx, hexbin_value);
			if (upsert)
				ps.setBytes(ins_idx, hexbin_value);
			break;
		case xs_base64Binary:
			byte[] base64bin_value = DatatypeConverter.parseBase64Binary(value);
			ps.setBytes(par_idx, base64bin_value);
			if (upsert)
				ps.setBytes(ins_idx, base64bin_value);
			break;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
			try {
				long long_value = Long.valueOf(value);
				ps.setLong(par_idx, long_value);
				if (upsert)
					ps.setLong(ins_idx, long_value);
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
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:			
			try {
				int int_value = Integer.valueOf(value);
				ps.setInt(par_idx, int_value);
				if (upsert)
					ps.setInt(ins_idx, int_value);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			break;
		case xs_float:
			try {
				float float_value = Float.valueOf(value);
				ps.setFloat(par_idx, float_value);
				if (upsert)
					ps.setFloat(ins_idx, float_value);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			break;
		case xs_double:
			try {
				double double_value = Double.valueOf(value);
				ps.setDouble(par_idx, double_value);
				if (upsert)
					ps.setDouble(ins_idx, double_value);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			break;
		case xs_decimal:
			try {
				BigDecimal bigdec_value = new BigDecimal(value);
				ps.setBigDecimal(par_idx, bigdec_value);
				if (upsert)
					ps.setBigDecimal(ins_idx, bigdec_value);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			break;
		case xs_dateTime:
			Timestamp timestamp = new java.sql.Timestamp(PgSchemaUtil.parseDate(value).getTime());
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required"))) {
				ps.setTimestamp(par_idx, timestamp);
				if (upsert)
					ps.setTimestamp(ins_idx, timestamp);
			} else {
				Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
				ps.setTimestamp(par_idx, timestamp, calendar);
				if (upsert)
					ps.setTimestamp(ins_idx, timestamp, calendar);
			}
			break;
		case xs_time:
			if (!restriction || (explicit_timezone != null && !explicit_timezone.equals("required"))) {
				try {
					Time time = java.sql.Time.valueOf(LocalTime.parse(value));
					ps.setTime(par_idx, time);
					if (upsert)
						ps.setTime(ins_idx, time);
				} catch (DateTimeParseException e) {
					try {
						Time time = java.sql.Time.valueOf(OffsetTime.parse(value).toLocalTime());
						ps.setTime(par_idx, time);
						if (upsert)
							ps.setTime(ins_idx, time);
					} catch (DateTimeParseException e2) {
					}
				}
			} else {
				Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
				try {
					Time time = java.sql.Time.valueOf(OffsetTime.parse(value).toLocalTime());
					ps.setTime(par_idx, time, calendar);
					if (upsert)
						ps.setTime(ins_idx, time, calendar);
				} catch (DateTimeParseException e) {
					Time time = java.sql.Time.valueOf(LocalTime.parse(value));
					try {
						ps.setTime(par_idx, time, calendar);
						if (upsert)
							ps.setTime(ins_idx, time, calendar);
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

			Date date = new java.sql.Date(cal.getTimeInMillis());

			ps.setDate(par_idx, date);
			if (upsert)
				ps.setDate(ins_idx, date);
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
			if (upsert)
				ps.setString(ins_idx, value);
			break;
		default: // xs_any, xs_anyAttribute
		}

	}

	/**
	 * Write XML object via PreparedStatement.
	 *
	 * @param ps prepared statement
	 * @param par_idx parameter index
	 * @param ins_idx parameter index for upsert
	 * @param xml_object XML object
	 * @throws SQLException the SQL exception
	 */
	public void write(PreparedStatement ps, int par_idx, int ins_idx, SQLXML xml_object) throws SQLException {

		boolean upsert = ins_idx != -1;

		switch (xs_type) {
		case xs_any:
		case xs_anyAttribute:
			ps.setSQLXML(par_idx, xml_object);
			if (upsert)
				ps.setSQLXML(ins_idx, xml_object);
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
	 * @param lucene_numeric_index whether numeric values are stored in Lucene index
	 */
	public void write(org.apache.lucene.document.Document lucene_doc, String name, String value, boolean min_word_len_filter, boolean lucene_numeric_index) {

		if (attr_sel_rdy) {

			switch (xs_type) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				lucene_doc.add(new LongPoint(name, Long.valueOf(value)));
				if (lucene_numeric_index)
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
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				lucene_doc.add(new IntPoint(name, Integer.valueOf(value)));
				if (lucene_numeric_index)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			case xs_float:
				lucene_doc.add(new FloatPoint(name, Float.valueOf(value)));
				if (lucene_numeric_index)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
				break;
			case xs_double:
			case xs_decimal:
				lucene_doc.add(new DoublePoint(name, Double.valueOf(value)));
				if (lucene_numeric_index)
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
			case xs_boolean:
			case xs_hexBinary:
			case xs_base64Binary:
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
	public void write(BufferedWriter buffw, String attr_name, String value, boolean min_word_len_filter) {

		try {

			boolean escaped = false;

			if (attr_sel_rdy || sph_mva) {

				buffw.write("<" + attr_name + ">");

				switch (xs_type) {
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
				case xs_float:
				case xs_double:
				case xs_decimal:
				case xs_short:
				case xs_byte:
				case xs_unsignedShort:
				case xs_unsignedByte:
					buffw.write(value);
					break;
				case xs_dateTime:
				case xs_date:
				case xs_gYearMonth:
				case xs_gYear:
					java.util.Date util_time = PgSchemaUtil.parseDate(value);
					buffw.write(String.valueOf(util_time.getTime() / 1000L));
					break;
				default: // free text
					buffw.write(value = StringEscapeUtils.escapeXml10(value));
					escaped = true;
				}

				buffw.write("</" + attr_name + ">\n");

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
				buffw.write(PgSchemaUtil.sph_start_simple_content_elem + value + PgSchemaUtil.sph_end_simple_content_elem);
				break;
			default: // not numeric
				if (min_word_len_filter)
					buffw.write(PgSchemaUtil.sph_start_simple_content_elem + (escaped ? value : StringEscapeUtils.escapeXml10(value)) + PgSchemaUtil.sph_end_simple_content_elem);
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
	 * @param fragment whether to write fragment JSON at XPath query evaluation
	 * @param concat_value_space the JSON key value space with concatenation
	 * @return boolean whether value is successfully set
	 */
	public boolean write(JsonSchemaVersion schema_ver, String value, boolean fragment, final String concat_value_space) {

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
			case xs_any:
			case xs_anyAttribute:
				if (fragment)
					jsonb.append("null" + concat_value_space);
				else
					jsonb.append("\t"); // TSV should be parsed in JSON builder

				return false;
			default: // string
				jsonb.append("\"\"");
			}

			jsonb.append(concat_value_space);

			return false;
		}

		if (!jsonb_not_empty)
			jsonb_not_empty = true;

		switch (xs_type) {
		case xs_boolean:
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
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			jsonb.append(value);
			break;
		case xs_date:
			if (schema_ver.isLatest())
				value = value.replaceFirst("Z$", "");
			jsonb.append("\"" + value + "\"");
			break;
		case xs_any:
		case xs_anyAttribute:
			if (fragment) {

				value = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(value).replace("\\/", "/").replace("\\'", "'"));

				if (!value.startsWith("\""))
					value = "\"" + value + "\"";

				jsonb.append(value + concat_value_space);	

			}

			else
				jsonb.append(value + "\t"); // TSV should be parsed in JSON builder

			return true;
		default: // free text
			value = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(value).replace("\\/", "/").replace("\\'", "'"));

			if (!value.startsWith("\""))
				value = "\"" + value + "\"";

			jsonb.append(value);

		}

		jsonb.append(concat_value_space);

		return true;
	}

	// XPath evaluation over PostgreSQL

	/**
	 * Retrieve content from ResultSet.
	 *
	 * @param rset result set
	 * @param par_idx parameter index id
	 * @param fill_default_value whether to fill default value in case of empty
	 * @return String retrieved content
	 * @throws SQLException the SQL exception
	 */
	public String retrieve(ResultSet rset, int par_idx, boolean fill_default_value) throws SQLException {

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
			default: // xs_gYear
				return ret.substring(0, ret.indexOf('-'));
			}
		case xs_float:
		case xs_double:
			switch (xs_type) {
			case xs_float:
				return String.valueOf((float) obj); // rset.getFloat(par_idx));
			default: // xs_double
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
	public String normalize(JsonSchemaVersion schema_ver, String value) {

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
			if (schema_ver.isLatest())
				value = value.replaceFirst("Z$", "");
			return "\"" + value + "\"";
		default: // free text
			value = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(value).replace("\\/", "/").replace("\\'", "'"));

			if (!value.startsWith("\""))
				value = "\"" + value + "\"";

			return value;
		}

	}

}
