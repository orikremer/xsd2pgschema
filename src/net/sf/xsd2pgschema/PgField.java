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

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * PostgreSQL field declaration.
 *
 * @author yokochi
 */
public class PgField {

	/** The target namespace URI. */
	String target_namespace = PgSchemaUtil.xs_namespace_uri;

	/** The field name in XML document. */
	String xname = "";

	/** The field name in PostgreSQL. */
	String name = "";

	/** The data type. */
	String type = "";

	/** The substitution group. */
	String substitution_group = "";

	/** The @maxOccurs. */
	String maxoccurs = "1";

	/** The @minOccurs. */
	String minoccurs = "1";

	/** The xs:annotation. */
	String anno = "";

	/** The XML Schema data type. */
	XsDataType xs_type;

	/** Whether if xs:element. */
	boolean element = false;

	/** Whether if xs:attribute. */
	boolean attribute = false;

	/** Whether if xs:simpleContent. */
	boolean simple_cont = false;

	/** Whether if xs:any. */
	boolean any = false;

	/** Whether if xs:anyAttribute. */
	boolean any_attribute = false;

	/** Whether if primary key. */
	boolean primary_key = false;

	/** Whether if unique key. */
	boolean unique_key = false;

	/** Whether if foreign key. */
	boolean foreign_key = false;

	/** Whether if nested key. */
	boolean nested_key = false;

	/** Whether if document key. */
	boolean document_key = false;

	/** Whether if serial key. */
	boolean serial_key = false;

	/** Whether if XPath key. */
	boolean xpath_key = false;

	/** Whether it has any system's administrative key (primary_key || foreign_key || nested_key). */
	boolean system_key = false;

	/** Whether it has any user's discretion key (document_key || serial_key || xpath_key). */
	boolean user_key = false;

	/** Whether if not @nillable. */
	boolean xrequired = false;

	/** Whether if not @nillable, but mutable in PostgreSQL when conflict occurs. */
	boolean required = false;

	/** Whether if @use is "prohibited". */
	boolean prohibited = false;

	/** Whether if @maxOccurs > 1 || @minOccurs > 1. */
	boolean list_holder = false;

	/** Whether if representative field of substitution group. */
	boolean rep_substitution_group = false;

	/** Whether if Sphinx attribute. */
	boolean sph_attr = false;

	/** Whether if Sphinx multi-valued attribute. */
	boolean sph_mva = false;

	/** Whether it is selected as field for partial index. */
	boolean field_sel = false;

	/** Whether it is selected as attribute for partial index. */
	boolean attr_sel = false;

	/** The constraint name in PostgreSQL. */
	String constraint_name = null;

	/** The foreign table id. */
	int foreign_table_id = -1;

	/** The foreign table name in PostgreSQL. */
	String foreign_table = null;

	/** The foreign field name in PostgreSQL. */
	String foreign_field = null;

	/** The parent node names. */
	String parent_node = null;

	/** Whether if @fixed. */
	String fixed_value = null;

	/** Whether if @default. */
	String default_value = null;

	/** Whether if @block. */
	String block_value = null;

	/** Whether if field has any restriction. */
	boolean restriction = false;

	/** The xs:enumeration. */
	String enum_name = null;

	/** The array of xs:enumeration in XML document. */
	String[] xenumeration = null;

	/** The array of xs:enumeration in PostgreSQL. */
	String[] enumeration = null;

	/** The xs:length restriction. */
	String length = null;

	/** The xs:minLength restriction. */
	String min_length = null;

	/** The xs:maxLength restriction. */
	String max_length = null;

	/** The xs:pattern restriction. */
	String pattern = null;

	/** The xs:maxInclusive restriction. */
	String max_inclusive = null;

	/** The xs:maxExclusive restriction. */
	String max_exclusive = null;

	/** The xs:minExclusive restriction. */
	String min_exclusive = null;

	/** The xs:minInclusive restriction. */
	String min_inclusive = null;

	/** The xs:totalDigits restriction. */
	String total_digits = null;

	/** The xs:fractionDigits restriction. */
	String fraction_digits = null;

	/** The xs:whiteSpace restriction. */
	String white_space = null;

	/** The xs:assertions restriction. (TODO) */
	String assertions = null;

	/** The xs:explicitTimezone restriction. */
	String explicit_timezone = null;

	/** Whether xs:list. */
	boolean _list = false;

	/** Whether xs:union. */
	boolean _union = false;

	/** The fill-this post XML edition. */
	boolean fill_this = false;

	/** The filled text used in post XML edition. */
	String filled_text = null;

	/** The filt-out post XML edition. */
	boolean filt_out = false;

	/** The filt-out pattern used in post XML edition. */
	String[] out_pattern = null;

	/** The counter of Sphinx attribute. */
	int sph_attr_occurs = 0;

	/** Whether if JSON buffer is not empty. */
	boolean jsonb_not_empty = false;

	/** The size of JSON item in JSON buffer. */
	int jsonb_col_size = 0;

	/** The size of null JSON value in JSON buffer. */
	int jsonb_null_size = 0;

	/** The JSON buffer. */
	StringBuilder jsonb = null;

	/**
	 * Extract @type, @itemType, @memberTypes or @base and set type.
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 */
	public void extractType(PgSchema schema, Node node) {

		String xs_prefix = schema.xs_prefix;
		String xs_prefix_ = schema.xs_prefix_;

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
					extractType(schema, child);

			}

		} finally {

			if (type == null)
				return;

			type = type.trim();

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

				xs_type1 = XsDataType.getLeastCommonDataType(xs_type1, xs_type2);

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
	public void extractNamespace(PgSchema schema, Node node) {

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

		target_namespace = type.contains(":") ? schema.getNamespaceURI(type.split(":")[0]) : schema.getNamespaceURI("");

	}

	/**
	 * Extract @maxOccurs.
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 */
	public void extractMaxOccurs(PgSchema schema, Node node) {

		String xs_prefix_ = schema.xs_prefix_;

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
				extractMaxOccurs(schema, child);

		}

	}

	/**
	 * Extract @minOccurs.
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 */
	public void extractMinOccurs(PgSchema schema, Node node) {

		String xs_prefix_ = schema.xs_prefix_;

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
				extractMinOccurs(schema, child);

		}

	}

	/**
	 * Extract @use or @nillable of current node.
	 *
	 * @param node current node
	 */
	public void extractRequired(Node node) {

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
	public void extractFixedValue(Node node) {

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
	public void extractDefaultValue(Node node) {

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
	public void extractBlockValue(Node node) {

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
	 * @param schema PostgreSQL data model
	 * @param node current node
	 */
	public void extractEnumeration(PgSchema schema, Node node) {

		String xs_prefix_ = schema.xs_prefix_;

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

							String enum_value = value.replaceAll("'", "''");

							xenumeration[_length] = enum_value;
							enumeration[_length] = enum_value.length() <= PgSchemaUtil.enum_max_len ? enum_value : enum_value.substring(0, PgSchemaUtil.enum_max_len);

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

				return;
			}

			if (child.hasChildNodes())
				extractEnumeration(schema, child);

		}

	}

	/**
	 * Extract xs:restriction/xs:any@value.
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 */
	public void extractRestriction(PgSchema schema, Node node) {

		String xs_prefix_ = schema.xs_prefix_;

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
		assertions = null;
		explicit_timezone = null;

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

							restriction = true;
							length = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "minLength")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							min_length = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "maxLength")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							max_length = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "pattern")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							pattern = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "maxInclusive")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							max_inclusive = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "maxExclusive")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							max_exclusive = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "minExclusive")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							min_exclusive = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "minInclusive")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							min_inclusive = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "totalDigits")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							total_digits = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "fractionDigits")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							fraction_digits = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "whiteSpace")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							white_space = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "assertions")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							//							restriction = true; // (TODO)
							assertions = value;

							break;
						}

					}

					else if (enum_node.getNodeName().equals(xs_prefix_ + "explicitTimezone")) {

						Element e = (Element) enum_node;

						String value = e.getAttribute("value");

						if (value != null && !value.isEmpty()) {

							restriction = true;
							explicit_timezone = value;

							break;
						}

					}

				}

				return;
			}

			if (child.hasChildNodes())
				extractRestriction(schema, child);

		}

	}

	/**
	 * Set hash key type.
	 *
	 * @param schema PostgreSQL data model
	 */
	public void setHashKeyType(PgSchema schema) {

		String xs_prefix_ = schema.xs_prefix_;

		switch (schema.option.hash_size) {
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
			break;
		}

	}

	/**
	 * Set serial key type.
	 *
	 * @param schema PostgreSQL data model
	 */
	public void setSerKeyType(PgSchema schema) {

		String xs_prefix_ = schema.xs_prefix_;

		switch (schema.option.ser_size) {
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
	 * Return whether if string matches enumeration.
	 *
	 * @param string string
	 * @return boolean whether if string matches enumeration
	 */
	public boolean matchesEnumeration(String string) {

		if (string == null || string.isEmpty())
			return !required;

		for (String enum_string : enumeration) {

			if (enum_string.equals(string))
				return true;

		}

		return false;
	}

	/**
	 * Return whether if string matches enumeration.
	 *
	 * @param string string
	 * @return boolean whether if string matches enumeration
	 */
	public boolean matchesXEnumeration(String string) {

		if (string == null || string.isEmpty())
			return !required;

		for (String enum_string : xenumeration) {

			if (enum_string.equals(string))
				return true;

		}

		return false;
	}

	/**
	 * Return whether if string matches out pattern.
	 *
	 * @param string string
	 * @return boolean whether if string matches out pattern
	 */
	public boolean matchesOutPattern(String string) {

		if (string == null || string.isEmpty())
			return false;

		if (out_pattern == null)
			return true;

		for (String regex_pattern : out_pattern) {

			if (string.matches(regex_pattern))
				return true;

		}

		return false;
	}

	/**
	 * Return whether if field is omitted.
	 *
	 * @param schema PostgreSQL data model
	 * @return boolean whether if field is omitted
	 */
	public boolean isOmitted(PgSchema schema) {
		return (!schema.option.document_key && document_key) ||
				(!schema.option.serial_key && serial_key) ||
				(!schema.option.xpath_key && xpath_key) ||
				(!schema.option.rel_data_ext && system_key);
	}

	/**
	 * Return whether if field is indexable.
	 *
	 * @param schema PostgreSQL data model
	 * @return boolean whether if field is indexable
	 */
	public boolean isIndexable(PgSchema schema) {
		return !schema.field_resolved || (schema.field_resolved && field_sel) || (schema.attr_resolved && attr_sel);
	}

	/**
	 * Return whether if field is JSON convertible.
	 *
	 * @param schema PostgreSQL data model
	 * @return boolean whether field is JSON convertible
	 */
	public boolean isJsonable(PgSchema schema) {

		if (schema.jsonb.has_discard_doc_key && xname.equals(schema.jsonb.discard_doc_key))
			return false;

		return !schema.field_resolved || (schema.field_resolved && field_sel);
	}

}
