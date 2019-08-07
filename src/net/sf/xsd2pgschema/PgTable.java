/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2019 Masashi Yokochi

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
import java.io.Serializable;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.nustaq.serialization.annotations.Flat;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.type.XsFieldType;
import net.sf.xsd2pgschema.type.XsTableType;

/**
 * PostgreSQL table declaration.
 *
 * @author yokochi
 */
public class PgTable implements Serializable {

	/** The default serial version ID. */
	private static final long serialVersionUID = 1L;

	/** The PostgreSQL schema name (default schema name is "public"). */
	public String schema_name;

	/** The target namespace. */
	public String target_namespace = "";

	/** The prefix of target namespace. */
	public String prefix = "";

	/** The canonical name in XML Schema. */
	public String xname = "";

	/** The table name in PostgreSQL. */
	public String pname = "";

	/** The table name. */
	public String name = "";

	/** The table type classified by xs_root (root node), xs_root_child (children node of root node), xs_admin_root (administrative root node), xs_admin_child (children node of administrative node). */
	public XsTableType xs_type;

	/** The PostgreSQL schema name (used in SQL clause). */
	public String schema_pgname;

	/** The table name in PostgreSQL (used in SQL clause). */
	public String pgname = null;

	/** The primary key name in PostgreSQL (used in SQL clause). */
	public String primary_key_pgname = null;

	/** The document key name in PostgreSQL (used in SQL clause). */
	public String doc_key_pgname = null;

	/** The document key name in PostgreSQL. */
	public String doc_key_pname = null;

	/** The field list. */
	public List<PgField> fields = null;

	/** The number of references. */
	public int refs = 0;

	/** The total number of field as nested key. */
	public int total_nested_fields = 0;

	/** The total number of field as foreign key. */
	public int total_foreign_fields = 0;

	/** The total number of SQL parameter. */
	public int total_sql_params = 0;

	/** Whether content holder. */
	public boolean content_holder = false;

	/** Whether list holder. */
	public boolean list_holder = false;

	/** Whether bridge table. */
	public boolean bridge = false;

	/** Whether xs_type equals xs_admin_root. */
	public boolean virtual = false;

	/** Whether bridge table | virtual table | !content_holder. */
	public boolean relational = false;

	/** Whether table has element. */
	public boolean has_element = false;

	/** Whether table has attribute. */
	public boolean has_attribute = false;

	/** Whether table has simple content. */
	public boolean has_simple_content = false;

	/** Whether table has any element. */
	public boolean has_any = false;

	/** Whether table has any attribute. */
	public boolean has_any_attribute = false;

	/** Whether table has unique primary key. */
	public boolean has_unique_primary_key = false;

	/** Whether table has @nillable="true" element. */
	public boolean has_nillable_element = false;

	/** Whether table has nested key as attribute. */
	public boolean has_nested_key_as_attr = false;

	/** Whether table has nested key to simple attribute. */
	public boolean has_nested_key_to_simple_attr = false;

	/** Whether table has simple content as attribute. */
	public boolean has_simple_attribute = false;

	/** Whether table has attributes (has_attribute || has_simple_attribute || has_any_attribute || has_nested_key_to_simple_attr). */
	public boolean has_attrs = false;

	/** Whether table has elements (has_simple_content || has_element || has_any). */
	public boolean has_elems = false;

	/** Whether table has nested key with parent/ancestor path restriction. */
	public boolean has_path_restriction = false;

	/** Whether table is referred from child table. */
	public boolean required = false;

	/** Whether table could have writer. */
	public boolean writable = false;

	/** The field list of attribute. */
	public List<PgField> attr_fields = null;

	/** The field list of element. */
	public List<PgField> elem_fields = null;

	/** The field list of nested key. */
	public List<PgField> nested_fields = null;

	/** The list of foreign table id. */
	public int[] ft_ids = null;

	/** Whether name collision occurs. */
	@Flat
	protected boolean name_collision = false;

	/** Whether table has pending group. */
	@Flat
	protected boolean has_pending_group = false;

	/** Whether table is indexable. */
	@Flat
	public boolean indexable = false;

	/** Whether table is JSON convertible. */
	@Flat
	public boolean jsonable = false;

	/** The depth of table. */
	@Flat
	protected int level = -1;

	/** The schema location. */
	@Flat
	protected String schema_location;

	/** The xs:annotation/xs:documentation (as is). */
	@Flat
	protected String xanno_doc = null;

	/** The xs:annotation. */
	@Flat
	public String anno = null;

	/** The table name in JSON. */
	@Flat
	public String jname = null;

	/** Whether table is subset of database (internal use only). */
	@Flat
	protected boolean filt_out = false;

	/** The visited key (internal use only). */
	@Flat
	public String visited_key = "";

	/** The current path of buffered writer (internal use only). */
	@Flat
	public Path pathw = null;

	/** The current buffered writer (internal use only). */
	@Flat
	public BufferedWriter buffw = null;

	/** Whether JSON buffer of arbitrary field is not empty (internal use only). */
	@Flat
	public boolean jsonb_not_empty = false;

	/** The primary prepared statement (internal use only). */
	@Flat
	public PreparedStatement ps = null;

	/** The secondary prepared statement (internal use only). */
	@Flat
	public PreparedStatement ps2 = null;

	/** The absolute XPath expression (internal use only). */
	@Flat
	public HashMap<String, String> abs_xpath_expr = null;

	/** The dictionary of attribute name. */
	@Flat
	public HashMap<String, PgField> attr_name_dic = null;

	/** The dictionary of element name. */
	@Flat
	public HashMap<String, PgField> elem_name_dic = null;

	/**
	 * Instance of PostgreSQL table.
	 *
	 * @param schema_name PosgreSQL schema name
	 * @param target_namespace target namespace URI
	 * @param schema_location schema location
	 */
	public PgTable(String schema_name, String target_namespace, String schema_location) {

		this.schema_name = schema_name;
		this.target_namespace = target_namespace;
		this.schema_location = schema_location;

	}

	/**
	 * Classify table type: content_holder, list_holder, bridge, hub, virtual.
	 */
	protected void classify() {

		// whether content holder table having one of arbitrary content field such as attribute, element, simple content

		content_holder = fields.stream().anyMatch(field -> !field.document_key && !field.primary_key && !field.foreign_key && !field.nested_key && !field.serial_key && !field.xpath_key);

		// whether list holder table having one of field whose occurrence is unbounded

		list_holder = fields.stream().filter(field -> field.nested_key).anyMatch(field -> field.list_holder);

		// whether bridge table having primary key and a nested key

		boolean has_primary_key = false;
		boolean has_single_nested_key = false;

		for (PgField field : fields) {

			if (field.primary_key)
				has_primary_key = true;

			else if (field.nested_key) {

				if (has_single_nested_key) { // has multiple nested keys

					has_single_nested_key = false;

					break;
				}

				else
					has_single_nested_key = true;

			}

		}

		bridge = (has_primary_key && has_single_nested_key);

		// whether virtual table equals administrative table (xs_admin_root)

		virtual = xs_type.equals(XsTableType.xs_admin_root);

		// whether table required for relational model extension

		relational = bridge || virtual || !content_holder;

		// the number of foreign key constraint

		total_foreign_fields = (int) fields.stream().filter(field -> field.foreign_key).count();

		// whether table has element

		has_element = fields.stream().anyMatch(field -> field.element);

		// whether table has attribute

		has_attribute = fields.stream().anyMatch(field -> field.attribute);

		// whether table has simple content

		has_simple_content = fields.stream().anyMatch(field -> field.simple_content);

		// whether table has any element

		has_any = fields.stream().anyMatch(field -> field.any);

		// whether table has any attribute

		has_any_attribute = fields.stream().anyMatch(field -> field.any_attribute);

		// whether table has attributes

		has_attrs = has_attribute || has_any_attribute;

		// whether table has elements

		has_elems = has_simple_content || has_element || has_any;

	}

	/**
	 * Suggest new name for a given field name avoiding name collision with current ones.
	 *
	 * @param option PostgreSQL data model option
	 * @param field_name candidate name of field in PostgreSQL
	 * @return String field name without name collision in PostgreSQL
	 */
	protected String avoidFieldDuplication(PgSchemaOption option, String field_name) {

		if (!option.case_sense)
			field_name = PgSchemaUtil.toCaseInsensitive(field_name);

		if (!option.rel_model_ext)
			return field_name;

		boolean duplicate;

		do {

			duplicate = false;

			for (PgField field : fields) {

				if (!option.rel_data_ext && (field.primary_key || field.foreign_key || field.nested_key))
					continue;

				if (field.pname.equals(field_name)) {

					duplicate = true;

					field_name = "_" + field_name;

					break;
				}

			}

		} while (duplicate);

		return field_name;
	}

	/**
	 * Add primary key.
	 *
	 * @param option PostgreSQL data model option
	 * @param unique_key whether the primary key should be unique
	 */
	protected void addPrimaryKey(PgSchemaOption option, boolean unique_key) {

		if (option.document_key) {

			PgField field = new PgField();

			field.name = field.pname = field.xname = option.document_key_name;
			field.type = option.xs_prefix_ + "string";
			field.xs_type = XsFieldType.xs_string;
			field.document_key = true;

			fields.add(field);

		}

		if (!option.rel_model_ext)
			return;

		PgField field = new PgField();

		field.xname = xname + "_id";
		field.name = name + "_id";
		field.pname = pname + "_id";
		field.setHashKeyType(option);
		field.primary_key = true;
		field.unique_key = unique_key;

		fields.add(field);

	}

	/**
	 * Add a nested key.
	 *
	 * @param option PostgreSQL data model option
	 * @param schema_name PostgreSQL schema name
	 * @param xname canonical name of nested key
	 * @param ref_field reference field
	 * @param node current node
	 * @return boolean whether reference field is unique
	 */
	protected boolean addNestedKey(PgSchemaOption option, String schema_name, String xname, PgField ref_field, Node node) {

		if (xname == null || xname.isEmpty())
			return false;

		if (this.schema_name.equals(schema_name) && this.xname.equals(xname))
			return false;

		String name = option.case_sense ? xname : PgSchemaUtil.toCaseInsensitive(xname);

		PgField field = new PgField();

		field.xname = name + "_id";
		field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
		field.pname = avoidFieldDuplication(option, field.xname);
		field.setHashKeyType(option);
		field.xtype = ref_field.xtype;
		field.nested_key = true;
		field.nested_key_as_attr = ref_field.attribute;
		field.constraint_name = "FK_" + PgSchemaUtil.avoidPgReservedOps(name);
		if (field.constraint_name.length() > PgSchemaUtil.max_enum_len)
			field.constraint_name = field.constraint_name.substring(0, PgSchemaUtil.max_enum_len);
		field.constraint_name = PgSchemaUtil.avoidPgReservedOps(field.constraint_name);
		field.foreign_schema = schema_name;
		field.foreign_table_xname = xname;
		field.foreign_table_pname = name.equals(this.pname) ? "_" + name : name;
		field.foreign_field_pname = name + "_id";

		field.maxoccurs = ref_field.maxoccurs;
		field.minoccurs = ref_field.minoccurs;
		field.list_holder = ref_field.list_holder;

		Node parent_node = node.getParentNode(), parent_attr;

		NamedNodeMap parent_attrs;

		while (parent_node != null) {

			if (parent_node.getNodeType() == Node.ELEMENT_NODE && parent_node.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri) && parent_node.getLocalName().equals("element"))
				break;

			parent_attrs = parent_node.getAttributes();

			if (parent_attrs != null) {

				for (int i = 0; i < parent_attrs.getLength(); i++) {

					parent_attr = parent_attrs.item(i);

					if (parent_attr != null && parent_attr.getNodeName().equals("name")) {

						field.parent_node = parent_attr.getNodeValue();

						break;
					}

				}

				if (field.parent_node != null && !field.parent_node.isEmpty())
					break;

			}

			parent_node = parent_node.getParentNode();

		}

		if (field.parent_node != null && field.parent_node.isEmpty())
			field.parent_node = null;

		fields.add(field);

		return field.maxoccurs.equals("1");
	}

	/**
	 * Add a nested key from foreign table.
	 *
	 * @param option PostgreSQL data model option
	 * @param schema_name PostgreSQL schema name
	 * @param xname canonical name of foreign table
	 */
	protected void addNestedKey(PgSchemaOption option, String schema_name, String xname) {

		if (!option.rel_model_ext)
			return;

		if (xname == null || xname.isEmpty())
			return;

		if (this.schema_name.equals(schema_name) && this.xname.equals(xname))
			return;

		String name = option.case_sense ? xname : PgSchemaUtil.toCaseInsensitive(xname);

		PgField field = new PgField();

		field.xname = name + "_id";
		field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
		field.pname = avoidFieldDuplication(option, field.xname);
		field.setHashKeyType(option);
		field.xtype = name;
		field.nested_key = true;
		field.constraint_name = "FK_" + PgSchemaUtil.avoidPgReservedOps(name);
		if (field.constraint_name.length() > PgSchemaUtil.max_enum_len)
			field.constraint_name = field.constraint_name.substring(0, PgSchemaUtil.max_enum_len);
		field.constraint_name = PgSchemaUtil.avoidPgReservedOps(field.constraint_name);
		field.foreign_schema = schema_name;
		field.foreign_table_xname = xname;
		field.foreign_table_pname = name.equals(this.pname) ? "_" + name : name;
		field.foreign_field_pname = name + "_id";

		fields.add(field);

	}

	/**
	 * Add a foreign key from foreign table.
	 *
	 * @param option PostgreSQL data model option
	 * @param foreign_table foreign table
	 */
	protected void addForeignKey(PgSchemaOption option, PgTable foreign_table) {

		if (schema_name.equals(foreign_table.schema_name) && xname.equals(foreign_table.xname))
			return;

		foreign_table.fields.stream().filter(foreign_field -> foreign_field.primary_key).forEach(foreign_field -> {

			PgField field = new PgField();

			field.xname = foreign_field.xname;
			field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
			field.pname = avoidFieldDuplication(option, field.xname);
			field.setHashKeyType(option);
			field.foreign_key = true;
			field.constraint_name = "FK_" + PgSchemaUtil.avoidPgReservedOps(pname) + "_" + PgSchemaUtil.avoidPgReservedOps(foreign_table.pname);
			if (field.constraint_name.length() > PgSchemaUtil.max_enum_len)
				field.constraint_name = field.constraint_name.substring(0, PgSchemaUtil.max_enum_len);
			field.constraint_name = PgSchemaUtil.avoidPgReservedOps(field.constraint_name);
			field.foreign_schema = foreign_table.schema_name;
			field.foreign_table_xname = foreign_table.xname;
			field.foreign_table_pname = foreign_table.pname;
			field.foreign_field_pname = foreign_field.pname;

			fields.add(field);

			return;
		});

	}

	/**
	 * Add a serial key.
	 *
	 * @param option PostgreSQL data model option
	 */
	protected void addSerialKey(PgSchemaOption option) {

		if (!option.rel_model_ext)
			return;

		if (fields.stream().anyMatch(field -> field.serial_key)) // already has a serial key
			return;

		PgField field = new PgField();

		field.xname = option.serial_key_name;
		field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
		field.pname = avoidFieldDuplication(option, field.xname);
		field.setSerKeyType(option);
		field.serial_key = true;

		fields.add(field);

	}

	/**
	 * Add an XPath key.
	 *
	 * @param option PostgreSQL data model option
	 */
	protected void addXPathKey(PgSchemaOption option) {

		if (fields.stream().anyMatch(field -> field.xpath_key)) // already has an xpath key
			return;

		PgField field = new PgField();

		field.xname = option.xpath_key_name;
		field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
		field.pname = avoidFieldDuplication(option, field.xname);
		field.setHashKeyType(option);
		field.xpath_key = true;

		fields.add(field);

	}

	/**
	 * Return field.
	 *
	 * @param field_name field name
	 * @return PgField PostgreSQL field
	 */
	public PgField getField(String field_name) {

		Optional<PgField> opt = fields.stream().filter(field -> field.name.equals(field_name)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Return field.
	 *
	 * @param field_xname canonical name of field
	 * @return PgField PostgreSQL field
	 */
	public PgField getCanField(String field_xname) {

		Optional<PgField> opt = fields.stream().filter(field -> field.xname.equals(field_xname)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Return field.
	 *
	 * @param field_pname field name in PostgreSQL
	 * @return PgField PostgreSQL field
	 */
	public PgField getPgField(String field_pname) {

		Optional<PgField> opt = fields.stream().filter(field -> field.pname.equals(field_pname)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Append substitution group property.
	 *
	 * @param field field
	 */
	protected void appendSubstitutionGroup(PgField field) {

		if (field == null)
			return;

		if (field.substitution_group == null || field.substitution_group.isEmpty())
			return;

		PgField rep_field = getCanField(field.substitution_group);

		if (rep_field == null)
			return;

		if (field.type == null || field.type.isEmpty())
			field.type = rep_field.type;

		if (!rep_field.rep_substitution_group) {

			if (rep_field.block_value == null || !rep_field.block_value.equals("substitution"))
				rep_field.rep_substitution_group = true;

		}

	}

	/**
	 * Remove prohibited attributes.
	 */
	protected void removeProhibitedAttrs() {

		fields.removeIf(field -> field.prohibited);

	}

	/**
	 * Remove blocked substitution group elements.
	 */
	protected void removeBlockedSubstitutionGroups() {

		fields.stream().filter(field -> field.rep_substitution_group && field.block_value != null && field.block_value.equals("substitution")).map(field -> field.xname).collect(Collectors.toList()).forEach(xname -> fields.removeIf(field -> field.substitution_group != null && field.substitution_group.equals(xname)));

	}

	/**
	 * Cancel unique key because of name collision.
	 */
	protected void cancelUniqueKey() {

		fields.stream().filter(field -> field.primary_key).forEach(field -> field.unique_key = false);

	}

	/**
	 * Determine the total number of field as nested key.
	 */
	protected void countNestedFields() {

		if ((total_nested_fields = (int) fields.stream().filter(field -> field.nested_key).count()) > 0)
			required = true;

	}

	/**
	 * Return whether node name matches.
	 *
	 * @param node_name node name
	 * @param wild_card whether wild card follows
	 * @return boolean whether node name matches
	 */
	public boolean matchesNodeName(String node_name, boolean wild_card) {

		if (node_name.startsWith(prefix + ":"))
			node_name = PgSchemaUtil.getUnqualifiedName(node_name);

		return wild_card ? xname.matches(node_name) : xname.equals(node_name) || node_name.equals("*");
	}

}
