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
import java.io.FileWriter;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.stream.Collectors;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * PostgreSQL table declaration.
 *
 * @author yokochi
 */
public class PgTable {

	/** The PostgreSQL schema name (default schema name is "public"). */
	protected String pg_schema_name = PgSchemaUtil.pg_public_schema_name;

	/** The target namespace. */
	protected String target_namespace = null;

	/** The prefix of target namespace. */
	protected String prefix = "";

	/** The schema location. */
	protected String schema_location = null;

	/** The table name in PostgreSQL. */
	protected String name = "";

	/** The xs:annotation/xs:documentation (as is). */
	protected String xanno_doc = null;

	/** The xs:annotation. */
	protected String anno = null;

	/** The table type classified by xs_root (root node), xs_root_child (children node of root node), xs_admin_root (administrative root node), xs_admin_child (children node of administrative node). */
	protected XsTableType xs_type;

	/** The field list. */
	protected List<PgField> fields = null;

	/** The depth of table. */
	protected int level = -1;

	/** the generation order in PostgreSQL DDL. */
	protected int order = 0;

	/** The number of nested field. */
	protected int nested_fields = 0;

	/** Whether content holder. */
	protected boolean content_holder = false;

	/** Whether list holder. */
	protected boolean list_holder = false;

	/** Whether bridge table. */
	protected boolean bridge = false;

	/** Whether xs_type equals xs_admin_root. */
	protected boolean virtual = false;

	/** Whether bridge table or virtual table or not content_holder. */
	protected boolean relational = false;

	/** Whether table has foreign key. */
	protected boolean has_foreign_key = false;

	/** Whether table has any element. */
	protected boolean has_any = false;

	/** Whether table has any attribute. */
	protected boolean has_any_attribute = false;

	/** Whether table has pending group. */
	protected boolean has_pending_group = false;

	/** Whether name collision occurs or not. */
	protected boolean conflict = false;

	/** Whether table is referred from child table. */
	protected boolean required = false;

	/** Whether table is realized in PostgreSQL DDL. */
	protected boolean realized = false;

	/** Whether table is subset of database. */
	protected boolean filt_out = false;

	/** The current file writer. */
	protected FileWriter filew = null;

	/** The current buffered writer. */
	protected BufferedWriter buffw = null;

	/** Whether JSON buffer of arbitrary field is not empty. */
	protected boolean jsonb_not_empty = false;

	/** The Lucene document. */
	protected org.apache.lucene.document.Document lucene_doc = null;

	/** The prepared statement. */
	protected PreparedStatement ps = null;

	/**
	 * Instance of PostgreSQL table.
	 *
	 * @param pg_schema_name PosgreSQL schema name
	 * @param target_namespace target namespace URI
	 * @param schema_location schema location
	 */
	public PgTable(String pg_schema_name, String target_namespace, String schema_location) {

		this.pg_schema_name = pg_schema_name;
		this.target_namespace = target_namespace;
		this.schema_location = schema_location;

	}

	/**
	 * Classify table type: content_holder, list_holder, bridge, hub, virtual.
	 */
	public void classify() {

		testContentHolder();
		testListHolder();
		testBridge();
		testVirtual();
		testForeignKey();
		testHasAny();
		testHasAnyAttribute();

	}

	/**
	 * Determine content holder table having one of arbitrary content field such as attribute, element, simple content.
	 */
	private void testContentHolder() {

		content_holder = fields.stream().anyMatch(arg -> !arg.document_key && !arg.primary_key && !arg.foreign_key && !arg.nested_key && !arg.serial_key && !arg.xpath_key);
		relational = bridge || virtual || !content_holder;

	}

	/**
	 * Determine list holder table having one of field whose occurrence is unbounded.
	 */
	private void testListHolder() {

		list_holder = fields.stream().filter(arg -> arg.nested_key).anyMatch(arg -> arg.list_holder);

	}

	/**
	 * Determine bridge table having primary key and a nested key.
	 */
	private void testBridge() {

		boolean has_primary_key = false;
		boolean has_nested_key = false;

		for (PgField field : fields) {

			if (field.primary_key)
				has_primary_key = true;

			else if (field.nested_key) {

				if (has_nested_key) {

					has_nested_key = false;

					break;
				}

				else
					has_nested_key = true;

			}

		}

		bridge = (has_primary_key && has_nested_key);
		relational = bridge || virtual || !content_holder;

	}

	/**
	 * Determine virtual table equals administrative table (xs_admin_root).
	 */
	private void testVirtual() {

		virtual = xs_type.equals(XsTableType.xs_admin_root);
		relational = bridge || virtual || !content_holder;

	}

	/**
	 * Determine table has foreign key constraint.
	 */
	private void testForeignKey() {

		has_foreign_key = fields.stream().anyMatch(arg -> arg.foreign_key);

	}

	/**
	 * Determine table has any element.
	 */
	private void testHasAny() {

		has_any = fields.stream().anyMatch(arg -> arg.any);

	}

	/**
	 * Determine table has any attribute.
	 */
	private void testHasAnyAttribute() {

		has_any_attribute = fields.stream().anyMatch(arg -> arg.any_attribute);

	}

	/**
	 * Suggest new name for a given field name avoiding name collision with current ones.
	 *
	 * @param option PostgreSQL data model option
	 * @param field_name candidate name of new field
	 * @return String field name without name collision
	 */
	public String avoidFieldDuplication(PgSchemaOption option, String field_name) {

		if (!option.rel_model_ext)
			return field_name;

		boolean duplicate;

		do {

			duplicate = false;

			for (PgField field : fields) {

				if (!option.rel_data_ext && (field.primary_key || field.foreign_key || field.nested_key))
					continue;

				if (field.name.equals(field_name)) {

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
	 * @param name name of primary key
	 * @param unique_key whether primary key should be unique
	 */
	public void addPrimaryKey(PgSchemaOption option, String name, boolean unique_key) {

		String xs_prefix_ = option.xs_prefix_;

		if (option.document_key) {

			PgField field = new PgField();

			field.name = field.xname = option.document_key_name;
			field.type = xs_prefix_ + "string";
			field.xs_type = XsDataType.xs_string;
			field.document_key = true;

			fields.add(field);

		}

		if (!option.rel_model_ext)
			return;

		PgField field = new PgField();

		field.name = field.xname = name + "_id";
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
	 * @param name name of nested key:0
	 * @param ref_field reference field
	 * @param node current node
	 * @return boolean whether success or not
	 */
	public boolean addNestedKey(PgSchemaOption option, String schema_name, String name, PgField ref_field, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		if (name == null || name.isEmpty())
			return false;

		name = option.getUnqualifiedName(name);

		PgField field = new PgField();

		field.name = field.xname = name + "_id";
		field.setHashKeyType(option);
		field.xtype = ref_field.xtype;
		field.nested_key = true;
		field.constraint_name = "FK_" + name;
		if (field.constraint_name.length() > PgSchemaUtil.max_enum_len)
			field.constraint_name = field.constraint_name.substring(0, PgSchemaUtil.max_enum_len);
		field.constraint_name = PgSchemaUtil.avoidPgReservedOps(field.constraint_name);
		field.foreign_schema = schema_name;
		field.foreign_table_name = name;
		field.foreign_field_name = field.name;

		field.maxoccurs = ref_field.maxoccurs;
		field.minoccurs = ref_field.minoccurs;
		field.list_holder = ref_field.list_holder;

		Node parent_node = node.getParentNode();

		while (parent_node != null) {

			NamedNodeMap parent_attrs = parent_node.getAttributes();

			if (parent_node.getNodeName().equals(xs_prefix_ + "element"))
				break;

			if (parent_attrs != null) {

				for (int i = 0; i < parent_attrs.getLength(); i++) {

					Node parent_attr = parent_attrs.item(i);

					if (parent_attr != null) {

						String node_name = parent_attr.getNodeName();

						if (node_name.equals("name")) {

							field.parent_node = parent_attr.getNodeValue();

							break;
						}

					}

				}

				if (field.parent_node != null && !field.parent_node.isEmpty())
					break;

			}

			parent_node = parent_node.getParentNode();

		}

		if (field.parent_node != null && field.parent_node.isEmpty())
			field.parent_node = null;

		if (this.pg_schema_name.equals(field.foreign_schema) && this.name.equals(field.foreign_table_name))
			return false;

		fields.add(field);

		return field.maxoccurs.equals("1");
	}

	/**
	 * Add a nested key from foreign table.
	 *
	 * @param option PostgreSQL data model option
	 * @param schema_name PostgreSQL schema name
	 * @param name name of foreign table
	 */
	public void addNestedKey(PgSchemaOption option, String schema_name, String name) {

		if (!option.rel_model_ext)
			return;

		if (name == null || name.isEmpty())
			return;

		PgField field = new PgField();

		field.name = field.xname = name + "_id";
		field.setHashKeyType(option);
		field.xtype = name;
		field.nested_key = true;
		field.constraint_name = "FK_" + name;
		if (field.constraint_name.length() > PgSchemaUtil.max_enum_len)
			field.constraint_name = field.constraint_name.substring(0, PgSchemaUtil.max_enum_len);
		field.constraint_name = PgSchemaUtil.avoidPgReservedOps(field.constraint_name);
		field.foreign_schema = schema_name;
		field.foreign_table_name = name;
		field.foreign_field_name = name + "_id";

		fields.add(field);

	}

	/**
	 * Add a foreign key from foreign table.
	 *
	 * @param option PostgreSQL data model option
	 * @param foreign_table foreign table
	 */
	public void addForeignKey(PgSchemaOption option, PgTable foreign_table) {

		if (name.equals(foreign_table.name))
			return;

		foreign_table.fields.stream().filter(arg -> arg.primary_key).forEach(arg -> {

			PgField field = new PgField();

			field.name = field.xname = arg.name;
			field.setHashKeyType(option);
			field.foreign_key = true;
			field.constraint_name = "FK_" + name + "_" + foreign_table.name;
			if (field.constraint_name.length() > PgSchemaUtil.max_enum_len)
				field.constraint_name = field.constraint_name.substring(0, PgSchemaUtil.max_enum_len);
			field.constraint_name = PgSchemaUtil.avoidPgReservedOps(field.constraint_name);
			field.foreign_schema = foreign_table.pg_schema_name;
			field.foreign_table_name = foreign_table.name;
			field.foreign_field_name = arg.name;

			fields.add(field);

			return;
		});

	}

	/**
	 * Add a serial key.
	 *
	 * @param option PostgreSQL data model option
	 */
	public void addSerialKey(PgSchemaOption option) {

		if (!option.rel_model_ext)
			return;

		if (fields.stream().anyMatch(arg -> arg.serial_key)) // already has a serial key
			return;

		PgField field = new PgField();

		field.name = field.xname = avoidFieldDuplication(option, option.serial_key_name);
		field.setSerKeyType(option);
		field.serial_key = true;

		fields.add(field);

	}

	/**
	 * Add an XPath key.
	 *
	 * @param option PostgreSQL data model option
	 */
	public void addXPathKey(PgSchemaOption option) {

		if (fields.stream().anyMatch(arg -> arg.xpath_key)) // already has an xpath key
			return;

		PgField field = new PgField();

		field.name = field.xname = avoidFieldDuplication(option, option.xpath_key_name);
		field.setHashKeyType(option);
		field.xpath_key = true;

		fields.add(field);

	}

	/**
	 * Return target namespace.
	 *
	 * @return String target namespace
	 */
	public String getTargetNamespace() {
		return target_namespace;
	}

	/**
	 * Return table name.
	 *
	 * @return String table name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Return PostgreSQL field.
	 *
	 * @param field_name name of field
	 * @return PgField PostgreSQL field
	 */
	public PgField getField(String field_name) {

		int f = getFieldId(field_name);

		return f < 0 ? null : fields.get(f);
	}

	/**
	 * Return field id.
	 *
	 * @param field_name name of field
	 * @return int the field id, -1 represents not found
	 */
	public int getFieldId(String field_name) {

		for (int f = 0; f < fields.size(); f++) {

			if (fields.get(f).name.equals(field_name))
				return f;

		}

		return -1;
	}

	/**
	 * Append substitution group property.
	 *
	 * @param field_name name of field
	 */
	public void appendSubstitutionGroup(String field_name) {

		PgField field = getField(field_name);

		if (field == null)
			return;

		if (field.substitution_group == null || field.substitution_group.isEmpty())
			return;

		PgField rep_field = getField(field.substitution_group);

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
	public void removeProhibitedAttrs() {

		fields.removeIf(arg -> arg.prohibited);

	}

	/**
	 * Remove blocked substitution group elements.
	 */
	public void removeBlockedSubstitutionGroups() {

		fields.stream().filter(arg -> arg.rep_substitution_group && arg.block_value != null && arg.block_value.equals("substitution")).map(arg -> arg.name).collect(Collectors.toList()).forEach(name -> fields.removeIf(arg -> arg.substitution_group != null && arg.substitution_group.equals(name)));

	}

	/**
	 * Cancel unique key because of name collision.
	 */
	public void cancelUniqueKey() {

		fields.stream().filter(arg -> arg.primary_key).forEach(arg -> arg.unique_key = false);

	}

	/**
	 * Determine the total number of nested keys.
	 */
	public void countNestedFields() {

		if ((nested_fields = (int) fields.stream().filter(arg -> arg.nested_key).count()) > 0)
			required = true;

	}

	/**
	 * Return whether node name matches.
	 *
	 * @param node_name node name
	 * @param wild_card whether wild card follows or not
	 * @return boolean whether node name matches
	 */
	public boolean matchesNodeName(String node_name, boolean wild_card) {

		if (wild_card)
			return name.matches(node_name);

		return node_name.equals("*") || name.equals(node_name);
	}

}
