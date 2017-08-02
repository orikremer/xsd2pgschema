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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.apache.commons.text.StringEscapeUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathParser.AbbreviatedStepContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.AxisSpecifierContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.NCNameContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.NameTestContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.NodeTestContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.PredicateContext;

/**
 * PostgreSQL schema constructor.
 *
 * @author yokochi
 */
public class PgSchema {

	/** The root node (internal use only). */
	private Node root_node = null;

	/** The parent of default schema location. */
	private String def_schema_parent = null;

	/** The default schema location. */
	private String def_schema_location = null;

	/** The included or imported schema locations. */
	private HashSet<String> schema_locations = null;

	/** The attribute group definitions. */
	private List<PgTable> attr_groups = null;

	/** The model group definitions. */
	private List<PgTable> model_groups = null;

	/** The PostgreSQL tables. */
	private List<PgTable> tables = null;

	/** The PostgreSQL foreign keys. */
	private List<PgForeignKey> foreign_keys = null;

	/** The PostgreSQL schema option. */
	PgSchemaOption option = null;

	/** Whether name collision occurs or not. */
	private boolean conflicted = false;

	/** The current depth of table (internal use only). */
	private int level;

	/** The root table id (internal use only). */
	private int root_table_id = -1;

	/** The PostgreSQL root table. */
	private PgTable root_table = null;

	/** The minimum word length for index. */
	int min_word_len = PgSchemaUtil.min_word_len;

	/** Whether numeric index are stored in Lucene index. */
	boolean numeric_index = false;

	/** The prefix of xs_namespace_uri. */
	String xs_prefix = null;

	/** The xs_prefix.isEmpty() ? "" : xs_prefix + ":". */
	String xs_prefix_ = null;

	/** The default namespace (key=prefix, value=namespace_uri). */
	private HashMap<String, String> def_namespaces = null;

	/** The top level xs:annotation. */
	private String def_anno = null;

	/** The top level xs:annotation/xs:appinfo. */
	private String def_appinfo = null;

	/** The top level xs:annotation/xs:documentation. */
	private String def_doc = null;

	/** The default attributes. */
	private String def_attrs = null;

	/** The statistics message on schema. */
	private StringBuilder def_stat_msg = null;

	/** Whether used for PostgreSQL DDL realization (internal use only). */
	private boolean[] realized = null;

	/** The generation or destruction order of table (internal use only). */
	private int[] table_order = null;

	/** The table lock object. */
	private Object[] table_lock = null;

	/** The content of document key. */
	private String document_id;

	/** The instance of message digest. */
	private MessageDigest message_digest = null;

	/**
	 * PostgreSQL schema constructor.
	 *
	 * @param doc_builder Document builder used for xs:include or xs:import
	 * @param doc XML Schema document
	 * @param root_schema root schema object (should be null at first)
	 * @param def_schema_location default schema location
	 * @param option PostgreSQL schema option
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchema(DocumentBuilder doc_builder, Document doc, PgSchema root_schema, String def_schema_location, PgSchemaOption option) throws NoSuchAlgorithmException, PgSchemaException {

		this.option = option;

		// check existence of root element

		root_node = doc.getDocumentElement();

		if (root_node == null)
			throw new PgSchemaException("Not found root element in XML Schema: " + def_schema_location);

		NodeList node_list = doc.getElementsByTagNameNS(PgSchemaUtil.xs_namespace_uri, "*");

		if (node_list == null)
			throw new PgSchemaException("No namespace declaration stands for " + PgSchemaUtil.xs_namespace_uri + " in XML Schema: " + def_schema_location);

		Node xs_namespace_uri_node = node_list.item(0);

		xs_prefix = xs_namespace_uri_node != null ? xs_namespace_uri_node.getPrefix() : null;

		if (xs_prefix == null || xs_prefix.isEmpty())
			xs_prefix_ = xs_prefix = "";
		else
			xs_prefix_ = xs_prefix + ":";

		// check root element name

		if (!root_node.getNodeName().equals(xs_prefix_ + "schema"))
			throw new PgSchemaException("Not found " + xs_prefix_ + "schema root element in XML Schema: " + def_schema_location);

		// retrieve top level schema annotation

		def_anno = PgSchemaUtil.extractAnnotation(this, root_node, true);
		def_appinfo = PgSchemaUtil.extractAppinfo(this, root_node);
		def_doc = PgSchemaUtil.extractDocumentation(this, root_node);

		def_stat_msg = new StringBuilder();

		// default namespace and default attributes

		def_namespaces = new HashMap<String, String>();

		NamedNodeMap root_attrs = root_node.getAttributes();

		for (int i = 0; i < root_attrs.getLength(); i++) {

			Node root_attr = root_attrs.item(i);

			if (root_attr != null) {

				String node_name = root_attr.getNodeName();

				if (node_name.equals("targetNamespace"))
					def_namespaces.putIfAbsent("", root_attr.getNodeValue().split(" ")[0]);

				else if (node_name.startsWith("xmlns"))
					def_namespaces.putIfAbsent(node_name.replaceFirst("^xmlns:?", ""), root_attr.getNodeValue().split(" ")[0]);

				else if (node_name.equals("defaultAttributes"))
					def_attrs = root_attr.getNodeValue();

			}

		}

		PgSchema _root_schema = root_schema == null ? this : root_schema;

		def_schema_parent = root_schema == null ? PgSchemaUtil.getParent(def_schema_location) : root_schema.def_schema_parent;

		this.def_schema_location = def_schema_location;

		schema_locations = new HashSet<String>();

		schema_locations.add(PgSchemaUtil.getFileName(def_schema_location));

		attr_groups = new ArrayList<PgTable>();

		model_groups = new ArrayList<PgTable>();

		tables = new ArrayList<PgTable>();

		foreign_keys = new ArrayList<PgForeignKey>();

		// include or import namespace

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeName().equals(xs_prefix_ + "include") || child.getNodeName().equals(xs_prefix_ + "import")) {

				Element e = (Element) child;

				String schema_location = e.getAttribute("schemaLocation");
				String schema_file_name = PgSchemaUtil.getFileName(schema_location);

				if (schema_location != null && !schema_location.isEmpty()) {

					// prevent infinite cyclic reference which is allowed in XML Schema 1.1

					if (_root_schema.schema_locations.contains(schema_file_name))
						continue;

					_root_schema.schema_locations.add(schema_file_name);

					PgSchemaUtil.copyFileIfNotExist(schema_location, def_schema_parent);

					// local XML Schema file

					InputStream is2 = PgSchemaUtil.getInputStream(schema_location, def_schema_parent);

					if (is2 == null)
						throw new PgSchemaException("Could not access to schema location: " + schema_location);

					try {

						Document doc2 = doc_builder.parse(is2);

						is2.close();

						// referred XML Schema (xs:include|xs:import/@schemaLocation) analysis

						PgSchema schema2 = new PgSchema(doc_builder, doc2, _root_schema, PgSchemaUtil.getName(schema_location), option);

						// add schema location to prevent infinite cyclic reference

						schema2.schema_locations.forEach(arg -> _root_schema.schema_locations.add(arg));

						// copy default namespace from referred XML Schema

						if (!schema2.def_namespaces.isEmpty())
							schema2.def_namespaces.entrySet().forEach(arg -> _root_schema.def_namespaces.putIfAbsent(arg.getKey(), arg.getValue()));

						if ((schema2.tables == null || schema2.tables.size() == 0) && (schema2.attr_groups == null || schema2.attr_groups.size() == 0) && (schema2.model_groups == null || schema2.model_groups.size() == 0)) {

							_root_schema.def_stat_msg.append("--  Not found any root element (/" + xs_prefix_ + "schema/" + xs_prefix_ + "element) or administrative elements (/" + xs_prefix_ + "schema/[" + xs_prefix_ + "complexType | " + xs_prefix_ + "simpleType | " + xs_prefix_ + "attributeGroup | " + xs_prefix_ + "group]) in XML Schema: " + schema_location + "\n");

							continue;
						}

						// copy attribute group definitions from referred XML Schema

						schema2.attr_groups.forEach(arg -> {

							if (_root_schema.avoidTableDuplication(attr_groups, arg))
								_root_schema.attr_groups.add(arg);

						});

						// copy model group definitions from referred XML Schema

						schema2.model_groups.forEach(arg -> {

							if (_root_schema.avoidTableDuplication(model_groups, arg))
								_root_schema.model_groups.add(arg);

						});

						// copy administrative tables from referred XML Schema

						schema2.tables.stream().filter(arg -> arg.xs_type.equals(XsTableType.xs_admin_root) || arg.xs_type.equals(XsTableType.xs_admin_child)).forEach(arg -> {

							if (_root_schema.avoidTableDuplication(tables, arg))
								_root_schema.tables.add(arg);

						});

					} catch (SAXException | IOException e2) {
						throw new PgSchemaException(e2);
					}

				}

			}

		}

		// extract attribute group elements

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeName().equals(xs_prefix_ + "attributeGroup"))
				extractAdminAttributeGroup(child);

		}

		// extract model group elements

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeName().equals(xs_prefix_ + "group"))
				extractAdminModelGroup(child);

		}

		// create table for root element

		boolean root_element = root_schema == null;

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeName().equals(xs_prefix_ + "element")) {

				Element e = (Element) child;

				String _abstract = e.getAttribute("abstract");

				if (_abstract != null && _abstract.equals("true"))
					continue;

				extractRootElement(child, root_element);

				if (root_schema == null)
					break;

				root_element = false;

			}

		}

		// create table for administrative elements

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeName().equals(xs_prefix_ + "complexType"))
				extractAdminElement(child, true, false);

			else if (child.getNodeName().equals(xs_prefix_ + "simpleType"))
				extractAdminElement(child, false, false);

		}

		if (tables.size() == 0) {

			if (root_schema == null)
				throw new PgSchemaException("Not found any root element (/" + xs_prefix_ + "schema/" + xs_prefix_ + "element) or administrative elements (/" + xs_prefix_ + "schema/[" + xs_prefix_ + "complexType | " + xs_prefix_ + "simpleType]) in XML Schema: " + def_schema_location);

		}

		else
			_root_schema.def_stat_msg.append("--  " + (root_schema == null ? "Generated" : "Found") + " " + tables.size() + " tables (" + tables.stream().map(table -> table.fields.size()).reduce((arg0, arg1) -> arg0 + arg1).get() + " fields), " + attr_groups.size() + " attr groups, " + model_groups.size() + " model groups " + (root_schema == null ? "in total" : "in XML Schema: " + def_schema_location) + "\n");

		// statistics on this schema

		if (root_schema == null) {

			StringBuilder sb = new StringBuilder();

			HashSet<String> namespace_uri = new HashSet<String>();

			def_namespaces.entrySet().stream().map(arg -> arg.getValue()).forEach(arg -> namespace_uri.add(arg));

			namespace_uri.forEach(arg -> sb.append(arg + ", "));
			namespace_uri.clear();

			_root_schema.def_stat_msg.append("--   Namespaces:\n");
			_root_schema.def_stat_msg.append("--    " + sb.substring(0, sb.length() - 2) + "\n");

			sb.setLength(0);

			schema_locations.forEach(arg -> sb.append(arg + ", "));

			_root_schema.def_stat_msg.append("--   Schema locations:\n");
			_root_schema.def_stat_msg.append("--    " + sb.substring(0, sb.length() - 2) + "\n");

			sb.setLength(0);

			_root_schema.def_stat_msg.append("--   Table types:\n");
			_root_schema.def_stat_msg.append("--    " + tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_root)).count() + " root, ");
			_root_schema.def_stat_msg.append(tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_root_child)).count() + " root children, ");
			_root_schema.def_stat_msg.append(tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_root)).count() + " admin roots, ");
			_root_schema.def_stat_msg.append(tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_child)).count() + " admin children\n");
			_root_schema.def_stat_msg.append("--   System keys:\n");
			_root_schema.def_stat_msg.append("--    " + tables.stream().map(table -> table.fields.stream().filter(field -> field.primary_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " primary keys (" + tables.stream().map(table -> table.fields.stream().filter(field -> field.unique_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " unique constraints), ");
			_root_schema.def_stat_msg.append(tables.stream().map(table -> table.fields.stream().filter(field -> field.foreign_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " foreign keys (" + foreign_keys.size() + " key references), ");
			_root_schema.def_stat_msg.append(tables.stream().map(table -> table.fields.stream().filter(field -> field.nested_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " nested keys\n");
			_root_schema.def_stat_msg.append("--   User keys:\n");
			_root_schema.def_stat_msg.append("--    " + tables.stream().map(table -> table.fields.stream().filter(field -> field.document_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " document keys, ");
			_root_schema.def_stat_msg.append(tables.stream().map(table -> table.fields.stream().filter(field -> field.serial_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " serial keys, ");
			_root_schema.def_stat_msg.append(tables.stream().map(table -> table.fields.stream().filter(field -> field.xpath_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " xpath keys\n");
			_root_schema.def_stat_msg.append("--   Contents:\n");
			_root_schema.def_stat_msg.append("--    " + tables.stream().map(table -> table.fields.stream().filter(field -> field.attribute).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " attributes, ");
			_root_schema.def_stat_msg.append(tables.stream().map(table -> table.fields.stream().filter(field -> field.element).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " elements, ");
			_root_schema.def_stat_msg.append(tables.stream().map(table -> table.fields.stream().filter(field -> field.simple_cont).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " simple contents\n");
			_root_schema.def_stat_msg.append("--   Wild cards:\n");
			_root_schema.def_stat_msg.append("--    " + tables.stream().map(table -> table.fields.stream().filter(field -> field.any).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " any elements, ");
			_root_schema.def_stat_msg.append(tables.stream().map(table -> table.fields.stream().filter(field -> field.any_attribute).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " any attributes\n");

		}

		// append annotation of root table if possible

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeName().equals(xs_prefix_ + "element")) {

				Element e = (Element) child;

				String _abstract = e.getAttribute("abstract");

				if (_abstract != null && _abstract.equals("true"))
					continue;

				extractAdminElement(child, false, true);

				if (root_schema == null)
					break;

			}

		}

		// append annotation of administrative tables if possible

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeName().equals(xs_prefix_ + "complexType"))
				extractAdminElement(child, true, true);

			else if (child.getNodeName().equals(xs_prefix_ + "simpleType"))
				extractAdminElement(child, false, true);

		}

		if (root_schema != null)
			return;

		// whether name collision occurs or not

		conflicted = tables.stream().anyMatch(table -> table.conflict);

		// classify type of table

		tables.forEach(table -> {

			table.classify();

			// set foreign_table_id as table pointer otherwise remove orphaned nested key

			if (table.required) {

				Iterator<PgField> iterator = table.fields.iterator();

				while (iterator.hasNext()) {

					PgField field = iterator.next();

					if (field.nested_key) {

						int t;

						if ((t = getTableId(field.foreign_table)) >= 0) {

							field.foreign_table_id = t;
							tables.get(t).required = true;

						}

						else
							iterator.remove();

					}

				}

			}

		});

		// avoid virtual duplication of nested key

		tables.forEach(table -> {

			Iterator<PgField> iterator = table.fields.iterator();

			while (iterator.hasNext()) {

				PgField field = iterator.next();

				if (!field.nested_key)
					continue;

				PgTable nested_table = tables.get(field.foreign_table_id);

				if (nested_table.virtual)
					continue;

				boolean changed = false;

				Iterator<PgField> _iterator = table.fields.iterator();

				while (_iterator.hasNext()) {

					PgField _field = _iterator.next();

					if (!_field.nested_key)
						continue;

					if (_field.equals(field))
						continue;

					PgTable _nested_table = tables.get(_field.foreign_table_id);

					if (!_nested_table.virtual)
						continue;

					for (PgField __field : _nested_table.fields) {

						if (!__field.nested_key)
							continue;

						if (field.foreign_key == __field.foreign_key) {

							changed = true;

							iterator.remove();

							break;
						}

					}

					if (changed)
						break;

				}

			}

		});

		// cancel unique key constraint if parent table is list holder

		tables.stream().filter(table -> table.list_holder).forEach(table -> table.fields.stream().filter(field -> field.nested_key).forEach(field -> tables.get(field.foreign_table_id).cancelUniqueKey()));

		// decide parent node name

		tables.stream().filter(table -> table.nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && field.parent_node != null).forEach(field -> {

			if (table.conflict) // tolerate parent node name check because of name collision
				field.parent_node = null;

			else {

				String[] parent_nodes = field.parent_node.split(" ");

				field.parent_node = null;

				boolean infinite_loop = false;

				for (String parent_node : parent_nodes) {

					PgTable parent_table = getTable(parent_node);

					if (parent_table == null)
						continue;

					boolean has_content = false;
					boolean has_foreign_key = false;

					do {

						PgTable _parent_table = parent_table;

						for (PgField parent_field : _parent_table.fields) {

							if (parent_field.foreign_key) {

								has_foreign_key = true;

								if (field.parent_node == null)
									field.parent_node = parent_field.foreign_table;
								else
									field.parent_node += " " + parent_field.foreign_table;

							}

						}

						if (!has_foreign_key && _parent_table.cont_holder)
							has_content = true;

						for (PgTable ancestor_table : tables) { // ancestor table

							if (ancestor_table.nested_fields == 0)
								continue;

							for (PgField ancestor_field : ancestor_table.fields) {

								if (!ancestor_field.nested_key)
									continue;

								if (ancestor_field.foreign_table.equals(_parent_table.name)) {

									parent_table = ancestor_table;

									break;
								}

							}

						}

						if (parent_table.equals(_parent_table)) { // escape from infinite loop

							field.parent_node = null;

							infinite_loop = true;

							break;
						}

					} while (!has_content && !has_foreign_key && !infinite_loop);

				}

			}

		}));

		// update requirement flag because of foreign keys

		foreign_keys.forEach(foreign_key -> {

			PgTable table;

			table = getTable(foreign_key.child_table);

			if (table != null)
				table.required = true;

			table = getTable(foreign_key.parent_table);

			if (table != null)
				table.required = true;

		});

		// add serial key if parent table is list holder

		if (option.serial_key)
			tables.stream().filter(table -> table.list_holder).forEach(table -> table.fields.stream().filter(field -> field.nested_key).forEach(field -> tables.get(field.foreign_table_id).addSerialKey(this)));

		// add XPath key

		if (option.xpath_key)
			tables.forEach(table -> table.addXPathKey(this));

		// update system key flag and user key flag

		tables.forEach(table -> {

			table.setSystemKey();
			table.setUserKey();

		});

		// instance of message digest

		if (!option.hash_algorithm.isEmpty() && !option.hash_size.equals(PgHashSize.debug_string))
			message_digest = MessageDigest.getInstance(option.hash_algorithm);

		// realize PostgreSQL DDL

		realize();

	}

	/**
	 * Extract root element of XML Schema.
	 *
	 * @param node current node
	 * @param root_element whether it is root element or not
	 */
	private void extractRootElement(Node node, boolean root_element) {

		PgTable table = new PgTable(def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String name = e.getAttribute("name");

		table.name = getUnqualifiedName(name);

		if (table.name.isEmpty())
			return;

		table.required = true;

		table.anno = PgSchemaUtil.extractAnnotation(this, node, true);

		table.xs_type = root_element ? XsTableType.xs_root : XsTableType.xs_admin_root;

		table.fields = new ArrayList<PgField>();

		table.level = level = 0;

		table.addPrimaryKey(this, table.name, true);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			String child_name = child.getNodeName();

			if (child_name.equals(xs_prefix_ + "complexType") || child_name.equals(xs_prefix_ + "simpleType"))
				extractField(child, table);

			else if (child_name.equals(xs_prefix_ + "keyref"))
				extractForeignKeyRef(child, node);

		}

		table.removeProhibitedAttrs();
		table.removeBlockedSubstitutionGroups();
		table.countNestedFields();

		if (table.fields.size() < option.getMinimumSizeOfField())
			return;

		tables.add(table);

		if (root_element) {

			addRootOrphanItem(node, table);

			for (int t = 0; t < tables.size(); t++) {

				root_table = tables.get(t);

				if (root_table.xs_type.equals(XsTableType.xs_root)) {
					root_table_id = t;
					break;
				}

			}

			if (root_table_id < 0)
				root_table = null;

		}

	}

	/**
	 * Add orphan item of root element.
	 *
	 * @param node current node
	 * @param table root table
	 */
	private void addRootOrphanItem(Node node, PgTable table) {

		PgField dummy = new PgField();

		Element e = (Element) node;

		String name = e.getAttribute("name");

		if (name != null && !name.isEmpty()) {

			dummy.extractType(this, node);

			if (dummy.type == null || dummy.type.isEmpty()) {

				level++;

				table.required = true;

				addChildItem(node, table);

				level--;

			}

			else {

				String[] type = dummy.type.contains(" ") ? dummy.type.split(" ")[0].split(":") : dummy.type.split(":");

				// primitive data type

				if (type.length != 0 && type[0].equals(xs_prefix)) {

				}

				// non-primitive data type

				else {

					boolean unique_key = table.addNestedKey(this, e.getAttribute("name"), node);

					level++;

					PgTable child_table = new PgTable(def_namespaces.get(""), def_schema_location);

					Element child_e = (Element) node;

					String child_name = child_e.getAttribute("name");

					child_table.name = getUnqualifiedName(child_name);

					table.required = child_table.required = true;

					child_table.anno = PgSchemaUtil.extractAnnotation(this, node, true);

					child_table.xs_type = XsTableType.xs_admin_root;

					child_table.fields = new ArrayList<PgField>();

					child_table.level = level;

					child_table.addPrimaryKey(this, child_table.name, unique_key);
					if (!child_table.addNestedKey(this, dummy.type, node))
						child_table.cancelUniqueKey();

					child_table.removeProhibitedAttrs();
					child_table.removeBlockedSubstitutionGroups();
					child_table.countNestedFields();

					if (child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
						tables.add(child_table);

					addChildItem(node, child_table);

					level--;

				}

			}

		}

	}

	/**
	 * Extract administrative element of XML Schema.
	 *
	 * @param node current node
	 * @param complex_type whether it is complexType or not (simpleType)
	 * @param annotation whether corrects annotation only
	 */
	private void extractAdminElement(Node node, boolean complex_type, boolean annotation) {

		PgTable table = new PgTable(def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String name = e.getAttribute("name");

		table.name = getUnqualifiedName(name);

		if (table.name.isEmpty())
			return;

		table.anno = PgSchemaUtil.extractAnnotation(this, node, true);

		if (annotation) {

			if (table.anno != null && !table.anno.isEmpty()) {

				PgTable known_table = getTable(table.name);

				if (known_table != null) {

					if ((known_table.anno == null || known_table.anno.isEmpty()) && table.anno != null && !table.anno.isEmpty())
						known_table.anno = table.anno;

				}

			}

			return;
		}

		table.xs_type = XsTableType.xs_admin_root;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		table.addPrimaryKey(this, table.name, true);

		if (complex_type) {

			extractAttributeGroup(node, table); // default attribute group

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
				extractField(child, table);

		} else
			extractSimpleContent(node, table);

		table.removeProhibitedAttrs();
		table.removeBlockedSubstitutionGroups();
		table.countNestedFields();

		if (table.fields.size() < option.getMinimumSizeOfField())
			return;

		if (avoidTableDuplication(tables, table))
			tables.add(table);

	}

	/**
	 * Extract administrative attribute group of XML Schema.
	 *
	 * @param node current node
	 */
	private void extractAdminAttributeGroup(Node node) {

		PgTable table = new PgTable(def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String name = e.getAttribute("name");

		table.name = getUnqualifiedName(name);

		if (table.name.isEmpty())
			return;

		table.anno = PgSchemaUtil.extractAnnotation(this, node, true);

		table.xs_type = XsTableType.xs_attr_group;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table);

		table.fields.removeIf(field -> !field.attribute);

		if (table.fields.size() == 0)
			return;

		if (avoidTableDuplication(attr_groups, table))
			attr_groups.add(table);

	}

	/**
	 * Extract administrative model group of XML Schema.
	 *
	 * @param node current node
	 */
	private void extractAdminModelGroup(Node node) {

		PgTable table = new PgTable(def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String name = e.getAttribute("name");

		table.name = getUnqualifiedName(name);

		if (table.name.isEmpty())
			return;

		table.anno = PgSchemaUtil.extractAnnotation(this, node, true);

		table.xs_type = XsTableType.xs_model_group;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table);

		if (table.fields.size() == 0)
			return;

		if (avoidTableDuplication(model_groups, table))
			model_groups.add(table);

	}

	/**
	 * Extract field of table.
	 *
	 * @param node current node
	 * @param table the table
	 * @table new table
	 */
	private void extractField(Node node, PgTable table) {

		String node_name = node.getNodeName();

		if (node_name.equals(xs_prefix_ + "any")) {
			extractAny(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "anyAttribute")) {
			extractAnyAttribute(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "attribute")) {
			extractAttribute(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "attributeGroup")) {
			extractAttributeGroup(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "element")) {
			extractElement(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "group")) {
			extractModelGroup(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "simpleContent")) {
			extractSimpleContent(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "complexContent")) {
			extractComplexContent(node, table);
			return;
		}

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			String child_name = child.getNodeName();

			if (child_name.equals(xs_prefix_ + "annotation"))
				continue;

			else if (child_name.equals(xs_prefix_ + "any"))
				extractAny(child, table);

			else if (child_name.equals(xs_prefix_ + "anyAttribute"))
				extractAnyAttribute(child, table);

			else if (child_name.equals(xs_prefix_ + "attribute"))
				extractAttribute(child, table);

			else if (child_name.equals(xs_prefix_ + "attributeGroup"))
				extractAttributeGroup(child, table);

			else if (child_name.equals(xs_prefix_ + "element"))
				extractElement(child, table);

			else if (child_name.equals(xs_prefix_ + "group"))
				extractModelGroup(child, table);

			else if (child_name.equals(xs_prefix_ + "simpleContent"))
				extractSimpleContent(child, table);

			else if (child_name.equals(xs_prefix_ + "complexContent"))
				extractComplexContent(child, table);

			else
				extractField(child, table);

		}

	}

	/**
	 * Extract foreign key under xs:keyref.
	 *
	 * @param node current node
	 * @param parent_node parent node
	 */
	private void extractForeignKeyRef(Node node, Node parent_node) {

		Element e = (Element) node;

		String name = e.getAttribute("name");
		String refer = e.getAttribute("refer");

		if (name == null || name.isEmpty() || refer == null || refer.isEmpty() || !refer.contains(":"))
			return;

		String key_name = getUnqualifiedName(refer);

		PgForeignKey foreign_key = new PgForeignKey();

		foreign_key.name = name;

		foreign_key.extractChildTable(this, node);

		if (foreign_key.child_table == null || foreign_key.child_table.isEmpty())
			return;

		foreign_key.extractParentTable(this, parent_node, key_name);

		if (foreign_key.parent_table == null || foreign_key.parent_table.isEmpty())
			return;

		foreign_key.extractChildFields(this, node);

		if (foreign_key.child_fields == null || foreign_key.child_fields.isEmpty())
			return;

		foreign_key.extractParentFields(this, parent_node, key_name);

		if (foreign_key.parent_fields == null || foreign_key.parent_fields.isEmpty())
			return;

		foreign_keys.add(foreign_key);

	}

	/**
	 * Extract any.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractAny(Node node, PgTable table) {

		PgField field = new PgField();

		field.any = true;

		field.extractMaxOccurs(this, node);
		field.extractMinOccurs(this, node);

		field.name = field.xname = table.avoidFieldDuplication(this, PgSchemaUtil.any_elem_name);
		field.anno = PgSchemaUtil.extractAnnotation(this, node, false);

		field.xs_type = XsDataType.xs_any;
		field.type = field.xs_type.name();

		field.extractNamespace(this, node); // require type definition

		if (option.wild_card)
			table.fields.add(field);

	}

	/**
	 * Extract any attribute.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractAnyAttribute(Node node, PgTable table) {

		PgField field = new PgField();

		field.any_attribute = true;

		field.name = field.xname = table.avoidFieldDuplication(this, PgSchemaUtil.any_attr_name);
		field.anno = PgSchemaUtil.extractAnnotation(this, node, false);

		field.xs_type = XsDataType.xs_anyAttribute;
		field.type = field.xs_type.name();

		field.extractNamespace(this, node); // require type definition

		if (option.wild_card)
			table.fields.add(field);

	}

	/**
	 * Extract attribute.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractAttribute(Node node, PgTable table) {

		extractInfoItem(node, table, true);

	}

	/**
	 * Extract element.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractElement(Node node, PgTable table) {

		extractInfoItem(node, table, false);

	}

	/**
	 * Concrete extractor for both attribute and element.
	 *
	 * @param node current node
	 * @param table new table
	 * @param attribute whether it is attribute or not (element)
	 */
	private void extractInfoItem(Node node, PgTable table, boolean attribute) {

		PgField field = new PgField();

		Element e = (Element) node;

		String name = e.getAttribute("name");
		String ref = e.getAttribute("ref");

		if (attribute)
			field.attribute = true;

		else {

			field.element = true;

			field.extractMaxOccurs(this, node);
			field.extractMinOccurs(this, node);

		}

		if (name != null && !name.isEmpty()) {

			field.xname = getUnqualifiedName(name);
			field.name = table.avoidFieldDuplication(this, field.xname);
			field.anno = PgSchemaUtil.extractAnnotation(this, node, false);

			field.extractType(this, node);
			field.extractNamespace(this, node); // require type definition
			field.extractRequired(node);
			field.extractFixedValue(node);
			field.extractDefaultValue(node);
			field.extractBlockValue(node);
			field.extractEnumeration(this, node);
			field.extractRestriction(this, node);

			if (field.substitution_group != null && !field.substitution_group.isEmpty())
				table.appendSubstitutionGroup(field.name);

			if (field.enumeration != null && field.enumeration.length > 0) {

				field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.name) + "_" + PgSchemaUtil.avoidPgReservedOps(field.name);

				if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
					field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

			}

			if (field.type == null || field.type.isEmpty()) {

				if (!table.addNestedKey(this, e.getAttribute("name"), node))
					table.cancelUniqueKey();

				level++;

				table.required = true;

				addChildItem(node, table);

				level--;

			}

			else {

				String[] type = field.type.contains(" ") ? field.type.split(" ")[0].split(":") : field.type.split(":");

				// primitive data type

				if (type.length != 0 && type[0].equals(xs_prefix)) {

					field.xs_type = XsDataType.valueOf("xs_" + type[1]);

					table.fields.add(field);

				}

				// non-primitive data type

				else {

					boolean unique_key = table.addNestedKey(this, e.getAttribute("name"), node);

					if (!unique_key)
						table.cancelUniqueKey();

					level++;

					PgTable child_table = new PgTable(def_namespaces.get(""), def_schema_location);

					Element child_e = (Element) node;

					String child_name = child_e.getAttribute("name");

					child_table.name = getUnqualifiedName(child_name);

					table.required = child_table.required = true;

					child_table.anno = PgSchemaUtil.extractAnnotation(this, node, true);

					child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

					child_table.fields = new ArrayList<PgField>();

					child_table.level = level;

					child_table.addPrimaryKey(this, child_table.name, unique_key);
					if (!child_table.addNestedKey(this, field.type, node))
						child_table.cancelUniqueKey();

					child_table.removeProhibitedAttrs();
					child_table.removeBlockedSubstitutionGroups();
					child_table.countNestedFields();

					if (child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
						tables.add(child_table);

					addChildItem(node, child_table);

					level--;

				}

			}

		}

		else if (ref != null && !ref.isEmpty()) {

			for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeName().equals(attribute ? xs_prefix_ + "attribute" : xs_prefix_ + "element")) {

					e = (Element) child;

					name = e.getAttribute("name");

					if (name.equals(ref)) {

						if (attribute)
							field.attribute = true;
						else
							field.element = true;

						field.xname = getUnqualifiedName(name);
						field.name = table.avoidFieldDuplication(this, field.xname);
						field.anno = PgSchemaUtil.extractAnnotation(this, child, false);

						field.extractType(this, child);
						field.extractNamespace(this, child); // require type definition
						field.extractRequired(child);
						field.extractFixedValue(child);
						field.extractDefaultValue(child);
						field.extractBlockValue(child);
						field.extractEnumeration(this, child);
						field.extractRestriction(this, child);

						if (field.substitution_group != null && !field.substitution_group.isEmpty())
							table.appendSubstitutionGroup(field.name);

						if (field.enumeration != null && field.enumeration.length > 0) {

							field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.name) + "_" + PgSchemaUtil.avoidPgReservedOps(field.name);

							if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
								field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

						}

						if (field.type == null || field.type.isEmpty()) {

							if (!table.addNestedKey(this, e.getAttribute("name"), child))
								table.cancelUniqueKey();

							level++;

							table.required = true;

							addChildItem(child, table);

							level--;

						}

						else {

							String[] type = field.type.contains(" ") ? field.type.split(" ")[0].split(":") : field.type.split(":");

							// primitive data type

							if (type.length != 0 && type[0].equals(xs_prefix)) {

								field.xs_type = XsDataType.valueOf("xs_" + type[1]);

								table.fields.add(field);

							}

							// non-primitive data type

							else {

								boolean unique_key = table.addNestedKey(this, e.getAttribute("name"), child);

								if (!unique_key)
									table.cancelUniqueKey();

								level++;

								PgTable child_table = new PgTable(def_namespaces.get(""), def_schema_location);

								Element child_e = (Element) child;

								String child_name = child_e.getAttribute("name");

								child_table.name = getUnqualifiedName(child_name);

								table.required = child_table.required = true;

								child_table.anno = PgSchemaUtil.extractAnnotation(this, child, true);

								child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

								child_table.fields = new ArrayList<PgField>();

								child_table.level = level;

								child_table.addPrimaryKey(this, child_table.name, unique_key);
								if (!child_table.addNestedKey(this, field.type, child))
									child_table.cancelUniqueKey();

								child_table.removeProhibitedAttrs();
								child_table.removeBlockedSubstitutionGroups();
								child_table.countNestedFields();

								if (child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
									tables.add(child_table);

								addChildItem(child, child_table);

								level--;

							}

						}

						break;
					}

				}

			}

		}

	}

	/**
	 * Add arbitrary child item.
	 *
	 * @param node current node
	 * @param foreign_table foreign table
	 */
	private void addChildItem(Node node, PgTable foreign_table) {

		PgTable table = new PgTable(def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String type = e.getAttribute("type");

		if (type == null || type.isEmpty()) {

			String name = e.getAttribute("name");

			table.name = getUnqualifiedName(name);
			table.xs_type = foreign_table.xs_type.equals(XsTableType.xs_root) || foreign_table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;


		}

		else {

			table.name = getUnqualifiedName(type);
			table.xs_type = XsTableType.xs_admin_root;

		}

		if (table.name.isEmpty())
			return;

		table.required = true;

		table.anno = PgSchemaUtil.extractAnnotation(this, node, true);

		table.fields = new ArrayList<PgField>();

		table.level = level;

		table.addPrimaryKey(this, table.name, true);
		table.addForeignKey(this, foreign_table);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table);

		table.removeProhibitedAttrs();
		table.removeBlockedSubstitutionGroups();
		table.countNestedFields();

		if (table.fields.size() < option.getMinimumSizeOfField())
			return;

		if (avoidTableDuplication(tables, table))
			tables.add(table);

	}

	/**
	 * Extract attribute group.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractAttributeGroup(Node node, PgTable table) {

		Element e = (Element) node;

		String ref = e.getAttribute("ref");

		if (ref == null || ref.isEmpty()) {

			ref = e.getAttribute("defaultAttributesApply");

			if (ref == null || !ref.equals("true"))
				return;

			if (def_attrs == null || def_attrs.isEmpty())
				return;

			ref = def_attrs;

			return;
		}

		int t = getAttributeGroupId(getUnqualifiedName(ref));

		if (t < 0)
			return;

		attr_groups.get(t).fields.forEach(field -> table.fields.add(field));

	}

	/**
	 * Extract model group.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractModelGroup(Node node, PgTable table) {

		Element e = (Element) node;

		String ref = e.getAttribute("ref");

		if (ref == null || ref.isEmpty())
			return;

		int t = getModelGroupId(getUnqualifiedName(ref));

		if (t < 0)
			return;

		model_groups.get(t).fields.forEach(field -> table.fields.add(field));

	}

	/**
	 * Extract simple content.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractSimpleContent(Node node, PgTable table) {

		PgField field = new PgField();

		String name = PgSchemaUtil.simple_cont_name; // anonymous simple content

		field.simple_cont = true;
		field.xname = getUnqualifiedName(name);
		field.name = table.avoidFieldDuplication(this, field.xname);
		field.anno = PgSchemaUtil.extractAnnotation(this, node, false);

		field.extractType(this, node);
		field.extractNamespace(this, node); // require type definition
		field.extractRequired(node);
		field.extractFixedValue(node);
		field.extractDefaultValue(node);
		field.extractBlockValue(node);
		field.extractEnumeration(this, node);
		field.extractRestriction(this, node);

		if (field.substitution_group != null && !field.substitution_group.isEmpty())
			table.appendSubstitutionGroup(field.name);

		if (field.enumeration != null && field.enumeration.length > 0) {

			field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.name) + "_" + PgSchemaUtil.avoidPgReservedOps(field.name);

			if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
				field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

		}

		String[] type = field.type.contains(" ") ? field.type.split(" ")[0].split(":") : field.type.split(":");

		// primitive data type

		if (type.length != 0 && type[0].equals(xs_prefix)) {

			field.xs_type = XsDataType.valueOf("xs_" + type[1]);

			table.fields.add(field);

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (!child.getNodeName().equals(xs_prefix_ + "extension"))
					continue;

				for (Node subchild = child.getFirstChild(); subchild != null; subchild = subchild.getNextSibling()) {

					String subchild_name = subchild.getNodeName();

					if (subchild_name.equals(xs_prefix_ + "attribute"))
						extractAttribute(subchild, table);

					else if (subchild_name.equals(xs_prefix_ + "attributeGroup"))
						extractAttributeGroup(subchild, table);

				}

				break;
			}

		}

		// non-primitive data type

		else
			extractComplexContent(node, table);

	}

	/**
	 * Extract complex content.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractComplexContent(Node node, PgTable table) {

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeName().equals(xs_prefix_ + "extension")) {

				Element e = (Element) child;

				String type = getUnqualifiedName(e.getAttribute("base"));

				table.addNestedKey(this, type);

				extractComplexContentExt(child, table);

			}

		}

	}

	/**
	 * Extract complex content under xs:extension.
	 *
	 * @param node current node
	 * @param table new table
	 */
	private void extractComplexContentExt(Node node, PgTable table) {

		String node_name = node.getNodeName();

		if (node_name.equals(xs_prefix_ + "any")) {
			extractAny(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "anyAttribute")) {
			extractAnyAttribute(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "attribute")) {
			extractAttribute(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "attributeGroup")) {
			extractAttributeGroup(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "element")) {
			extractElement(node, table);
			return;
		}

		else if (node_name.equals(xs_prefix_ + "group")) {
			extractModelGroup(node, table);
			return;
		}

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			String child_name = child.getNodeName();

			if (child_name.equals(xs_prefix_ + "annotation"))
				continue;

			else if (child_name.equals(xs_prefix_ + "any"))
				extractAny(child, table);

			else if (child_name.equals(xs_prefix_ + "anyAttribute"))
				extractAnyAttribute(child, table);

			else if (child_name.equals(xs_prefix_ + "attribute"))
				extractAttribute(child, table);

			else if (child_name.equals(xs_prefix_ + "attributeGroup"))
				extractAttributeGroup(child, table);

			else if (child_name.equals(xs_prefix_ + "element"))
				extractElement(child, table);

			else if (child_name.equals(xs_prefix_ + "group"))
				extractModelGroup(child, table);

			else
				extractComplexContentExt(child, table);

		}

	}

	/**
	 * Avoid table duplication while merging two equivalent tables.
	 *
	 * @param tables target PostgreSQL table list
	 * @param table new table having table name at least
	 * @return boolean whether no name collision occurs
	 */
	private boolean avoidTableDuplication(List<PgTable> tables, PgTable table) {

		if (tables == null)
			return true;

		List<PgField> fields = table.fields;

		int known_t = tables == this.tables ? getTableId(table.name) : tables == this.attr_groups ? getAttributeGroupId(table.name) : getModelGroupId(table.name);

		if (known_t < 0)
			return true;

		PgTable known_table = tables.get(known_t);

		boolean changed = false;
		boolean conflict = false;

		if (table.required && !known_table.required)
			known_table.required = true;

		// copy annotation if available

		if ((known_table.anno == null || known_table.anno.isEmpty()) && table.anno != null && !table.anno.isEmpty())
			known_table.anno = table.anno;

		// append target namespace if available

		if (table.target_namespace != null && !table.target_namespace.isEmpty()) {

			if (known_table.target_namespace == null || known_table.target_namespace.isEmpty())
				known_table.target_namespace = table.target_namespace;

			else if (!known_table.target_namespace.contains(table.target_namespace))
				known_table.target_namespace += " " + table.target_namespace;

		}

		// append schema location if available

		if (table.schema_location != null && !table.schema_location.isEmpty()) {

			if (known_table.schema_location == null || known_table.schema_location.isEmpty())
				known_table.schema_location = table.schema_location;

			else if (!known_table.schema_location.contains(table.schema_location))
				known_table.schema_location += " " + table.schema_location;

		}

		List<PgField> known_fields = known_table.fields;

		for (PgField field : fields) {

			PgField known_field = known_table.getField(field.name);

			if (known_field == null) { // append new field to known table

				changed = true;

				if (!field.primary_key && field.required && !table.xs_type.equals(XsTableType.xs_admin_root) && known_table.xs_type.equals(XsTableType.xs_admin_root))
					conflict = true;

				known_fields.add(field);

			}

			else { // update field

				// append target namespace if available

				if (field.target_namespace != null && !field.target_namespace.isEmpty()) {

					if (known_field.target_namespace == null || known_field.target_namespace.isEmpty())
						known_field.target_namespace = table.target_namespace;

					else if (!known_field.target_namespace.contains(field.target_namespace))
						known_field.target_namespace += " " + field.target_namespace;

				}

				// append parent node if available

				if (field.nested_key && field.parent_node != null) {

					if (known_field.nested_key) {

						if (known_field.parent_node != null)
							known_field.parent_node += " " + field.parent_node;

					}

				}

			}

		}

		if (fields.size() > 0) {

			if (fields.stream().anyMatch(field -> !field.primary_key && field.required)) {

				for (PgField known_field : known_fields) {

					if (table.getFieldId(known_field.name) < 0) {

						changed = true;

						if (!known_field.primary_key && known_field.required)
							conflict = true;

					}

				}

			}

		}

		if (known_table.xs_type.equals(XsTableType.xs_admin_root) && (table.xs_type.equals(XsTableType.xs_root_child) || table.xs_type.equals(XsTableType.xs_admin_child))) {

			known_table.xs_type = table.xs_type;
			known_table.level = table.level;

		}

		if (changed) {

			known_table.countNestedFields();

			if (conflict) { // avoid conflict

				known_fields.stream().filter(field -> !field.system_key && !field.user_key && field.required).forEach(field -> field.required = false);
				known_table.conflict = true;

			}

		}

		return false;
	}

	/**
	 * Return namespace URI.
	 *
	 * @param prefix prefix of namespace URI
	 * @return String namespace URI
	 */
	protected String getNamespaceURI(String prefix) {
		return def_namespaces.get(prefix);
	}

	/**
	 * Return PostgreSQL table.
	 *
	 * @param table_id table id
	 * @return PgTable PostgreSQL table
	 */
	protected PgTable getTable(int table_id) {

		if (table_id < 0 || table_id >= tables.size())
			return null;

		return tables.get(table_id);
	}

	/**
	 * Return PostgreSQL table.
	 *
	 * @param table_name table name
	 * @return PgTable PostgreSQL table
	 */
	protected PgTable getTable(String table_name) {
		return getTable(getTableId(table_name));
	}

	/**
	 * Return table id from table name.
	 *
	 * @param table_name table name
	 * @return int table id, -1 represents not found
	 */
	private int getTableId(String table_name) {

		for (int t = 0; t < tables.size(); t++) {

			if (tables.get(t).name.equals(table_name))
				return t;

		}

		return -1;
	}

	/**
	 * Return attribute group id from attribute group name.
	 *
	 * @param table_name table name
	 * @return int table id, -1 represents not found
	 */
	private int getAttributeGroupId(String table_name) {

		for (int t = 0; t < attr_groups.size(); t++) {

			if (attr_groups.get(t).name.equals(table_name))
				return t;

		}

		return -1;
	}

	/**
	 * Return model group id from model group name.
	 *
	 * @param table_name table name
	 * @return int table id, -1 represents not found
	 */
	private int getModelGroupId(String table_name) {

		for (int t = 0; t < model_groups.size(); t++) {

			if (model_groups.get(t).name.equals(table_name))
				return t;

		}

		return -1;
	}

	/**
	 * Return unqualified name.
	 *
	 * @param name qualified name or unqualified name
	 * @return String unqualified name
	 */
	protected String getUnqualifiedName(String name) {

		if (name == null)
			return null;

		if (name.contains(" "))
			name = name.trim();

		if (!option.case_sense)
			name = name.toLowerCase();

		int last_pos = name.lastIndexOf(':');

		return last_pos == -1 ? name : name.substring(last_pos + 1);
	}

	/**
	 * Realize PostgreSQL DDL.
	 */
	private void realize() {

		// initialize realization flag and table order

		realized = new boolean[tables.size()];
		table_order = new int [tables.size()];

		Arrays.fill(realized, false);
		Arrays.fill(table_order, -1);

		level = 0;

		// root table

		realize(root_table, root_table_id, false);

		// referenced administrative tables at first

		foreign_keys.forEach(foreign_key -> {

			int t;

			if ((t = getTableId(foreign_key.parent_table)) >= 0) {

				PgTable table = tables.get(t);

				realizeAdmin(table, false);

				realize(table, t, false);

			}

		});

		// administrative tables ordered by level

		int max_child_level = tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_root_child) || table.xs_type.equals(XsTableType.xs_admin_child)).max(Comparator.comparingInt(table -> table.level)).get().level;

		for (int l = 1; l <= max_child_level; l++) {

			for (int t = 0; t < tables.size(); t++) {

				PgTable table = tables.get(t);

				if ((table.xs_type.equals(XsTableType.xs_root_child) || table.xs_type.equals(XsTableType.xs_admin_child))&& table.level == l) {

					realizeAdmin(table, false);

					realize(table, t, false);

				}

			}

		}

		// remaining administrative tables

		for (int t = 0; t < tables.size(); t++) {

			PgTable table = tables.get(t);

			if (table.xs_type.equals(XsTableType.xs_admin_root) && !realized[t]) {

				realizeAdmin(table, false);

				realize(table, t, false);

			}

		}

		if (!option.ddl_output)
			return;

		System.out.println("--");
		System.out.println("-- PostgreSQL DDL generated from " + def_schema_location + " using xsd2pgschema");
		System.out.println("--  xsd2pgschema - Database replication tool based on XML Schema");
		System.out.println("--  https://sourceforge.net/projects/xsd2pgschema/");
		System.out.println("--");
		System.out.println("-- Schema modeling options:");
		System.out.println("--  relational extension: " + option.rel_data_ext);
		System.out.println("--  wild card extension: " + option.wild_card);
		System.out.println("--  case sensitive name: " + option.case_sense);
		System.out.println("--  no name collision: " + !conflicted);
		System.out.println("--  appended document key: " + option.document_key);
		System.out.println("--  appended serial key: " + option.serial_key);
		System.out.println("--  appended xpath key: " + option.xpath_key);
		System.out.println("--  retained constraint of primary/foreign key: " + option.retain_key);
		System.out.println("--  retrieved field annotation: " + !option.no_field_anno);
		if (option.rel_data_ext || option.serial_key)
			System.out.println("--  " + (message_digest == null ? "assumed " : "") + "hash algorithm: " + (message_digest == null ? PgSchemaUtil.def_hash_algorithm : message_digest.getAlgorithm()));
		if (option.rel_data_ext)
			System.out.println("--  hash key type: " + option.hash_size.name().replaceAll("_", " ") + " bits");
		if (option.serial_key)
			System.out.println("--  searial key type: " + option.ser_size.name().replaceAll("_", " ") + " bits");
		System.out.println("--");
		System.out.println("-- Statistics of schema:");
		System.out.print(def_stat_msg.toString());
		System.out.println("--\n");

		if (def_anno != null) {

			System.out.println("--");
			System.out.println("-- " + def_anno);
			System.out.println("--\n");

		}

		for (--level; level >= 0; --level) {

			for (int t = 0; t < tables.size(); t++) {

				PgTable table = tables.get(t);

				if (!table.required)
					continue;

				if (!option.rel_data_ext && table.relational)
					continue;

				if (table_order[t] == level)
					System.out.println("DROP TABLE IF EXISTS " + PgSchemaUtil.avoidPgReservedWords(table.name) + " CASCADE;");

			}

		}

		System.out.println("");

		Arrays.fill(realized, false);

		for (level = 0; level < tables.size(); level++) {

			for (int t = 0; t < tables.size(); t++) {

				if (table_order[t] == level)
					realize(tables.get(t), t, true);

			}

		}

		// add primary key/foreign key

		if (!option.retain_key) {

			for (level = 0; level < tables.size(); level++) {

				for (int t = 0; t < tables.size(); t++) {

					if (table_order[t] == level) {

						PgTable table = tables.get(t);

						if (table.bridge)
							continue;

						table.fields.forEach(field -> {

							if (field.unique_key)
								System.out.println("--ALTER TABLE " + PgSchemaUtil.avoidPgReservedWords(table.name) + " ADD PRIMARY KEY ( " + PgSchemaUtil.avoidPgReservedWords(field.name) + " );\n");

							else if (field.foreign_key) {

								if (!tables.get(field.foreign_table_id).bridge)
									System.out.println("--ALTER TABLE " + PgSchemaUtil.avoidPgReservedWords(table.name) + " ADD FOREIGN KEY " + field.constraint_name + " REFERENCES " + PgSchemaUtil.avoidPgReservedWords(field.foreign_table) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field) + " );\n");

							}

						});

					}

				}

			}

		}

		// add unique key for foreign key

		for (int fk = 0; fk < foreign_keys.size(); fk++) {

			PgForeignKey foreign_key = foreign_keys.get(fk);

			boolean unique = true;

			for (int fk2 = 0; fk2 < fk; fk2++) {

				PgForeignKey foreign_key2 = foreign_keys.get(fk2);

				if (foreign_key.parent_table.equals(foreign_key2.parent_table)) {
					unique = false;
					break;
				}

			}

			if (!unique)
				continue;

			PgTable table = getTable(foreign_key.parent_table);

			if (table != null) {

				if (!option.rel_data_ext && table.relational)
					unique = false;

				PgField field = table.getField(foreign_key.parent_fields);

				if (field != null) {

					if (field.primary_key)
						unique = false;

				}

			}

			if (!unique)
				continue;

			String constraint_name = "UNQ_" + foreign_key.parent_table;

			if (constraint_name.length() > PgSchemaUtil.max_enum_len)
				constraint_name = constraint_name.substring(0, PgSchemaUtil.max_enum_len);

			System.out.println((option.retain_key ? "" : "--") + "ALTER TABLE " + PgSchemaUtil.avoidPgReservedWords(foreign_key.parent_table) + " ADD CONSTRAINT " + PgSchemaUtil.avoidPgReservedOps(constraint_name) + " UNIQUE ( " + PgSchemaUtil.avoidPgReservedWords(foreign_key.parent_fields) + " );\n");

		}

		// add foreign key constraint

		for (PgForeignKey foreign_key : foreign_keys) {

			boolean relational = false;

			PgTable child_table = getTable(foreign_key.child_table);

			if (child_table != null)
				relational = child_table.relational;

			if (!option.rel_data_ext && relational)
				continue;

			PgTable parent_table = getTable(foreign_key.parent_table);

			if (parent_table != null)
				relational = parent_table.relational;

			if (!option.rel_data_ext && relational)
				continue;

			String[] child_fields = foreign_key.child_fields.split(" ");
			String[] parent_fields = foreign_key.parent_fields.split(" ");

			if (child_fields.length == parent_fields.length) {

				for (int i = 0; i < child_fields.length; i++) {

					child_fields[i] = child_fields[i].replaceFirst(",$", "");
					parent_fields[i] = parent_fields[i].replaceFirst(",$", "");

					String constraint_name = "KR_" + foreign_key.name + (child_fields.length > 1 ? "_" + i : "");

					if (constraint_name.length() > PgSchemaUtil.max_enum_len)
						constraint_name = constraint_name.substring(0, PgSchemaUtil.max_enum_len);

					System.out.println((option.retain_key ? "" : "--") + "ALTER TABLE " + PgSchemaUtil.avoidPgReservedWords(foreign_key.child_table) + " ADD CONSTRAINT " + PgSchemaUtil.avoidPgReservedOps(constraint_name) + " FOREIGN KEY ( " + PgSchemaUtil.avoidPgReservedWords(child_fields[i]) + " ) REFERENCES " + PgSchemaUtil.avoidPgReservedWords(foreign_key.parent_table) + " ( " + PgSchemaUtil.avoidPgReservedWords(parent_fields[i]) + " ) NOT VALID;\n");
				}

			}

		}

		realized = null;
		table_order = null;

	}

	/**
	 * Realize PostgreSQL DDL of administrative table.
	 *
	 * @param table the table
	 * @param output whether outputs PostgreSQL DDL via standard output
	 */
	private void realizeAdmin(PgTable table, boolean output) {

		// realize parent table at first

		foreign_keys.stream().filter(foreign_key -> foreign_key.child_table.equals(table.name)).forEach(foreign_key -> {

			int admin_t = getTableId(foreign_key.parent_table);

			if (admin_t >= 0) {

				PgTable admin_table = tables.get(admin_t);

				if (!realized[admin_t]) {

					realizeAdmin(admin_table, output);

					realize(admin_table, admin_t, output);

				}

			}

		});

		// set foreign_table_id as table pointer otherwise remove foreign key

		Iterator<PgField> iterator = table.fields.iterator();

		while (iterator.hasNext()) {

			PgField field = iterator.next();

			if (field.foreign_key) {

				int admin_t = getTableId(field.foreign_table);

				if (admin_t >= 0) {

					field.foreign_table_id = admin_t;

					PgTable admin_table = tables.get(admin_t);

					if (!realized[admin_t]) {

						realizeAdmin(admin_table, output);

						realize(admin_table, admin_t, output);

					}

				}

				else
					iterator.remove();

			}

		}

	}

	/**
	 * Realize PostgreSQL DDL of arbitrary table.
	 *
	 * @param table the table
	 * @param table_id table id
	 * @param output whether outputs PostgreSQL DDL via standard output
	 */
	private void realize(PgTable table, int table_id, boolean output) {

		if (realized[table_id])
			return;

		realized[table_id] = true;

		if (!output) {
			table_order[table_id] = level++;
			return;
		}

		if (!table.required)
			return;

		if (!option.rel_data_ext && table.relational)
			return;

		System.out.println("--");
		System.out.println("-- " + table.anno);
		System.out.println("-- xmlns: " + table.target_namespace + ", schema location: " + table.schema_location);
		System.out.println("-- type: " + table.xs_type.toString().replaceFirst("^xs_", "").replaceAll("_",  " ") + ", content: " + table.cont_holder + ", list: " + table.list_holder + ", bridge: " + table.bridge + ", virtual: " + table.virtual + (conflicted ? ", name collision: " + table.conflict : ""));
		System.out.println("--");

		List<PgField> fields = table.fields;

		fields.stream().filter(field -> field.enum_name != null && !field.enum_name.isEmpty()).forEach(field -> {

			System.out.println("DROP TYPE IF EXISTS " + field.enum_name + ";");

			System.out.print("CREATE TYPE " + field.enum_name + " AS ENUM (");

			for (int i = 0; i < field.enumeration.length; i++) {

				System.out.print(" '" + field.enumeration[i] + "'");

				if (i < field.enumeration.length - 1)
					System.out.print(",");

			}

			System.out.println(" );");

		});

		System.out.println("CREATE TABLE " + PgSchemaUtil.avoidPgReservedWords(table.name) + " (");

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.document_key)
				System.out.println("-- DOCUMENT KEY is pointer to data source (aka. Entry ID)");

			else if (field.serial_key)
				System.out.println("-- SERIAL KEY");

			else if (field.xpath_key)
				System.out.println("-- XPATH KEY");

			else if (field.unique_key)
				System.out.println("-- PRIMARY KEY");

			else if (field.foreign_key)
				System.out.println("-- FOREIGN KEY : " + PgSchemaUtil.avoidPgReservedWords(field.foreign_table) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field) + " )");

			else if (field.nested_key)
				System.out.println("-- NESTED KEY : " + PgSchemaUtil.avoidPgReservedWords(field.foreign_table) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field) + " )" + (field.parent_node != null ? ", PARENT NODE : " + field.parent_node : ""));

			else if (field.attribute)
				System.out.println("-- ATTRIBUTE");

			else if (field.simple_cont)
				System.out.println("-- SIMPLE CONTENT");

			else if (field.any)
				System.out.println("-- ANY ELEMENT");

			else if (field.any_attribute)
				System.out.println("-- ANY ATTRIBUTE");

			if (!field.required && field.xrequired) {

				if (field.fixed_value == null || field.fixed_value.isEmpty())
					System.out.println("-- must not be NULL, but dismissed because of name collision");

				else {
					System.out.print("-- must have a constraint ");

					switch (field.xs_type) {
					case xs_anyType:
					case xs_string:
					case xs_normalizedString:
					case xs_token:
					case xs_language:
					case xs_Name:
					case xs_QName:
					case xs_NCName:
					case xs_anyURI:
					case xs_NOTATION:
					case xs_NMTOKEN:
					case xs_NMTOKENS:
					case xs_ID:
					case xs_IDREF:
					case xs_IDREFS:
					case xs_ENTITY:
					case xs_ENTITIES:
						System.out.print("CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.name) + " = '" + field.fixed_value + "' ) ");
						break;
					default:
						System.out.print("CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.name) + " = " + field.fixed_value + " ) ");
					}

					System.out.println(", but dismissed because of name collision");
				}
			}

			if (field.enum_name == null || field.enum_name.isEmpty())
				System.out.print("\t" + PgSchemaUtil.avoidPgReservedWords(field.name) + " " + XsDataType.getPgDataType(field) + " ");
			else
				System.out.print("\t" + PgSchemaUtil.avoidPgReservedWords(field.name) + " " + field.enum_name + " ");

			if ((field.required || !field.xrequired) && field.fixed_value != null && !field.fixed_value.isEmpty()) {

				switch (field.xs_type) {
				case xs_anyType:
				case xs_string:
				case xs_normalizedString:
				case xs_token:
				case xs_language:
				case xs_Name:
				case xs_QName:
				case xs_NCName:
				case xs_anyURI:
				case xs_NOTATION:
				case xs_NMTOKEN:
				case xs_NMTOKENS:
				case xs_ID:
				case xs_IDREF:
				case xs_IDREFS:
				case xs_ENTITY:
				case xs_ENTITIES:
					System.out.print("CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.name) + " = '" + field.fixed_value + "' ) ");
					break;
				default:
					System.out.print("CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.name) + " = " + field.fixed_value + " ) ");
				}

			}

			if (field.required)
				System.out.print("NOT NULL ");

			if (option.retain_key) {

				if (field.unique_key) {

					System.out.print("PRIMARY KEY ");

				} else if (field.foreign_key) {

					PgTable foreign_table = tables.get(field.foreign_table_id);

					PgField foreign_field = foreign_table.getField(field.foreign_field);

					if (foreign_field != null) {

						if (foreign_field.unique_key)
							System.out.print("CONSTRAINT " + field.constraint_name + " REFERENCES " + PgSchemaUtil.avoidPgReservedWords(field.foreign_table) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field) + " ) ");

					}

				}

			}

			if (f < fields.size() - 1)
				System.out.println("," + (option.no_field_anno || field.anno == null || field.anno.isEmpty() ? "" : " -- " + field.anno));
			else
				System.out.println((option.no_field_anno || field.anno == null || field.anno.isEmpty() ? "" : " -- " + field.anno));

		}

		System.out.println(");\n");

	}

	// post XML editorial functions

	/** Whether filt-in option are resolved. */
	private boolean filt_in_resolved = false;

	/**
	 * Apply filt-in option.
	 *
	 * @param filt_ins filt-in option
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyFiltIn(List<String> filt_ins) throws PgSchemaException {

		if (filt_in_resolved)
			return;

		filt_in_resolved = true;

		if (filt_ins.size() == 0)
			return;

		for (String filt_in : filt_ins) {

			String[] key_val = filt_in.split(":");
			String[] key = key_val[0].split("\\.");

			if (key_val.length != 1 || key.length > 2)
				throw new PgSchemaException(filt_in + ": argument should be expressed by \"table_name.column_name\".");

			String table_name = key[0];
			String field_name = key.length > 1 ? key[1] : null;

			PgTable table = getTable(table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + " table.");

			if (field_name != null && !field_name.isEmpty()) {

				if (table.getFieldId(field_name) >= 0)
					table.filt_out = true;

				else
					throw new PgSchemaException("Not found " + field_name + " field in " + table_name + " table.");

			} else {

				if (table.cont_holder)
					table.filt_out = true;

			}

		}

		// select nested table

		boolean append_table;

		do {

			append_table = false;

			for (PgTable table : tables) {

				if (table.filt_out) {

					for (PgField field : table.fields) {

						if (!field.nested_key)
							continue;

						PgTable nested_table = tables.get(field.foreign_table_id);

						if (nested_table.filt_out)
							continue;

						nested_table.filt_out = true;

						append_table = true;

					}

				}

			}

		} while (append_table);

		// inverse filt_out flag and update requirement

		tables.forEach(table -> {

			table.filt_out = !table.filt_out;

			if (table.filt_out)
				table.required = false;

		});

	}

	/** Whether filt-out option are resolved. */
	private boolean filt_out_resolved = false;

	/**
	 * Apply filt-out option.
	 *
	 * @param filt_outs filt-out option
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyFiltOut(List<String> filt_outs) throws PgSchemaException {

		if (filt_out_resolved)
			return;

		filt_out_resolved = true;

		if (filt_outs.size() == 0)
			return;

		for (String filt_out : filt_outs) {

			String[] key_val = filt_out.split(":");
			String[] key = key_val[0].split("\\.");

			if (key_val.length > 2 || key.length > 2)
				throw new PgSchemaException(filt_out + ": argument should be expressed by \"table_name.column_name:regex_pattern(|regex_pattern)\".");

			String table_name = key[0];
			String field_name = key.length > 1 ? key[1] : null;
			String[] regex_pattern = key_val.length > 2 ? key_val[1].split("\\|") : null;

			PgTable table = getTable(table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + " table.");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " table is unselectable (root table).");

			if (field_name != null && !field_name.isEmpty()) {

				PgField field = table.getField(field_name);

				if (field != null) {

					if (field.system_key || field.user_key)
						throw new PgSchemaException(field_name + " field of " + table_name + " table is administrative key).");

					field.filt_out = true;
					field.out_pattern = regex_pattern;

				}

				else
					throw new PgSchemaException("Not found " + field_name + " field in " + table_name + " table.");

			} else {

				table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> {

					field.filt_out = true;
					field.out_pattern = regex_pattern;

				});

			}

		}

	}

	/** Whether fill-this option are resolved. */
	private boolean fill_this_resolved = false;

	/**
	 * Apply fill-this option.
	 *
	 * @param fill_these fill-this option
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyFillThis(List<String> fill_these) throws PgSchemaException {

		if (fill_this_resolved)
			return;

		fill_this_resolved = true;

		if (fill_these.size() == 0)
			return;

		for (String fill_this : fill_these) {

			String[] key_val = fill_this.split(":");
			String[] key = key_val[0].split("\\.");

			if (key_val.length > 2 || key.length != 2)
				throw new PgSchemaException(fill_this + ": argument should be expressed by \"table_name.column_name:filling_text\".");

			String table_name = key[0];
			String field_name = key[1];
			String filled_text = key_val.length == 2 ? key_val[1] : "";

			PgTable table = getTable(table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + " table.");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " table is unselectable (root table).");

			PgField field = table.getField(field_name);

			if (field != null) {

				if (field.system_key || field.user_key)
					throw new PgSchemaException(field_name + " field of " + table_name + " table is administrative key).");

				field.fill_this = true;
				field.filled_text = filled_text;

			}

			else
				throw new PgSchemaException("Not found " + field_name + " field in " + table_name + " table.");

		}

	}

	/** Whether selected attributes are resolved. */
	protected boolean attr_resolved = false;

	/**
	 * Apply attr option for full-text indexing.
	 *
	 * @param index_filter index filter
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyAttr(IndexFilter index_filter) throws PgSchemaException {

		if (attr_resolved)
			return;

		attr_resolved = true;

		// type dependent attribute selection

		if (index_filter.attr_string || index_filter.attr_integer || index_filter.attr_float || index_filter.attr_date)
			tables.forEach(table -> table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> XsDataType.appendAttr(table, field, index_filter)));

		// select all attributes

		if (index_filter.attrs == null) {

			tables.forEach(table -> table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> field.attr_sel = true));

			return;
		}

		if (index_filter.attrs.size() == 0)
			return;

		for (String sph_attr : index_filter.attrs) {

			String[] key = sph_attr.split("\\.");

			if (key.length > 2)
				throw new PgSchemaException(sph_attr + ": argument should be expressed by \"table_name.column_name\".");

			String table_name = key[0];
			String field_name = key.length > 1 ? key[1] : null;

			PgTable table = getTable(table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + " table.");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " table is unselectable (root table).");

			if (field_name != null && !field_name.isEmpty()) {

				PgField field = table.getField(field_name);

				if (field != null) {

					if (field.system_key || field.user_key)
						throw new PgSchemaException(field_name + " field of " + table_name + " table is administrative key).");

					field.attr_sel = true;

				}

				else
					throw new PgSchemaException("Not found " + field_name + " field in " + table_name + " table.");

			} else
				table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> field.attr_sel = true);

		}

	}

	/** Whether selected fields are resolved. */
	protected boolean field_resolved = false;

	/**
	 * Apply field option for full-text indexing.
	 *
	 * @param index_filer index filter
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyField(IndexFilter index_filter) throws PgSchemaException {

		if (field_resolved)
			return;

		if (index_filter.fields.size() == 0)
			return;

		field_resolved = true;

		for (String field : index_filter.fields) {

			String[] key = field.split("\\.");

			if (key.length > 2)
				throw new PgSchemaException(field + ": argument should be expressed by \"table_name.column_name\".");

			String table_name = key[0];
			String field_name = key.length > 1 ? key[1] : null;

			PgTable _table = getTable(table_name);

			if (_table == null)
				throw new PgSchemaException("Not found " + table_name + " table.");

			if (_table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " table is unselectable (root table).");

			if (field_name != null && !field_name.isEmpty()) {

				PgField _field = _table.getField(field_name);

				if (_field != null) {

					if (_field.system_key || _field.user_key)
						throw new PgSchemaException(field_name + " field of " + table_name + " table is administrative key).");

					_field.field_sel = true;

				}

				else
					throw new PgSchemaException("Not found " + field_name + " field in " + table_name + " table.");

			} else
				_table.fields.stream().filter(_field -> !_field.system_key && !_field.user_key).forEach(_field -> _field.field_sel = true);

		}

		for (int t = 0; t < tables.size(); t++) {

			PgTable _table = tables.get(t);

			List<PgField> _fields = _table.fields;

			int f = 0;

			for (; f < _fields.size(); f++) {

				PgField _field = _fields.get(f);

				if (_field.system_key || _field.user_key)
					continue;

				if (_field.attr_sel || _field.field_sel)
					break;

			}

			if (f < _fields.size())
				continue;

			_table.required = false;

		}

	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return String hash key
	 */
	public synchronized String getHashKeyString(String key_name) {

		if (message_digest == null) // debug mode
			return key_name;

		message_digest.reset();

		byte[] bytes = message_digest.digest(key_name.getBytes());

		switch (option.hash_size) {
		case native_default:
			return "E'\\\\x" + DatatypeConverter.printHexBinary(bytes) + "'"; // PostgreSQL hex format
		case unsigned_long_64:
			BigInteger blong = new BigInteger(bytes);
			return Long.toString(Math.abs(blong.longValue())); // use lower order 64bit
		case unsigned_int_32:
			BigInteger bint = new BigInteger(bytes);
			return Integer.toString(Math.abs(bint.intValue())); // use lower order 32bit
		default:
			return key_name;
		}

	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return bytes[] hash key
	 */
	synchronized protected byte[] getHashKeyBytes(String key_name) {

		message_digest.reset();

		return message_digest.digest(key_name.getBytes());
	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return int hash key
	 */
	synchronized protected int getHashKeyInt(String key_name) {

		message_digest.reset();

		byte[] hash = message_digest.digest(key_name.getBytes());

		BigInteger bint = new BigInteger(hash);

		return Math.abs(bint.intValue()); // use lower order 32bit
	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return long hash key
	 */
	synchronized protected long getHashKeyLong(String key_name) {

		message_digest.reset();

		byte[] hash = message_digest.digest(key_name.getBytes());

		BigInteger bint = new BigInteger(hash);

		return Math.abs(bint.longValue()); // use lower order 64bit
	}

	/**
	 * Check whether schema has root table.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	private void hasRootTable() throws PgSchemaException {

		if (root_table_id < 0)
			throw new PgSchemaException("Not found root table in XML Schema: " + def_schema_location);

	}

	/**
	 * Return root node of document.
	 *
	 * @param xml_parser XML document
	 * @param xml_post_editor XML post editing
	 * @return Node root node of document
	 * @throws PgSchemaException the pg schema exception
	 */
	private Node getRootNode(XmlParser xml_parser, XmlPostEditor xml_post_editor) throws PgSchemaException {

		hasRootTable();

		if (xml_post_editor.filt_ins != null)
			applyFiltIn(xml_post_editor.filt_ins);

		if (xml_post_editor.filt_outs != null)
			applyFiltOut(xml_post_editor.filt_outs);

		if (xml_post_editor.fill_these != null)
			applyFillThis(xml_post_editor.fill_these);

		Node node = xml_parser.document.getDocumentElement();

		// check root element name

		if (!node.getNodeName().endsWith(root_table.name))
			throw new PgSchemaException("Not found root element (node_name: " + root_table.name + ") in XML: " + document_id);

		this.document_id = xml_parser.document_id;

		return node;
	}

	/**
	 * Return root node of document for full-text indexing.
	 *
	 * @param xml_parser XML document
	 * @param xml_post_editor XML post editing
	 * @param index_filter index filter
	 * @return Node root node of document
	 * @throws PgSchemaException the pg schema exception
	 */
	private Node getRootNode(XmlParser xml_parser, XmlPostEditor xml_post_editor, IndexFilter index_filter) throws PgSchemaException {

		Node node = getRootNode(xml_parser, xml_post_editor);

		applyAttr(index_filter);

		if (index_filter.fields != null)
			applyField(index_filter);

		this.min_word_len = index_filter.min_word_len;
		this.numeric_index = index_filter.numeric_index;

		return node;
	}

	/**
	 * Return current document id.
	 *
	 * @return String document id
	 */
	protected String getDocumentId() {
		return document_id;
	}

	/**
	 * Initialize table lock objects.
	 *
	 * @param single_lock whether single object no all objects
	 */
	private void initTableLock(boolean single_lock) {

		if (single_lock) {

			if (table_lock == null) {

				table_lock = new Object[1];
				table_lock[0] = new Object();

			}

		}

		else {

			if (table_lock == null || table_lock.length < tables.size()) {

				table_lock = new Object[tables.size()];

				for (int t = 0; t < tables.size(); t++)
					table_lock[t] = new Object();

			}

		}

	}

	/**
	 * Close table lock objects.
	 */
	private void closeTableLock() {

		if (table_lock != null) {

			for (int t = 0; t < tables.size() && t < table_lock.length; t++)
				table_lock[t] = null;

			table_lock = null;

		}

	}

	// CSV conversion

	/**
	 * CSV conversion.
	 *
	 * @param xml_parser XML document
	 * @param csv_dir_name directory name of CSV files
	 * @param db_conn Database connection
	 * @param xml_post_editor XML post editing
	 * @param append_csv whether allows to append CSV files
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgCsv(XmlParser xml_parser, String csv_dir_name, Connection db_conn, XmlPostEditor xml_post_editor, boolean append_csv) throws ParserConfigurationException, TransformerException, PgSchemaException {

		Node node = getRootNode(xml_parser, xml_post_editor);

		initTableLock(false);

		tables.forEach(table -> {

			if (table.required && (option.rel_data_ext || !table.relational)) {

				if (table.filew == null) {

					File csv_file = new File(csv_dir_name + table.name + ".csv");

					try {

						table.filew = new FileWriter(csv_file, append_csv && csv_file.exists());

					} catch (IOException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

			}

			else
				table.filew = null;

		});

		// parse root node and write to CSV file

		try {

			PgSchemaNode2PgCsv node2pgcsv = new PgSchemaNode2PgCsv(this, null, root_table);

			node2pgcsv.parseRootNode(node);

			node2pgcsv.invokeRootNestedNode();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close xml2PgCsv.
	 */
	public void closeXml2PgCsv() {

		closeTableLock();

		tables.stream().filter(table -> table.filew != null).forEach(table -> {

			try {

				table.filew.close();
				table.filew = null;

			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		});

	}

	/**
	 * Parse current node and write to CSV file.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 */
	protected void parseChildNode2PgCsv(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) throws IOException, ParserConfigurationException, TransformerException {

		final int table_id = getTableId(table.name);

		PgSchemaNode2PgCsv node2pgcsv = new PgSchemaNode2PgCsv(this, parent_table, table);

		for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

			if (node.getNodeType() != Node.ELEMENT_NODE)
				continue;

			PgSchemaNodeTester node_test = new PgSchemaNodeTester(this, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

			if (node_test.omitted)
				continue;

			synchronized (table_lock[table_id]) {
				node2pgcsv.parseChildNode(node_test);
			}

			node2pgcsv.invokeChildNestedNode(node_test);

			if (node_test.isLastNode())
				break;

		}

		if (!node2pgcsv.invoked) {

			synchronized (table_lock[table_id]) {
				node2pgcsv.parseChildNode(parent_node, parent_key, proc_key, nested);
			}

			node2pgcsv.invokeChildNestedNode();

		}

	}

	// PostgreSQL data migration via prepared statement

	/**
	 * PostgreSQL data migration.
	 *
	 * @param xml_parser XML document
	 * @param db_conn Database connection
	 * @param xml_post_editor XML post editing
	 * @param update delete before insert
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgSql(XmlParser xml_parser, Connection db_conn, XmlPostEditor xml_post_editor, boolean update) throws ParserConfigurationException, TransformerException, IOException, PgSchemaException {

		Node node = getRootNode(xml_parser, xml_post_editor);

		// parse root node and send to PostgreSQL

		try {

			db_conn.setAutoCommit(false); // transaction begins

			if (update)
				deleteBeforeUpdate(db_conn);

			PgSchemaNode2PgSql node2pgsql = new PgSchemaNode2PgSql(this, null, root_table, db_conn);

			node2pgsql.parseRootNode(node);
			node2pgsql.executeBatch();

			node2pgsql.invokeRootNestedNode();

			db_conn.commit(); // transaction ends

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Parse current node and send to PostgreSQL.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node
	 * @param db_conn Database connection
	 * @throws SQLException the SQL exception
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void parseChildNode2PgSql(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id, final Connection db_conn) throws SQLException, ParserConfigurationException, TransformerException, IOException {

		PgSchemaNode2PgSql node2pgsql = null;

		node2pgsql = new PgSchemaNode2PgSql(this, parent_table, table, db_conn);

		for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

			if (node.getNodeType() != Node.ELEMENT_NODE)
				continue;

			PgSchemaNodeTester node_test = new PgSchemaNodeTester(this, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

			if (node_test.omitted)
				continue;

			node2pgsql.parseChildNode(node_test);

			node2pgsql.invokeChildNestedNode(node_test);

			if (node_test.isLastNode())
				break;

		}

		if (!node2pgsql.invoked) {

			node2pgsql.parseChildNode(parent_node, parent_key, proc_key, nested);

			node2pgsql.invokeChildNestedNode();

		}

		node2pgsql.executeBatch();

	}

	/**
	 * Execute PostgreSQL DELETE command before INSERT for all tables of current document.
	 *
	 * @param db_conn Database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	private void deleteBeforeUpdate(Connection db_conn) throws PgSchemaException {

		try {

			List<String> table_list = new ArrayList<String>();

			DatabaseMetaData meta = db_conn.getMetaData();
			ResultSet rset = meta.getTables(null, null, null, null);

			while (rset.next())
				table_list.add(rset.getString("TABLE_NAME"));

			rset.close();

			Statement stat = db_conn.createStatement();

			for (level = 0; level < tables.size(); level++) {

				for (int t = 0; t < tables.size(); t++) {

					if (table_order[t] == level) {

						PgTable table = tables.get(t);

						if (!table.required)
							continue;

						if (!option.rel_data_ext && table.relational)
							continue;

						String table_name = table.name;

						boolean has_db_table = false;

						for (String db_table_name : table_list) {

							if (option.case_sense ? db_table_name.equals(db_table_name) : db_table_name.equalsIgnoreCase(table_name)) {

								has_db_table = true;

								String sql = "DELETE FROM " + PgSchemaUtil.avoidPgReservedWords(db_table_name) + " WHERE " + PgSchemaUtil.document_key_name + "='" + document_id + "'";

								stat.execute(sql);

								break;
							}

						}

						if (!has_db_table)
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + " not found.");

					}

				}

			}

			stat.close();

			table_list.clear();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Execute PostgreSQL COPY command for all CSV files.
	 *
	 * @param db_conn Database connection
	 * @param csv_dir_name directory name of CSV files
	 * @return whether success or not
	 * @throws PgSchemaException the pg schema exception
	 */
	public boolean pgCsv2PgSql(Connection db_conn, String csv_dir_name) throws PgSchemaException {

		try {

			List<String> table_list = new ArrayList<String>();

			DatabaseMetaData meta = db_conn.getMetaData();
			ResultSet rset = meta.getTables(null, null, null, null);

			while (rset.next())
				table_list.add(rset.getString("TABLE_NAME"));

			rset.close();

			CopyManager copy_man = new CopyManager((BaseConnection) db_conn);

			for (level = 0; level < tables.size(); level++) {

				for (int t = 0; t < tables.size(); t++) {

					if (table_order[t] == level) {

						PgTable table = tables.get(t);

						if (!table.required)
							continue;

						if (!option.rel_data_ext && table.relational)
							continue;

						String table_name = table.name;

						File csv_file = new File(csv_dir_name + table_name + ".csv");

						boolean has_db_table = false;

						for (String db_table_name : table_list) {

							if (option.case_sense ? db_table_name.equals(db_table_name) : db_table_name.equalsIgnoreCase(table_name)) {

								has_db_table = true;

								String sql = "COPY " + PgSchemaUtil.avoidPgReservedWords(db_table_name) + " FROM STDIN CSV";

								if (copy_man.copyIn(sql, new FileInputStream(csv_file)) < 0)
									return false;

								break;
							}

						}

						if (!has_db_table)
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + " not found.");

					}

				}

			}

			table_list.clear();

		} catch (SQLException | IOException e) {
			throw new PgSchemaException(e);
		}

		return true;
	}

	/**
	 * Test PostgreSQL data model.
	 *
	 * @param db_conn Database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	public void testPgSqlSchema(Connection db_conn) throws PgSchemaException {

		try {

			List<String> table_list = new ArrayList<String>();

			DatabaseMetaData meta = db_conn.getMetaData();
			ResultSet rset = meta.getTables(null, null, null, null);

			while (rset.next())
				table_list.add(rset.getString("TABLE_NAME"));

			rset.close();

			Statement stat = db_conn.createStatement();

			for (level = 0; level < tables.size(); level++) {

				for (int t = 0; t < tables.size(); t++) {

					if (table_order[t] == level) {

						PgTable table = tables.get(t);

						if (!table.required)
							continue;

						if (!option.rel_data_ext && table.relational)
							continue;

						String table_name = table.name;

						boolean has_db_table = false;

						for (String db_table_name : table_list) {

							int found_columns = 0;

							if (option.case_sense ? db_table_name.equals(db_table_name) : db_table_name.equalsIgnoreCase(table_name)) {

								ResultSet rset_col = meta.getColumns(null, null, db_table_name, null);

								while (rset_col.next()) {

									String db_column_name = rset_col.getString("COLUMN_NAME");

									if (table.fields.stream().anyMatch(field -> option.case_sense ? field.name.equals(db_column_name) : field.name.equalsIgnoreCase(db_column_name)))
										found_columns++;

								}

								rset_col.close();

								if (found_columns < table.fields.size()) {

									for (PgField field : table.fields) {

										String field_name = option.case_sense ? field.name : field.name.toLowerCase();

										rset_col = meta.getColumns(null, null, db_table_name, field_name);

										if (!rset_col.next())
											throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + field_name + " not found.");

										rset_col.close();

									}

								}

								has_db_table = true;

								break;
							}

						}

						if (!has_db_table)
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + " not found.");

					}

				}

			}

			stat.close();

			table_list.clear();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Lucene document conversion.
	 *
	 * @param xml_parser XML document
	 * @param lucene_doc Lucene document
	 * @param xml_post_editor XML post editing
	 * @param index_filter index filter
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2LucIdx(XmlParser xml_parser, org.apache.lucene.document.Document lucene_doc, XmlPostEditor xml_post_editor, IndexFilter index_filter) throws ParserConfigurationException, TransformerException, IOException, PgSchemaException {

		Node node = getRootNode(xml_parser, xml_post_editor, index_filter);

		initTableLock(true);

		tables.forEach(table -> table.lucene_doc = table.required ? lucene_doc : null);

		// parse root node and store into Lucene document

		PgSchemaNode2LucIdx node2lucidx = new PgSchemaNode2LucIdx(this, null, root_table);

		node2lucidx.parseRootNode(node);

		node2lucidx.invokeRootNestedNode();

	}

	/**
	 * Close xml2LucIdx.
	 */
	public void closeXml2LucIdx() {

		closeTableLock();

		tables.stream().filter(table -> table.lucene_doc != null).forEach(table -> table.lucene_doc = null);

	}

	/**
	 * Parse current node and store into Lucene document.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void parseChildNode2LucIdx(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) throws ParserConfigurationException, TransformerException, IOException {

		PgSchemaNode2LucIdx node2lucidx = new PgSchemaNode2LucIdx(this, parent_table, table);

		for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

			if (node.getNodeType() != Node.ELEMENT_NODE)
				continue;

			PgSchemaNodeTester node_test = new PgSchemaNodeTester(this, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

			if (node_test.omitted)
				continue;

			synchronized (table_lock[0]) {
				node2lucidx.parseChildNode(node_test);
			}

			node2lucidx.invokeChildNestedNode(node_test);

			if (node_test.isLastNode())
				break;

		}

		if (!node2lucidx.invoked) {

			synchronized (table_lock[0]) {
				node2lucidx.parseChildNode(parent_node, parent_key, proc_key, nested);
			}

			node2lucidx.invokeChildNestedNode();

		}

	}

	// Sphinx full-text indexing

	/**
	 * Extract Sphinx schema part and map sphinx:field and sphinx:attr in PgSchema.
	 *
	 * @param sph_doc Sphinx xmlpipe2 file
	 * @throws PgSchemaException the pg schema exception
	 */
	public void syncSphSchema(Document sph_doc) throws PgSchemaException {

		// check /sphinx:schema element

		Node node = sph_doc.getDocumentElement();

		if (!node.getNodeName().equals("sphinx:schema"))
			throw new PgSchemaException("Not found sphinx:schema root element in Sphinx data source: " + PgSchemaUtil.sphinx_schema_name);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			String node_name = child.getNodeName();
			boolean sph_field = node_name.equals("sphinx:field");
			boolean sph_attr = node_name.equals("sphinx:attr");

			if (sph_field || sph_attr) {

				if (child.hasAttributes()) {

					Element e = (Element) child;

					String[] name_attr = e.getAttribute("name").replaceFirst("__", "\\.").split("\\.");

					if (name_attr.length != 2)
						continue;

					String table_name = name_attr[0];
					String field_name = name_attr[1];

					PgTable table = getTable(table_name);

					if (table == null)
						throw new PgSchemaException("Not found " + table_name + " table.");

					PgField field = table.getField(field_name);

					if (field == null)
						throw new PgSchemaException("Not found " + field_name + " field.");

					field.sph_attr = sph_attr;

					String type_attr = e.getAttribute("type");

					if (type_attr != null && type_attr.contains("multi"))
						field.sph_mva = true;

				}

			}

		}

	}

	/**
	 * Write Sphinx schema part.
	 *
	 * @param sphinx_schema Sphinx xmlpipe2 file
	 * @param data_source whether it is data source or schema
	 * @throws PgSchemaException the pg schema exception
	 */
	public void writeSphSchema(File sphinx_schema, boolean data_source) throws PgSchemaException {

		try {

			FileWriter filew = new FileWriter(sphinx_schema);

			filew.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");

			if (data_source)
				filew.write("<sphinx:docset>\n");

			filew.write("<sphinx:schema>\n");

			if (data_source) {

				filew.write("<sphinx:attr name=\"" + PgSchemaUtil.document_key_name + "\" type=\"string\"/>\n"); // default attr
				filew.write("<sphinx:field name=\"" + PgSchemaUtil.simple_cont_name + "\"/>\n"); // default field

			}

			tables.forEach(table -> table.fields.stream().filter(field -> field.sph_attr).forEach(field -> {

				try {

					filew.write("<sphinx:attr name=\"" + table.name + "__" + field.xname + "\"");

					String attrs = null;

					switch (field.xs_type) {
					case xs_boolean:
						attrs = " type=\"bool\"";
						break;
					case xs_hexBinary:
					case xs_base64Binary:
						attrs = " type=\"string\"";
						break;
					case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_duration:
						attrs = " type=\"bigint\"";
						break;
					case xs_serial:
					case xs_integer:
					case xs_int:
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
						attrs = " type=\"int\" bits=\"32\"";
						break;
					case xs_unsignedInt:
						attrs = " type=\"int\" bits=\"32\"";
						break;
					case xs_float:
					case xs_double:
					case xs_decimal:
						attrs = " type=\"float\"";
						break;
					case xs_short:
						attrs = " type=\"int\" bits=\"16\"";
						break;
					case xs_unsignedShort:
						attrs = " type=\"int\" bits=\"16\"";
						break;
					case xs_byte:
						attrs = " type=\"int\" bits=\"8\"";
						break;
					case xs_unsignedByte:
						attrs = " type=\"int\" bits=\"8\"";
						break;
					case xs_dateTime:
					case xs_time:
					case xs_date:
					case xs_gYearMonth:
					case xs_gYear:
						attrs = " type=\"timestamp\"";
						break;
					case xs_gMonthDay:
					case xs_gMonth:
					case xs_gDay:
					case xs_anyType:
					case xs_string:
					case xs_normalizedString:
					case xs_token:
					case xs_language:
					case xs_Name:
					case xs_QName:
					case xs_NCName:
					case xs_anyURI:
					case xs_NOTATION:
					case xs_NMTOKEN:
					case xs_NMTOKENS:
					case xs_ID:
					case xs_IDREF:
					case xs_IDREFS:
					case xs_ENTITY:
					case xs_ENTITIES:
						attrs = " type=\"string\"";
						break;
					default: // xs_any, xs_anyAttribute
					}

					if (field.sph_mva) {
						/**
					if (attrs.contains("bigint"))
						attrs = " type=\"multi64\"";
					else
						 */
						attrs = " type=\"multi\"";

					}

					if (field.default_value != null && !field.default_value.isEmpty())
						attrs += " default=\"" + StringEscapeUtils.escapeCsv(field.default_value) + "\"";

					filew.write(attrs + "/>\n");

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}));

			filew.write("</sphinx:schema>\n");

			filew.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Write Sphinx configuration file.
	 *
	 * @param sphinx_conf Sphinx configuration file
	 * @param idx_name name of Sphinx index
	 * @param data_source Sphinx xmlpipe2 file
	 * @throws PgSchemaException the pg schema exception
	 */
	public void writeSphConf(File sphinx_conf, String idx_name, File data_source) throws PgSchemaException {

		try {

			FileWriter filew = new FileWriter(sphinx_conf);

			filew.write("#\n# Sphinx configuration file sample\n#\n# WARNING! While this sample file mentions all available options,\n#\n# it contains (very) short helper descriptions only. Please refer to\n# doc/sphinx.html for details.\n#\n\n#############################################################################\n## data source definition\n#############################################################################\n\n");

			filew.write("source " + idx_name + "\n{\n");
			filew.write("\ttype                    = xmlpipe2\n");
			filew.write("\txmlpipe_command         = cat " + data_source.getAbsolutePath() + "\n");
			filew.write("\txmlpipe_attr_string     = " + PgSchemaUtil.document_key_name + "\n");
			filew.write("\txmlpipe_field           = " + PgSchemaUtil.simple_cont_name + "\n");

			tables.forEach(table -> table.fields.stream().filter(field -> field.sph_attr).forEach(field -> {

				String attr_name = table.name + "__" + field.xname;

				try {

					switch (field.xs_type) {
					case xs_boolean:
						filew.write("\txmlpipe_attr_bool       = " + attr_name + "\n");
						break;
					case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_duration:
						if (field.sph_mva)
							filew.write("\txmlpipe_attr_multi_64   = " + attr_name + "\n");
						else
							filew.write("\txmlpipe_attr_bigint     = " + attr_name + "\n");
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
					case xs_unsignedShort:
					case xs_byte:
					case xs_unsignedByte:
						if (field.sph_mva)
							filew.write("\txmlpipe_attr_multi      = " + attr_name + "\n");
						else
							filew.write("\txmlpipe_attr_uint       = " + attr_name + "\n");
						break;
					case xs_float:
					case xs_double:
					case xs_decimal:
						filew.write("\txmlpipe_attr_float      = " + attr_name + "\n");
						break;
					case xs_dateTime:
					case xs_time:
					case xs_date:
					case xs_gYearMonth:
					case xs_gYear:
						filew.write("\txmlpipe_attr_timestamp  = " + attr_name + "\n");
						break;
					case xs_hexBinary:
					case xs_base64Binary:
					case xs_gMonthDay:
					case xs_gMonth:
					case xs_gDay:
					case xs_anyType:
					case xs_string:
					case xs_normalizedString:
					case xs_token:
					case xs_language:
					case xs_Name:
					case xs_QName:
					case xs_NCName:
					case xs_anyURI:
					case xs_NOTATION:
					case xs_NMTOKEN:
					case xs_NMTOKENS:
					case xs_ID:
					case xs_IDREF:
					case xs_IDREFS:
					case xs_ENTITY:
					case xs_ENTITIES:
						filew.write("\txmlpipe_attr_string     = " + attr_name + "\n");
						break;
					default: // xs_any, xs_anyAttribute
					}

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}));

			filew.write("}\n\n");

			filew.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Sphinx xmlpipe2 conversion.
	 *
	 * @param xml_parser XML document
	 * @param writer writer of Sphinx xmlpipe2 file
	 * @param xml_post_editor XML post editing
	 * @param index_filter index filter
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2SphDs(XmlParser xml_parser, FileWriter writer, XmlPostEditor xml_post_editor, IndexFilter index_filter) throws ParserConfigurationException, TransformerException, IOException, PgSchemaException {

		Node node = getRootNode(xml_parser, xml_post_editor, index_filter);

		initTableLock(true);

		tables.forEach(table -> {

			if (table.required) {

				table.filew = writer;

				table.fields.forEach(field -> field.sph_attr_occurs = 0);

			}

			else
				table.filew = null;

		});

		// parse root node and write to Sphinx xmlpipe2 file

		PgSchemaNode2SphDs node2sphds = new PgSchemaNode2SphDs(this, null, root_table);

		node2sphds.parseRootNode(node);

		node2sphds.invokeRootNestedNode();

	}

	/**
	 * Close xml2SphDs.
	 */
	public void closeXml2SphDs() {

		closeTableLock();

		tables.stream().filter(table -> table.filew != null).forEach(table -> table.filew = null);

	}

	/**
	 * Parse current node and store to Sphinx xmlpipe2 file.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void parseChildNode2SphDs(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) throws ParserConfigurationException, TransformerException, IOException {

		PgSchemaNode2SphDs node2sphds = new PgSchemaNode2SphDs(this, parent_table, table);

		for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

			if (node.getNodeType() != Node.ELEMENT_NODE)
				continue;

			PgSchemaNodeTester node_test = new PgSchemaNodeTester(this, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

			if (node_test.omitted)
				continue;

			synchronized (table_lock[0]) {
				node2sphds.parseChildNode(node_test);
			}

			node2sphds.invokeChildNestedNode(node_test);

			if (node_test.isLastNode())
				break;

		}

		if (!node2sphds.invoked) {

			synchronized (table_lock[0]) {
				node2sphds.parseChildNode(parent_node, parent_key, proc_key, nested);
			}

			node2sphds.invokeChildNestedNode();

		}

	}

	/**
	 * Return whether Sphinx attribute.
	 *
	 * @param table_name table name
	 * @param field_name field name
	 * @return boolean whether Sphinx attribute
	 */
	public boolean isSphAttr(String table_name, String field_name) {

		PgTable table = getTable(table_name);

		if (table == null)
			return false;

		PgField field = table.getField(field_name);

		if (field == null)
			return false;

		return field.sph_attr;
	}

	/**
	 * Return whether Sphinx multi-valued attribute.
	 *
	 * @param table_name table name
	 * @param field_name field name
	 * @return boolean whether Sphinx multi-valued attribute
	 */
	public boolean isSphMVAttr(String table_name, String field_name) {

		PgTable table = getTable(table_name);

		if (table == null)
			return false;

		PgField field = table.getField(field_name);

		if (field == null)
			return false;

		if (!field.sph_attr)
			return false;

		return field.sph_mva;
	}

	// JSON conversion

	/** The JSON builder. */
	protected JsonBuilder jsonb = null;

	/**
	 * Initialize JSON builder.
	 *
	 * @param option the option
	 */
	public void initJsonBuilder(JsonBuilderOption option) {

		jsonb = new JsonBuilder(option);

	}

	/**
	 * Clear JSON builder.
	 */
	private void clearJsonBuilder() {

		jsonb.clear();

		tables.stream().filter(table -> table.required && table.cont_holder).forEach(table -> {

			table.jsonb_not_empty = false;

			table.fields.forEach(field -> {

				field.jsonb_not_empty = false;

				if (field.jsonb == null)
					field.jsonb = new StringBuilder();
				else if (field.jsonb.length() > 0)
					field.jsonb.setLength(0);

				field.jsonb_col_size = field.jsonb_null_size = 0;

			});

		});

	}

	/**
	 * Close JSON builder.
	 */
	private void closeJsonBuilder() {

		jsonb.close();

		tables.stream().filter(table -> table.required && table.cont_holder).forEach(table -> table.fields.stream().filter(field -> field.jsonb != null).forEach(field -> {

			if (field.jsonb.length() > 0)
				field.jsonb.setLength(0);

			field.jsonb = null;

		}));

	}

	// Object-oriented JSON conversion

	/**
	 * Realize Object-oriented JSON Schema.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void realizeObjJsonSchema() throws PgSchemaException {

		hasRootTable();

		List<PgField> fields = root_table.fields;

		System.out.print("{" + jsonb.linefeed); // JSON document start

		System.out.print(jsonb.getIndentSpaces(1) + "\"$schema\":" + jsonb.key_value_space + "\"" + PgSchemaUtil.json_schema_def + "\"," + jsonb.linefeed); // declaring a JSON Schema

		if (def_namespaces != null) {

			String def_namespace = def_namespaces.get("");

			if (def_namespace != null)
				System.out.print(jsonb.getIndentSpaces(1) + "\"id\":" + jsonb.key_value_space + "\"" + def_namespace + "\"," + jsonb.linefeed); // declaring a unique identifier

		}

		if (def_appinfo != null) {

			String _def_appinfo = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(def_appinfo).replaceAll("\\\\/", "/").replaceAll("\\\\'", "'"));

			if (!_def_appinfo.startsWith("\""))
				_def_appinfo = "\"" + _def_appinfo + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + _def_appinfo + "," + jsonb.linefeed);

		}

		if (def_doc != null) {

			String _def_doc = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(def_doc).replaceAll("\\\\/", "/").replaceAll("\\\\'", "'"));

			if (!_def_doc.startsWith("\""))
				_def_doc = "\"" + _def_doc + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + _def_doc + "," + jsonb.linefeed);

		}

		if (!root_table.virtual) {

			System.out.print(jsonb.getIndentSpaces(1) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.linefeed);

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + "\"" + root_table.name + "\"," + jsonb.linefeed);

			if (root_table.anno != null && !root_table.anno.isEmpty()) {

				String table_anno = StringEscapeUtils.escapeCsv(root_table.anno.replaceAll("\n--", ""));

				if (!table_anno.startsWith("\""))
					table_anno = "\"" + table_anno + "\"";

				System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.linefeed);

			}

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_cont ? jsonb.simple_cont_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(1) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_spaces + 1)) + "]," + jsonb.linefeed);

			jsonb.builder.setLength(0);

		}

		if (!root_table.virtual)
			System.out.print(jsonb.getIndentSpaces(1) + "\"items\": {" + jsonb.linefeed); // JSON own items start

		fields.stream().filter(field -> !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> jsonb.writeSchemaFieldProperty(field, true, false, 2));

		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.linefeed.equals("\n") ? 2 : 1) + (root_table.virtual ? 1 : 0)) + jsonb.linefeed);

		jsonb.builder.setLength(0);

		if (!root_table.virtual)
			System.out.print(jsonb.getIndentSpaces(2) + "\"items\":" + jsonb.key_value_space + "{" + jsonb.linefeed); // JSON child items start

		int[] list_id = { 0 };

		fields.stream().filter(field -> field.nested_key).forEach(field -> realizeObjJsonSchema(root_table, tables.get(field.foreign_table_id), list_id[0]++, root_table.nested_fields, root_table.virtual ? 1 : 3));

		if (!root_table.virtual) {

			System.out.print(jsonb.getIndentSpaces(2) + "}" + jsonb.linefeed); // JSON child items end

			System.out.print(jsonb.getIndentSpaces(1) + "}" + jsonb.linefeed); // JSON own items end

		}

		System.out.print("}" + jsonb.linefeed); // JSON document end

		jsonb.builder.setLength(0);

	}

	/**
	 * Realize Object-oriented JSON Schema (child).
	 *
	 * @param parent_table parent table
	 * @param table current table
	 * @param list_id the list id
	 * @param list_size the list size
	 * @param json_indent_level current indent level
	 */
	private void realizeObjJsonSchema(final PgTable parent_table, final PgTable table, final int list_id, final int list_size, int json_indent_level) {

		List<PgField> fields = table.fields;

		if (table.list_holder)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"type\":" + jsonb.key_value_space + "\"array\"," + jsonb.linefeed);

		if (!table.virtual) {

			if (!parent_table.list_holder)
				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.linefeed);

			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"title\":" + jsonb.key_value_space + "\"" + table.name + "\"," + jsonb.linefeed);

			if (table.anno != null && !table.anno.isEmpty()) {

				String table_anno = StringEscapeUtils.escapeCsv(table.anno.replaceAll("\n--", ""));

				if (!table_anno.startsWith("\""))
					table_anno = "\"" + table_anno + "\"";

				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.linefeed);

			}

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_cont ? jsonb.simple_cont_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_spaces + 1)) + "]," + jsonb.linefeed);

			jsonb.builder.setLength(0);

		}

		if (!table.virtual)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"items\": {" + jsonb.linefeed); // JSON own object start

		fields.stream().filter(field -> !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> jsonb.writeSchemaFieldProperty(field, true, false, json_indent_level + (table.virtual ? 0 : 1)));

		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.linefeed.equals("\n") ? 2 : 1) + (table.virtual ? 1 : 0)) + jsonb.linefeed);

		jsonb.builder.setLength(0);

		if (table.nested_fields > 0) {

			if (!table.virtual)
				System.out.print(jsonb.getIndentSpaces(json_indent_level + 1) + "\"items\":" + jsonb.key_value_space + "{" + jsonb.linefeed); // JSON child items start

			int[] _list_id = { 0 };

			fields.stream().filter(field -> field.nested_key).forEach(field -> realizeObjJsonSchema(table, tables.get(field.foreign_table_id), _list_id[0]++, table.nested_fields, json_indent_level + (table.virtual ? 0 : 2)));

			if (!table.virtual)
				System.out.print(jsonb.getIndentSpaces(json_indent_level + 1) + "}" + jsonb.linefeed); // JSON child items end

		}

		if (!table.virtual)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "}" + (list_id < list_size - 1 ? "," : "") + jsonb.linefeed); // JSON own items end

	}

	/**
	 * Object-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file_name JSON file name
	 * @param xml_post_editor XML post editing
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2ObjJson(XmlParser xml_parser, String json_file_name, XmlPostEditor xml_post_editor) throws ParserConfigurationException, TransformerException, IOException, PgSchemaException {

		Node node = getRootNode(xml_parser, xml_post_editor);

		initTableLock(true);
		clearJsonBuilder();

		// parse root node and store to JSON builder

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

		node2json.parseRootNode(node);

		if (node2json.filled) {

			int json_indent_level = 0;

			int jsonb_header_end = jsonb.writeHeader(root_table, true, ++json_indent_level);
			jsonb.writeContent(root_table, json_indent_level + 1);

			node2json.invokeRootNestedNodeObj(json_indent_level);

			jsonb.writeFooter(root_table, json_indent_level--, 0, jsonb_header_end);

		}

		try {

			FileWriter json_writer = new FileWriter(new File(json_file_name));

			json_writer.write("{" + jsonb.linefeed); // JSON document start

			json_writer.write(jsonb.builder.toString());

			json_writer.write("}" + jsonb.linefeed); // JSON document end

			json_writer.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close xml2Json.
	 */
	public void closeXml2Json() {

		closeTableLock();
		closeJsonBuilder();

	}

	/**
	 * Parse current node and store to JSON builder (Object-oriented JSON format).
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node
	 * @param json_indent_level current indent level
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void parseChildNode2ObjJson(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id, int json_indent_level) throws ParserConfigurationException, TransformerException, IOException {

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, parent_table, table);

		node2json.jsonb_header_begin = node2json.jsonb_header_end = jsonb.builder.length();

		if (!table.virtual && table.bridge) {
			node2json.jsonb_header_end = jsonb.writeArrayHeader(table, ++json_indent_level);
			++json_indent_level;
		}

		for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

			if (node.getNodeType() != Node.ELEMENT_NODE)
				continue;

			PgSchemaNodeTester node_test = new PgSchemaNodeTester(this, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

			if (node_test.omitted)
				continue;

			synchronized (table_lock[0]) {

				node2json.parseChildNode(node_test);

				if (node2json.written) {

					if (!table.virtual) {

						int _jsonb_header_begin = jsonb.builder.length();
						int _jsonb_header_end = jsonb.writeHeader(table, true, json_indent_level);
						jsonb.writeContent(table, json_indent_level + 1);
						jsonb.writeFooter(table, json_indent_level, _jsonb_header_begin, _jsonb_header_end);

					}

					else
						jsonb.writeContent(table, json_indent_level + 1);

				}

			}

			if (node_test.isLastNode()) {

				if (node2json.filled)
					node2json.invokeChildNestedNodeObj(node_test, json_indent_level);

				break;
			}

		}

		if (!node2json.invoked) {

			int _jsonb_header_begin = jsonb.builder.length();
			int _jsonb_header_end = node2json.jsonb_header_end;

			synchronized (table_lock[0]) {

				node2json.parseChildNode(parent_node, parent_key, proc_key, nested);

				if (node2json.written) {

					if (!table.virtual)
						_jsonb_header_end = jsonb.writeHeader(table, !parent_table.bridge, json_indent_level);

					jsonb.writeContent(table, json_indent_level + 1);

				}

			}

			if (node2json.filled) {

				node2json.invokeChildNestedNodeObj(json_indent_level);

				if (!table.virtual)
					jsonb.writeFooter(table, json_indent_level + 1, _jsonb_header_begin, _jsonb_header_end);

			}

		}

		if (!table.virtual && table.bridge) {
			json_indent_level--;
			jsonb.writeArrayFooter(table, json_indent_level--, node2json.jsonb_header_begin, node2json.jsonb_header_end);
		}

	}

	// Column-oriented JSON conversion

	/**
	 * Realize Column-oriented JSON Schema.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void realizeColJsonSchema() throws PgSchemaException {

		hasRootTable();

		List<PgField> fields = root_table.fields;

		System.out.print("{" + jsonb.linefeed); // JSON document start

		System.out.print(jsonb.getIndentSpaces(1) + "\"$schema\":" + jsonb.key_value_space + "\"" + PgSchemaUtil.json_schema_def + "\"," + jsonb.linefeed); // declaring a JSON Schema

		if (def_namespaces != null) {

			String def_namespace = def_namespaces.get("");

			if (def_namespace != null)
				System.out.print(jsonb.getIndentSpaces(1) + "\"id\":" + jsonb.key_value_space + "\"" + def_namespace + "\"," + jsonb.linefeed); // declaring a unique identifier

		}

		if (def_appinfo != null) {

			String _def_appinfo = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(def_appinfo).replaceAll("\\\\/", "/").replaceAll("\\\\'", "'"));

			if (!_def_appinfo.startsWith("\""))
				_def_appinfo = "\"" + _def_appinfo + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + _def_appinfo + "," + jsonb.linefeed);

		}

		if (def_doc != null) {

			String _def_doc = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(def_doc).replaceAll("\\\\/", "/").replaceAll("\\\\'", "'"));

			if (!_def_doc.startsWith("\""))
				_def_doc = "\"" + _def_doc + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + _def_doc + "," + jsonb.linefeed);

		}

		if (!root_table.virtual) {

			System.out.print(jsonb.getIndentSpaces(1) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.linefeed);

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + "\"" + root_table.name + "\"," + jsonb.linefeed);

			if (root_table.anno != null && !root_table.anno.isEmpty()) {

				String table_anno = StringEscapeUtils.escapeCsv(root_table.anno.replaceAll("\n--", ""));

				if (!table_anno.startsWith("\""))
					table_anno = "\"" + table_anno + "\"";

				System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.linefeed);

			}

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_cont ? jsonb.simple_cont_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(1) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_spaces + 1)) + "]," + jsonb.linefeed);

			jsonb.builder.setLength(0);

		}

		if (!root_table.virtual)
			System.out.print(jsonb.getIndentSpaces(1) + "\"items\": {" + jsonb.linefeed); // JSON own items start

		fields.stream().filter(field -> !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> jsonb.writeSchemaFieldProperty(field, !field.list_holder, field.list_holder, 2));

		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.linefeed.equals("\n") ? 2 : 1) + (root_table.virtual ? 1 : 0)) + jsonb.linefeed);

		jsonb.builder.setLength(0);

		if (!root_table.virtual)
			System.out.print(jsonb.getIndentSpaces(2) + "\"items\":" + jsonb.key_value_space + "{" + jsonb.linefeed); // JSON child items start

		int[] list_id = { 0 };

		fields.stream().filter(field -> field.nested_key).forEach(field -> realizeColJsonSchema(root_table, tables.get(field.foreign_table_id), list_id[0]++, root_table.nested_fields, root_table.virtual ? 1 : 3));

		if (!root_table.virtual) {

			System.out.print(jsonb.getIndentSpaces(2) + "}" + jsonb.linefeed); // JSON child items end

			System.out.print(jsonb.getIndentSpaces(1) + "}" + jsonb.linefeed); // JSON own items end

		}

		System.out.print("}" + jsonb.linefeed); // JSON document end

		jsonb.builder.setLength(0);

	}

	/**
	 * Realize Column-oriented JSON Schema (child).
	 *
	 * @param parent_table parent table
	 * @param table current table
	 * @param list_id the list id
	 * @param list_size the list size
	 * @param json_indent_level current indent level
	 */
	private void realizeColJsonSchema(final PgTable parent_table, final PgTable table, final int list_id, final int list_size, final int json_indent_level) {

		List<PgField> fields = table.fields;

		boolean obj_json = table.virtual || !jsonb.array_all;

		if (!table.virtual) {

			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.linefeed);

			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"title\":" + jsonb.key_value_space + "\"" + table.name + "\"," + jsonb.linefeed);

			if (table.anno != null && !table.anno.isEmpty()) {

				String table_anno = StringEscapeUtils.escapeCsv(table.anno.replaceAll("\n--", ""));

				if (!table_anno.startsWith("\""))
					table_anno = "\"" + table_anno + "\"";

				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.linefeed);

			}

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_cont ? jsonb.simple_cont_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_spaces + 1)) + "]," + jsonb.linefeed);

			jsonb.builder.setLength(0);

		}

		if (!table.virtual)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"items\": {" + jsonb.linefeed); // JSON own items start

		fields.stream().filter(field -> !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> jsonb.writeSchemaFieldProperty(field, obj_json && !field.list_holder, !table.virtual || field.list_holder, json_indent_level + (table.virtual ? 0 : 1)));

		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.linefeed.equals("\n") ? 2 : 1) + (table.virtual ? 1 : 0)) + jsonb.linefeed);

		jsonb.builder.setLength(0);

		if (table.nested_fields > 0) {

			if (!table.virtual)
				System.out.print(jsonb.getIndentSpaces(json_indent_level + 1) + "\"items\":" + jsonb.key_value_space + "{" + jsonb.linefeed); // JSON child items start

			int[] _list_id = { 0 };

			fields.stream().filter(field -> field.nested_key).forEach(field -> realizeColJsonSchema(table, tables.get(field.foreign_table_id), _list_id[0]++, table.nested_fields, json_indent_level + (table.virtual ? 0 : 2)));

			if (!table.virtual)
				System.out.print(jsonb.getIndentSpaces(json_indent_level + 1) + "}" + jsonb.linefeed); // JSON child items end

		}

		if (!table.virtual)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "}" + jsonb.linefeed); // JSON own items end

	}

	/**
	 * Column-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file_name JSON file name
	 * @param xml_post_editor XML post editing
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2ColJson(XmlParser xml_parser, String json_file_name, XmlPostEditor xml_post_editor) throws ParserConfigurationException, TransformerException, IOException, PgSchemaException {

		Node node = getRootNode(xml_parser, xml_post_editor);

		initTableLock(true);
		clearJsonBuilder();

		// parse root node and write to JSON file

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

		node2json.parseRootNode(node);

		if (node2json.filled) {

			int json_indent_level = 0;

			int jsonb_header_end = jsonb.writeHeader(root_table, true, ++json_indent_level);
			jsonb.writeContent(root_table, json_indent_level + 1);

			node2json.invokeRootNestedNodeCol(json_indent_level);

			jsonb.writeFooter(root_table, json_indent_level--, 0, jsonb_header_end);

		}

		try {

			FileWriter json_writer = new FileWriter(new File(json_file_name));

			json_writer.write("{" + jsonb.linefeed); // JSON document start

			json_writer.write(jsonb.builder.toString());

			json_writer.write("}" + jsonb.linefeed); // JSON document end

			json_writer.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Parse current node and store to JSON builder (Column-oriented JSON format).
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node
	 * @param json_indent_level current indent level
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void parseChildNode2ColJson(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id, int json_indent_level) throws ParserConfigurationException, TransformerException, IOException {

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, parent_table, table);

		if (!table.virtual) {

			node2json.jsonb_header_begin = jsonb.builder.length();
			node2json.jsonb_header_end = jsonb.writeHeader(table, true, json_indent_level);

		}

		for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

			if (node.getNodeType() != Node.ELEMENT_NODE)
				continue;

			PgSchemaNodeTester node_test = new PgSchemaNodeTester(this, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

			if (node_test.omitted)
				continue;

			synchronized (table_lock[0]) {
				node2json.parseChildNode(node_test);
			}

			if (node_test.isLastNode()) {

				if (node2json.filled) {

					if (table.jsonb_not_empty)
						jsonb.writeContent(table, json_indent_level + (table.virtual ? 0 : 1));

					node2json.invokeChildNestedNodeCol(node_test, json_indent_level);

				}

				break;
			}

		}

		if (!node2json.invoked) {

			synchronized (table_lock[0]) {

				node2json.parseChildNode(parent_node, parent_key, proc_key, nested);

				if (node2json.written)
					jsonb.writeContent(table, json_indent_level + (table.virtual ? 0 : 1));

			}

			node2json.invokeChildNestedNodeCol(json_indent_level);

		}

		if (!table.virtual)
			jsonb.writeFooter(table, json_indent_level, node2json.jsonb_header_begin, node2json.jsonb_header_end);

	}

	// Relational-oriented JSON conversion

	/**
	 * Realize Relational-oriented JSON Schema.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void realizeRelJsonSchema() throws PgSchemaException {

		hasRootTable();

		System.out.print("{" + jsonb.linefeed); // JSON document start

		System.out.print(jsonb.getIndentSpaces(1) + "\"$schema\":" + jsonb.key_value_space + "\"" + PgSchemaUtil.json_schema_def + "\"," + jsonb.linefeed); // declaring a JSON Schema

		if (def_namespaces != null) {

			String def_namespace = def_namespaces.get("");

			if (def_namespace != null)
				System.out.print(jsonb.getIndentSpaces(1) + "\"id\":" + jsonb.key_value_space + "\"" + def_namespace + "\"," + jsonb.linefeed); // declaring a unique identifier

		}

		if (def_appinfo != null) {

			String _def_appinfo = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(def_appinfo).replaceAll("\\\\/", "/").replaceAll("\\\\'", "'"));

			if (!_def_appinfo.startsWith("\""))
				_def_appinfo = "\"" + _def_appinfo + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + _def_appinfo + "," + jsonb.linefeed);

		}

		if (def_doc != null) {

			String _def_doc = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(def_doc).replaceAll("\\\\/", "/").replaceAll("\\\\'", "'"));

			if (!_def_doc.startsWith("\""))
				_def_doc = "\"" + _def_doc + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + _def_doc + "," + jsonb.linefeed);

		}

		boolean has_category = false;

		for (level = 0; level < tables.size(); level++) {

			for (int t = 0; t < tables.size(); t++) {

				if (table_order[t] == level) {

					PgTable table = tables.get(t);

					if (!table.cont_holder)
						continue;

					if (has_category)
						System.out.print("," + jsonb.linefeed);

					realizeRelJsonSchema(table, 1);

					has_category = true;

				}

			}

		}

		System.out.print(jsonb.linefeed + "}" + jsonb.linefeed); // JSON document end

		jsonb.builder.setLength(0);

	}

	/**
	 * Realize Relational-oriented JSON Schema (child).
	 *
	 * @param table current table
	 * @param json_indent_level current indent level
	 */
	private void realizeRelJsonSchema(final PgTable table, final int json_indent_level) {

		List<PgField> fields = table.fields;

		boolean obj_json = table.virtual || !jsonb.array_all;

		System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.linefeed);

		System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"title\":" + jsonb.key_value_space + "\"" + table.name + "\"," + jsonb.linefeed);

		if (table.anno != null && !table.anno.isEmpty()) {

			String table_anno = StringEscapeUtils.escapeCsv(table.anno.replaceAll("\n--", ""));

			if (!table_anno.startsWith("\""))
				table_anno = "\"" + table_anno + "\"";

			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.linefeed);

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_cont ? jsonb.simple_cont_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_spaces + 1)) + "]," + jsonb.linefeed);

			jsonb.builder.setLength(0);

		}

		System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"items\": {" + jsonb.linefeed); // JSON own items start

		fields.stream().filter(field -> !field.user_key && !field.system_key && !(jsonb.has_discard_doc_key && field.xname.equals(jsonb.discard_doc_key))).forEach(field -> {

			if (table.xs_type.equals(XsTableType.xs_root))
				jsonb.writeSchemaFieldProperty(field, !field.list_holder, field.list_holder, json_indent_level + 1);
			else
				jsonb.writeSchemaFieldProperty(field, obj_json && !field.list_holder, !table.virtual || field.list_holder, json_indent_level + 1);

		});


		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.linefeed.equals("\n") ? 2 : 1)) + jsonb.linefeed);

		jsonb.builder.setLength(0);

		System.out.print(jsonb.getIndentSpaces(json_indent_level) + "}"); // JSON own items end

	}

	/**
	 * Relational-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file_name JSON file name
	 * @param xml_post_editor XML post editing
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2RelJson(XmlParser xml_parser, String json_file_name, XmlPostEditor xml_post_editor) throws ParserConfigurationException, TransformerException, IOException, PgSchemaException {

		Node node = getRootNode(xml_parser, xml_post_editor);

		initTableLock(false);
		clearJsonBuilder();

		// parse root node and write to JSON file

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

		node2json.parseRootNode(node);

		node2json.invokeRootNestedNode();

		try {

			FileWriter json_writer = new FileWriter(new File(json_file_name));

			json_writer.write("{" + jsonb.linefeed); // JSON document start

			boolean has_category = false;

			for (level = 0; level < tables.size(); level++) {

				for (int t = 0; t < tables.size(); t++) {

					if (table_order[t] == level) {

						PgTable _table = tables.get(t);

						if (!_table.jsonb_not_empty)
							continue;

						boolean array_json = !_table.virtual && jsonb.array_all;

						if (has_category)
							json_writer.write("," + jsonb.linefeed);

						json_writer.write(jsonb.getIndentSpaces(1) + "\"" + _table.name + "\":" + jsonb.key_value_space + "{" + jsonb.linefeed); // JSON object start

						boolean has_field = false;

						List<PgField> _fields = _table.fields;

						for (int f = 0; f < _fields.size(); f++) {

							PgField _field = _fields.get(f);

							if (!_field.jsonb_not_empty || _field.jsonb == null)
								continue;

							if (_field.jsonb_col_size > 0 && _field.jsonb.length() > 2) {

								boolean array_field = (t != root_table_id && array_json) || _field.list_holder || _field.jsonb_col_size > 1;

								if (has_field)
									json_writer.write("," + jsonb.linefeed);

								json_writer.write(jsonb.getIndentSpaces(2) + "\"" + (_field.attribute || _field.any_attribute ? jsonb.attr_prefix : "") + (_field.simple_cont ? jsonb.simple_cont_key : _field.xname) + "\":" + jsonb.key_value_space + (array_field ? "[" : ""));

								_field.jsonb.setLength(_field.jsonb.length() - (jsonb.key_value_spaces + 1));
								json_writer.write(_field.jsonb.toString());

								if (array_field)
									json_writer.write("]");

								has_field = true;

							}

							if (_field.jsonb.length() > 0)
								_field.jsonb.setLength(0);

						}

						if (has_field)
							json_writer.write(jsonb.linefeed);

						json_writer.write(jsonb.getIndentSpaces(1) + "}"); // JSON object end

						has_category = true;

					}

				}

			}

			if (has_category)
				json_writer.write(jsonb.linefeed);

			json_writer.write("}" + jsonb.linefeed); // JSON document end

			json_writer.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Parse current node and store to JSON builder (Relational-oriented JSON format).
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ParserConfigurationException the parser configuration exception
	 */
	protected void parseChildNode2Json(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) throws TransformerException, IOException, ParserConfigurationException {

		final int table_id = getTableId(table.name);

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, parent_table, table);

		for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

			if (node.getNodeType() != Node.ELEMENT_NODE)
				continue;

			PgSchemaNodeTester node_test = new PgSchemaNodeTester(this, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

			if (node_test.omitted)
				continue;

			synchronized (table_lock[table_id]) {
				node2json.parseChildNode(node_test);
			}

			node2json.invokeChildNestedNode(node_test);

			if (node_test.isLastNode())
				break;

		}

		if (!node2json.invoked) {

			synchronized (table_lock[table_id]) {
				node2json.parseChildNode(parent_node, parent_key, proc_key, nested);
			}

			node2json.invokeChildNestedNode();

		}

	}

	// Schema-aware XPath parser

	/**
	 * Test nodes of XPath tree against schema.
	 *
	 * @param list XPath component list as serialized XPath tree
	 * @param ends_with_text whether append text node in the ends, if possible
	 * @param verbose whether output parse tree for predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	public void testXPathNodes(XPathCompList list, boolean ends_with_text, boolean verbose) throws PgSchemaException {

		hasRootTable();

		for (int union_id = 0; union_id <= list.getLastUnionId(); union_id++) {

			for (int step_id = 0; step_id <= list.getLastStepId(union_id); step_id++) {

				XPathComp[] comps = list.arrayOf(union_id, step_id);

				for (XPathComp comp : comps) {

					Class<?> anyClass = comp.tree.getClass();

					// TerminalNodeImpl node

					if (anyClass.equals(TerminalNodeImpl.class))
						list.testTerminalNodeImpl(comp, false);

					// AbbreviatedStepContext node

					else if (anyClass.equals(AbbreviatedStepContext.class))
						list.testAbbreviateStepContext(comp, false);

					// AxisSpecifierContext node

					else if (anyClass.equals(AxisSpecifierContext.class))
						list.testAxisSpecifierContext(comp, comps);

					// NCNameContext node

					else if (anyClass.equals(NCNameContext.class))
						testNCNameContext(list, comp, comps, false);

					// NodeTestContext node

					else if (anyClass.equals(NodeTestContext.class))
						testNodeTestContext(list, comp, comps, false);

					// NameTestContext node

					else if (anyClass.equals(NameTestContext.class))
						testNameTestContext(list, comp, comps, false);

					// PredicateContext node

					else if (anyClass.equals(PredicateContext.class)) {

						XPathComp[] pred_comps = list.arrayOfPredicateContext(union_id, step_id);

						for (XPathComp pred_comp : pred_comps)
							testPredicateContext(list, pred_comp, verbose);

						break;
					}

					else
						throw new PgSchemaException(comp.tree);

				}

			}

		}

		list.applyUnionExpr();

		if (ends_with_text && list.hasPathEndsWithoutTextNode()) {

			list.removePathEndsWithTableNode();
			list.appendTextNode();

		}

		list.removeDuplicatePath();

	}

	/**
	 * Test NCNameContext node.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param comps array of XPath component of the same step
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContext(XPathCompList list, XPathComp comp, XPathComp[] comps, boolean predicate) throws PgSchemaException {

		boolean wild_card = false;

		if (comps.length == 1)
			testNCNameContextWithChildAxis(list, comp, list.isAbsolutePath(comp.union_id), true, wild_card, null, predicate);

		else {

			boolean target_comp = false;

			for (XPathComp _comp : comps) {

				Class<?> _anyClass = _comp.tree.getClass();

				if (_anyClass.equals(NameTestContext.class) || _anyClass.equals(PredicateContext.class))
					break;

				else if (_anyClass.equals(NCNameContext.class)) {

					if (_comp.equals(comp))
						target_comp = true;

					break;
				}

			}

			if (!target_comp)
				return;

			XPathComp first_comp = comps[0];

			for (XPathComp _comp : comps) {

				Class<?> _anyClass = _comp.tree.getClass();

				if (_anyClass.equals(PredicateContext.class))
					break;

				else if (_anyClass.equals(AxisSpecifierContext.class)) {

					if (!_comp.equals(first_comp))
						throw new PgSchemaException(_comp.tree);

				}

				else if (_anyClass.equals(TerminalNodeImpl.class))
					wild_card = true;

			}

			String composite_text = null;

			if (wild_card) {

				StringBuilder sb = new StringBuilder();

				for (XPathComp _comp : comps) {

					Class<?> _anyClass = _comp.tree.getClass();
					String _text = _comp.tree.getText();

					if (_anyClass.equals(PredicateContext.class))
						break;

					else if (_anyClass.equals(NCNameContext.class))
						sb.append(_text);

					else if (_anyClass.equals(NameTestContext.class)) {

						String local_part = _text;

						if (local_part.contains(":"))
							local_part = local_part.split(":")[1];

						sb.append((local_part.equals("*") ? "." : "") + local_part); // '*' -> regular expression '.*'

					}

					else if (_anyClass.equals(TerminalNodeImpl.class)) // '*' -> regular expression '.*'
						sb.append("." + _text);

					else if (!_anyClass.equals(AxisSpecifierContext.class))
						throw new PgSchemaException(_comp.tree);

				}

				composite_text = sb.toString();

				sb.setLength(0);

			}

			if (first_comp.tree.getClass().equals(AxisSpecifierContext.class)) {

				switch (first_comp.tree.getText()) {
				case "ancestor::":
					list.testNCNameContextWithAncestorAxis(comp, false, wild_card, composite_text, predicate);
					break;
				case "ancestor-or-self::":
					list.testNCNameContextWithAncestorAxis(comp, true, wild_card, composite_text, predicate);
					break;
				case "attribute::":
				case "@":
					testNCNameContextWithAttributeAxis(list, comp, wild_card, composite_text, predicate);
					break;
				case "child::":
					testNCNameContextWithChildAxis(list, comp, list.isAbsolutePath(comp.union_id), true, wild_card, composite_text, predicate);
					break;
				case "descendant::":
					testNCNameContextWithChildAxis(list, comp, false, false, wild_card, composite_text, predicate);
					break;
				case "descendant-or-self::":
					testNCNameContextWithChildAxis(list, comp, false, true, wild_card, composite_text, predicate);
					break;
				case "preceding-sibling::":	// non-sense in schema analysis
				case "following-sibling::": // non-sense in schema analysis
				case "self::":
					testNCNameContextWithChildAxis(list, comp, true, true, wild_card, composite_text, predicate);
					break;
				case "following::": // non-sense in schema analysis
				case "preceding::": // non-sense in schema analysis
					testNCNameContextWithChildAxis(list, comp, false, true, wild_card, composite_text, predicate);
					break;
				case "parent::":
					list.testNCNameContextWithParentAxis(comp, wild_card, composite_text, predicate);
					break;
				default: // namespace
					throw new PgSchemaException(first_comp.tree);
				}

			}

			else
				testNCNameContextWithChildAxis(list, comp, list.isAbsolutePath(comp.union_id), true, wild_card, composite_text, predicate);

		}

	}

	/**
	 * Test NCNameContext node having child axis.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param abs_path whether absolute location path or abbreviate location path
	 * @param inc_self whether include self node or not
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContextWithChildAxis(XPathCompList list, XPathComp comp, boolean abs_path, boolean inc_self, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : comp.tree.getText();

		boolean init_path = list.path_exprs.isEmpty();

		// first NCNameContext node

		if (init_path) {

			if (abs_path) {

				if (!matchesNodeName(root_table.name, text, wild_card))
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

				if (inc_self)
					list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, new StringBuilder()), XPathCompType.table));

			}

			else {

				tables.stream().filter(table -> matchesNodeName(table.name, text, wild_card) && !table.virtual).forEach(table -> {

					String table_xpath = getAbsoluteXPathOfTable(table, new StringBuilder());

					if (table_xpath != null && inc_self)
						list.add(new XPathExpr(table_xpath, XPathCompType.table));

					if (table.fields.stream().anyMatch(field -> field.simple_cont)) {

						String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(table);

						if (simple_cont_xpath != null && inc_self)
							list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

					}

				});

				for (PgTable table : tables) {

					table.fields.stream().filter(field -> matchesNodeName(field.xname, text, wild_card) && field.element).forEach(field -> {

						String elem_xpath = getAbsoluteXPathOfElement(table, field.xname);

						if (elem_xpath != null && inc_self)
							list.add(new XPathExpr(elem_xpath, XPathCompType.element));

					});

				}

				if (list.path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

			}

		}

		// succeeding NCNameContext node

		else {

			XPathCompList rep_list = new XPathCompList();

			list.path_exprs.forEach(path_expr -> {

				XPathCompList _list = new XPathCompList();

				String cur_table = getTableNameOfPath(path_expr.path);

				// not specified table

				if (cur_table == null) {

					if (abs_path) {

						if (matchesNodeName(root_table.name, text, wild_card) && inc_self)
							_list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, new StringBuilder()), XPathCompType.table));

					}

					else {

						tables.stream().filter(table -> matchesNodeName(table.name, text, wild_card) && !table.virtual).forEach(table -> {

							String table_xpath = getAbsoluteXPathOfTable(table, new StringBuilder());

							if (table_xpath != null && inc_self)
								_list.add(new XPathExpr(table_xpath, XPathCompType.table));

							if (table.fields.stream().anyMatch(field -> field.simple_cont)) {

								String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(table);

								if (simple_cont_xpath != null && inc_self)
									_list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

							}

						});

						for (PgTable table : tables) {

							table.fields.stream().filter(field -> matchesNodeName(field.xname, text, wild_card) && field.element).forEach(field -> {

								String elem_xpath = getAbsoluteXPathOfElement(table, field.xname);

								if (elem_xpath != null && inc_self)
									_list.add(new XPathExpr(elem_xpath, XPathCompType.element));

							});

						}

					}

				}

				// specified table

				else {

					PgTable table = getTable(cur_table);

					if (table == null) {

						XPathComp prev_comp = list.previousOf(comp);

						try {

							if (prev_comp != null)
								throw new PgSchemaException(comp.tree, prev_comp.tree);
							else
								throw new PgSchemaException(comp.tree);

						} catch (PgSchemaException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

					// check current element

					table.fields.stream().filter(field -> field.element && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

						String elem_xpath = field.xname;

						if (elem_xpath != null && inc_self)
							_list.add(new XPathExpr(path_expr.path + "/" + elem_xpath, XPathCompType.element));

					});

					// check current nested_key

					Integer[] foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
					Integer[] _foreign_table_ids = null;

					while (foreign_table_ids != null && foreign_table_ids.length > 0 && _list.path_exprs.size() == 0) {

						boolean first_nest = _foreign_table_ids == null;

						for (Integer foreign_table_id : foreign_table_ids) {

							PgTable foreign_table = tables.get(foreign_table_id);

							// check foreign table

							if (matchesNodeName(foreign_table.name, text, wild_card) && !foreign_table.virtual) {

								String table_xpath = getAbsoluteXPathOfTable(foreign_table, new StringBuilder());

								if (table_xpath != null && (inc_self || first_nest))
									_list.add(new XPathExpr(table_xpath, XPathCompType.table));

								if (foreign_table.fields.stream().anyMatch(field -> field.simple_cont)) {

									String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(foreign_table);

									if (simple_cont_xpath != null && (inc_self || first_nest))
										_list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

								}

							}

							// check foreign element

							foreign_table.fields.stream().filter(field -> field.element && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

								String elem_xpath = getAbsoluteXPathOfElement(foreign_table, field.xname);

								if (elem_xpath != null && (inc_self || first_nest))
									_list.add(new XPathExpr(elem_xpath, XPathCompType.element));

							});

							// check foreign nested_key

							if (foreign_table.virtual || !abs_path) {

								Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

								if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
									_foreign_table_ids = __foreign_table_ids;

							}

						}

						foreign_table_ids = _foreign_table_ids;

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			list.replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

		}

	}

	/**
	 * Test NCNameContext node having attribute axis.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContextWithAttributeAxis(XPathCompList list, XPathComp comp, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : comp.tree.getText();

		boolean init_path = list.path_exprs.isEmpty();

		// first NCNameContext node

		if (init_path) {

			if (list.isAbsolutePath(comp.union_id))
				throw new PgSchemaException(comp.tree);

			else {

				for (PgTable table : tables) {

					table.fields.stream().filter(field -> matchesNodeName(field.xname, text, wild_card) && field.attribute).forEach(field -> {

						String attr_xpath = getAbsoluteXPathOfAttribute(table, field.xname);

						if (attr_xpath != null)
							list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

					});

				}

				if (list.path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

			}

		}

		// succeeding NCNameContext node

		else {

			boolean abs_location_path = list.isAbsolutePath(comp.union_id);

			XPathCompList rep_list = new XPathCompList();

			list.path_exprs.forEach(path_expr -> {

				XPathCompList _list = new XPathCompList();

				String cur_table = getTableNameOfPath(path_expr.path);

				// not specified table

				if (cur_table == null) {

					if (abs_location_path) {

						for (PgTable table : tables) {

							table.fields.stream().filter(field -> matchesNodeName(field.xname, text, wild_card) && field.attribute).forEach(field -> {

								String attr_xpath = getAbsoluteXPathOfAttribute(table, field.xname);

								if (attr_xpath != null)
									_list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

							});

						}

					}

				}

				// specified table

				else {

					PgTable table = getTable(cur_table);

					if (table == null) {

						XPathComp prev_comp = list.previousOf(comp);

						try {

							if (prev_comp != null)
								throw new PgSchemaException(comp.tree, prev_comp.tree);
							else
								throw new PgSchemaException(comp.tree);

						} catch (PgSchemaException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

					// check current attribute

					table.fields.stream().filter(field -> field.attribute && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

						String attr_xpath = "@" + field.xname;

						if (attr_xpath != null)
							_list.add(new XPathExpr(path_expr.path + "/" + attr_xpath, XPathCompType.attribute));

					});

					// check current nested_key

					Integer[] foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
					Integer[] _foreign_table_ids = null;

					while (foreign_table_ids != null && foreign_table_ids.length > 0 && _list.path_exprs.size() == 0) {

						for (Integer foreign_table_id : foreign_table_ids) {

							PgTable foreign_table = tables.get(foreign_table_id);

							// check foreign attribute

							foreign_table.fields.stream().filter(field -> field.attribute && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

								String attr_xpath = getAbsoluteXPathOfAttribute(foreign_table, field.xname);

								if (attr_xpath != null)
									_list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

							});

							// check foreign nested_key

							if (foreign_table.virtual || !abs_location_path) {

								Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

								if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
									_foreign_table_ids = __foreign_table_ids;

							}

						}

						foreign_table_ids = _foreign_table_ids;

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			list.replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

		}

	}

	/**
	 * Test NodeTestContext node.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param comps array of XPath component of the same step
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNodeTestContext(XPathCompList list, XPathComp comp, XPathComp[] comps, boolean predicate) throws PgSchemaException {

		XPathComp first_comp = comps[0];

		String text = comp.tree.getText();

		if (comps.length == 1) {

			switch (text) {
			case "node()":
				testNodeTestContextWithChildAxis(list, comp, list.isAbsolutePath(comp.union_id), true, predicate);
				break;
			case "text()":
				list.removePathEndsWithTableNode();

				if (list.hasPathEndsWithTextNode())
					throw new PgSchemaException(comp.tree);

				list.appendTextNode();
				break;
			case "comment()":
				if (list.hasPathEndsWithTextNode())
					throw new PgSchemaException(comp.tree);

				list.appendCommentNode();
				break;
			default:
				if (text.startsWith("processing-instruction"))
					list.appendProcessingInstructionNode(text);

				else
					throw new PgSchemaException(comp.tree);
			}

		}

		else if (comps.length == 2 && first_comp.tree.getClass().equals(AxisSpecifierContext.class)) {

			switch (text) {
			case "node()":
				switch (first_comp.tree.getText()) {
				case "ancestor::":
					list.testNodeTestContextWithAncestorAxis(comp, false, predicate);
					break;
				case "ancestor-or-self::":
					list.testNodeTestContextWithAncestorAxis(comp, true, predicate);
					break;
				case "attribute::":
				case "@":
					testNodeTestContextWithAttributeAxis(list, comp, predicate);
					break;
				case "child::":
					testNodeTestContextWithChildAxis(list, comp, list.isAbsolutePath(comp.union_id), true, predicate);
					break;
				case "descendant::":
					testNodeTestContextWithChildAxis(list, comp, false, false, predicate);
					break;
				case "descendant-or-self::":
					testNodeTestContextWithChildAxis(list, comp, false, true, predicate);
					break;
				case "preceding-sibling::":	// non-sense in schema analysis
				case "following-sibling::": // non-sense in schema analysis
				case "self::":
					testNodeTestContextWithChildAxis(list, comp, true, true, predicate);
					break;
				case "following::": // non-sense in schema analysis
				case "preceding::": // non-sense in schema analysis
					testNodeTestContextWithChildAxis(list, comp, false, true, predicate);
					break;
				case "parent::":
					list.testNodeTestContextWithParentAxis(comp, predicate);
					break;
				default: // namespace
					throw new PgSchemaException(first_comp.tree);
				}
				break;
			default:
				throw new PgSchemaException(comp.tree, first_comp.tree);
			}

		}

		else
			throw new PgSchemaException(comp.tree);

	}

	/**
	 * Test NodeTestContext node having child axis.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param abs_path whether absolute location path or abbreviate location path
	 * @param inc_self whether include self node or not
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNodeTestContextWithChildAxis(XPathCompList list, XPathComp comp, boolean abs_path, boolean inc_self, boolean predicate) throws PgSchemaException {

		boolean init_path = list.path_exprs.isEmpty();

		// first NodeTestContext node

		if (init_path) {

			if (abs_path) {

				if (inc_self)
					list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, new StringBuilder()), XPathCompType.table));

			}

			else {

				tables.stream().filter(table -> !table.virtual).forEach(table -> {

					String table_xpath = getAbsoluteXPathOfTable(table, new StringBuilder());

					if (table_xpath != null && inc_self)
						list.add(new XPathExpr(table_xpath, XPathCompType.table));

					if (table.fields.stream().anyMatch(field -> field.simple_cont)) {

						String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(table);

						if (simple_cont_xpath != null && inc_self)
							list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

					}

				});

				for (PgTable table : tables) {

					table.fields.stream().filter(field -> field.element).forEach(field -> {

						String elem_xpath = getAbsoluteXPathOfElement(table, field.xname);

						if (elem_xpath != null && inc_self)
							list.add(new XPathExpr(elem_xpath, XPathCompType.element));

					});

				}

				if (list.path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, def_schema_location);

			}

		}

		// succeeding NodeTestContext node

		else {

			XPathCompList rep_list = new XPathCompList();

			list.path_exprs.forEach(path_expr -> {

				XPathCompList _list = new XPathCompList();

				String cur_table = getTableNameOfPath(path_expr.path);

				// not specified table

				if (cur_table == null) {

					if (abs_path) {

						if (inc_self)
							_list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, new StringBuilder()), XPathCompType.table));

					}

					else {

						tables.stream().filter(table -> !table.virtual).forEach(table -> {

							String table_xpath = getAbsoluteXPathOfTable(table, new StringBuilder());

							if (table_xpath != null && inc_self)
								_list.add(new XPathExpr(table_xpath, XPathCompType.table));

							table.fields.stream().filter(field -> field.simple_cont).forEach(field -> {

								String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(table);

								if (simple_cont_xpath != null && inc_self)
									_list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

							});

						});

						for (PgTable table : tables) {

							table.fields.stream().filter(field -> field.element).forEach(field -> {

								String elem_xpath = getAbsoluteXPathOfElement(table, field.xname);

								if (elem_xpath != null && inc_self)
									_list.add(new XPathExpr(elem_xpath, XPathCompType.element));

							});

						}

					}

				}

				// specified table

				else {

					PgTable table = getTable(cur_table);

					if (table == null) {

						XPathComp prev_comp = list.previousOf(comp);

						try {

							if (prev_comp != null)
								throw new PgSchemaException(comp.tree, prev_comp.tree);
							else
								throw new PgSchemaException(comp.tree);

						} catch (PgSchemaException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

					// check current element

					table.fields.stream().filter(field -> field.element).forEach(field -> {

						String elem_xpath = field.xname;

						if (elem_xpath != null && inc_self)
							_list.add(new XPathExpr(path_expr.path + "/" + elem_xpath, XPathCompType.element));

					});

					// check current nested_key

					Integer[] foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
					Integer[] _foreign_table_ids = null;

					while (foreign_table_ids != null && foreign_table_ids.length > 0 && _list.path_exprs.size() == 0) {

						boolean first_nest = _foreign_table_ids == null;

						for (Integer foreign_table_id : foreign_table_ids) {

							PgTable foreign_table = tables.get(foreign_table_id);

							// check foreign table

							if (!foreign_table.virtual) {

								String table_xpath = getAbsoluteXPathOfTable(foreign_table, new StringBuilder());

								if (table_xpath != null && (inc_self || _foreign_table_ids == null))
									_list.add(new XPathExpr(table_xpath, XPathCompType.table));

								if (foreign_table.fields.stream().anyMatch(field -> field.simple_cont)) {

									String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(foreign_table);

									if (simple_cont_xpath != null && (inc_self || first_nest))
										_list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

								}

							}

							// check foreign element

							foreign_table.fields.stream().filter(field -> field.element).forEach(field -> {

								String elem_xpath = getAbsoluteXPathOfElement(foreign_table, field.xname);

								if (elem_xpath != null && (inc_self || first_nest))
									_list.add(new XPathExpr(elem_xpath, XPathCompType.element));

							});

							// check foreign nested_key

							if (foreign_table.virtual || !abs_path) {

								Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

								if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
									_foreign_table_ids = __foreign_table_ids;

							}

						}

						foreign_table_ids = _foreign_table_ids;

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			list.replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, def_schema_location);

		}

	}

	/**
	 * Test NodeTestContext node having attribute axis.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNodeTestContextWithAttributeAxis(XPathCompList list, XPathComp comp, boolean predicate) throws PgSchemaException {

		boolean init_path = list.path_exprs.isEmpty();

		// first NodeTestContext node

		if (init_path) {

			if (list.isAbsolutePath(comp.union_id))
				throw new PgSchemaException(comp.tree);

			else {

				for (PgTable table : tables) {

					table.fields.stream().filter(field -> field.attribute).forEach(field -> {

						String attr_xpath = getAbsoluteXPathOfAttribute(table, field.xname);

						if (attr_xpath != null)
							list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

					});

				}

				if (list.path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, def_schema_location);

			}

		}

		// succeeding NodeTestContext node

		else {

			boolean abs_location_path = list.isAbsolutePath(comp.union_id);

			XPathCompList rep_list = new XPathCompList();

			list.path_exprs.forEach(path_expr -> {

				XPathCompList _list = new XPathCompList();

				String cur_table = getTableNameOfPath(path_expr.path);

				// not specified table

				if (cur_table == null) {

					if (abs_location_path) {

						for (PgTable table : tables) {

							table.fields.stream().filter(field -> field.attribute).forEach(field -> {

								String attr_xpath = getAbsoluteXPathOfAttribute(table, field.xname);

								if (attr_xpath != null)
									_list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

							});

						}

					}

				}

				// specified table

				else {

					PgTable table = getTable(cur_table);

					if (table == null) {

						XPathComp prev_comp = list.previousOf(comp);

						try {

							if (prev_comp != null)
								throw new PgSchemaException(comp.tree, prev_comp.tree);
							else
								throw new PgSchemaException(comp.tree);

						} catch (PgSchemaException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

					// check current attribute

					table.fields.stream().filter(field -> field.attribute).forEach(field -> {

						String attr_xpath = "@" + field.xname;

						if (attr_xpath != null)
							_list.add(new XPathExpr(path_expr.path + "/" + attr_xpath, XPathCompType.attribute));

					});

					// check current nested_key

					Integer[] foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
					Integer[] _foreign_table_ids = null;

					while (foreign_table_ids != null && foreign_table_ids.length > 0 && _list.path_exprs.size() == 0) {

						for (Integer foreign_table_id : foreign_table_ids) {

							PgTable foreign_table = tables.get(foreign_table_id);

							// check foreign attribute

							foreign_table.fields.stream().filter(field -> field.attribute).forEach(field -> {

								String attr_xpath = getAbsoluteXPathOfAttribute(foreign_table, field.xname);

								if (attr_xpath != null)
									_list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

							});

							// check foreign nested_key

							if (foreign_table.virtual || !abs_location_path) {

								Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

								if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
									_foreign_table_ids = __foreign_table_ids;

							}

						}

						foreign_table_ids = _foreign_table_ids;

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			list.replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, def_schema_location);

		}

	}

	/**
	 * Test NameTestContext node.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param comps array of XPath component of the same step
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContext(XPathCompList list, XPathComp comp, XPathComp[] comps, boolean predicate) throws PgSchemaException {

		boolean wild_card = false;

		String text = comp.tree.getText();

		if (comps.length == 1) {

			String prefix = "";
			String local_part = text;

			if (text.contains(":")) {

				String[] _text = text.split(":");

				prefix = _text[0];
				local_part = _text[1];

			}

			String namespace_uri = def_namespaces.get(prefix);

			if (namespace_uri == null || namespace_uri.isEmpty())
				throw new PgSchemaException(comp.tree, def_schema_location, prefix);

			testNameTestContextWithChildAxis(list, comp, namespace_uri, local_part, list.isAbsolutePath(comp.union_id), true, wild_card, null, predicate);

		}

		else {

			XPathComp first_comp = comps[0];

			for (XPathComp _comp : comps) {

				Class<?> _anyClass = _comp.tree.getClass();

				if (_anyClass.equals(PredicateContext.class))
					break;

				else if (_anyClass.equals(AxisSpecifierContext.class)) {

					if (!_comp.equals(first_comp))
						throw new PgSchemaException(_comp.tree);

				}

				else if (_anyClass.equals(TerminalNodeImpl.class))
					wild_card = true;

			}

			String composite_text = null;

			if (wild_card) {

				StringBuilder sb = new StringBuilder();

				for (XPathComp _comp : comps) {

					Class<?> _anyClass = _comp.tree.getClass();
					String _text = _comp.tree.getText();

					if (_anyClass.equals(PredicateContext.class))
						break;

					if (_anyClass.equals(NCNameContext.class))
						sb.append(_text);

					else if (_anyClass.equals(NameTestContext.class)) {

						String local_part = _text;

						if (local_part.contains(":"))
							local_part = local_part.split(":")[1];

						sb.append((local_part.equals("*") ? "." : "") + local_part); // '*' -> regular expression '.*'

					}

					else if (_anyClass.equals(TerminalNodeImpl.class)) // '*' -> regular expression '.*'
						sb.append("." + _text);

					else if (!_anyClass.equals(AxisSpecifierContext.class))
						throw new PgSchemaException(_comp.tree);

				}

				composite_text = sb.toString();

				sb.setLength(0);

			}

			String prefix = "";
			String local_part = text;

			if (text.contains(":")) {

				String[] _text = text.split(":");

				prefix = _text[0];
				local_part = _text[1];

			}

			String namespace_uri = def_namespaces.get(prefix);

			if (namespace_uri == null || namespace_uri.isEmpty())
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location, prefix);

			if (first_comp.tree.getClass().equals(AxisSpecifierContext.class)) {

				switch (first_comp.tree.getText()) {
				case "ancestor::":
					list.testNameTestContextWithAncestorAxis(comp, namespace_uri, local_part, false, wild_card, composite_text, predicate);
					break;
				case "ancestor-or-self::":
					list.testNameTestContextWithAncestorAxis(comp, namespace_uri, local_part, true, wild_card, composite_text, predicate);
					break;
				case "attribute::":
				case "@":
					testNameTestContextWithAttributeAxis(list, comp, prefix.isEmpty() ? PgSchemaUtil.xs_namespace_uri : namespace_uri, local_part, wild_card, composite_text, predicate);
					break;
				case "child::":
					testNameTestContextWithChildAxis(list, comp, namespace_uri, local_part, list.isAbsolutePath(comp.union_id), true, wild_card, composite_text, predicate);
					break;
				case "descendant::":
					testNameTestContextWithChildAxis(list, comp, namespace_uri, local_part, false, false, wild_card, composite_text, predicate);
					break;
				case "descendant-or-self::":
					testNameTestContextWithChildAxis(list, comp, namespace_uri, local_part, false, true, wild_card, composite_text, predicate);
					break;
				case "preceding-sibling::":	// non-sense in schema analysis
				case "following-sibling::": // non-sense in schema analysis
				case "self::":
					testNameTestContextWithChildAxis(list, comp, namespace_uri, local_part, true, true, wild_card, composite_text, predicate);
					break;
				case "following::": // non-sense in schema analysis
				case "preceding::": // non-sense in schema analysis
					testNameTestContextWithChildAxis(list, comp, namespace_uri, local_part, false, true, wild_card, composite_text, predicate);
					break;
				case "parent::":
					list.testNameTestContextWithParentAxis(comp, namespace_uri, local_part, wild_card, composite_text, predicate);
					break;
				default: // namespace
					throw new PgSchemaException(first_comp.tree);
				}

			}

			else
				testNameTestContextWithChildAxis(list, comp, namespace_uri, local_part, list.isAbsolutePath(comp.union_id), true, wild_card, composite_text, predicate);

		}

	}

	/**
	 * Test NameTestContext node having child axis.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param namespace_uri namespace URI of current QName
	 * @param local_part local part name of current QName
	 * @param abs_path whether absolute location path or abbreviate location path
	 * @param inc_self whether include self node or not
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContextWithChildAxis(XPathCompList list, XPathComp comp, String namespace_uri, String local_part, boolean abs_path, boolean inc_self, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : comp.tree.getText();

		boolean init_path = list.path_exprs.isEmpty();

		// first NameTestContext node

		if (init_path) {

			if (abs_path) {

				if (!root_table.target_namespace.contains(namespace_uri) || !matchesNodeName(root_table.name, text, wild_card))
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

				if (inc_self)
					list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, new StringBuilder()), XPathCompType.table));

			}

			else {

				tables.stream().filter(table -> !table.virtual && table.target_namespace.contains(namespace_uri) && matchesNodeName(table.name, text, wild_card)).forEach(table -> {

					String table_xpath = getAbsoluteXPathOfTable(table, new StringBuilder());

					if (table_xpath != null && inc_self)
						list.add(new XPathExpr(table_xpath, XPathCompType.table));

					if (table.fields.stream().anyMatch(field -> field.simple_cont && field.target_namespace.contains(PgSchemaUtil.xs_namespace_uri) && matchesNodeName(field.xname, text, wild_card))) {

						String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(table);

						if (simple_cont_xpath != null && inc_self)
							list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

					}

				});

				for (PgTable table : tables) {

					table.fields.stream().filter(field -> field.element && field.target_namespace.contains(namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

						String elem_xpath = getAbsoluteXPathOfElement(table, field.xname);

						if (elem_xpath != null && inc_self)
							list.add(new XPathExpr(elem_xpath, XPathCompType.element));

					});

				}

				if (list.path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

			}

		}

		// succeeding NameTestContext node

		else {

			XPathCompList rep_list = new XPathCompList();

			list.path_exprs.forEach(path_expr -> {

				XPathCompList _list = new XPathCompList();

				String cur_table = getTableNameOfPath(path_expr.path);

				// not specified table

				if (cur_table == null) {

					if (abs_path) {

						if (inc_self && root_table.target_namespace.contains(namespace_uri) && matchesNodeName(root_table.name, text, wild_card))
							_list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, new StringBuilder()), XPathCompType.table));

					}

					else {

						tables.stream().filter(table -> !table.virtual && table.target_namespace.contains(namespace_uri) && matchesNodeName(table.name, text, wild_card)).forEach(table -> {

							String table_xpath = getAbsoluteXPathOfTable(table, new StringBuilder());

							if (table_xpath != null && inc_self)
								_list.add(new XPathExpr(table_xpath, XPathCompType.table));

							table.fields.stream().filter(field -> field.simple_cont && field.target_namespace.contains(PgSchemaUtil.xs_namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

								String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(table);

								if (simple_cont_xpath != null && inc_self)
									_list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

							});

						});

						for (PgTable table : tables) {

							table.fields.stream().filter(field -> field.element && field.target_namespace.contains(namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

								String elem_xpath = getAbsoluteXPathOfElement(table, field.xname);

								if (elem_xpath != null && inc_self)
									_list.add(new XPathExpr(elem_xpath, XPathCompType.element));

							});

						}

					}

				}

				// specified table

				else {

					PgTable table = getTable(cur_table);

					if (table == null) {

						XPathComp prev_comp = list.previousOf(comp);

						try {

							if (prev_comp != null)
								throw new PgSchemaException(comp.tree, prev_comp.tree);
							else
								throw new PgSchemaException(comp.tree);

						} catch (PgSchemaException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

					// check current element

					table.fields.stream().filter(field -> field.element && field.target_namespace.contains(namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

						String elem_xpath = field.xname;

						if (elem_xpath != null && inc_self)
							_list.add(new XPathExpr(path_expr.path + "/" + elem_xpath, XPathCompType.element));

					});

					// check current nested_key

					Integer[] foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
					Integer[] _foreign_table_ids = null;

					while (foreign_table_ids != null && foreign_table_ids.length > 0 && _list.path_exprs.size() == 0) {

						boolean first_nest = _foreign_table_ids == null;

						for (Integer foreign_table_id : foreign_table_ids) {

							PgTable foreign_table = tables.get(foreign_table_id);

							// check foreign table

							if (!foreign_table.virtual && foreign_table.target_namespace.contains(namespace_uri) && matchesNodeName(foreign_table.name, text, wild_card)) {

								String table_xpath = getAbsoluteXPathOfTable(foreign_table, new StringBuilder());

								if (table_xpath != null && (inc_self || _foreign_table_ids == null))
									_list.add(new XPathExpr(table_xpath, XPathCompType.table));

								if (foreign_table.fields.stream().anyMatch(field -> field.simple_cont && field.target_namespace.contains(PgSchemaUtil.xs_namespace_uri) && matchesNodeName(field.xname, text, wild_card))) {

									String simple_cont_xpath = getAbsoluteXPathOfSimpleContent(foreign_table);

									if (simple_cont_xpath != null && (inc_self || first_nest))
										_list.add(new XPathExpr(simple_cont_xpath, XPathCompType.simple_content));

								}

							}

							// check foreign element

							foreign_table.fields.stream().filter(field -> field.element && field.target_namespace.contains(namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

								String elem_xpath = getAbsoluteXPathOfElement(foreign_table, field.xname);

								if (elem_xpath != null && (inc_self || first_nest))
									_list.add(new XPathExpr(elem_xpath, XPathCompType.element));

							});

							// check foreign nested_key

							if (foreign_table.virtual || !abs_path) {

								Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

								if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
									_foreign_table_ids = __foreign_table_ids;

							}

						}

						foreign_table_ids = _foreign_table_ids;

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			list.replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

		}

	}

	/**
	 * Test NameTestContext node having attribute axis.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param namespace_uri namespace URI of current QName
	 * @param local_part local part of current QName
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContextWithAttributeAxis(XPathCompList list, XPathComp comp, String namespace_uri, String local_part, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : comp.tree.getText();

		boolean init_path = list.path_exprs.isEmpty();

		// first NameTestContext node

		if (init_path) {

			if (list.isAbsolutePath(comp.union_id))
				throw new PgSchemaException(comp.tree);

			else {

				for (PgTable table : tables) {

					table.fields.stream().filter(field -> field.attribute && field.target_namespace.contains(namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

						String attr_xpath = getAbsoluteXPathOfAttribute(table, field.xname);

						if (attr_xpath != null)
							list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

					});

				}

				if (list.path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

			}

		}

		// succeeding NameTestContext node

		else {

			boolean abs_location_path = list.isAbsolutePath(comp.union_id);

			XPathCompList rep_list = new XPathCompList();

			list.path_exprs.forEach(path_expr -> {

				XPathCompList _list = new XPathCompList();

				String cur_table = getTableNameOfPath(path_expr.path);

				// not specified table

				if (cur_table == null) {

					if (abs_location_path) {

						for (PgTable table : tables) {

							table.fields.stream().filter(field -> field.attribute && field.target_namespace.contains(namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

								String attr_xpath = getAbsoluteXPathOfAttribute(table, field.xname);

								if (attr_xpath != null)
									_list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

							});

						}

					}

				}

				// specified table

				else {

					PgTable table = getTable(cur_table);

					if (table == null) {

						XPathComp prev_comp = list.previousOf(comp);

						try {

							if (prev_comp != null)
								throw new PgSchemaException(comp.tree, prev_comp.tree);
							else
								throw new PgSchemaException(comp.tree);

						} catch (PgSchemaException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

					// check current attribute

					table.fields.stream().filter(field -> field.attribute && field.target_namespace.contains(namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

						String attr_xpath = "@" + field.xname;

						if (attr_xpath != null)
							_list.add(new XPathExpr(path_expr.path + "/" + attr_xpath, XPathCompType.attribute));

					});

					// check current nested_key

					Integer[] foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
					Integer[] _foreign_table_ids = null;

					while (foreign_table_ids != null && foreign_table_ids.length > 0 && _list.path_exprs.size() == 0) {

						for (Integer foreign_table_id : foreign_table_ids) {

							PgTable foreign_table = tables.get(foreign_table_id);

							// check foreign attribute

							foreign_table.fields.stream().filter(field -> field.attribute && field.target_namespace.contains(namespace_uri) && matchesNodeName(field.xname, text, wild_card)).forEach(field -> {

								String attr_xpath = getAbsoluteXPathOfAttribute(foreign_table, field.xname);

								if (attr_xpath != null)
									_list.add(new XPathExpr(attr_xpath, XPathCompType.attribute));

							});

							// check foreign nested_key

							if (foreign_table.virtual || !abs_location_path) {

								Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

								if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
									_foreign_table_ids = __foreign_table_ids;

							}

						}

						foreign_table_ids = _foreign_table_ids;

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			list.replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

		}

	}

	/**
	 * Test PredicateContext node.
	 *
	 * @param list XPath component list
	 * @param comp current XPath component
	 * @param verbose whether output parse tree for predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testPredicateContext(XPathCompList list, XPathComp comp, boolean verbose) throws PgSchemaException {

		if (list.predicates == null)
			list.predicates = new ArrayList<XPathPredicateExpr>();

		int pred_size = list.predicates.size();

		XPathCompList pred_list = new XPathCompList(this, comp.tree, list.variables, verbose);

		int path_expr_size = pred_list.sizeOfPathExpr();

		for (XPathExpr path_expr : list.path_exprs) {

			// no path expression in predicate

			if (path_expr_size == 0)
				list.predicates.add(new XPathPredicateExpr(comp, path_expr, -1));

			// otherwise, validate path expression with schema

			else {

				for (int union_id = 0; union_id <= pred_list.getLastUnionId(); union_id++) {

					XPathComp[] union_comps = pred_list.arrayOf(union_id);

					if (union_comps.length == 0) // no path expression
						continue;

					XPathPredicateExpr predicate = new XPathPredicateExpr(comp, path_expr, union_id);

					pred_list.replacePathExprs(predicate);

					for (int step_id = 0; step_id <= pred_list.getLastStepId(union_id); step_id++) {

						XPathComp[] pred_comps = pred_list.arrayOf(union_id, step_id);

						for (XPathComp pred_comp : pred_comps) {

							Class<?> anyClass = pred_comp.tree.getClass();

							// TerminalNodeImpl node

							if (anyClass.equals(TerminalNodeImpl.class))
								pred_list.testTerminalNodeImpl(pred_comp, true);

							// AbbreviatedStepContext node

							else if (anyClass.equals(AbbreviatedStepContext.class))
								pred_list.testAbbreviateStepContext(pred_comp, true);

							// AxisSpecifierContext node

							else if (anyClass.equals(AxisSpecifierContext.class))
								pred_list.testAxisSpecifierContext(pred_comp, pred_comps);

							// NCNameContext node

							else if (anyClass.equals(NCNameContext.class))
								testNCNameContext(pred_list, pred_comp, pred_comps, true);

							// NodeTestContext node

							else if (anyClass.equals(NodeTestContext.class))
								testNodeTestContext(pred_list, pred_comp, pred_comps, true);

							// NameTestContext node

							else if (anyClass.equals(NameTestContext.class))
								testNameTestContext(pred_list, pred_comp, pred_comps, true);

							else
								throw new PgSchemaException(pred_comp.tree);

							if (pred_list.path_exprs.isEmpty())
								break;

						}

						if (pred_list.path_exprs.isEmpty())
							break;

					}

					// store valid path expressions in predicate

					if (pred_list.path_exprs.size() > 0) {

						predicate.replaceDstPathExprs(pred_list.path_exprs);
						list.predicates.add(predicate);

					}

					else
						throw new PgSchemaException(union_comps[0].tree, def_schema_location);

					pred_list.clearPathExprs();

				}

			}

		}

		if (pred_size == list.predicates.size()) // invalid predicate
			throw new PgSchemaException(comp.tree, def_schema_location);

	}

	/**
	 * Return whether node name matches.
	 *
	 * @param node_name node name
	 * @param prefix prefix text
	 * @param wild_card whether wild card follows or not
	 * @return boolean whether node name matches
	 */
	protected boolean matchesNodeName(String node_name, String prefix, boolean wild_card) {

		if (wild_card)
			return node_name.matches(prefix);

		return prefix.equals("*") || node_name.equals(prefix);
	}

	/**
	 * Return table name of current path.
	 *
	 * @param path current path
	 * @return String current table name
	 */
	protected String getTableNameOfPath(String path) {

		String[] _path = path.replaceFirst("//$", "").split("/");

		int position = _path.length - 1;

		if (position < 0)
			return null;

		return _path[position];
	}

	/**
	 * Return XPath SQL expression of current path.
	 *
	 * @param path current path
	 * @param terminus current terminus type
	 * @return XPathSqlExpr XPath SQL expression
	 */
	protected XPathSqlExpr getXPathSqlExprOfPath(String path, XPathCompType terminus) {

		String[] _path = path.replaceFirst("//$", "").split("/");

		int position = _path.length - 1;

		if (position < 0)
			return null;

		String table_name;
		String field_name;

		switch (terminus) {
		case element:
			if (position - 1 < 0)
				return null;
			table_name = _path[position - 1];
			field_name = _path[position];
			break;
		case simple_content:
			table_name = _path[position];
			field_name = PgSchemaUtil.simple_cont_name;
			break;
		case attribute:
			if (position - 1 < 0)
				return null;
			table_name = _path[position - 1];
			field_name = _path[position].replaceFirst("^@", "");
			break;
		default:
			return null;
		}

		PgTable table = getTable(table_name);

		if (table == null)
			return null;

		try {

			return new XPathSqlExpr(this, path, table_name, field_name, null, terminus);

		} catch (PgSchemaException e) {

			Integer[] foreign_table_ids = null;
			Integer[] _foreign_table_ids = null;

			switch (terminus) {
			case element:
				foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				_foreign_table_ids = null;

				while (foreign_table_ids != null && foreign_table_ids.length > 0) {

					for (Integer foreign_table_id : foreign_table_ids) {

						PgTable foreign_table = tables.get(foreign_table_id);

						// check foreign element

						if (foreign_table.fields.stream().anyMatch(field -> field.element && field.xname.equals(field_name))) {

							try {
								return new XPathSqlExpr(this, path, foreign_table.name, field_name, null, terminus);
							} catch (PgSchemaException e2) {
							}

						}

						// check foreign nested_key

						if (foreign_table.virtual) {

							Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
								_foreign_table_ids = __foreign_table_ids;

						}

					}

					foreign_table_ids = _foreign_table_ids;

				}
				break;
			case simple_content:
				foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				_foreign_table_ids = null;

				while (foreign_table_ids != null && foreign_table_ids.length > 0) {

					for (Integer foreign_table_id : foreign_table_ids) {

						PgTable foreign_table = tables.get(foreign_table_id);

						// check foreign simple_cont

						if (foreign_table.fields.stream().anyMatch(field -> field.simple_cont)) {

							try {
								return new XPathSqlExpr(this, path, foreign_table.name, field_name, null, terminus);
							} catch (PgSchemaException e2) {
							}

						}

						// check foreign nested_key

						if (foreign_table.virtual) {

							Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
								_foreign_table_ids = __foreign_table_ids;

						}

					}

					foreign_table_ids = _foreign_table_ids;

				}
				break;
			case attribute:
				foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				_foreign_table_ids = null;

				while (foreign_table_ids != null && foreign_table_ids.length > 0) {

					for (Integer foreign_table_id : foreign_table_ids) {

						PgTable foreign_table = tables.get(foreign_table_id);

						// check foreign attribute

						if (foreign_table.fields.stream().anyMatch(field -> field.attribute && field.xname.equals(field_name))) {

							try {
								return new XPathSqlExpr(this, path, foreign_table.name, field_name, null, terminus);
							} catch (PgSchemaException e2) {
							}

						}

						// check foreign nested_key

						if (foreign_table.virtual) {

							Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
								_foreign_table_ids = __foreign_table_ids;

						}

					}

					foreign_table_ids = _foreign_table_ids;

				}
				break;
			default:
				return null;
			}

		}

		return null;
	}

	/**
	 * Return absolute XPath expression of current table.
	 *
	 * @param table current table
	 * @param sb StringBuilder to store path
	 * @return String absolute XPath expression of current table
	 */
	private String getAbsoluteXPathOfTable(PgTable table, StringBuilder sb) {

		String table_name = table.name;

		if (table.equals(root_table)) {

			sb.append((sb.length() > 0 ? "/" : "") + table_name);

			return getReversedPath(sb.toString());
		}

		if (!table.virtual)
			sb.append((sb.length() > 0 ? "/" : "") + table_name);

		for (PgTable parent_table : tables) {

			if (parent_table.nested_fields == 0)
				continue;

			if (parent_table.fields.stream().anyMatch(field -> field.nested_key && field.foreign_table.equals(table_name)))
				return getAbsoluteXPathOfTable(parent_table, sb);
		}

		return null;
	}

	/**
	 * Return absolute XPath expression of current attribute.
	 *
	 * @param table current table
	 * @param text current attribute name
	 * @return String absolute XPath expression of current attribute
	 */
	private String getAbsoluteXPathOfAttribute(PgTable table, String text) {

		StringBuilder sb = new StringBuilder();

		sb.append("@" + text);

		return getAbsoluteXPathOfTable(table, sb);
	}

	/**
	 * Return absolute XPath expression of current element.
	 *
	 * @param table current table
	 * @param text current element name
	 * @return String absolute XPath expression of current attribute
	 */
	private String getAbsoluteXPathOfElement(PgTable table, String text) {

		StringBuilder sb = new StringBuilder();

		sb.append(text);

		return getAbsoluteXPathOfTable(table, sb);
	}

	/**
	 * Return absolute XPath expression of current simple content.
	 *
	 * @param table current table
	 * @return String absolute XPath expression of current attribute
	 */
	private String getAbsoluteXPathOfSimpleContent(PgTable table) {
		return getAbsoluteXPathOfTable(table, new StringBuilder());
	}

	/**
	 * Return reversed path.
	 *
	 * @param path normal path
	 * @return String reversed path
	 */
	private String getReversedPath(String path) {

		String[] _path = path.split("/");

		StringBuilder sb = new StringBuilder();

		for (int l = _path.length - 1; l >= 0; l--)
			sb.append("/" + _path[l]);

		return sb.toString();
	}

}
