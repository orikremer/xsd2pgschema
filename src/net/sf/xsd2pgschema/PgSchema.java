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
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

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

	/** The schema locations. */
	private HashSet<String> schema_locations = null;

	/** The unique schema locations (value) with its target namespace (key). */
	private HashMap<String, String> unq_schema_locations = null;

	/** The duplicated schema locations (key=duplicated schema location, value=unique schema location). */
	private HashMap<String, String> dup_schema_locations = null;

	/** The attribute group definitions. */
	private List<PgTable> attr_groups = null;

	/** The model group definitions. */
	private List<PgTable> model_groups = null;

	/** The list of PostgreSQL table. */
	private List<PgTable> tables = null;

	/** The list of PostgreSQL foreign key. */
	private List<PgForeignKey> foreign_keys = null;

	/** The pending list of attribute groups. */
	private List<PgPendingGroup> pending_attr_groups = null;

	/** The pending list of model groups. */
	private List<PgPendingGroup> pending_model_groups = null;

	/** The PostgreSQL data model option. */
	protected PgSchemaOption option = null;

	/** The full-text index filter. */
	protected IndexFilter index_filter = null;

	/** The current depth of table (internal use only). */
	private int level;

	/** The PostgreSQL root table. */
	private PgTable root_table = null;

	/** The PostgreSQL table for questing document id. */
	private PgTable doc_id_table = null;

	/** The default namespaces (key=prefix, value=namespace_uri). */
	private HashMap<String, String> def_namespaces = null;

	/** The top level xs:annotation. */
	private String def_anno = null;

	/** The top level xs:annotation/xs:appinfo. */
	private String def_anno_appinfo = null;

	/** The top level xs:annotation/xs:documentation. */
	private String def_anno_doc = null;

	/** The top level xs:annotation/xs:documentation (as is).*/
	@SuppressWarnings("unused")
	private String def_xanno_doc = null;

	/** The default attributes. */
	private String def_attrs = null;

	/** The statistics message on schema. */
	private StringBuilder def_stat_msg = null;

	/** The table lock object. */
	private Object[] table_lock = null;

	/** The current document id. */
	private String document_id = null;

	/** The instance of message digest for hash_key. */
	private MessageDigest md_hash_key = null;

	/** The root schema object. */
	private PgSchema _root_schema = null;

	/**
	 * PostgreSQL schema constructor.
	 *
	 * @param doc_builder Document builder used for xs:include or xs:import
	 * @param doc XML Schema document
	 * @param root_schema root schema object (should be null at first)
	 * @param def_schema_location default schema location
	 * @param option PostgreSQL data model option
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchema(DocumentBuilder doc_builder, Document doc, PgSchema root_schema, String def_schema_location, PgSchemaOption option) throws PgSchemaException {

		this.option = option;
		this.def_schema_location = def_schema_location;

		// check existence of root element

		root_node = doc.getDocumentElement();

		if (root_node == null)
			throw new PgSchemaException("Not found root element in XML Schema: " + def_schema_location);

		option.setPrefixOfXmlSchema(doc, def_schema_location);

		// check root element name

		if (!root_node.getNodeName().equals(option.xs_prefix_ + "schema"))
			throw new PgSchemaException("Not found " + option.xs_prefix_ + "schema root element in XML Schema: " + def_schema_location);

		// detect entry point of XML schemata

		_root_schema = root_schema == null ? this : root_schema;

		def_schema_parent = root_schema == null ? PgSchemaUtil.getSchemaParent(def_schema_location) : root_schema.def_schema_parent;

		// prepare dictionary of unique schema locations and duplicated schema locations

		if (root_schema == null) {

			unq_schema_locations = new HashMap<String, String>();
			dup_schema_locations = new HashMap<String, String>();

		}

		// extract default namespace and default attributes

		def_namespaces = new HashMap<String, String>();

		NamedNodeMap root_attrs = root_node.getAttributes();

		for (int i = 0; i < root_attrs.getLength(); i++) {

			Node root_attr = root_attrs.item(i);

			if (root_attr != null) {

				String node_name = root_attr.getNodeName();

				if (node_name.equals("targetNamespace")) {

					String target_namespace = root_attr.getNodeValue().split(" ")[0];

					_root_schema.unq_schema_locations.putIfAbsent(target_namespace, def_schema_location);

					def_namespaces.putIfAbsent("", target_namespace);

				}

				else if (node_name.startsWith("xmlns"))
					def_namespaces.putIfAbsent(node_name.replaceFirst("^xmlns:?", ""), root_attr.getNodeValue().split(" ")[0]);

				else if (node_name.equals("defaultAttributes"))
					def_attrs = root_attr.getNodeValue();

			}

		}

		// retrieve top level schema annotation

		def_anno = option.extractAnnotation(root_node, true);
		def_anno_appinfo = option.extractAppinfo(root_node);

		if ((def_anno_doc = option.extractDocumentation(root_node, true)) != null)
			def_xanno_doc = option.extractDocumentation(root_node, false);

		def_stat_msg = new StringBuilder();

		// prepare schema location holder

		schema_locations = new HashSet<String>();

		schema_locations.add(def_schema_location);

		// prepare table holder, attribute group holder and model group holder

		tables = new ArrayList<PgTable>();

		attr_groups = new ArrayList<PgTable>();

		model_groups = new ArrayList<PgTable>();

		// prepare foreign key holder, and pending group holder for attribute group and model group

		if (root_schema == null) {

			foreign_keys = new ArrayList<PgForeignKey>();

			pending_attr_groups = new ArrayList<PgPendingGroup>();
			pending_model_groups = new ArrayList<PgPendingGroup>();

		}

		tables = new ArrayList<PgTable>();

		// include or import namespace

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name = child.getNodeName();

			if (!child_name.contains("include") && !child_name.contains("import"))
				continue;

			// reset prefix of XSD because import or include process may override

			option.setPrefixOfXmlSchema(doc, def_schema_location);

			if (child_name.equals(option.xs_prefix_ + "include") || child_name.equals(option.xs_prefix_ + "import")) {

				Element child_e = (Element) child;

				String schema_location = child_e.getAttribute("schemaLocation");

				if (schema_location != null && !schema_location.isEmpty()) {

					// prevent infinite cyclic reference which is allowed in XML Schema 1.1

					if (!_root_schema.schema_locations.add(schema_location))
						continue;

					// copy XML Schema if not exists

					PgSchemaUtil.getSchemaFile(schema_location, def_schema_parent, option.cache_xsd);

					// local XML Schema file

					InputStream is2 = PgSchemaUtil.getSchemaInputStream(schema_location, def_schema_parent, option.cache_xsd);

					if (is2 == null)
						throw new PgSchemaException("Could not access to schema location: " + schema_location);

					try {

						Document doc2 = doc_builder.parse(is2);

						is2.close();

						doc_builder.reset();

						// referred XML Schema (xs:include|xs:import/@schemaLocation) analysis

						PgSchema schema2 = new PgSchema(doc_builder, doc2, _root_schema, schema_location, option);

						if ((schema2.tables == null || schema2.tables.size() == 0) && (schema2.attr_groups == null || schema2.attr_groups.size() == 0) && (schema2.model_groups == null || schema2.model_groups.size() == 0)) {
							/*
							_root_schema.def_stat_msg.append("--  Not found any root element (/" + option.xs_prefix_ + "schema/" + option.xs_prefix_ + "element) or administrative elements (/" + option.xs_prefix_ + "schema/[" + option.xs_prefix_ + "complexType | " + option.xs_prefix_ + "simpleType | " + option.xs_prefix_ + "attributeGroup | " + option.xs_prefix_ + "group]) in XML Schema: " + schema_location + "\n");
							 */
							continue;
						}

						// add schema location to prevent infinite cyclic reference

						schema2.schema_locations.forEach(arg -> _root_schema.schema_locations.add(arg));

						// copy default namespace from referred XML Schema

						if (!schema2.def_namespaces.isEmpty())
							schema2.def_namespaces.entrySet().forEach(arg -> _root_schema.def_namespaces.putIfAbsent(arg.getKey(), arg.getValue()));

						// copy administrative tables from referred XML Schema

						schema2.tables.stream().filter(arg -> arg.xs_type.equals(XsTableType.xs_admin_root) || arg.xs_type.equals(XsTableType.xs_admin_child)).forEach(arg -> {

							if (_root_schema.avoidTableDuplication(tables, arg))
								_root_schema.tables.add(arg);

						});

					} catch (SAXException | IOException e) {
						throw new PgSchemaException(e);
					}

				}

			}

		}

		// reset prefix of XSD because import or include process may override

		option.setPrefixOfXmlSchema(doc, def_schema_location);

		// extract attribute group elements

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (child.getNodeName().equals(option.xs_prefix_ + "attributeGroup"))
				extractAdminAttributeGroup(child);

		}

		// extract model group elements

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (child.getNodeName().equals(option.xs_prefix_ + "group"))
				extractAdminModelGroup(child);

		}

		// create table for root element

		boolean root_element = root_schema == null;

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (child.getNodeName().equals(option.xs_prefix_ + "element")) {

				Element child_e = (Element) child;

				String _abstract = child_e.getAttribute("abstract");

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

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (child.getNodeName().equals(option.xs_prefix_ + "complexType"))
				extractAdminElement(child, true, false);

			else if (child.getNodeName().equals(option.xs_prefix_ + "simpleType"))
				extractAdminElement(child, false, false);

		}

		if (tables.size() == 0) {

			if (root_schema == null)
				throw new PgSchemaException("Not found any root element (/" + option.xs_prefix_ + "schema/" + option.xs_prefix_ + "element) or administrative elements (/" + option.xs_prefix_ + "schema/[" + option.xs_prefix_ + "complexType | " + option.xs_prefix_ + "simpleType]) in XML Schema: " + def_schema_location);

		}

		else {

			if (!option.rel_model_ext)
				tables.forEach(table -> table.classify());

			if (!_root_schema.dup_schema_locations.containsKey(def_schema_location))
				_root_schema.def_stat_msg.append("--  " + (root_schema == null ? "Generated" : "Found") + " " + tables.stream().filter(table -> option.rel_model_ext || !table.relational).count() + " tables (" + tables.stream().map(table -> option.rel_model_ext || !table.relational ? table.fields.size() : 0).reduce((arg0, arg1) -> arg0 + arg1).get() + " fields), " + attr_groups.size() + " attr groups, " + model_groups.size() + " model groups " + (root_schema == null ? "in total" : "in XML Schema: " + def_schema_location) + "\n");

		}

		// append annotation of root table if possible

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (child.getNodeName().equals(option.xs_prefix_ + "element")) {

				Element child_e = (Element) child;

				String _abstract = child_e.getAttribute("abstract");

				if (_abstract != null && _abstract.equals("true"))
					continue;

				extractAdminElement(child, false, true);

				if (root_schema == null)
					break;

			}

		}

		// append annotation of administrative tables if possible

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (child.getNodeName().equals(option.xs_prefix_ + "complexType"))
				extractAdminElement(child, true, true);

			else if (child.getNodeName().equals(option.xs_prefix_ + "simpleType"))
				extractAdminElement(child, false, true);

		}

		if (root_schema != null)
			return;

		// apply pending attribute groups (lazy evaluation)

		if (pending_attr_groups.size() > 0) {

			pending_attr_groups.forEach(arg -> {

				try {

					int t = getAttributeGroupId(arg.ref_group, true);

					PgTable table = getPendingTable(arg);

					if (table != null && table.has_pending_group) {

						table.fields.addAll(arg.insert_position, attr_groups.get(t).fields);

						table.removeProhibitedAttrs();
						table.removeBlockedSubstitutionGroups();
						table.countNestedFields();

					}

				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

			pending_attr_groups.clear();

		}

		// apply pending model groups (lazy evaluation)

		if (pending_model_groups.size() > 0) {

			pending_model_groups.forEach(arg -> {

				try {

					int t = getModelGroupId(arg.ref_group, true);

					PgTable table = getPendingTable(arg);

					if (table != null && table.has_pending_group) {

						table.fields.addAll(arg.insert_position, model_groups.get(t).fields);

						table.removeProhibitedAttrs();
						table.removeBlockedSubstitutionGroups();
						table.countNestedFields();

					}

				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

			pending_model_groups.clear();

		}

		// resolved pending groups

		tables.stream().filter(table -> table.has_pending_group).forEach(table -> table.has_pending_group = false);

		// classify type of table

		tables.forEach(table -> {

			table.classify();

			// set foreign_table_id as table pointer otherwise remove orphaned nested key

			if (table.required) {

				Iterator<PgField> iterator = table.fields.iterator();

				while (iterator.hasNext()) {

					PgField field = iterator.next();

					if (field.nested_key) {

						PgTable foreign_table = getForeignTable(field);

						if (foreign_table != null) {

							field.foreign_table_id = tables.indexOf(foreign_table);
							foreign_table.required = true;

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

				PgTable nested_table = getForeignTable(field);

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

					PgTable _nested_table = getForeignTable(_field);

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

		// append annotation of nested tables if possible

		tables.stream().filter(table -> table.virtual && table.anno != null).forEach(table -> {

			table.fields.stream().filter(field -> field.nested_key).forEach(field -> {

				PgTable nested_table = getForeignTable(field);

				if (nested_table != null && nested_table.anno == null) {
					nested_table.anno = "(quoted from " + table.name + ")\n-- " + table.anno;
					nested_table.xanno_doc = "(quoted from " + table.name + ")\n" + table.xanno_doc;
				}

			});

		});

		// cancel unique key constraint if parent table is list holder

		tables.stream().filter(table -> table.list_holder).forEach(table -> table.fields.stream().filter(field -> field.nested_key).forEach(field -> getForeignTable(field).cancelUniqueKey()));

		// decide parent node name

		tables.stream().filter(table -> table.nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && field.parent_node != null).forEach(field -> {

			if (table.conflict) // tolerate parent node name check because of name collision
				field.parent_node = null;

			else {

				String[] parent_nodes = field.parent_node.split(" ");

				field.parent_node = null;

				boolean infinite_loop = false;

				for (String parent_node : parent_nodes) {

					PgTable parent_table = getTable(field.foreign_schema, parent_node);

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
									field.parent_node = parent_field.foreign_table_name;

								else {

									String[] _parent_nodes = field.parent_node.split(" ");

									boolean has_parent_node = false;

									for (String _parent_node : _parent_nodes) {

										if (_parent_node.equals(parent_field.foreign_table_name)) {
											has_parent_node = true;
											break;
										}

									}

									if (!has_parent_node)
										field.parent_node += " " + parent_field.foreign_table_name;

								}

							}

						}

						if (!has_foreign_key && _parent_table.content_holder)
							has_content = true;

						for (PgTable ancestor_table : tables) { // ancestor table

							if (ancestor_table.nested_fields == 0)
								continue;

							for (PgField ancestor_field : ancestor_table.fields) {

								if (!ancestor_field.nested_key)
									continue;

								if (getForeignTable(ancestor_field).equals(_parent_table)) {

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

		// decide ancestor node name

		tables.stream().filter(table -> table.has_foreign_key && table.nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && field.parent_node != null).forEach(field -> {

			Optional<PgField> opt = table.fields.stream().filter(foreign_field -> foreign_field.foreign_key && foreign_field.foreign_table_name.equals(field.parent_node)).findFirst();

			if (opt.isPresent()) {

				Optional<PgField> opt2 = getForeignTable(opt.get()).fields.stream().filter(nested_field -> nested_field.nested_key && getForeignTable(nested_field).equals(table)).findFirst();

				if (opt2.isPresent())
					field.ancestor_node = opt2.get().parent_node;

			}

		}));

		tables.stream().filter(table -> !table.has_foreign_key && table.nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && field.parent_node != null && field.ancestor_node == null).forEach(field -> {

			StringBuilder sb = new StringBuilder();

			tables.stream().filter(ancestor_table -> ancestor_table.nested_fields > 0).forEach(ancestor_table -> {

				Optional<PgField> opt = ancestor_table.fields.stream().filter(ancestor_field -> ancestor_field.nested_key && getForeignTable(ancestor_field).equals(table) && ancestor_field.xtype.equals(field.xtype) && ancestor_field.ancestor_node != null).findFirst();

				if (opt.isPresent()) {

					String[] ancestor_nodes = opt.get().ancestor_node.split(" ");

					String[] _ancestor_nodes = sb.toString().split(" ");

					for (String ancestor_node : ancestor_nodes) {

						boolean has_ancestor_node = false;

						for (String _ancestor_node : _ancestor_nodes) {

							if (_ancestor_node.equals(ancestor_node)) {
								has_ancestor_node = true;
								break;
							}

						}

						if (!has_ancestor_node)
							sb.append(ancestor_node + " ");

					}

				}

			});

			if (sb.length() > 0) {

				field.ancestor_node = sb.substring(0, sb.length() - 1);
				sb.setLength(0);

			}

		}));

		// update requirement flag because of foreign keys

		foreign_keys.forEach(foreign_key -> {

			PgTable child_table = getChildTable(foreign_key);

			if (child_table != null) {

				PgTable parent_table = getParentTable(foreign_key);

				if (parent_table != null)
					child_table.required = parent_table.required = true;

			}

		});

		// add serial key if parent table is list holder

		if (option.serial_key)
			tables.stream().filter(table -> table.list_holder).forEach(table -> table.fields.stream().filter(field -> field.nested_key).forEach(field -> getForeignTable(field).addSerialKey(option)));

		// add XPath key

		if (option.xpath_key)
			tables.forEach(table -> table.addXPathKey(option));

		// remove nested key if relational model extension is disabled

		if (!option.rel_model_ext) {

			tables.stream().filter(table -> table.nested_fields > 0).forEach(table -> {

				table.fields.removeIf(field -> field.nested_key);
				table.countNestedFields();

			});

		}

		// retrieve document key if in-place document key no exists

		if (!option.document_key && option.inplace_document_key && option.document_key_if_no_in_place) {

			tables.stream().filter(table -> table.required && !table.relational && !table.fields.stream().anyMatch(field -> field.name.equals(option.document_key_name)) && !table.fields.stream().anyMatch(field -> (field.attribute || field.element) && (option.inplace_document_key_names.contains(field.xname) || option.inplace_document_key_names.contains(table.name + "." + field.xname)))).forEach(table -> {

				PgField field = new PgField();

				field.name = field.xname = option.document_key_name;
				field.type = option.xs_prefix_ + "string";
				field.xs_type = XsDataType.xs_string;
				field.document_key = true;

				table.fields.add(0, field);

			});

		}

		// update system key, user key, omissible and jsonable flags

		tables.forEach(table -> table.fields.forEach(field -> {

			field.setSystemKey();
			field.setUserKey();
			field.setOmissible(table, option);
			field.setJsonable(table, option);


		}));

		// decide prefix of target namespace

		tables.stream().filter(table -> table.required && !table.relational).forEach(table -> {

			table.prefix = getAbsolutePrefixOf(table.target_namespace, "");

			table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> {

				field.prefix = getAbsolutePrefixOf(field.target_namespace, "");
				field.is_xs_namespace = field.target_namespace.equals(PgSchemaUtil.xs_namespace_uri);


			});

		});

		// instance of message digest

		if (!option.hash_algorithm.isEmpty() && !option.hash_size.equals(PgHashSize.debug_string)) {

			try {
				md_hash_key = MessageDigest.getInstance(option.hash_algorithm);
			} catch (NoSuchAlgorithmException e) {
				throw new PgSchemaException(e);
			}

		}

		// statistics

		StringBuilder sb = new StringBuilder();

		HashSet<String> namespace_uri = new HashSet<String>();

		def_namespaces.entrySet().stream().map(arg -> arg.getValue()).forEach(arg -> namespace_uri.add(arg));

		namespace_uri.forEach(arg -> sb.append(arg + " (" + getAbsolutePrefixOf(arg, "default") + "), "));
		namespace_uri.clear();

		_root_schema.def_stat_msg.append("--   Namespaces:\n");
		_root_schema.def_stat_msg.append("--    " + sb.substring(0, sb.length() - 2) + "\n");

		sb.setLength(0);

		schema_locations.stream().filter(arg -> !dup_schema_locations.containsKey(arg)).forEach(arg -> sb.append(arg + ", "));

		_root_schema.def_stat_msg.append("--   Schema locations:\n");
		_root_schema.def_stat_msg.append("--    " + sb.substring(0, sb.length() - 2) + "\n");

		sb.setLength(0);

		_root_schema.def_stat_msg.append("--   Table types:\n");
		_root_schema.def_stat_msg.append("--    " + tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_root) && (option.rel_model_ext || !table.relational)).count() + " root, ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_root_child) && (option.rel_model_ext || !table.relational)).count() + " root children, ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_root) && (option.rel_model_ext || !table.relational)).count() + " admin roots, ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_child) && (option.rel_model_ext || !table.relational)).count() + " admin children\n");
		_root_schema.def_stat_msg.append("--   System keys:\n");
		_root_schema.def_stat_msg.append("--    " + tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.primary_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " primary keys (" + tables.stream().map(table -> table.fields.stream().filter(field -> field.unique_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " unique constraints), ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.foreign_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " foreign keys (" + countForeignKeyReferences() + " key references), ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.nested_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " nested keys\n");
		_root_schema.def_stat_msg.append("--   User keys:\n");
		_root_schema.def_stat_msg.append("--    " + tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.document_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " document keys, ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.serial_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " serial keys, ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.xpath_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " xpath keys\n");
		_root_schema.def_stat_msg.append("--   Contents:\n");
		_root_schema.def_stat_msg.append("--    " + tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.attribute && !option.discarded_document_key_names.contains(field.xname) && !option.discarded_document_key_names.contains(table.name + "." + field.xname)).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " attributes ("
				+ (option.document_key || !option.inplace_document_key ? 0 : tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.attribute && !option.discarded_document_key_names.contains(field.xname) && !option.discarded_document_key_names.contains(table.name + "." + field.xname) && (option.inplace_document_key_names.contains(field.xname) || option.inplace_document_key_names.contains(table.name + "." + field.xname))).count()).reduce((arg0, arg1) -> arg0 + arg1).get()) + " in-place document keys), ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.element && !option.discarded_document_key_names.contains(field.xname) && !option.discarded_document_key_names.contains(table.name + "." + field.xname)).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " elements ("
				+ (option.document_key || !option.inplace_document_key ? 0 : tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.element && !option.discarded_document_key_names.contains(field.xname) && !option.discarded_document_key_names.contains(table.name + "." + field.xname) && (option.inplace_document_key_names.contains(field.xname) || option.inplace_document_key_names.contains(table.name + "." + field.xname))).count()).reduce((arg0, arg1) -> arg0 + arg1).get()) + " in-place document keys), ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.simple_content).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " simple contents\n");
		_root_schema.def_stat_msg.append("--   Wild cards:\n");
		_root_schema.def_stat_msg.append("--    " + tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.any).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " any elements, ");
		_root_schema.def_stat_msg.append(tables.stream().filter(table -> option.rel_model_ext || !table.relational).map(table -> table.fields.stream().filter(field -> field.any_attribute).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " any attributes\n");

		// update schema locations to unique ones

		tables.forEach(table -> table.schema_location = getUniqueSchemaLocations(table.schema_location));
		attr_groups.forEach(attr_group -> attr_group.schema_location = getUniqueSchemaLocations(attr_group.schema_location));
		model_groups.forEach(model_group -> model_group.schema_location = getUniqueSchemaLocations(model_group.schema_location));

		// realize PostgreSQL DDL

		realize();

		// check root table exists

		hasRootTable();

		// check in-place document keys

		if (!option.document_key && option.inplace_document_key) {

			tables.stream().filter(table -> table.required && !table.relational).forEach(table -> {

				try {
					getDocKeyName(table);
				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

		}

	}

	/**
	 * Extract root element of XML Schema.
	 *
	 * @param node current node
	 * @param root_element whether it is root element or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractRootElement(Node node, boolean root_element) throws PgSchemaException {

		PgTable table = new PgTable(getPgSchemaOf(def_namespaces.get("")), def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String name = e.getAttribute("name");

		table.name = option.getUnqualifiedName(name);

		if (table.name.isEmpty())
			return;

		table.required = true;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		table.xs_type = root_element ? XsTableType.xs_root : XsTableType.xs_admin_root;

		table.fields = new ArrayList<PgField>();

		table.level = level = 0;

		table.addPrimaryKey(option, table.name, true);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name = child.getNodeName();

			if (child_name.equals(option.xs_prefix_ + "complexType") || child_name.equals(option.xs_prefix_ + "simpleType"))
				extractField(child, table);

			else if (child_name.equals(option.xs_prefix_ + "keyref"))
				extractForeignKeyRef(child, node);

		}

		table.removeProhibitedAttrs();
		table.removeBlockedSubstitutionGroups();
		table.countNestedFields();

		if (!table.has_pending_group && table.fields.size() < option.getMinimumSizeOfField())
			return;

		tables.add(table);

		if (root_element) {

			addRootOrphanItem(node, table);

			if (tables.stream().anyMatch(_table -> _table.xs_type.equals(XsTableType.xs_root)))
				root_table = tables.stream().filter(_table -> _table.xs_type.equals(XsTableType.xs_root)).findFirst().get();

		}

	}

	/**
	 * Add orphan item of root element.
	 *
	 * @param node current node
	 * @param table root table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void addRootOrphanItem(Node node, PgTable table) throws PgSchemaException {

		PgField dummy = new PgField();

		Element e = (Element) node;

		String name = e.getAttribute("name");

		if (name != null && !name.isEmpty()) {

			dummy.extractType(option, node);

			if (dummy.type != null && !dummy.type.isEmpty()) {

				String[] type = dummy.type.contains(" ") ? dummy.type.split(" ")[0].split(":") : dummy.type.split(":");

				// primitive data type

				if (type.length != 0 && type[0].equals(option.xs_prefix)) { } // nothing to do

				// non-primitive data type

				else {

					level++;

					PgTable child_table = new PgTable(getPgSchemaOf(getNamespaceUriOfQName(dummy.type)), getNamespaceUriOfQName(dummy.type), def_schema_location);

					boolean unique_key = table.addNestedKey(option, child_table.pg_schema_name, name, dummy, node);

					Element child_e = (Element) node;

					String child_name = child_e.getAttribute("name");

					child_table.name = option.getUnqualifiedName(child_name);

					table.required = child_table.required = true;

					if ((child_table.anno = option.extractAnnotation(node, true)) != null)
						child_table.xanno_doc = option.extractDocumentation(node, false);

					child_table.xs_type = XsTableType.xs_admin_root;

					child_table.fields = new ArrayList<PgField>();

					child_table.level = level;

					child_table.addPrimaryKey(option, child_table.name, unique_key);

					if (!child_table.addNestedKey(option, table.pg_schema_name, dummy.type, dummy, node))
						child_table.cancelUniqueKey();

					child_table.removeProhibitedAttrs();
					child_table.removeBlockedSubstitutionGroups();
					child_table.countNestedFields();

					if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
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
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractAdminElement(Node node, boolean complex_type, boolean annotation) throws PgSchemaException {

		PgTable table = new PgTable(getPgSchemaOf(def_namespaces.get("")), def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String name = e.getAttribute("name");

		table.name = option.getUnqualifiedName(name);

		if (table.name.isEmpty())
			return;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		if (annotation) {

			if (table.anno != null && !table.anno.isEmpty()) {

				PgTable known_table = getTable(table.pg_schema_name, table.name);

				if (known_table != null) {

					if ((known_table.anno == null || known_table.anno.isEmpty()) && table.anno != null && !table.anno.isEmpty()) {

						known_table.anno = table.anno;
						known_table.xanno_doc = table.xanno_doc;

					}

				}

			}

			return;
		}

		table.xs_type = XsTableType.xs_admin_root;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		table.addPrimaryKey(option, table.name, true);

		if (complex_type) {

			extractAttributeGroup(node, table); // default attribute group

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
				extractField(child, table);

		}

		else
			extractSimpleContent(node, table);

		table.removeProhibitedAttrs();
		table.removeBlockedSubstitutionGroups();
		table.countNestedFields();

		if (!table.has_pending_group && table.fields.size() < option.getMinimumSizeOfField())
			return;

		if (avoidTableDuplication(tables, table))
			tables.add(table);

	}

	/**
	 * Extract administrative attribute group of XML Schema.
	 *
	 * @param node current node
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractAdminAttributeGroup(Node node) throws PgSchemaException {

		PgTable table = new PgTable(getPgSchemaOf(def_namespaces.get("")), def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String name = e.getAttribute("name");

		table.name = option.getUnqualifiedName(name);

		if (table.name.isEmpty())
			return;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		table.xs_type = XsTableType.xs_attr_group;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table);
		/*
		if (table.fields.size() == 0)
			return;
		 */
		if (avoidTableDuplication(_root_schema.attr_groups, table)) {

			attr_groups.add(table);

			if (!this.equals(_root_schema))
				_root_schema.attr_groups.add(table);

		}

	}

	/**
	 * Extract administrative model group of XML Schema.
	 *
	 * @param node current node
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractAdminModelGroup(Node node) throws PgSchemaException {

		PgTable table = new PgTable(getPgSchemaOf(def_namespaces.get("")), def_namespaces.get(""), def_schema_location);

		Element e = (Element) node;

		String name = e.getAttribute("name");

		table.name = option.getUnqualifiedName(name);

		if (table.name.isEmpty())
			return;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		table.xs_type = XsTableType.xs_model_group;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table);
		/*
		if (table.fields.size() == 0)
			return;
		 */
		if (avoidTableDuplication(_root_schema.model_groups, table)) {

			model_groups.add(table);

			if (!this.equals(_root_schema))
				_root_schema.model_groups.add(table);

		}

	}

	/**
	 * Extract field of table.
	 *
	 * @param node current node
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractField(Node node, PgTable table) throws PgSchemaException {

		String node_name = node.getNodeName();

		if (node_name.equals(option.xs_prefix_ + "any")) {
			extractAny(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "anyAttribute")) {
			extractAnyAttribute(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "attribute")) {
			extractAttribute(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "attributeGroup")) {
			extractAttributeGroup(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "element")) {
			extractElement(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "group")) {
			extractModelGroup(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "simpleContent")) {
			extractSimpleContent(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "complexContent")) {
			extractComplexContent(node, table);
			return;
		}

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name = child.getNodeName();

			if (child_name.equals(option.xs_prefix_ + "annotation"))
				continue;

			else if (child_name.equals(option.xs_prefix_ + "any"))
				extractAny(child, table);

			else if (child_name.equals(option.xs_prefix_ + "anyAttribute"))
				extractAnyAttribute(child, table);

			else if (child_name.equals(option.xs_prefix_ + "attribute"))
				extractAttribute(child, table);

			else if (child_name.equals(option.xs_prefix_ + "attributeGroup"))
				extractAttributeGroup(child, table);

			else if (child_name.equals(option.xs_prefix_ + "element"))
				extractElement(child, table);

			else if (child_name.equals(option.xs_prefix_ + "group"))
				extractModelGroup(child, table);

			else if (child_name.equals(option.xs_prefix_ + "simpleContent"))
				extractSimpleContent(child, table);

			else if (child_name.equals(option.xs_prefix_ + "complexContent"))
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

		PgForeignKey foreign_key = new PgForeignKey(option, getPgSchemaOf(def_namespaces.get("")), node, parent_node, name, option.getUnqualifiedName(refer));

		if (foreign_key.isEmpty())
			return;

		if (_root_schema.foreign_keys.stream().anyMatch(_foreign_key -> _foreign_key.equals(foreign_key)))
			return;

		_root_schema.foreign_keys.add(foreign_key);

	}

	/**
	 * Extract any.
	 *
	 * @param node current node
	 * @param table current table
	 */
	private void extractAny(Node node, PgTable table) {

		PgField field = new PgField();

		field.any = true;

		field.extractMaxOccurs(option, node);
		field.extractMinOccurs(option, node);

		field.name = field.xname = table.avoidFieldDuplication(option, PgSchemaUtil.any_name);

		if ((field.anno = option.extractAnnotation(node, false)) != null)
			field.xanno_doc = option.extractDocumentation(node, false);

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
	 * @param table current table
	 */
	private void extractAnyAttribute(Node node, PgTable table) {

		PgField field = new PgField();

		field.any_attribute = true;

		field.name = field.xname = table.avoidFieldDuplication(option, PgSchemaUtil.any_attribute_name);

		if ((field.anno = option.extractAnnotation(node, false)) != null)
			field.xanno_doc = option.extractDocumentation(node, false);

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
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractAttribute(Node node, PgTable table) throws PgSchemaException {

		extractInfoItem(node, table, true);

	}

	/**
	 * Extract element.
	 *
	 * @param node current node
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractElement(Node node, PgTable table) throws PgSchemaException {

		extractInfoItem(node, table, false);

	}

	/**
	 * Concrete extractor for both attribute and element.
	 *
	 * @param node current node
	 * @param table current table
	 * @param attribute whether it is attribute or not (element)
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractInfoItem(Node node, PgTable table, boolean attribute) throws PgSchemaException {

		PgField field = new PgField();

		Element e = (Element) node;

		String name = e.getAttribute("name");
		String ref = e.getAttribute("ref");

		if (attribute)
			field.attribute = true;

		else {

			field.element = true;

			field.extractMaxOccurs(option, node);
			field.extractMinOccurs(option, node);

		}

		if (name != null && !name.isEmpty()) {

			field.xname = option.getUnqualifiedName(name);
			field.name = table.avoidFieldDuplication(option, field.xname);

			if ((field.anno = option.extractAnnotation(node, false)) != null)
				field.xanno_doc = option.extractDocumentation(node, false);

			field.extractType(option, node);
			field.extractNamespace(this, node); // require type definition
			field.extractRequired(node);
			field.extractFixedValue(node);
			field.extractDefaultValue(node);
			field.extractBlockValue(node);
			field.extractEnumeration(option, node);
			field.extractRestriction(option, node);

			if (field.substitution_group != null && !field.substitution_group.isEmpty())
				table.appendSubstitutionGroup(field.name);

			if (field.enumeration != null && field.enumeration.length > 0) {

				field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.name) + "_" + PgSchemaUtil.avoidPgReservedOps(field.name);

				if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
					field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

			}

			if (field.type == null || field.type.isEmpty()) {

				if (!table.addNestedKey(option, table.pg_schema_name, name, field, node))
					table.cancelUniqueKey();

				level++;

				table.required = true;

				addChildItem(node, table);

				level--;

			}

			else {

				String[] type = field.type.contains(" ") ? field.type.split(" ")[0].split(":") : field.type.split(":");

				// primitive data type

				if (type.length != 0 && type[0].equals(option.xs_prefix)) {

					field.xs_type = XsDataType.valueOf("xs_" + type[1]);

					table.fields.add(field);

				}

				// non-primitive data type

				else {

					level++;

					PgTable child_table = new PgTable(getPgSchemaOf(getNamespaceUriOfQName(field.type)), getNamespaceUriOfQName(field.type), def_schema_location);

					boolean unique_key = table.addNestedKey(option, child_table.pg_schema_name, name, field, node);

					if (!unique_key)
						table.cancelUniqueKey();

					Element child_e = (Element) node;

					String child_name = child_e.getAttribute("name");

					child_table.name = option.getUnqualifiedName(child_name);

					table.required = child_table.required = true;

					if ((child_table.anno = option.extractAnnotation(node, true)) != null)
						child_table.xanno_doc = option.extractDocumentation(node, false);

					child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

					child_table.fields = new ArrayList<PgField>();

					child_table.level = level;

					child_table.addPrimaryKey(option, child_table.name, unique_key);

					if (!child_table.addNestedKey(option, table.pg_schema_name, field.type, field, node))
						child_table.cancelUniqueKey();

					child_table.removeProhibitedAttrs();
					child_table.removeBlockedSubstitutionGroups();
					child_table.countNestedFields();

					if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
						tables.add(child_table);

					addChildItem(node, child_table);

					level--;

				}

			}

		}

		else if (ref != null && !ref.isEmpty()) {

			for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (child.getNodeName().equals(attribute ? option.xs_prefix_ + "attribute" : option.xs_prefix_ + "element")) {

					Element child_e = (Element) child;

					String child_name = child_e.getAttribute("name");

					if (child_name.equals(option.getUnqualifiedName(ref)) && (
							(table.target_namespace != null && table.target_namespace.equals(getNamespaceUriOfQName(ref))) ||
							(table.target_namespace == null && getNamespaceUriOfQName(ref) == null))) {

						field.xname = option.getUnqualifiedName(child_name);
						field.name = table.avoidFieldDuplication(option, field.xname);

						if ((field.anno = option.extractAnnotation(child, false)) != null)
							field.xanno_doc = option.extractDocumentation(child, false);

						field.extractType(option, child);
						field.extractNamespace(this, child); // require type definition
						field.extractRequired(child);
						field.extractFixedValue(child);
						field.extractDefaultValue(child);
						field.extractBlockValue(child);
						field.extractEnumeration(option, child);
						field.extractRestriction(option, child);

						if (field.substitution_group != null && !field.substitution_group.isEmpty())
							table.appendSubstitutionGroup(field.name);

						if (field.enumeration != null && field.enumeration.length > 0) {

							field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.name) + "_" + PgSchemaUtil.avoidPgReservedOps(field.name);

							if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
								field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

						}

						if (field.type == null || field.type.isEmpty()) {

							if (!table.addNestedKey(option, table.pg_schema_name, child_name, field, child))
								table.cancelUniqueKey();

							level++;

							table.required = true;

							addChildItem(child, table);

							level--;

						}

						else {

							String[] type = field.type.contains(" ") ? field.type.split(" ")[0].split(":") : field.type.split(":");

							// primitive data type

							if (type.length != 0 && type[0].equals(option.xs_prefix)) {

								field.xs_type = XsDataType.valueOf("xs_" + type[1]);

								table.fields.add(field);

							}

							// non-primitive data type

							else {

								level++;

								PgTable child_table = new PgTable(getPgSchemaOf(getNamespaceUriOfQName(field.type)), getNamespaceUriOfQName(field.type), def_schema_location);

								boolean unique_key = table.addNestedKey(option, child_table.pg_schema_name, child_name, field, child);

								if (!unique_key)
									table.cancelUniqueKey();

								child_table.name = option.getUnqualifiedName(child_name);

								table.required = child_table.required = true;

								if ((child_table.anno = option.extractAnnotation(child, true)) != null)
									child_table.xanno_doc = option.extractDocumentation(child, false);

								child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

								child_table.fields = new ArrayList<PgField>();

								child_table.level = level;

								child_table.addPrimaryKey(option, child_table.name, unique_key);

								if (!child_table.addNestedKey(option, table.pg_schema_name, field.type, field, child))
									child_table.cancelUniqueKey();

								child_table.removeProhibitedAttrs();
								child_table.removeBlockedSubstitutionGroups();
								child_table.countNestedFields();

								if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
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
	 * @throws PgSchemaException the pg schema exception
	 */
	private void addChildItem(Node node, PgTable foreign_table) throws PgSchemaException {

		PgTable table = new PgTable(getPgSchemaOf(foreign_table.target_namespace), foreign_table.target_namespace, def_schema_location);

		Element e = (Element) node;

		String type = e.getAttribute("type");

		if (type == null || type.isEmpty()) {

			String name = e.getAttribute("name");

			table.name = option.getUnqualifiedName(name);
			table.xs_type = foreign_table.xs_type.equals(XsTableType.xs_root) || foreign_table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;


		}

		else {

			table.name = option.getUnqualifiedName(type);
			table.xs_type = XsTableType.xs_admin_root;

		}

		if (table.name.isEmpty())
			return;

		table.required = true;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		table.fields = new ArrayList<PgField>();

		table.level = level;

		table.addPrimaryKey(option, table.name, true);
		table.addForeignKey(option, foreign_table);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table);

		table.removeProhibitedAttrs();
		table.removeBlockedSubstitutionGroups();
		table.countNestedFields();

		if (!table.has_pending_group && table.fields.size() < option.getMinimumSizeOfField())
			return;

		if (avoidTableDuplication(tables, table))
			tables.add(table);

	}

	/**
	 * Extract attribute group.
	 *
	 * @param node current node
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractAttributeGroup(Node node, PgTable table) throws PgSchemaException {

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

		ref = option.getUnqualifiedName(ref);

		int t = getAttributeGroupId(ref, false);

		if (t < 0) {

			_root_schema.pending_attr_groups.add(new PgPendingGroup(ref, table.pg_schema_name, table.name, table.fields.size()));
			table.has_pending_group = true;

			return;
		}

		table.fields.addAll(_root_schema.attr_groups.get(t).fields);

	}

	/**
	 * Extract model group.
	 *
	 * @param node current node
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractModelGroup(Node node, PgTable table) throws PgSchemaException {

		Element e = (Element) node;

		String ref = e.getAttribute("ref");

		if (ref == null || ref.isEmpty())
			return;

		ref = option.getUnqualifiedName(ref);

		int t = getModelGroupId(ref, false);

		if (t < 0) {

			_root_schema.pending_model_groups.add(new PgPendingGroup(ref, table.pg_schema_name, table.name, table.fields.size()));
			table.has_pending_group = true;

			return;
		}

		table.fields.addAll(_root_schema.model_groups.get(t).fields);

	}

	/**
	 * Extract simple content.
	 *
	 * @param node current node
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractSimpleContent(Node node, PgTable table) throws PgSchemaException {

		PgField field = new PgField();

		String name = PgSchemaUtil.simple_content_name; // anonymous simple content

		field.simple_content = true;
		field.xname = option.getUnqualifiedName(name);
		field.name = table.avoidFieldDuplication(option, field.xname);

		if ((field.anno = option.extractAnnotation(node, false)) != null)
			field.xanno_doc = option.extractDocumentation(node, false);

		field.extractType(option, node);
		field.extractNamespace(this, node); // require type definition
		field.extractRequired(node);
		field.extractFixedValue(node);
		field.extractDefaultValue(node);
		field.extractBlockValue(node);
		field.extractEnumeration(option, node);
		field.extractRestriction(option, node);

		if (field.substitution_group != null && !field.substitution_group.isEmpty())
			table.appendSubstitutionGroup(field.name);

		if (field.enumeration != null && field.enumeration.length > 0) {

			field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.name) + "_" + PgSchemaUtil.avoidPgReservedOps(field.name);

			if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
				field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

		}

		String[] type = field.type.contains(" ") ? field.type.split(" ")[0].split(":") : field.type.split(":");

		// primitive data type

		if (type.length != 0 && type[0].equals(option.xs_prefix)) {

			field.xs_type = XsDataType.valueOf("xs_" + type[1]);

			table.fields.add(field);

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (!child.getNodeName().equals(option.xs_prefix_ + "extension"))
					continue;

				for (Node subchild = child.getFirstChild(); subchild != null; subchild = subchild.getNextSibling()) {

					String subchild_name = subchild.getNodeName();

					if (subchild_name.equals(option.xs_prefix_ + "attribute"))
						extractAttribute(subchild, table);

					else if (subchild_name.equals(option.xs_prefix_ + "attributeGroup"))
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
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractComplexContent(Node node, PgTable table) throws PgSchemaException {

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (child.getNodeName().equals(option.xs_prefix_ + "extension")) {

				Element child_e = (Element) child;

				String type = option.getUnqualifiedName(child_e.getAttribute("base"));

				table.addNestedKey(option, table.pg_schema_name, type);

				extractComplexContentExt(child, table);

			}

		}

	}

	/**
	 * Extract complex content under xs:extension.
	 *
	 * @param node current node
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractComplexContentExt(Node node, PgTable table) throws PgSchemaException {

		String node_name = node.getNodeName();

		if (node_name.equals(option.xs_prefix_ + "any")) {
			extractAny(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "anyAttribute")) {
			extractAnyAttribute(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "attribute")) {
			extractAttribute(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "attributeGroup")) {
			extractAttributeGroup(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "element")) {
			extractElement(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "group")) {
			extractModelGroup(node, table);
			return;
		}

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name = child.getNodeName();

			if (child_name.equals(option.xs_prefix_ + "annotation"))
				continue;

			else if (child_name.equals(option.xs_prefix_ + "any"))
				extractAny(child, table);

			else if (child_name.equals(option.xs_prefix_ + "anyAttribute"))
				extractAnyAttribute(child, table);

			else if (child_name.equals(option.xs_prefix_ + "attribute"))
				extractAttribute(child, table);

			else if (child_name.equals(option.xs_prefix_ + "attributeGroup"))
				extractAttributeGroup(child, table);

			else if (child_name.equals(option.xs_prefix_ + "element"))
				extractElement(child, table);

			else if (child_name.equals(option.xs_prefix_ + "group"))
				extractModelGroup(child, table);

			else
				extractComplexContentExt(child, table);

		}

	}

	/**
	 * Avoid table duplication while merging two equivalent tables.
	 *
	 * @param tables target table list
	 * @param table current table having table name at least
	 * @return boolean whether no name collision occurs
	 */
	private boolean avoidTableDuplication(List<PgTable> tables, PgTable table) {

		if (tables == null)
			return true;

		List<PgField> fields = table.fields;

		int known_t = -1;

		try {
			known_t = tables.equals(this.tables) ? getTableId(table.pg_schema_name, table.name) : tables.equals(_root_schema.attr_groups) ? getAttributeGroupId(table.name, false) : tables.equals(_root_schema.model_groups) ? getModelGroupId(table.name, false) : -1;
		} catch (PgSchemaException e) {
			e.printStackTrace();
			System.exit(1);
		}

		if (known_t < 0)
			return true;

		PgTable known_table = tables.get(known_t);

		boolean changed = false;
		boolean conflict = false;

		if (table.required && !known_table.required)
			known_table.required = true;

		// copy annotation if available

		if ((known_table.anno == null || known_table.anno.isEmpty()) && table.anno != null && !table.anno.isEmpty()) {

			known_table.anno = table.anno;
			known_table.xanno_doc = table.xanno_doc;

		}

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

			else if (!known_table.schema_location.contains(table.schema_location)) {

				if (!known_table.schema_location.contains(_root_schema.unq_schema_locations.get(table.target_namespace)))
					known_table.schema_location += " " + table.schema_location;
				else
					_root_schema.dup_schema_locations.put(table.schema_location, _root_schema.unq_schema_locations.get(table.target_namespace));

			}

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

						if (known_field.parent_node != null && !known_field.parent_node.contains(field.parent_node))
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
	 * Return default schema location.
	 *
	 * @return String default schema location
	 */
	protected String getDefaultSchemaLocation() {
		return def_schema_location;
	}

	/**
	 * Return unique schema locations.
	 *
	 * @param schema_locations schema locations
	 * @return String unique schema locations
	 */
	private String getUniqueSchemaLocations(String schema_locations) {

		StringBuilder sb = new StringBuilder();

		try {

			for (String schema_location : schema_locations.split(" ")) {

				if (_root_schema.dup_schema_locations.containsKey(schema_location))
					sb.append(_root_schema.dup_schema_locations.get(schema_location) + " ");
				else
					sb.append(schema_location + " ");

			}

			return sb.substring(0, sb.length() - 1);

		} finally {
			sb.setLength(0);
		}

	}

	/**
	 * Return namespace URI for prefix.
	 *
	 * @param prefix prefix of namespace URI
	 * @return String namespace URI
	 */
	protected String getNamespaceUriForPrefix(String prefix) {
		return def_namespaces.get(prefix);
	}

	/**
	 * Return namespace URI of qualified name.
	 *
	 * @param qname qualified name
	 * @return String namespace URI
	 */
	private String getNamespaceUriOfQName(String qname) {

		String name = option.getUnqualifiedName(qname);

		return getNamespaceUriForPrefix(name.equals(qname) ? "" : qname.substring(0, qname.length() - name.length() - 1));
	}

	/**
	 * Return prefix of namespace URI.
	 *
	 * @param namespace_uri namespace URI
	 * @return String prefix of namespace URI
	 */
	public String getPrefixOf(String namespace_uri) {
		return def_namespaces.entrySet().stream().filter(arg -> arg.getValue().equals(namespace_uri)).findFirst().get().getKey();
	}

	/**
	 * Return absolute prefix of namespace URI.
	 *
	 * @param namespace_uri namespace URI
	 * @param def_prefix prefix for default namespace URI
	 * @return String prefix of namespace URI
	 */
	private String getAbsolutePrefixOf(String namespace_uri, String def_prefix) {
		return def_namespaces.entrySet().stream().anyMatch(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()) ? def_namespaces.entrySet().stream().filter(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()).findFirst().get().getKey() : def_prefix;
	}

	/**
	 * Return PostgreSQL schema name of namespace URI.
	 *
	 * @param namespace_uri namespace URI
	 * @return String PostgreSQL schema name of namespace URI
	 */
	private String getPgSchemaOf(String namespace_uri) {

		String pg_schema_name = option.pg_named_schema ? (def_namespaces.entrySet().stream().anyMatch(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()) ? def_namespaces.entrySet().stream().filter(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()).findFirst().get().getKey() : PgSchemaUtil.pg_public_schema_name) : PgSchemaUtil.pg_public_schema_name;

		return option.case_sense ? pg_schema_name : pg_schema_name.toLowerCase();
	}

	/**
	 * Return PostgreSQL name of table.
	 *
	 * @param table table
	 * @return String PostgreSQL name of table
	 */
	protected String getPgNameOf(PgTable table) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(table.name);
	}

	/**
	 * Return PostgreSQL name of table.
	 *
	 * @param db_conn database connection
	 * @param table table
	 * @return String PostgreSQL name of table
	 * @throws PgSchemaException the pg schema exception
	 */
	private String getPgNameOf(Connection db_conn, PgTable table) throws PgSchemaException {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(getDbTableName(db_conn, table.name));
	}

	/**
	 * Return PostgreSQL name of parent table.
	 *
	 * @param foreign_key foreign key
	 * @return String PostgreSQL name of parent table
	 */
	private String getPgParentNameOf(PgForeignKey foreign_key) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(foreign_key.pg_schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(foreign_key.parent_table_name);
	}

	/**
	 * Return PostgreSQL name of child table.
	 *
	 * @param foreign_key foreign key
	 * @return String PostgreSQL name of child table
	 */
	private String getPgChildNameOf(PgForeignKey foreign_key) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(foreign_key.pg_schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(foreign_key.child_table_name);
	}

	/**
	 * Return PostgreSQL name of foreign table.
	 *
	 * @param field field of either nested key or foreign key
	 * @return String PostgreSQL name of foreign table
	 */
	private String getPgForeignNameOf(PgField field) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(field.foreign_schema) + "." : "") + PgSchemaUtil.avoidPgReservedWords(field.foreign_table_name);
	}

	/**
	 * Return data (CSV/TSV) file name of table.
	 *
	 * @param table table
	 * @return String PostgreSQL name of table
	 */
	protected String getDataFileNameOf(PgTable table) {
		return (option.pg_named_schema ? table.pg_schema_name + "." : "") + table.name + (option.pg_tab_delimiter ? ".tsv" : ".csv");
	}

	/**
	 * Return table list.
	 *
	 * @return List table list
	 */
	protected List<PgTable> getTableList() {
		return tables;
	}

	/**
	 * Return root table.
	 *
	 * @return PgTable root table
	 */
	protected PgTable getRootTable() {
		return root_table;
	}

	/**
	 * Return table.
	 *
	 * @param table_id table id
	 * @return PgTable table
	 */
	protected PgTable getTable(int table_id) {
		return table_id < 0 || table_id >= tables.size() ? null : tables.get(table_id);
	}

	/**
	 * Return table.
	 *
	 * @param pg_schema_name PostgreSQL schema name
	 * @param table_name table name
	 * @return PgTable table
	 */
	private PgTable getTable(String pg_schema_name, String table_name) {
		return getTable(getTableId(pg_schema_name, table_name));
	}

	/**
	 * Return pending table.
	 *
	 * @param pending_group pending group
	 * @return PgTable pending table
	 */
	private PgTable getPendingTable(PgPendingGroup pending_group) {
		return getTable(pending_group.pg_schema_name, pending_group.name);
	}

	/**
	 * Return parent table of foreign key.
	 *
	 * @param foreign_key foreign key
	 * @return PgTable parent table
	 */
	private PgTable getParentTable(PgForeignKey foreign_key) {
		return getTable(foreign_key.pg_schema_name, foreign_key.parent_table_name);
	}

	/**
	 * Return parent table of child table from foreign keys.
	 *
	 * @param child_table child table
	 * @return PgTable parent table
	 */
	protected PgTable getFKParentTable(PgTable child_table) {

		Optional<PgForeignKey> opt = foreign_keys.stream().filter(foreign_key -> foreign_key.pg_schema_name.equals(child_table.pg_schema_name) && foreign_key.child_table_name.equals(child_table.name)).findFirst();

		return opt.isPresent() ? getParentTable(opt.get()) : null;	
	}

	/**
	 * Return child table of foreign key.
	 *
	 * @param foreign_key foreign key
	 * @return PgTable child table
	 */
	private PgTable getChildTable(PgForeignKey foreign_key) {
		return getTable(foreign_key.pg_schema_name, foreign_key.child_table_name);
	}

	/**
	 * Return foreign table of either nested key or foreign key.
	 *
	 * @param field field of either nested key of foreign key
	 * @return PgTable foreign table
	 */
	protected PgTable getForeignTable(PgField field) {
		return field.foreign_table_id == -1 ? getTable(field.foreign_schema, field.foreign_table_name) : getTable(field.foreign_table_id);
	}

	/**
	 * Return table id.
	 *
	 * @param pg_schema_name PostgreSQL schema name
	 * @param table_name table name
	 * @return int the table id, -1 represents not found
	 */
	private int getTableId(String pg_schema_name, String table_name) {

		if (!option.pg_named_schema)
			pg_schema_name = PgSchemaUtil.pg_public_schema_name;

		else if (pg_schema_name == null || pg_schema_name.isEmpty())
			pg_schema_name = root_table.pg_schema_name;

		for (int t = 0; t < tables.size(); t++) {

			PgTable table = tables.get(t);

			if (table.pg_schema_name.equals(pg_schema_name) && table.name.equals(table_name))
				return t;

		}

		return -1;
	}

	/**
	 * Return attribute group id from attribute group name.
	 *
	 * @param attr_group_name attribute group name
	 * @param throwable throws exception if declaration does not exist
	 * @return int the attribute group id, -1 represents not found
	 * @throws PgSchemaException the pg schema exception
	 */
	private int getAttributeGroupId(String attr_group_name, boolean throwable) throws PgSchemaException {

		for (int t = 0; t < _root_schema.attr_groups.size(); t++) {

			if (_root_schema.attr_groups.get(t).name.equals(attr_group_name))
				return t;

		}

		if (throwable)
			throw new PgSchemaException("Not found attribute group declaration: " + attr_group_name + ".");

		return -1;
	}

	/**
	 * Return model group id from model group name.
	 *
	 * @param model_group_name model group name
	 * @param throwable throws exception if declaration does not exist
	 * @return int the model group id, -1 represents not found
	 * @throws PgSchemaException the pg schema exception
	 */
	private int getModelGroupId(String model_group_name, boolean throwable) throws PgSchemaException {

		for (int t = 0; t < _root_schema.model_groups.size(); t++) {

			if (_root_schema.model_groups.get(t).name.equals(model_group_name))
				return t;

		}

		if (throwable)
			throw new PgSchemaException("Not found model group declaration: " + model_group_name + ".");

		return -1;
	}

	/**
	 * Realize PostgreSQL DDL.
	 */
	private void realize() {

		level = 0;

		// root table

		realize(root_table, false);

		// referenced administrative tables at first

		foreign_keys.stream().map(foreign_key -> getParentTable(foreign_key)).filter(table -> table != null).forEach(table -> {

			realizeAdmin(table, false);

			realize(table, false);

		});

		// administrative tables ordered by level

		tables.stream().filter(table -> (table.xs_type.equals(XsTableType.xs_root_child) || table.xs_type.equals(XsTableType.xs_admin_child)) && table.level > 0).sorted(Comparator.comparingInt(table -> table.level)).forEach(table -> {

			realizeAdmin(table, false);

			realize(table, false);

		});

		// remaining administrative tables

		tables.stream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_root)).forEach(table -> {

			realizeAdmin(table, false);

			realize(table, false);

		});

		setDocIdTable();

		if (!option.ddl_output)
			return;

		System.out.println("--");
		System.out.println("-- PostgreSQL DDL generated from " + def_schema_location + " using xsd2pgschema");
		System.out.println("--  xsd2pgschema - Database replication tool based on XML Schema");
		System.out.println("--  https://sourceforge.net/projects/xsd2pgschema/");
		System.out.println("--");
		System.out.println("-- Schema modeling options:");
		System.out.println("--  explicit named schema: " + option.pg_named_schema);
		System.out.println("--  relational extension: " + option.rel_model_ext);
		System.out.println("--  wild card extension: " + option.wild_card);
		System.out.println("--  case sensitive name: " + option.case_sense);
		System.out.println("--  no name collision: " + !tables.stream().anyMatch(table -> table.conflict));
		System.out.println("--  appended document key: " + option.document_key);
		System.out.println("--  appended serial key: " + option.serial_key);
		System.out.println("--  appended xpath key: " + option.xpath_key);
		System.out.println("--  retained constraint of primary/foreign key: " + option.retain_key);
		System.out.println("--  retrieved field annotation: " + !option.no_field_anno);
		if (option.rel_model_ext || option.serial_key)
			System.out.println("--  " + (md_hash_key == null ? "assumed " : "") + "hash algorithm: " + (md_hash_key == null ? PgSchemaUtil.def_hash_algorithm : md_hash_key.getAlgorithm()));
		if (option.rel_model_ext)
			System.out.println("--  hash key type: " + option.hash_size.name().replace("_", " ") + " bits");
		if (option.serial_key)
			System.out.println("--  searial key type: " + option.ser_size.name().replace("_", " ") + " bits");
		System.out.println("--");
		System.out.println("-- Statistics of schema:");
		System.out.print(def_stat_msg.toString());
		System.out.println("--\n");

		if (option.pg_named_schema) {

			HashSet<String> named_schemas = new HashSet<String>();

			tables.stream().filter(table -> table.required && (option.rel_model_ext || !table.relational)).forEach(table -> named_schemas.add(table.pg_schema_name));

			if (!named_schemas.isEmpty()) {

				named_schemas.forEach(named_schema -> System.out.println("DROP SCHEMA IF EXISTS " + PgSchemaUtil.avoidPgReservedWords(named_schema) + " CASCADE;"));

				System.out.println("");

				named_schemas.forEach(named_schema -> System.out.println("CREATE SCHEMA " + PgSchemaUtil.avoidPgReservedWords(named_schema) + ";"));

				System.out.print("\nSET search_path TO ");
				named_schemas.forEach(named_schema -> System.out.print(PgSchemaUtil.avoidPgReservedWords(named_schema) + ", "));
				System.out.println("public;");

			}

			System.out.println("");

			named_schemas.clear();

		}

		if (def_anno != null) {

			System.out.println("--");
			System.out.println("-- " + def_anno);
			System.out.println("--\n");

		}

		tables.stream().filter(table -> table.required && (option.rel_model_ext || !table.relational)).sorted(Comparator.comparingInt(table -> -table.order)).forEach(table -> {

			System.out.println("DROP TABLE IF EXISTS " + getPgNameOf(table) + " CASCADE;");

		});

		System.out.println("");

		tables.stream().filter(table -> !table.realized).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> realize(table, true));

		// add primary key/foreign key

		if (!option.retain_key) {

			tables.stream().filter(table -> !table.bridge).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

				table.fields.forEach(field -> {

					if (field.unique_key)
						System.out.println("--ALTER TABLE " + getPgNameOf(table) + " ADD PRIMARY KEY ( " + PgSchemaUtil.avoidPgReservedWords(field.name) + " );\n");

					else if (field.foreign_key) {

						if (!getForeignTable(field).bridge)
							System.out.println("--ALTER TABLE " + getPgNameOf(table) + " ADD FOREIGN KEY " + field.constraint_name + " REFERENCES " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_name) + " );\n");

					}

				});

			});

		}

		// add unique key for foreign key

		for (int fk = 0; fk < foreign_keys.size(); fk++) {

			PgForeignKey foreign_key = foreign_keys.get(fk);

			boolean unique = true;

			for (int fk2 = 0; fk2 < fk; fk2++) {

				PgForeignKey foreign_key2 = foreign_keys.get(fk2);

				if (foreign_key.parent_table_name.equals(foreign_key2.parent_table_name)) {
					unique = false;
					break;
				}

			}

			if (!unique)
				continue;

			PgTable table = getParentTable(foreign_key);

			if (table != null) {

				if (!option.rel_model_ext && table.relational)
					unique = false;

				PgField field = table.getField(foreign_key.parent_field_names);

				if (field != null) {

					if (field.primary_key)
						unique = false;

				}

				else
					continue;

			}

			if (!unique)
				continue;

			String constraint_name = "UNQ_" + foreign_key.parent_table_name;

			if (constraint_name.length() > PgSchemaUtil.max_enum_len)
				constraint_name = constraint_name.substring(0, PgSchemaUtil.max_enum_len);

			System.out.println((option.retain_key ? "" : "--") + "ALTER TABLE " + getPgParentNameOf(foreign_key) + " ADD CONSTRAINT " + PgSchemaUtil.avoidPgReservedOps(constraint_name) + " UNIQUE ( " + PgSchemaUtil.avoidPgReservedWords(foreign_key.parent_field_names) + " );\n");

		}

		// add foreign key constraint

		for (PgForeignKey foreign_key : foreign_keys) {

			boolean relational = false;

			PgTable child_table = getChildTable(foreign_key);

			if (child_table != null)
				relational = child_table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			PgTable parent_table = getParentTable(foreign_key);

			if (parent_table != null)
				relational = parent_table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			String[] child_field_names = foreign_key.child_field_names.split(" ");
			String[] parent_field_names = foreign_key.parent_field_names.split(" ");

			if (child_field_names.length == parent_field_names.length) {

				for (int i = 0; i < child_field_names.length; i++) {

					child_field_names[i] = child_field_names[i].replaceFirst(",$", "");
					parent_field_names[i] = parent_field_names[i].replaceFirst(",$", "");

					String constraint_name = "KR_" + foreign_key.name + (child_field_names.length > 1 ? "_" + i : "");

					if (constraint_name.length() > PgSchemaUtil.max_enum_len)
						constraint_name = constraint_name.substring(0, PgSchemaUtil.max_enum_len);

					System.out.println((option.retain_key ? "" : "--") + "ALTER TABLE " + getPgChildNameOf(foreign_key) + " ADD CONSTRAINT " + PgSchemaUtil.avoidPgReservedOps(constraint_name) + " FOREIGN KEY ( " + PgSchemaUtil.avoidPgReservedWords(child_field_names[i]) + " ) REFERENCES " + getPgNameOf(getParentTable(foreign_key)) + " ( " + PgSchemaUtil.avoidPgReservedWords(parent_field_names[i]) + " ) ON DELETE CASCADE NOT VALID;\n");

				}

			}

		}

	}

	/**
	 * Realize PostgreSQL DDL of administrative table.
	 *
	 * @param table current table
	 * @param output whether outputs PostgreSQL DDL via standard output
	 */
	private void realizeAdmin(PgTable table, boolean output) {

		// realize parent table at first

		foreign_keys.stream().filter(foreign_key -> foreign_key.pg_schema_name.equals(table.pg_schema_name) && foreign_key.child_table_name.equals(table.name)).map(foreign_key -> getParentTable(foreign_key)).filter(admin_table -> admin_table != null).forEach(admin_table -> {

			realizeAdmin(admin_table, output);

			realize(admin_table, output);

		});

		// set foreign_table_id as table pointer otherwise remove foreign key

		Iterator<PgField> iterator = table.fields.iterator();

		while (iterator.hasNext()) {

			PgField field = iterator.next();

			if (field.foreign_key) {

				PgTable admin_table = getForeignTable(field);

				if (admin_table != null) {

					field.foreign_table_id = tables.indexOf(admin_table);

					realizeAdmin(admin_table, output);

					realize(admin_table, output);

				}

				else
					iterator.remove();

			}

		}

	}

	/**
	 * Realize PostgreSQL DDL of arbitrary table.
	 *
	 * @param table current table
	 * @param output whether outputs PostgreSQL DDL via standard output
	 */
	private void realize(PgTable table, boolean output) {

		if (table == null)
			return;

		if (table.realized)
			return;

		if (!output) {
			table.order--;
			return;
		}

		if (!table.required)
			return;

		if (!option.rel_model_ext && table.relational)
			return;

		System.out.println("--");
		System.out.println("-- " + (table.anno != null && !table.anno.isEmpty() ? table.anno : "No annotation is available"));

		StringBuilder sb = new StringBuilder();

		if (table.target_namespace != null && !table.target_namespace.isEmpty()) {

			for (String namespace_uri : table.target_namespace.split(" "))
				sb.append(namespace_uri + " (" + getAbsolutePrefixOf(namespace_uri, "default") + "), ");

		}

		else
			sb.append("null, ");

		System.out.println("-- xmlns: " + sb.toString() + "schema location: " + table.schema_location);
		System.out.println("-- type: " + table.xs_type.toString().replaceFirst("^xs_", "").replace("_",  " ") + ", content: " + table.content_holder + ", list: " + table.list_holder + ", bridge: " + table.bridge + ", virtual: " + table.virtual + (tables.stream().anyMatch(_table -> _table.conflict) ? ", name collision: " + table.conflict : ""));
		System.out.println("--");

		sb.setLength(0);

		List<PgField> fields = table.fields;

		fields.stream().filter(field -> field.enum_name != null && !field.enum_name.isEmpty()).forEach(field -> {

			System.out.println("DROP TYPE IF EXISTS " + (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + field.enum_name + ";");

			System.out.print("CREATE TYPE " + (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + field.enum_name + " AS ENUM (");

			for (int i = 0; i < field.enumeration.length; i++) {

				System.out.print(" '" + field.enumeration[i] + "'");

				if (i < field.enumeration.length - 1)
					System.out.print(",");

			}

			System.out.println(" );");

		});

		System.out.println("CREATE TABLE " + getPgNameOf(table) + " (");

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
				System.out.println("-- FOREIGN KEY : " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_name) + " )");

			else if (field.nested_key)
				System.out.println("-- NESTED KEY : " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_name) + " )" + (field.parent_node != null ? ", PARENT NODE : " + field.parent_node : "") + (field.ancestor_node != null ? ", ANCESTOR NODE : " + field.ancestor_node : ""));

			else if (field.attribute) {

				if (option.discarded_document_key_names.contains(field.xname) || option.discarded_document_key_names.contains(table.name + "." + field.xname))
					continue;

				if (!option.document_key && option.inplace_document_key && (option.inplace_document_key_names.contains(field.xname) || option.inplace_document_key_names.contains(table.name + "." + field.xname)))
					System.out.println("-- ATTRIBUTE, IN-PLACE DOCUMENT KEY");
				else
					System.out.println("-- ATTRIBUTE");

			}

			else if (field.simple_content)
				System.out.println("-- SIMPLE CONTENT");

			else if (field.any)
				System.out.println("-- ANY ELEMENT");

			else if (field.any_attribute)
				System.out.println("-- ANY ATTRIBUTE");

			else if (option.discarded_document_key_names.contains(field.xname) || option.discarded_document_key_names.contains(table.name + "." + field.xname))
				continue;

			else if (!option.document_key && option.inplace_document_key && (option.inplace_document_key_names.contains(field.xname) || option.inplace_document_key_names.contains(table.name + "." + field.xname)))
				System.out.println("-- IN-PLACE DOCUMENT KEY");

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
				System.out.print("\t" + PgSchemaUtil.avoidPgReservedWords(field.name) + " " + field.getPgDataType());
			else
				System.out.print("\t" + PgSchemaUtil.avoidPgReservedWords(field.name) + " " + (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + field.enum_name);

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
					System.out.print(" CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.name) + " = '" + field.fixed_value + "' )");
					break;
				default:
					System.out.print(" CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.name) + " = " + field.fixed_value + " )");
				}

			}

			if (field.required)
				System.out.print(" NOT NULL");

			if (option.retain_key) {

				if (field.unique_key) {

					System.out.print(" PRIMARY KEY");

				}

				else if (field.foreign_key) {

					PgTable foreign_table = getForeignTable(field);

					PgField foreign_field = foreign_table.getField(field.foreign_field_name);

					if (foreign_field != null) {

						if (foreign_field.unique_key)
							System.out.print(" CONSTRAINT " + field.constraint_name + " REFERENCES " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_name) + " ) ON DELETE CASCADE");

					}

				}

			}

			if (f < fields.size() - 1)
				System.out.println(" ," + (option.no_field_anno || field.anno == null || field.anno.isEmpty() ? "" : " -- " + field.anno));
			else
				System.out.println((option.no_field_anno || field.anno == null || field.anno.isEmpty() ? "" : " -- " + field.anno));

		}

		System.out.println(");\n");

	}

	/**
	 * Count the total number of effective foreign key references.
	 *
	 * @return int the total number of effective foreign key references
	 */
	private int countForeignKeyReferences() {

		int key_references = 0;

		for (PgForeignKey foreign_key : foreign_keys) {

			boolean relational = false;

			PgTable child_table = getChildTable(foreign_key);

			if (child_table != null)
				relational = child_table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			PgTable parent_table = getParentTable(foreign_key);

			if (parent_table != null)
				relational = parent_table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			String[] child_field_names = foreign_key.child_field_names.split(" ");
			String[] parent_field_names = foreign_key.parent_field_names.split(" ");

			if (child_field_names.length == parent_field_names.length)
				key_references += child_field_names.length;

		}

		return key_references;
	}

	// post XML editorial functions

	/**
	 * Apply XML post editor.
	 *
	 * @param xml_post_editor XML post editor
	 * @throws PgSchemaException the pg schema exception
	 */
	public void applyXmlPostEditor(XmlPostEditor xml_post_editor) throws PgSchemaException {

		if (xml_post_editor.filt_ins != null) {

			xml_post_editor.filt_in_resolved = false;

			applyFiltIn(xml_post_editor);

		}

		if (xml_post_editor.filt_outs != null) {

			xml_post_editor.filt_out_resolved = false;

			applyFiltOut(xml_post_editor);

		}

		if (xml_post_editor.fill_these != null) {

			xml_post_editor.fill_this_resolved = false;

			applyFillThis(xml_post_editor);

		}

	}

	/**
	 * Apply filt-in option.
	 *
	 * @param xml_post_editor XML post editor
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyFiltIn(XmlPostEditor xml_post_editor) throws PgSchemaException {

		if (xml_post_editor.filt_in_resolved)
			return;

		xml_post_editor.filt_in_resolved = true;

		if (xml_post_editor.filt_ins.size() == 0)
			return;

		for (String filt_in : xml_post_editor.filt_ins) {

			String[] key_val = filt_in.split(":");
			String[] key = key_val[0].split("\\.");

			String schema_name = null;
			String table_name = null;
			String field_name = null;

			if (option.pg_named_schema) {

				if (key_val.length != 1)
					throw new PgSchemaException(filt_in + ": argument should be expressed by \"schema_name.table_name.column_name\".");

				switch (key.length) {
				case 3:
					field_name = key[2];
				case 2:
					table_name = key[1];
					schema_name = key[0];
					break;
				default:
					throw new PgSchemaException(filt_in + ": argument should be expressed by \"schema_name.table_name.column_name\".");
				}

			}

			else {

				if (key_val.length != 1)
					throw new PgSchemaException(filt_in + ": argument should be expressed by \"table_name.column_name\".");

				switch (key.length) {
				case 2:
					field_name = key[1];
				case 1:
					table_name = key[0];
					schema_name = PgSchemaUtil.pg_public_schema_name;
					break;
				default:
					throw new PgSchemaException(filt_in + ": argument should be expressed by \"table_name.column_name\".");
				}

			}

			PgTable table = getTable(schema_name, table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (field_name != null && !field_name.isEmpty()) {

				if (table.getFieldId(field_name) >= 0)
					table.filt_out = true;

				else
					throw new PgSchemaException("Not found " + table_name + "." + field_name + ".");

			}

			else {

				if (table.content_holder)
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

						PgTable nested_table = getForeignTable(field);

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

	/**
	 * Apply filt-out option.
	 *
	 * @param xml_post_editor XML post editor
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyFiltOut(XmlPostEditor xml_post_editor) throws PgSchemaException {

		if (xml_post_editor.filt_out_resolved)
			return;

		xml_post_editor.filt_out_resolved = true;

		if (xml_post_editor.filt_outs.size() == 0)
			return;

		for (String filt_out : xml_post_editor.filt_outs) {

			String[] key_val = filt_out.split(":");
			String[] key = key_val[0].split("\\.");

			String schema_name = null;
			String table_name = null;
			String field_name = null;

			if (option.pg_named_schema) {

				if (key_val.length != 2)
					throw new PgSchemaException(filt_out + ": argument should be expressed by \"schema_name.table_name.column_name:regex_pattern(|regex_pattern)\".");

				switch (key.length) {
				case 3:
					field_name = key[2];
				case 2:
					table_name = key[1];
					schema_name = key[0];
					break;
				default:
					throw new PgSchemaException(filt_out + ": argument should be expressed by \"schema_name.table_name.column_name:regex_pattern(|regex_pattern)\".");
				}

			}

			else {

				if (key_val.length != 2)
					throw new PgSchemaException(filt_out + ": argument should be expressed by \"table_name.column_name:regex_pattern(|regex_pattern)\".");

				switch (key.length) {
				case 2:
					field_name = key[1];
				case 1:
					table_name = key[0];
					schema_name = PgSchemaUtil.pg_public_schema_name;
					break;
				default:
					throw new PgSchemaException(filt_out + ": argument should be expressed by \"table_name.column_name:regex_pattern(|regex_pattern)\".");
				}

			}

			String[] rex_pattern = key_val[1].split("\\|");

			PgTable table = getTable(schema_name, table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " is unselectable (root table).");

			if (field_name != null && !field_name.isEmpty()) {

				PgField field = table.getField(field_name);

				if (field != null) {

					if (field.system_key || field.user_key)
						throw new PgSchemaException(table_name + "." + field_name + " is administrative key.");

					field.filt_out = true;
					field.filter_pattern = rex_pattern;

				}

				else
					throw new PgSchemaException("Not found " + table_name + "." + field_name + ".");

			}

			else {

				table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> {

					field.filt_out = true;
					field.filter_pattern = rex_pattern;

				});

			}

		}

	}

	/**
	 * Apply fill-this option.
	 *
	 * @param xml_post_editor XML post editor
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyFillThis(XmlPostEditor xml_post_editor) throws PgSchemaException {

		if (xml_post_editor.fill_this_resolved)
			return;

		xml_post_editor.fill_this_resolved = true;

		if (xml_post_editor.fill_these.size() == 0)
			return;

		for (String fill_this : xml_post_editor.fill_these) {

			String[] key_val = fill_this.split(":");
			String[] key = key_val[0].split("\\.");

			String schema_name = null;
			String table_name = null;
			String field_name = null;

			String filled_text = key_val.length > 1 ? key_val[1] : "";

			if (option.pg_named_schema) {

				if (key_val.length < 1 || key_val.length > 2)
					throw new PgSchemaException(fill_this + ": argument should be expressed by \"schema_name.table_name.column_name:filling_text\".");

				switch (key.length) {
				case 3:
					field_name = key[2];
				case 2:
					table_name = key[1];
					schema_name = key[0];
					break;
				default:
					throw new PgSchemaException(fill_this + ": argument should be expressed by \"schema_name.table_name.column_name:filling_text\".");
				}

			}

			else {

				if (key_val.length < 1 || key_val.length > 2)
					throw new PgSchemaException(fill_this + ": argument should be expressed by \"table_name.column_name:filling_text\".");

				switch (key.length) {
				case 2:
					field_name = key[1];
				case 1:
					table_name = key[0];
					schema_name = PgSchemaUtil.pg_public_schema_name;
					break;
				default:
					throw new PgSchemaException(fill_this + ": argument should be expressed by \"table_name.column_name:filling_text\".");
				}

			}

			PgTable table = getTable(schema_name, table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " is unselectable (root table).");

			PgField field = table.getField(field_name);

			if (field != null) {

				if (field.system_key || field.user_key)
					throw new PgSchemaException(table_name + "." + field_name + " is administrative key.");

				field.fill_this = true;
				field.filled_text = filled_text;

			}

			else
				throw new PgSchemaException("Not found " + table + "." + field_name + ".");

		}

	}

	/**
	 * Apply filter for full-text indexing.
	 *
	 * @param index_filter index filter
	 * @throws PgSchemaException the pg schema exception
	 */
	public void applyIndexFilter(IndexFilter index_filter) throws PgSchemaException {

		this.index_filter = index_filter;

		option.attr_resolved = false;

		applyAttr(index_filter);

		if (index_filter.fields != null) {

			option.field_resolved = false;

			applyField(index_filter);

		}

		// update indexable flag

		tables.forEach(table -> table.fields.forEach(field -> field.setIndexable(table, option)));

	}

	/**
	 * Apply attr option for full-text indexing.
	 *
	 * @param index_filter index filter
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyAttr(IndexFilter index_filter) throws PgSchemaException {

		if (option.attr_resolved)
			return;

		option.attr_resolved = true;

		// select all xs:ID as attribute

		tables.forEach(table -> table.fields.stream().filter(field -> field.xs_type.equals(XsDataType.xs_ID)).forEach(field -> field.attr_sel = true));

		// type dependent attribute selection

		if (index_filter.attr_string || index_filter.attr_integer || index_filter.attr_float || index_filter.attr_date)
			tables.forEach(table -> table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> index_filter.appendAttrByType(table.name, field)));

		// select all attributes

		if (index_filter.attrs == null) {

			tables.forEach(table -> table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> field.attr_sel = true));

			applySphMVA(index_filter);

			return;
		}

		if (index_filter.attrs.size() == 0)
			return;

		for (String attr : index_filter.attrs) {

			String[] key = attr.split("\\.");

			String schema_name = PgSchemaUtil.pg_public_schema_name;
			String table_name = null;
			String field_name = null;

			switch (key.length) {
			case 2:
				field_name = key[1];
			case 1:
				table_name = key[0];
				break;
			default:
				throw new PgSchemaException(attr + ": argument should be expressed by \"table_name.column_name\".");
			}

			PgTable table = getTable(schema_name, table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " is unselectable (root table).");

			if (field_name != null && !field_name.isEmpty()) {

				PgField field = table.getField(field_name);

				if (field != null) {

					if (field.system_key || field.user_key)
						throw new PgSchemaException(table_name + "." + field_name + " is administrative key.");

					field.attr_sel = true;

				}

				else
					throw new PgSchemaException("Not found " + table_name + "." + field_name + ".");

			}

			else
				table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> field.attr_sel = true);

		}

		applySphMVA(index_filter);

	}

	/**
	 * Apply mva option for full-text indexing.
	 *
	 * @param index_filter index filter
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applySphMVA(IndexFilter index_filter) throws PgSchemaException {

		if (index_filter.sph_mvas.size() == 0)
			return;

		for (String sph_mva : index_filter.sph_mvas) {

			String[] key = sph_mva.split("\\.");

			String schema_name = PgSchemaUtil.pg_public_schema_name;
			String table_name = null;
			String field_name = null;

			switch (key.length) {
			case 2:
				field_name = key[1];
			case 1:
				table_name = key[0];
				break;
			default:
				throw new PgSchemaException(sph_mva + ": argument should be expressed by \"table_name.column_name\".");
			}

			PgTable table = getTable(schema_name, table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " is unselectable (root table).");

			PgField field = table.getField(field_name);

			if (field != null) {

				if (field.system_key || field.user_key)
					throw new PgSchemaException(table_name + "." + field_name + " is administrative key.");

				switch (field.xs_type) {
				case xs_bigserial:
				case xs_long:
				case xs_bigint:
				case xs_unsignedLong:
				case xs_duration:
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
					field.sph_mva = true;
					break;
				default:
					throw new PgSchemaException("Data type of " + table_name + "." + field_name + " is not integer.");
				}

			}

			else
				throw new PgSchemaException("Not found " + table_name + "." + field_name + ".");

		}

	}

	/**
	 * Apply field option for full-text indexing.
	 *
	 * @param index_filer index filter
	 * @throws PgSchemaException the pg schema exception
	 */
	private void applyField(IndexFilter index_filter) throws PgSchemaException {

		if (option.field_resolved)
			return;

		if (index_filter.fields.size() == 0)
			return;

		option.field_resolved = true;

		for (String field : index_filter.fields) {

			String[] key = field.split("\\.");

			String schema_name = null;
			String table_name = null;
			String field_name = null;

			if (option.pg_named_schema) {

				switch (key.length) {
				case 3:
					field_name = key[2];
				case 2:
					table_name = key[1];
					schema_name = key[0];
					break;
				default:
					throw new PgSchemaException(field + ": argument should be expressed by \"schema_name.table_name.column_name\".");
				}

			}

			else {

				switch (key.length) {
				case 2:
					field_name = key[1];
				case 1:
					table_name = key[0];
					schema_name = PgSchemaUtil.pg_public_schema_name;
					break;
				default:
					throw new PgSchemaException(field + ": argument should be expressed by \"table_name.column_name\".");
				}

			}

			PgTable _table = getTable(schema_name, table_name);

			if (_table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (_table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " is unselectable (root table).");

			if (field_name != null && !field_name.isEmpty()) {

				PgField _field = _table.getField(field_name);

				if (_field != null) {

					if (_field.system_key || _field.user_key)
						throw new PgSchemaException(table_name + "." + field_name + " is administrative key.");

					_field.field_sel = true;

				}

				else
					throw new PgSchemaException("Not found " + table_name + "." + field_name + ".");

			}

			else
				_table.fields.stream().filter(_field -> !_field.system_key && !_field.user_key).forEach(_field -> _field.field_sel = true);

		}

		tables.forEach(_table -> {

			if (!_table.fields.stream().anyMatch(_field -> !_field.system_key && !_field.user_key && (_field.attr_sel || _field.field_sel)))
				_table.required = false;

		});

	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return String hash key
	 */
	protected synchronized String getHashKeyString(String key_name) {

		if (md_hash_key == null) // debug mode
			return key_name;

		try {

			byte[] bytes = md_hash_key.digest(key_name.getBytes());

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

		} finally {
			md_hash_key.reset();
		}

	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return bytes[] hash key
	 */
	protected synchronized byte[] getHashKeyBytes(String key_name) {

		try {

			return md_hash_key.digest(key_name.getBytes());

		} finally {
			md_hash_key.reset();
		}

	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return int the hash key
	 */
	protected synchronized int getHashKeyInt(String key_name) {

		try {

			byte[] hash = md_hash_key.digest(key_name.getBytes());

			BigInteger bint = new BigInteger(hash);

			return Math.abs(bint.intValue()); // use lower order 32bit

		} finally {
			md_hash_key.reset();
		}

	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return long hash key
	 */
	protected synchronized long getHashKeyLong(String key_name) {

		try {

			byte[] hash = md_hash_key.digest(key_name.getBytes());

			BigInteger bint = new BigInteger(hash);

			return Math.abs(bint.longValue()); // use lower order 64bit

		} finally {
			md_hash_key.reset();
		}

	}

	/**
	 * Check whether schema has root table.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	private void hasRootTable() throws PgSchemaException {

		if (root_table == null)
			throw new PgSchemaException("Not found root table in XML Schema: " + def_schema_location);

	}

	/**
	 * Return root node of document.
	 *
	 * @param xml_parser XML document
	 * @return Node root node of document
	 * @throws PgSchemaException the pg schema exception
	 */
	private Node getRootNode(XmlParser xml_parser) throws PgSchemaException {

		hasRootTable();

		Node node = xml_parser.document.getDocumentElement();

		// check root element name

		if (!option.getUnqualifiedName(node.getNodeName()).equals(root_table.name))
			throw new PgSchemaException("Not found root element (node_name: " + root_table.name + ") in XML: " + document_id);

		document_id = xml_parser.document_id;

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
	 * @param single_lock whether single lock object or individual lock objects for all tables
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

		if (table_lock == null)
			return;

		for (int t = 0; t < tables.size() && t < table_lock.length; t++)
			table_lock[t] = null;

		table_lock = null;

	}

	// Data (CSV/TSV) conversion

	/**
	 * Data (CSV/TSV) conversion.
	 *
	 * @param xml_parser XML document
	 * @param work_dir working directory contains data (CSV/TSV) files
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgCsv(XmlParser xml_parser, File work_dir) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(false);

		tables.forEach(table -> {

			if (table.required && (option.rel_data_ext || !table.relational)) {

				if (table.buffw == null) {

					File data_file = new File(work_dir, getDataFileNameOf(table));

					try {

						table.filew = new FileWriter(data_file);
						table.buffw = new BufferedWriter(table.filew);

					} catch (IOException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

			}

			else {
				table.buffw = null;
				table.filew = null;
			}

		});

		// parse root node and write to data (CSV/TSV) file

		try {

			PgSchemaNode2PgCsv node2pgcsv = new PgSchemaNode2PgCsv(this, null, root_table);

			node2pgcsv.parseRootNode(node);

			node2pgcsv.invokeRootNestedNode();

		} catch (IOException | ParserConfigurationException | TransformerException e) {
			throw new PgSchemaException(e);
		}

		xml_parser.clear();

	}

	/**
	 * Close xml2PgCsv.
	 */
	public void closeXml2PgCsv() {

		closeTableLock();

		tables.stream().filter(table -> table.buffw != null).forEach(table -> {

			try {

				table.buffw.close();
				table.filew.close();

				table.buffw = null;
				table.filew = null;

			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		});

	}

	/**
	 * Parse current node and write to data (CSV/TSV) file.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2PgCsv(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) throws PgSchemaException {

		final int table_id = tables.indexOf(table);

		try {

			PgSchemaNode2PgCsv node2pgcsv = new PgSchemaNode2PgCsv(this, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

				if (node_test.omissible)
					continue;

				synchronized (table_lock[table_id]) {
					node2pgcsv.parseChildNode(node_test);
				}

				node2pgcsv.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2pgcsv.invoked)
				return;

			synchronized (table_lock[table_id]) {
				node2pgcsv.parseChildNode(parent_node, parent_key, proc_key, nested);
			}

			node2pgcsv.invokeChildNestedNode();

		} catch (ParserConfigurationException | IOException | TransformerException e) {
			throw new PgSchemaException(e);
		}

	}

	// PostgreSQL data migration via prepared statement

	/**
	 * PostgreSQL data migration.
	 *
	 * @param xml_parser XML document
	 * @param update whether update or insert
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgSql(XmlParser xml_parser, boolean update, Connection db_conn) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		// parse root node and send to PostgreSQL

		try {

			if (update) {

				deleteBeforeUpdate(db_conn, option.rel_data_ext);

				if (!option.rel_data_ext)
					update = false;

			}

			PgSchemaNode2PgSql node2pgsql = new PgSchemaNode2PgSql(this, null, root_table, update, db_conn);

			node2pgsql.parseRootNode(node);
			node2pgsql.executeBatch();

			node2pgsql.invokeRootNestedNode();

			db_conn.commit(); // transaction ends

		} catch (SQLException | ParserConfigurationException | TransformerException | IOException e) {
			throw new PgSchemaException(e);
		}

		xml_parser.clear();

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
	 * @param update whether update or insert
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2PgSql(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id, final boolean update, final Connection db_conn) throws PgSchemaException {

		try {

			PgSchemaNode2PgSql node2pgsql = new PgSchemaNode2PgSql(this, parent_table, table, update, db_conn);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

				if (node_test.omissible)
					continue;

				node2pgsql.parseChildNode(node_test);

				node2pgsql.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			try {

				if (node2pgsql.invoked)
					return;

				node2pgsql.parseChildNode(parent_node, parent_key, proc_key, nested);

				node2pgsql.invokeChildNestedNode();

			} finally {
				node2pgsql.executeBatch();
			}

		} catch (SQLException | ParserConfigurationException | IOException | TransformerException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Return document key name.
	 *
	 * @param table current table
	 * @return String document key name
	 * @throws PgSchemaException the pg schema exception
	 */
	protected String getDocKeyName(PgTable table) throws PgSchemaException {

		if (option.document_key)
			return option.document_key_name;

		if (!option.inplace_document_key)
			throw new PgSchemaException("Not defined document key, or select either --doc-key or --doc-key-if-no-inplace option.");

		if (!table.fields.stream().anyMatch(field -> (field.attribute || field.element) && (option.inplace_document_key_names.contains(field.xname) || option.inplace_document_key_names.contains(table.name + "." + field.xname)))) {

			if (option.document_key_if_no_in_place)
				return option.document_key_name;

			throw new PgSchemaException("Not found in-place document key in " + table.name + ", or select --doc-key-if-no-inplace option.");
		}

		return table.fields.stream().filter(field -> (field.attribute || field.element) && (option.inplace_document_key_names.contains(field.xname) || option.inplace_document_key_names.contains(table.name + "." + field.xname))).findFirst().get().name;
	}

	/**
	 * Decide primary table for questing document id.
	 */
	private void setDocIdTable() {

		if (doc_id_table != null)
			return;

		doc_id_table = root_table;

		if (!option.rel_data_ext)
			doc_id_table = tables.stream().filter(table -> table.required && (option.rel_data_ext || !table.relational)).min(Comparator.comparingInt(table -> table.order)).get();

	}

	/**
	 * Return set of document ids stored in PostgreSQL.
	 *
	 * @param db_conn database connection
	 * @return HashSet set of document ids stored in PostgreSQL
	 * @throws PgSchemaException the pg schema exception
	 */
	public HashSet<String> getDocIdRows(Connection db_conn) throws PgSchemaException {

		HashSet<String> set = new HashSet<String>();

		try {

			Statement stat = db_conn.createStatement();

			String sql = "SELECT " + PgSchemaUtil.avoidPgReservedWords(getDocKeyName(doc_id_table)) + " FROM " + getPgNameOf(db_conn, doc_id_table);

			ResultSet rset = stat.executeQuery(sql);

			while (rset.next())
				set.add(rset.getString(1));

			rset.close();

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

		return set;
	}

	/** Whether PostgreSQL table (key) has any rows or not (value). */
	private HashMap<String, Boolean> has_db_rows = null;

	/**
	 * Execute PostgreSQL DELETE command for strict synchronization.
	 *
	 * @param db_conn database connection
	 * @param set set of target document ids
	 * @throws PgSchemaException the pg schema exception
	 */
	public void deleteRows(Connection db_conn, HashSet<String> set) throws PgSchemaException {

		if (has_db_rows == null && !option.rel_data_ext) {

			try {

				Statement stat = db_conn.createStatement();

				has_db_rows = new HashMap<String, Boolean>();

				String doc_id_table_name = doc_id_table.name;

				String sql1 = "SELECT EXISTS(SELECT 1 FROM " + getPgNameOf(db_conn, doc_id_table) + " LIMIT 1)";

				ResultSet rset1 = stat.executeQuery(sql1);

				if (rset1.next())
					has_db_rows.put(doc_id_table_name, rset1.getBoolean(1));

				rset1.close();

				boolean has_doc_id = has_db_rows.get(doc_id_table_name);

				tables.stream().filter(table -> table.required && !table.relational && !table.equals(doc_id_table)).forEach(table -> {

					String table_name = table.name;

					if (has_doc_id) {

						try {

							String sql2 = "SELECT EXISTS(SELECT 1 FROM " + getPgNameOf(db_conn, table) + " LIMIT 1)";

							ResultSet rset2 = stat.executeQuery(sql2);

							if (rset2.next())
								has_db_rows.put(table_name, rset2.getBoolean(1));

							rset2.close();

						} catch (SQLException | PgSchemaException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

					else
						has_db_rows.put(table_name, false);

				});

				stat.close();

			} catch (SQLException e) {
				throw new PgSchemaException(e);
			}

		}

		set.forEach(id -> {

			document_id = id;

			try {

				deleteBeforeUpdate(db_conn, false);

			} catch (PgSchemaException e) {
				e.printStackTrace();
				System.exit(1);
			}

		});

		document_id = null;

	}

	/** The set of PostgreSQL table. */
	private HashSet<String> db_tables = null;

	/**
	 * Return exact table name in PostgreSQL
	 *
	 * @param db_conn database connection
	 * @param table_name table name
	 * @throws PgSchemaException the pg schema exception
	 */
	private String getDbTableName(Connection db_conn, String table_name) throws PgSchemaException {

		if (db_tables == null) {

			db_tables = new HashSet<String>();

			try {

				DatabaseMetaData meta = db_conn.getMetaData();
				ResultSet rset = meta.getTables(null, null, null, null);

				while (rset.next())
					db_tables.add(rset.getString("TABLE_NAME"));

				rset.close();

			} catch (SQLException e) {
				throw new PgSchemaException(e);
			}

		}

		Optional<String> opt = db_tables.stream().filter(db_table_name -> option.case_sense ? db_table_name.equals(table_name) : db_table_name.equalsIgnoreCase(table_name)).findFirst();

		if (!opt.isPresent())
			throw new PgSchemaException(db_conn.toString() + " : " + table_name + " not found in the database."); // not found in the database

		return opt.get();
	}

	/**
	 * Execute PostgreSQL DELETE command before INSERT for all tables of current document.
	 *
	 * @param db_conn database connection
	 * @param no_pkey whether delete relations not having primary key or non selective
	 * @throws PgSchemaException the pg schema exception
	 */
	private void deleteBeforeUpdate(Connection db_conn, boolean no_pkey) throws PgSchemaException {

		try {

			Statement stat = db_conn.createStatement();

			boolean has_doc_id = false;

			if (has_db_rows == null || (has_db_rows != null && has_db_rows.get(doc_id_table.name))) {

				String sql = "DELETE FROM " + getPgNameOf(db_conn, doc_id_table) + " WHERE " + PgSchemaUtil.avoidPgReservedWords(getDocKeyName(doc_id_table)) + "='" + document_id + "'";

				has_doc_id = stat.executeUpdate(sql) > 0;

			}

			if (has_doc_id) {

				tables.stream().filter(table -> table.required && (option.rel_data_ext || !table.relational) && !table.equals(doc_id_table) && ((no_pkey && !table.fields.stream().anyMatch(field -> field.primary_key && field.unique_key)) || !no_pkey)).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

					if (has_db_rows == null || (has_db_rows != null && has_db_rows.get(table.name))) {

						try {

							String sql = "DELETE FROM " + getPgNameOf(db_conn, table) + " WHERE " + PgSchemaUtil.avoidPgReservedWords(getDocKeyName(table)) + "='" + document_id + "'";

							stat.executeUpdate(sql);

						} catch (PgSchemaException | SQLException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

				});

			}

			stat.close();

			if (has_doc_id)
				db_conn.commit(); // transaction ends

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Execute PostgreSQL COPY command for all data (CSV/TSV) files.
	 *
	 * @param db_conn database connection
	 * @param work_dir working directory contains data (CSV/TSV) files
	 * @throws PgSchemaException the pg schema exception
	 */
	public void pgCsv2PgSql(Connection db_conn, File work_dir) throws PgSchemaException {

		try {

			CopyManager copy_man = new CopyManager((BaseConnection) db_conn);

			tables.stream().filter(table -> table.required && (option.rel_data_ext || !table.relational)).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

				File data_file = new File(work_dir, getDataFileNameOf(table));

				try {

					String sql = "COPY " + getPgNameOf(db_conn, table) + " FROM STDIN" + (option.pg_tab_delimiter ? "" : " CSV");

					copy_man.copyIn(sql, new FileInputStream(data_file));

				} catch (SQLException | IOException | PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Perform consistency test on PostgreSQL DDL.
	 *
	 * @param db_conn database connection
	 * @param strict whether perform strict consistency test
	 * @throws PgSchemaException the pg schema exception
	 */
	public void testPgSql(Connection db_conn, boolean strict) throws PgSchemaException {

		try {

			DatabaseMetaData meta = db_conn.getMetaData();

			Statement stat = db_conn.createStatement();

			tables.stream().filter(table -> table.required && (option.rel_data_ext || !table.relational)).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

				try {

					String table_name = table.name;
					String db_table_name = getDbTableName(db_conn, table_name);

					ResultSet rset_col = meta.getColumns(null, null, db_table_name, null);

					while (rset_col.next()) {

						String db_column_name = rset_col.getString("COLUMN_NAME");

						if (!table.fields.stream().filter(field -> !field.omissible).anyMatch(field -> option.case_sense ? field.name.equals(db_column_name) : field.name.equalsIgnoreCase(db_column_name)))
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + (option.case_sense ? db_column_name : db_column_name.toLowerCase()) + " found without declaration in the data model."); // found without declaration in the data model

					}

					rset_col.close();

					for (PgField field : table.fields) {

						if (field.omissible)
							continue;

						String field_name = option.case_sense ? field.name : field.name.toLowerCase();

						rset_col = meta.getColumns(null, null, db_table_name, field_name);

						if (!rset_col.next())
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + field_name + " not found in the relation."); // not found in the relation

						rset_col.close();

					}

					if (strict) {

						rset_col = meta.getColumns(null, null, db_table_name, null);

						List<PgField> fields = table.fields.stream().filter(field -> !field.omissible).collect(Collectors.toList());

						int col_id = 0;

						while (rset_col.next()) {

							String db_column_name = rset_col.getString("COLUMN_NAME");
							int db_column_type = rset_col.getInt("DATA_TYPE");

							if (db_column_type == java.sql.Types.NUMERIC) // NUMERIC and DECIMAL are equivalent in PostgreSQL
								db_column_type = java.sql.Types.DECIMAL;

							PgField field = fields.get(col_id++);

							String field_name = option.case_sense ? field.name : field.name.toLowerCase();

							if (!field_name.equals(db_column_name))
								throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + (option.case_sense ? db_column_name : db_column_name.toLowerCase()) + " found in an incorrect order."); // found in an incorrect order

							if (field.getSqlDataType() != db_column_type)
								throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + (option.case_sense ? db_column_name : db_column_name.toLowerCase()) + " column type " + JDBCType.valueOf(db_column_type) + " is incorrect with " + JDBCType.valueOf(field.getSqlDataType()) + "."); // column type is incorrect

						}

						fields.clear();

						rset_col.close();

					}

				} catch (SQLException | PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Reset attr_sel_rdy flag.
	 */
	private void resetAttrSelRdy() {

		tables.forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> field.attr_sel_rdy = true));

	}

	/**
	 * Lucene document conversion.
	 *
	 * @param xml_parser XML document
	 * @param lucene_doc Lucene document
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2LucIdx(XmlParser xml_parser, org.apache.lucene.document.Document lucene_doc) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(true);

		resetAttrSelRdy();

		tables.forEach(table -> table.lucene_doc = table.required ? lucene_doc : null);

		// parse root node and store into Lucene document

		try {

			lucene_doc.add(new StringField(option.document_key_name, document_id, Field.Store.YES));

			PgSchemaNode2LucIdx node2lucidx = new PgSchemaNode2LucIdx(this, null, root_table);

			node2lucidx.parseRootNode(node);

			node2lucidx.invokeRootNestedNode();

		} catch (ParserConfigurationException | TransformerException | IOException e) {
			throw new PgSchemaException(e);
		}

		xml_parser.clear();

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
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2LucIdx(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) throws PgSchemaException {

		try {

			PgSchemaNode2LucIdx node2lucidx = new PgSchemaNode2LucIdx(this, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

				if (node_test.omissible)
					continue;

				synchronized (table_lock[0]) {
					node2lucidx.parseChildNode(node_test);
				}

				node2lucidx.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2lucidx.invoked)
				return;

			synchronized (table_lock[0]) {
				node2lucidx.parseChildNode(parent_node, parent_key, proc_key, nested);
			}

			node2lucidx.invokeChildNestedNode();

		} catch (ParserConfigurationException | IOException | TransformerException e) {
			throw new PgSchemaException(e);
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
			throw new PgSchemaException("Not found sphinx:schema root element in Sphinx data source: " + PgSchemaUtil.sph_schema_name);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String node_name = child.getNodeName();
			boolean sph_field = node_name.equals("sphinx:field");
			boolean sph_attr = node_name.equals("sphinx:attr");

			if (sph_field || sph_attr) {

				if (child.hasAttributes()) {

					Element child_e = (Element) child;

					String[] name_attr = child_e.getAttribute("name").replaceFirst(PgSchemaUtil.sph_member_op, "\\.").split("\\.");

					if (name_attr.length != 2)
						continue;

					String table_name = name_attr[0];
					String field_name = name_attr[1];

					PgTable table = getTable(PgSchemaUtil.pg_public_schema_name, table_name);

					if (table == null)
						throw new PgSchemaException("Not found " + table_name + ".");

					PgField field = table.getField(field_name);

					if (field == null)
						throw new PgSchemaException("Not found " + table_name + "." + field_name + ".");

					field.attr_sel = sph_attr;

					String type_attr = child_e.getAttribute("type");

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
			BufferedWriter buffw = new BufferedWriter(filew);

			buffw.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");

			if (data_source)
				buffw.write("<sphinx:docset xmlns:sphinx=\"" + PgSchemaUtil.sph_namespace_uri + "\">\n");

			buffw.write("<sphinx:schema>\n");

			if (data_source) {

				buffw.write("<sphinx:attr name=\"" + option.document_key_name + "\" type=\"string\"/>\n"); // default attr
				buffw.write("<sphinx:field name=\"" + PgSchemaUtil.simple_content_name + "\"/>\n"); // default field

			}

			tables.forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> {

				try {

					buffw.write("<sphinx:attr name=\"" + table.name + PgSchemaUtil.sph_member_op + field.xname + "\"");

					String attrs = null;

					switch (field.xs_type) {
					case xs_boolean:
						attrs = " type=\"bool\"";
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
					default: // string
						attrs = " type=\"string\"";
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
						attrs += " default=\"" + StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeXml10(field.default_value)) + "\"";

					buffw.write(attrs + "/>\n");

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}));

			buffw.write("</sphinx:schema>\n");

			buffw.close();
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
			BufferedWriter buffw = new BufferedWriter(filew);

			buffw.write("#\n# Sphinx configuration file sample\n#\n# WARNING! While this sample file mentions all available options,\n#\n# it contains (very) short helper descriptions only. Please refer to\n# doc/sphinx.html for details.\n#\n\n#############################################################################\n## data source definition\n#############################################################################\n\n");

			buffw.write("source " + idx_name + "\n{\n");
			buffw.write("\ttype                    = xmlpipe2\n");
			buffw.write("\txmlpipe_command         = cat " + data_source.getAbsolutePath() + "\n");
			buffw.write("\txmlpipe_attr_string     = " + option.document_key_name + "\n");
			buffw.write("\txmlpipe_field           = " + PgSchemaUtil.simple_content_name + "\n");

			tables.forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> {

				try {

					String attr_name = table.name + PgSchemaUtil.sph_member_op + field.xname;

					switch (field.xs_type) {
					case xs_boolean:
						buffw.write("\txmlpipe_attr_bool       = " + attr_name + "\n");
						break;
					case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_duration:
						if (field.sph_mva)
							buffw.write("\txmlpipe_attr_multi_64   = " + attr_name + "\n");
						else
							buffw.write("\txmlpipe_attr_bigint     = " + attr_name + "\n");
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
							buffw.write("\txmlpipe_attr_multi      = " + attr_name + "\n");
						else
							buffw.write("\txmlpipe_attr_uint       = " + attr_name + "\n");
						break;
					case xs_float:
					case xs_double:
					case xs_decimal:
						buffw.write("\txmlpipe_attr_float      = " + attr_name + "\n");
						break;
					case xs_dateTime:
					case xs_time:
					case xs_date:
					case xs_gYearMonth:
					case xs_gYear:
						buffw.write("\txmlpipe_attr_timestamp  = " + attr_name + "\n");
						break;
					default: // string
						buffw.write("\txmlpipe_attr_string     = " + attr_name + "\n");
					}

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}));

			buffw.write("}\n\n");

			buffw.close();
			filew.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Sphinx xmlpipe2 conversion.
	 *
	 * @param xml_parser XML document
	 * @param buffw buffered writer of Sphinx xmlpipe2 file
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2SphDs(XmlParser xml_parser, BufferedWriter buffw) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(true);

		resetAttrSelRdy();

		tables.forEach(table -> table.buffw = table.required ? buffw : null);

		// parse root node and write to Sphinx xmlpipe2 file

		try {

			buffw.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
			buffw.write("<sphinx:document id=\"" + getHashKeyString(document_id) + "\">\n");
			buffw.write("<" + option.document_key_name + ">" + StringEscapeUtils.escapeXml10(document_id) + "</" + option.document_key_name + ">\n");

			PgSchemaNode2SphDs node2sphds = new PgSchemaNode2SphDs(this, null, root_table);

			node2sphds.parseRootNode(node);

			node2sphds.invokeRootNestedNode();

			buffw.write("</sphinx:document>\n");

		} catch (ParserConfigurationException | TransformerException | IOException e) {
			throw new PgSchemaException(e);
		}

		xml_parser.clear();

	}

	/**
	 * Close xml2SphDs.
	 */
	public void closeXml2SphDs() {

		closeTableLock();

		tables.stream().filter(table -> table.buffw != null).forEach(table -> table.buffw = null);

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
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2SphDs(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) throws PgSchemaException {

		try {

			PgSchemaNode2SphDs node2sphds = new PgSchemaNode2SphDs(this, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

				if (node_test.omissible)
					continue;

				synchronized (table_lock[0]) {
					node2sphds.parseChildNode(node_test);
				}

				node2sphds.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2sphds.invoked)
				return;

			synchronized (table_lock[0]) {
				node2sphds.parseChildNode(parent_node, parent_key, proc_key, nested);
			}

			node2sphds.invokeChildNestedNode();

		} catch (ParserConfigurationException | IOException | TransformerException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Return set of Sphinx attributes.
	 *
	 * @return HashSet set of Sphinx attributes
	 */
	public HashSet<String> getSphAttrs() {

		HashSet<String> sph_attrs = new HashSet<String>();

		tables.forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> sph_attrs.add(table.name + PgSchemaUtil.sph_member_op + field.xname)));

		return sph_attrs;
	}

	/**
	 * Return set of Sphinx multi-valued attributes.
	 *
	 * @return HashSet set of Sphinx multi-valued attributes
	 */
	public HashSet<String> getSphMVAs() {

		HashSet<String> sph_mvas = new HashSet<String>();

		tables.forEach(table -> table.fields.stream().filter(field -> field.sph_mva).forEach(field -> sph_mvas.add(table.name + PgSchemaUtil.sph_member_op + field.xname)));

		return sph_mvas;
	}

	// JSON conversion

	/** The JSON builder. */
	protected JsonBuilder jsonb = null;

	/**
	 * Initialize JSON builder.
	 *
	 * @param option JSON builder option
	 */
	public void initJsonBuilder(JsonBuilderOption option) {

		jsonb = new JsonBuilder(option);

	}

	/**
	 * Clear JSON builder.
	 */
	private void clearJsonBuilder() {

		jsonb.clear();

		tables.stream().filter(table -> table.required && table.content_holder).forEach(table -> {

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

		tables.stream().filter(table -> table.required && table.content_holder).forEach(table -> table.fields.stream().filter(field -> field.jsonb != null).forEach(field -> {

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

		System.out.print("{" + jsonb.line_feed_code); // JSON document start

		System.out.print(jsonb.getIndentSpaces(1) + "\"$schema\":" + jsonb.key_value_space + "\"" + PgSchemaUtil.json_schema_def + "\"," + jsonb.line_feed_code); // declaring a JSON Schema

		if (def_namespaces != null) {

			String def_namespace = def_namespaces.get("");

			if (def_namespace != null)
				System.out.print(jsonb.getIndentSpaces(1) + "\"id\":" + jsonb.key_value_space + "\"" + def_namespace + "\"," + jsonb.line_feed_code); // declaring a unique identifier

		}

		if (def_anno_appinfo != null) {

			String _def_anno_appinfo = jsonb.escapeAnnotation(def_anno_appinfo, false);

			if (!_def_anno_appinfo.startsWith("\""))
				_def_anno_appinfo = "\"" + _def_anno_appinfo + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + _def_anno_appinfo + "," + jsonb.line_feed_code);

		}

		if (def_anno_doc != null) {

			String _def_anno_doc = jsonb.escapeAnnotation(def_anno_doc, false);

			if (!_def_anno_doc.startsWith("\""))
				_def_anno_doc = "\"" + _def_anno_doc + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + _def_anno_doc + "," + jsonb.line_feed_code);

		}

		if (!root_table.virtual) {

			System.out.print(jsonb.getIndentSpaces(1) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.line_feed_code);

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + "\"" + root_table.name + "\"," + jsonb.line_feed_code);

			if (root_table.anno != null && !root_table.anno.isEmpty()) {

				String table_anno = jsonb.escapeAnnotation(root_table.anno, true);

				if (!table_anno.startsWith("\""))
					table_anno = "\"" + table_anno + "\"";

				System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.line_feed_code);

			}

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && field.jsonable).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_content ? jsonb.simple_content_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(1) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_offset + 1)) + "]," + jsonb.line_feed_code);

			jsonb.builder.setLength(0);

		}

		if (!root_table.virtual)
			System.out.print(jsonb.getIndentSpaces(1) + "\"items\": {" + jsonb.line_feed_code); // JSON own items start

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaFieldProperty(field, true, false, 2));

		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.line_feed_code.equals("\n") ? 2 : 1) + (root_table.virtual ? 1 : 0)) + jsonb.line_feed_code);

		jsonb.builder.setLength(0);

		if (!root_table.virtual)
			System.out.print(jsonb.getIndentSpaces(2) + "\"items\":" + jsonb.key_value_space + "{" + jsonb.line_feed_code); // JSON child items start

		int[] list_id = { 0 };

		fields.stream().filter(field -> field.nested_key).forEach(field -> realizeObjJsonSchema(root_table, getForeignTable(field), list_id[0]++, root_table.nested_fields, root_table.virtual ? 1 : 3));

		if (!root_table.virtual) {

			System.out.print(jsonb.getIndentSpaces(2) + "}" + jsonb.line_feed_code); // JSON child items end

			System.out.print(jsonb.getIndentSpaces(1) + "}" + jsonb.line_feed_code); // JSON own items end

		}

		System.out.print("}" + jsonb.line_feed_code); // JSON document end

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
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"type\":" + jsonb.key_value_space + "\"array\"," + jsonb.line_feed_code);

		if (!table.virtual) {

			if (!parent_table.list_holder)
				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.line_feed_code);

			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"title\":" + jsonb.key_value_space + "\"" + table.name + "\"," + jsonb.line_feed_code);

			if (table.anno != null && !table.anno.isEmpty()) {

				String table_anno = jsonb.escapeAnnotation(table.anno, true);

				if (!table_anno.startsWith("\""))
					table_anno = "\"" + table_anno + "\"";

				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.line_feed_code);

			}

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && field.jsonable).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_content ? jsonb.simple_content_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_offset + 1)) + "]," + jsonb.line_feed_code);

			jsonb.builder.setLength(0);

		}

		if (!table.virtual)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"items\": {" + jsonb.line_feed_code); // JSON own object start

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaFieldProperty(field, true, false, json_indent_level + (table.virtual ? 0 : 1)));

		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.line_feed_code.equals("\n") ? 2 : 1) + (table.virtual ? 1 : 0)) + jsonb.line_feed_code);

		jsonb.builder.setLength(0);

		if (table.nested_fields > 0) {

			if (!table.virtual)
				System.out.print(jsonb.getIndentSpaces(json_indent_level + 1) + "\"items\":" + jsonb.key_value_space + "{" + jsonb.line_feed_code); // JSON child items start

			int[] _list_id = { 0 };

			fields.stream().filter(field -> field.nested_key).forEach(field -> realizeObjJsonSchema(table, getForeignTable(field), _list_id[0]++, table.nested_fields, json_indent_level + (table.virtual ? 0 : 2)));

			if (!table.virtual)
				System.out.print(jsonb.getIndentSpaces(json_indent_level + 1) + "}" + jsonb.line_feed_code); // JSON child items end

		}

		if (!table.virtual)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "}" + (list_id < list_size - 1 ? "," : "") + jsonb.line_feed_code); // JSON own items end

	}

	/**
	 * Object-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file JSON file
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2ObjJson(XmlParser xml_parser, File json_file) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(true);

		clearJsonBuilder();

		// parse root node and store to JSON builder

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

			node2json.parseRootNode(node);

			if (node2json.filled) {

				int json_indent_level = 0;

				int jsonb_header_end = jsonb.writeHeader(root_table, true, ++json_indent_level);
				jsonb.writeContent(root_table, json_indent_level + 1);

				node2json.invokeRootNestedNodeObj(json_indent_level);

				jsonb.writeFooter(root_table, json_indent_level--, 0, jsonb_header_end);

			}

		} catch (ParserConfigurationException | TransformerException | IOException e) {
			throw new PgSchemaException(e);
		}

		try {

			FileWriter filew = new FileWriter(json_file);

			filew.write("{" + jsonb.line_feed_code + jsonb.builder.toString() + "}" + jsonb.line_feed_code);

			filew.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

		xml_parser.clear();

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
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2ObjJson(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id, int json_indent_level) throws PgSchemaException {

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, parent_table, table);

			node2json.jsonb_header_begin = node2json.jsonb_header_end = jsonb.builder.length();

			if (!table.virtual && table.bridge) {
				node2json.jsonb_header_end = jsonb.writeArrayHeader(table, ++json_indent_level);
				++json_indent_level;
			}

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

				if (node_test.omissible)
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

			try {

				if (node2json.invoked)
					return;

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

			} finally {

				if (!table.virtual && table.bridge) {
					json_indent_level--;
					jsonb.writeArrayFooter(table, json_indent_level--, node2json.jsonb_header_begin, node2json.jsonb_header_end);
				}

			}

		} catch (ParserConfigurationException | IOException | TransformerException e) {
			throw new PgSchemaException(e);
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

		System.out.print("{" + jsonb.line_feed_code); // JSON document start

		System.out.print(jsonb.getIndentSpaces(1) + "\"$schema\":" + jsonb.key_value_space + "\"" + PgSchemaUtil.json_schema_def + "\"," + jsonb.line_feed_code); // declaring a JSON Schema

		if (def_namespaces != null) {

			String def_namespace = def_namespaces.get("");

			if (def_namespace != null)
				System.out.print(jsonb.getIndentSpaces(1) + "\"id\":" + jsonb.key_value_space + "\"" + def_namespace + "\"," + jsonb.line_feed_code); // declaring a unique identifier

		}

		if (def_anno_appinfo != null) {

			String _def_anno_appinfo = jsonb.escapeAnnotation(def_anno_appinfo, false);

			if (!_def_anno_appinfo.startsWith("\""))
				_def_anno_appinfo = "\"" + _def_anno_appinfo + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + _def_anno_appinfo + "," + jsonb.line_feed_code);

		}

		if (def_anno_doc != null) {

			String _def_anno_doc = jsonb.escapeAnnotation(def_anno_doc, false);

			if (!_def_anno_doc.startsWith("\""))
				_def_anno_doc = "\"" + _def_anno_doc + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + _def_anno_doc + "," + jsonb.line_feed_code);

		}

		if (!root_table.virtual) {

			System.out.print(jsonb.getIndentSpaces(1) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.line_feed_code);

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + "\"" + root_table.name + "\"," + jsonb.line_feed_code);

			if (root_table.anno != null && !root_table.anno.isEmpty()) {

				String table_anno = jsonb.escapeAnnotation(root_table.anno, true);

				if (!table_anno.startsWith("\""))
					table_anno = "\"" + table_anno + "\"";

				System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.line_feed_code);

			}

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && field.jsonable).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_content ? jsonb.simple_content_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(1) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_offset + 1)) + "]," + jsonb.line_feed_code);

			jsonb.builder.setLength(0);

		}

		if (!root_table.virtual)
			System.out.print(jsonb.getIndentSpaces(1) + "\"items\": {" + jsonb.line_feed_code); // JSON own items start

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaFieldProperty(field, !field.list_holder, field.list_holder, 2));

		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.line_feed_code.equals("\n") ? 2 : 1) + (root_table.virtual ? 1 : 0)) + jsonb.line_feed_code);

		jsonb.builder.setLength(0);

		if (!root_table.virtual)
			System.out.print(jsonb.getIndentSpaces(2) + "\"items\":" + jsonb.key_value_space + "{" + jsonb.line_feed_code); // JSON child items start

		int[] list_id = { 0 };

		fields.stream().filter(field -> field.nested_key).forEach(field -> realizeColJsonSchema(root_table, getForeignTable(field), list_id[0]++, root_table.nested_fields, root_table.virtual ? 1 : 3));

		if (!root_table.virtual) {

			System.out.print(jsonb.getIndentSpaces(2) + "}" + jsonb.line_feed_code); // JSON child items end

			System.out.print(jsonb.getIndentSpaces(1) + "}" + jsonb.line_feed_code); // JSON own items end

		}

		System.out.print("}" + jsonb.line_feed_code); // JSON document end

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

			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.line_feed_code);

			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"title\":" + jsonb.key_value_space + "\"" + table.name + "\"," + jsonb.line_feed_code);

			if (table.anno != null && !table.anno.isEmpty()) {

				String table_anno = jsonb.escapeAnnotation(table.anno, true);

				if (!table_anno.startsWith("\""))
					table_anno = "\"" + table_anno + "\"";

				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.line_feed_code);

			}

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && field.jsonable).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_content ? jsonb.simple_content_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_offset + 1)) + "]," + jsonb.line_feed_code);

			jsonb.builder.setLength(0);

		}

		if (!table.virtual)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"items\": {" + jsonb.line_feed_code); // JSON own items start

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaFieldProperty(field, obj_json && !field.list_holder, !table.virtual || field.list_holder, json_indent_level + (table.virtual ? 0 : 1)));

		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.line_feed_code.equals("\n") ? 2 : 1) + (table.virtual ? 1 : 0)) + jsonb.line_feed_code);

		jsonb.builder.setLength(0);

		if (table.nested_fields > 0) {

			if (!table.virtual)
				System.out.print(jsonb.getIndentSpaces(json_indent_level + 1) + "\"items\":" + jsonb.key_value_space + "{" + jsonb.line_feed_code); // JSON child items start

			int[] _list_id = { 0 };

			fields.stream().filter(field -> field.nested_key).forEach(field -> realizeColJsonSchema(table, getForeignTable(field), _list_id[0]++, table.nested_fields, json_indent_level + (table.virtual ? 0 : 2)));

			if (!table.virtual)
				System.out.print(jsonb.getIndentSpaces(json_indent_level + 1) + "}" + jsonb.line_feed_code); // JSON child items end

		}

		if (!table.virtual)
			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "}" + jsonb.line_feed_code); // JSON own items end

	}

	/**
	 * Column-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file JSON file
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2ColJson(XmlParser xml_parser, File json_file) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(true);

		clearJsonBuilder();

		// parse root node and write to JSON file

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

			node2json.parseRootNode(node);

			if (node2json.filled) {

				int json_indent_level = 0;

				int jsonb_header_end = jsonb.writeHeader(root_table, true, ++json_indent_level);
				jsonb.writeContent(root_table, json_indent_level + 1);

				node2json.invokeRootNestedNodeCol(json_indent_level);

				jsonb.writeFooter(root_table, json_indent_level--, 0, jsonb_header_end);

			}

		} catch (ParserConfigurationException | TransformerException | IOException e) {
			throw new PgSchemaException(e);
		}

		try {

			FileWriter filew = new FileWriter(json_file);

			filew.write("{" + jsonb.line_feed_code + jsonb.builder.toString() + "}" + jsonb.line_feed_code);

			filew.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

		xml_parser.clear();

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
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2ColJson(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id, int json_indent_level) throws PgSchemaException {

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, parent_table, table);

			if (!table.virtual) {

				node2json.jsonb_header_begin = jsonb.builder.length();
				node2json.jsonb_header_end = jsonb.writeHeader(table, true, json_indent_level);

			}

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

				if (node_test.omissible)
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

			try {

				if (node2json.invoked)
					return;

				synchronized (table_lock[0]) {

					node2json.parseChildNode(parent_node, parent_key, proc_key, nested);

					if (node2json.written)
						jsonb.writeContent(table, json_indent_level + (table.virtual ? 0 : 1));

				}

				node2json.invokeChildNestedNodeCol(json_indent_level);

			} finally {

				if (!table.virtual)
					jsonb.writeFooter(table, json_indent_level, node2json.jsonb_header_begin, node2json.jsonb_header_end);

			}

		} catch (ParserConfigurationException | IOException | TransformerException e) {
			throw new PgSchemaException(e);
		}

	}

	// Relational-oriented JSON conversion

	/**
	 * Realize Relational-oriented JSON Schema.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void realizeJsonSchema() throws PgSchemaException {

		hasRootTable();

		System.out.print("{" + jsonb.line_feed_code); // JSON document start

		System.out.print(jsonb.getIndentSpaces(1) + "\"$schema\":" + jsonb.key_value_space + "\"" + PgSchemaUtil.json_schema_def + "\"," + jsonb.line_feed_code); // declaring a JSON Schema

		if (def_namespaces != null) {

			String def_namespace = def_namespaces.get("");

			if (def_namespace != null)
				System.out.print(jsonb.getIndentSpaces(1) + "\"id\":" + jsonb.key_value_space + "\"" + def_namespace + "\"," + jsonb.line_feed_code); // declaring a unique identifier

		}

		if (def_anno_appinfo != null) {

			String _def_anno_appinfo = jsonb.escapeAnnotation(def_anno_appinfo, false);

			if (!_def_anno_appinfo.startsWith("\""))
				_def_anno_appinfo = "\"" + _def_anno_appinfo + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"title\":" + jsonb.key_value_space + _def_anno_appinfo + "," + jsonb.line_feed_code);

		}

		if (def_anno_doc != null) {

			String _def_anno_doc = jsonb.escapeAnnotation(def_anno_doc, false);

			if (!_def_anno_doc.startsWith("\""))
				_def_anno_doc = "\"" + _def_anno_doc + "\"";

			System.out.print(jsonb.getIndentSpaces(1) + "\"description\":" + jsonb.key_value_space + _def_anno_doc + "," + jsonb.line_feed_code);

		}

		PgTable first = tables.stream().filter(table -> table.content_holder).sorted(Comparator.comparingInt(table -> table.order)).findFirst().get();

		tables.stream().filter(table -> table.content_holder).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

			if (!table.equals(first))
				System.out.print("," + jsonb.line_feed_code);

			realizeJsonSchema(table, 1);

		});

		System.out.print(jsonb.line_feed_code + "}" + jsonb.line_feed_code); // JSON document end

		jsonb.builder.setLength(0);

	}

	/**
	 * Realize Relational-oriented JSON Schema (child).
	 *
	 * @param table current table
	 * @param json_indent_level current indent level
	 */
	private void realizeJsonSchema(final PgTable table, final int json_indent_level) {

		List<PgField> fields = table.fields;

		boolean obj_json = table.virtual || !jsonb.array_all;

		System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"type\":" + jsonb.key_value_space + "\"object\"," + jsonb.line_feed_code);

		System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"title\":" + jsonb.key_value_space + "\"" + table.name + "\"," + jsonb.line_feed_code);

		if (table.anno != null && !table.anno.isEmpty()) {

			String table_anno = jsonb.escapeAnnotation(table.anno, true);

			if (!table_anno.startsWith("\""))
				table_anno = "\"" + table_anno + "\"";

			System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"description\":" + jsonb.key_value_space + table_anno + "," + jsonb.line_feed_code);

		}

		if (fields.stream().anyMatch(field -> field.required)) {

			fields.stream().filter(field -> field.required && field.jsonable).forEach(field -> {

				String field_name = (field.attribute || field.any_attribute ? jsonb.attr_prefix : "") + (field.simple_content ? jsonb.simple_content_key : field.xname);

				jsonb.builder.append("\"" + field_name + "\"," + jsonb.key_value_space);

			});

			if (jsonb.builder.length() > 2)
				System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"required\":" + jsonb.key_value_space + "[" + jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.key_value_offset + 1)) + "]," + jsonb.line_feed_code);

			jsonb.builder.setLength(0);

		}

		System.out.print(jsonb.getIndentSpaces(json_indent_level) + "\"items\": {" + jsonb.line_feed_code); // JSON own items start

		fields.stream().filter(field -> field.jsonable).forEach(field -> {

			if (table.xs_type.equals(XsTableType.xs_root))
				jsonb.writeSchemaFieldProperty(field, !field.list_holder, field.list_holder, json_indent_level + 1);
			else
				jsonb.writeSchemaFieldProperty(field, obj_json && !field.list_holder, !table.virtual || field.list_holder, json_indent_level + 1);

		});


		if (jsonb.builder.length() > 2)
			System.out.print(jsonb.builder.substring(0, jsonb.builder.length() - (jsonb.line_feed_code.equals("\n") ? 2 : 1)) + jsonb.line_feed_code);

		jsonb.builder.setLength(0);

		System.out.print(jsonb.getIndentSpaces(json_indent_level) + "}"); // JSON own items end

	}

	/**
	 * Relational-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file JSON file
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2Json(XmlParser xml_parser, File json_file) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(false);

		clearJsonBuilder();

		// parse root node and write to JSON file

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

			node2json.parseRootNode(node);

			node2json.invokeRootNestedNode();

		} catch (ParserConfigurationException | TransformerException | IOException e) {
			throw new PgSchemaException(e);
		}

		try {

			FileWriter filew = new FileWriter(json_file);
			BufferedWriter buffw = new BufferedWriter(filew);

			buffw.write("{" + jsonb.line_feed_code); // JSON document start

			PgTable first = tables.stream().filter(_table -> _table.jsonb_not_empty).sorted(Comparator.comparingInt(table -> table.order)).findFirst().get();

			tables.stream().filter(_table -> _table.jsonb_not_empty).sorted(Comparator.comparingInt(table -> table.order)).forEach(_table -> {

				try {

					boolean array_json = !_table.virtual && jsonb.array_all;

					if (!_table.equals(first))
						buffw.write("," + jsonb.line_feed_code);

					buffw.write(jsonb.getIndentSpaces(1) + "\"" + _table.name + "\":" + jsonb.key_value_space + "{" + jsonb.line_feed_code); // JSON object start

					boolean has_field = false;

					List<PgField> _fields = _table.fields;

					for (int f = 0; f < _fields.size(); f++) {

						PgField _field = _fields.get(f);

						if (_field.jsonb == null)
							continue;

						if ((_field.required || _field.jsonb_not_empty) && _field.jsonb_col_size > 0 && _field.jsonb.length() > 2) {

							boolean array_field = (!_table.equals(root_table) && array_json) || _field.list_holder || _field.jsonb_col_size > 1;

							if (has_field)
								buffw.write("," + jsonb.line_feed_code);

							buffw.write(jsonb.getIndentSpaces(2) + "\"" + (_field.attribute || _field.any_attribute ? jsonb.attr_prefix : "") + (_field.simple_content ? jsonb.simple_content_key : _field.xname) + "\":" + jsonb.key_value_space + (array_field ? "[" : ""));

							_field.jsonb.setLength(_field.jsonb.length() - (jsonb.key_value_offset + 1));
							buffw.write(_field.jsonb.toString());

							if (array_field)
								buffw.write("]");

							has_field = true;

						}

						if (_field.jsonb.length() > 0)
							_field.jsonb.setLength(0);

					}

					if (has_field)
						buffw.write(jsonb.line_feed_code);

					buffw.write(jsonb.getIndentSpaces(1) + "}"); // JSON object end

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

			if (first != null)
				buffw.write(jsonb.line_feed_code);

			buffw.write("}" + jsonb.line_feed_code); // JSON document end

			buffw.close();
			filew.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

		xml_parser.clear();

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
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2Json(final Node parent_node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) throws PgSchemaException {

		final int table_id = tables.indexOf(table);

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, parent_key, proc_key, list_holder, nested, nest_id);

				if (node_test.omissible)
					continue;

				synchronized (table_lock[table_id]) {
					node2json.parseChildNode(node_test);
				}

				node2json.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2json.invoked)
				return;

			synchronized (table_lock[table_id]) {
				node2json.parseChildNode(parent_node, parent_key, proc_key, nested);
			}

			node2json.invokeChildNestedNode();

		} catch (ParserConfigurationException | IOException | TransformerException e) {
			throw new PgSchemaException(e);
		}

	}

	// XML composer from PostgreSQL

	/** The XML builder. */
	private XmlBuilder xmlb = null;

	/** SAX parser for any attribute. */
	private SAXParser any_attr_parser = null;

	/** The instance of any retriever. */
	private PgAnyRetriever any_retriever = null;

	/**
	 * Compose XML document
	 *
	 * @param db_conn database connection
	 * @param table current table
	 * @param rset current result set
	 * @param xmlb XML builder
	 * @throws PgSchemaException the pg schema exception
	 */
	public void pgSql2Xml(Connection db_conn, PgTable table, ResultSet rset, XmlBuilder xmlb) throws PgSchemaException {

		this.xmlb = xmlb;

		try {

			if (option.wild_card) {

				if (any_attr_parser == null) {

					SAXParserFactory spf = SAXParserFactory.newInstance();
					any_attr_parser = spf.newSAXParser();

				}

				if (any_retriever == null)
					any_retriever = new PgAnyRetriever();

			}

			PgSchemaNestTester test = new PgSchemaNestTester(table, xmlb);

			xmlb.writer.writeStartElement(table.prefix, table.name, table.target_namespace);

			xmlb.writer.writeAttribute("xmlns" + (table.prefix.isEmpty() ? "" : ":" + table.prefix), table.target_namespace);

			List<PgField> fields = table.fields;

			// attribute, any_attribute

			int param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.attribute) {

					String content = field.retrieveValue(rset, param_id);

					if (content != null && !content.isEmpty()) {

						if (field.is_xs_namespace)
							xmlb.writer.writeAttribute(field.name, content);
						else
							xmlb.writer.writeAttribute(field.prefix, field.target_namespace, field.name, content);

						test.has_content = true;

					}

				}

				else if (field.any_attribute) {

					SQLXML xml_object = rset.getSQLXML(param_id);

					if (xml_object != null) {

						InputStream in = xml_object.getBinaryStream();

						if (in != null) {

							PgAnyAttrRetriever any_attr = new PgAnyAttrRetriever(table.name, xmlb.writer);

							any_attr_parser.parse(in, any_attr);

							test.has_content |= any_attr.has_content;

							in.close();

						}

						any_attr_parser.reset();

						xml_object.free();

					}

				}

				if (!field.omissible)
					param_id++;

			}

			// simple_content, element, any

			param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.simple_content) {

					String content = field.retrieveValue(rset, param_id);

					if (content != null && !content.isEmpty()) {

						xmlb.writer.writeCharacters(content);

						test.has_simple_content = true;

					}

				}

				else if (field.element) {

					String content = field.retrieveValue(rset, param_id);

					if (content != null && !content.isEmpty()) {

						xmlb.writer.writeCharacters((test.has_child_elem ? "" : xmlb.line_feed_code) + test.child_indent_space);

						if (field.is_xs_namespace)
							xmlb.writer.writeStartElement(table.prefix, field.name, table.target_namespace);
						else
							xmlb.writer.writeStartElement(field.prefix, field.name, field.target_namespace);

						xmlb.writer.writeCharacters(content);
						xmlb.writer.writeEndElement();

						xmlb.writer.writeCharacters(xmlb.line_feed_code);

						test.has_child_elem = test.has_content = true;

					}

				}

				else if (field.any) {

					SQLXML xml_object = rset.getSQLXML(param_id);

					if (xml_object != null) {

						InputStream in = xml_object.getBinaryStream();

						if (in != null) {

							test.mergeTest(any_retriever.exec(in, table, test, xmlb));
							in.close();

						}

						xml_object.free();

					}

				}

				if (!field.omissible)
					param_id++;

			}

			// nested key

			param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.nested_key) {

					Object key = rset.getObject(param_id);

					if (key != null)
						nestChildNode2Xml(db_conn, getTable(field.foreign_table_id), key, test);

				}

				if (!field.omissible)
					param_id++;

			}

			xmlb.writer.writeEndElement();

			xmlb.writer.writeCharacters(xmlb.line_feed_code);

		} catch (SQLException | XMLStreamException | ParserConfigurationException | SAXException | IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close pgSql2Xml.
	 */
	public void closePgSql2Xml() {

		tables.stream().filter(table -> table.ps != null).forEach(table -> {

			try {

				if (!table.ps.isClosed())
					table.ps.close();

			} catch (SQLException e) {
				e.printStackTrace();
				System.exit(1);
			}

			table.ps = null;

		});

	}

	/**
	 * Nest node and compose XML document.
	 *
	 * @param db_conn database connection
	 * @param table current table
	 * @param parent_key parent key
	 * @param parent_test nest test result of parent node
	 * @return PgSchemaNestTester nest test of this node
	 * @throws PgSchemaException the pg schema exception
	 */	
	private PgSchemaNestTester nestChildNode2Xml(final Connection db_conn, final PgTable table, final Object parent_key, PgSchemaNestTester parent_test) throws PgSchemaException {

		try {

			PgSchemaNestTester test = new PgSchemaNestTester(table, parent_test);

			if (!table.virtual && !table.list_holder) {

				xmlb.writer.writeCharacters((parent_test.has_child_elem ? "" : xmlb.line_feed_code) + test.current_indent_space);

				xmlb.writer.writeStartElement(table.prefix, table.name, table.target_namespace);

			}

			if (table.ps == null || table.ps.isClosed()) {

				String sql = "SELECT * FROM " + getPgNameOf(db_conn, table) + " WHERE " + PgSchemaUtil.avoidPgReservedWords(table.fields.stream().filter(field -> field.primary_key).findFirst().get().name) + " = ?";

				table.ps = db_conn.prepareStatement(sql);

			}

			switch (option.hash_size) {
			case native_default:
				table.ps.setBytes(1, (byte[]) parent_key);
				break;
			case unsigned_int_32:
				table.ps.setInt(1, (int) (parent_key));
				break;
			case unsigned_long_64:
				table.ps.setLong(1, (long) parent_key);
				break;
			default:
				table.ps.setString(1, (String) parent_key);
			}

			ResultSet rset = table.ps.executeQuery();

			List<PgField> fields = table.fields;

			while (rset.next()) {

				if (!table.virtual && table.list_holder) {

					xmlb.writer.writeCharacters((parent_test.has_content ? "" : xmlb.line_feed_code) + test.current_indent_space);

					xmlb.writer.writeStartElement(table.prefix, table.name, table.target_namespace);

				}

				// attribute, any_attribute

				int param_id = 1;

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.attribute) {

						String content = field.retrieveValue(rset, param_id);

						if (content != null && !content.isEmpty()) {

							if (field.is_xs_namespace)
								xmlb.writer.writeAttribute(field.name, content);
							else
								xmlb.writer.writeAttribute(field.prefix, field.target_namespace, field.name, content);

							test.has_content = true;

						}

					}

					else if (field.any_attribute) {

						SQLXML xml_object = rset.getSQLXML(param_id);

						if (xml_object != null) {

							InputStream in = xml_object.getBinaryStream();

							if (in != null) {

								PgAnyAttrRetriever any_attr = new PgAnyAttrRetriever(table.name, xmlb.writer);

								any_attr_parser.parse(in, any_attr);

								test.has_content |= any_attr.has_content;

								in.close();

							}

							any_attr_parser.reset();

							xml_object.free();

						}

					}

					if (!field.omissible)
						param_id++;

				}

				// simple_content, element, any

				param_id = 1;

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.simple_content) {

						String content = field.retrieveValue(rset, param_id);

						if (content != null && !content.isEmpty()) {

							xmlb.writer.writeCharacters(content);

							test.has_simple_content = true;

						}

					}

					else if (field.element) {

						String content = field.retrieveValue(rset, param_id);

						if (content != null && !content.isEmpty()) {

							xmlb.writer.writeCharacters((test.has_child_elem ? "" : xmlb.line_feed_code) + test.child_indent_space);

							if (field.is_xs_namespace)
								xmlb.writer.writeStartElement(table.prefix, field.name, table.target_namespace);
							else
								xmlb.writer.writeStartElement(field.prefix, field.name, field.target_namespace);

							xmlb.writer.writeCharacters(content);
							xmlb.writer.writeEndElement();

							xmlb.writer.writeCharacters(xmlb.line_feed_code);

							test.has_child_elem = test.has_content = true;

						}

					}

					else if (field.any) {

						SQLXML xml_object = rset.getSQLXML(param_id);

						if (xml_object != null) {

							InputStream in = xml_object.getBinaryStream();

							if (in != null) {

								test.mergeTest(any_retriever.exec(in, table, test, xmlb));
								in.close();

							}

							xml_object.free();

						}

					}

					if (!field.omissible)
						param_id++;

				}

				// nested key

				param_id = 1;

				for (int f = 0, n = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.nested_key) {

						Object key = rset.getObject(param_id);

						if (key != null) {

							test.has_child_elem |= n++ > 0;

							test.mergeTest(nestChildNode2Xml(db_conn, getTable(field.foreign_table_id), key, test));

						}

					}

					if (!field.omissible)
						param_id++;

				}

				if (!table.virtual && table.list_holder) {

					if (test.has_content && parent_test.has_simple_content)
						xmlb.writer.writeCharacters(test.current_indent_space);

					xmlb.writer.writeEndElement();

					xmlb.writer.writeCharacters(xmlb.line_feed_code);

					if (!test.has_content && (!test.has_child_elem || test.has_simple_content))
						xmlb.writer.writeCharacters(test.getParentIndentSpace());

				}

			}

			rset.close();

			if (!table.virtual && !table.list_holder) {

				if (test.has_content && (test.has_child_elem || !test.has_simple_content))
					xmlb.writer.writeCharacters(test.current_indent_space);

				xmlb.writer.writeEndElement();

				xmlb.writer.writeCharacters(xmlb.line_feed_code);

			}

			return test;

		} catch (XMLStreamException | SQLException | SAXException | IOException e) {
			throw new PgSchemaException(e);
		}

	}

}
