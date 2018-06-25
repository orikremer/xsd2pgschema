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
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
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
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;

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

	/** The parent of default schema location. */
	private String def_schema_parent = null;

	/** The default schema location. */
	private String def_schema_location = null;

	/** The schema locations. */
	private HashSet<String> schema_locations = null;

	/** The PostgreSQL named schemata. */
	private HashSet<String> pg_named_schemata = null;

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
	private List<PgSchemaPendingGroup> pending_attr_groups = null;

	/** The pending list of model groups. */
	private List<PgSchemaPendingGroup> pending_model_groups = null;

	/** The PostgreSQL data model option. */
	protected PgSchemaOption option = null;

	/** The full-text index filter. */
	protected IndexFilter index_filter = null;

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

	/** The instance of document builder for any content. */
	private DocumentBuilder any_doc_builder = null;

	/** The instance of transformer for any content. */
	private Transformer any_transformer = null;

	/** SAX parser for any content. */
	private SAXParser any_sax_parser = null;

	/** The root node (internal use only). */
	private Node root_node = null;

	/** The root schema object (internal use only). */
	private PgSchema root_schema = null;

	/** Whether name collision occurs in schema (internal use only). */
	private boolean name_collision = false;

	/** The current depth of table (internal use only). */
	private int level;

	/**
	 * PostgreSQL schema constructor.
	 *
	 * @param doc_builder Document builder used for xs:include or xs:import
	 * @param doc XML Schema document
	 * @param parent_schema parent schema object (should be null at first)
	 * @param def_schema_location default schema location
	 * @param option PostgreSQL data model option
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchema(DocumentBuilder doc_builder, Document doc, PgSchema parent_schema, String def_schema_location, PgSchemaOption option) throws PgSchemaException {

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

		root_schema = parent_schema == null ? this : parent_schema;

		def_schema_parent = parent_schema == null ? PgSchemaUtil.getSchemaParent(def_schema_location) : parent_schema.def_schema_parent;

		// prepare dictionary of unique schema locations and duplicated schema locations

		if (parent_schema == null) {

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

					root_schema.unq_schema_locations.putIfAbsent(target_namespace, def_schema_location);

					def_namespaces.putIfAbsent("", target_namespace);

				}

				else if (node_name.startsWith("xmlns")) {

					String target_namespace = root_attr.getNodeValue().split(" ")[0];

					if (!target_namespace.equals(PgSchemaUtil.xsi_namespace_uri))
						def_namespaces.putIfAbsent(node_name.replaceFirst("^xmlns:?", ""), target_namespace);
					else
						def_namespaces.putIfAbsent(PgSchemaUtil.xsi_prefix, target_namespace);

				}

				else if (node_name.equals("defaultAttributes"))
					def_attrs = root_attr.getNodeValue();

			}

		}

		// retrieve top level schema annotation

		def_anno = option.extractAnnotation(root_node, true);
		def_anno_appinfo = option.extractAppinfo(root_node);

		if ((def_anno_doc = option.extractDocumentation(root_node, true)) != null)
			def_xanno_doc = option.extractDocumentation(root_node, false);

		if (option.ddl_output)
			def_stat_msg = new StringBuilder();

		// prepare schema location holder

		schema_locations = new HashSet<String>();

		schema_locations.add(def_schema_location);

		// prepare table holder, attribute group holder and model group holder

		tables = new ArrayList<PgTable>();

		attr_groups = new ArrayList<PgTable>();

		model_groups = new ArrayList<PgTable>();

		// prepare foreign key holder, and pending group holder for attribute group and model group

		if (parent_schema == null) {

			foreign_keys = new ArrayList<PgForeignKey>();

			pending_attr_groups = new ArrayList<PgSchemaPendingGroup>();
			pending_model_groups = new ArrayList<PgSchemaPendingGroup>();

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

					if (!root_schema.schema_locations.add(schema_location))
						continue;

					// copy XML Schema if not exists

					PgSchemaUtil.getSchemaFilePath(schema_location, def_schema_parent, option.cache_xsd);

					// local XML Schema file

					InputStream is2 = PgSchemaUtil.getSchemaInputStream(schema_location, def_schema_parent, option.cache_xsd);

					if (is2 == null)
						throw new PgSchemaException("Could not access to schema location: " + schema_location);

					try {

						Document doc2 = doc_builder.parse(is2);

						is2.close();

						doc_builder.reset();

						// referred XML Schema (xs:include|xs:import/@schemaLocation) analysis

						PgSchema schema2 = new PgSchema(doc_builder, doc2, root_schema, schema_location, option);

						if ((schema2.tables == null || schema2.tables.size() == 0) && (schema2.attr_groups == null || schema2.attr_groups.size() == 0) && (schema2.model_groups == null || schema2.model_groups.size() == 0)) {
							/*
							if (option.ddl_output)
							root_schema.def_stat_msg.append("--  Not found any root element (/" + option.xs_prefix_ + "schema/" + option.xs_prefix_ + "element) or administrative elements (/" + option.xs_prefix_ + "schema/[" + option.xs_prefix_ + "complexType | " + option.xs_prefix_ + "simpleType | " + option.xs_prefix_ + "attributeGroup | " + option.xs_prefix_ + "group]) in XML Schema: " + schema_location + "\n");
							 */
							continue;
						}

						// add schema location to prevent infinite cyclic reference

						schema2.schema_locations.forEach(arg -> root_schema.schema_locations.add(arg));

						// copy default namespace from referred XML Schema

						if (!schema2.def_namespaces.isEmpty())
							schema2.def_namespaces.entrySet().forEach(arg -> root_schema.def_namespaces.putIfAbsent(arg.getKey(), arg.getValue()));

						// copy administrative tables from referred XML Schema

						schema2.tables.stream().filter(arg -> arg.xs_type.equals(XsTableType.xs_admin_root) || arg.xs_type.equals(XsTableType.xs_admin_child)).forEach(arg -> {

							if (root_schema.avoidTableDuplication(tables, arg))
								root_schema.tables.add(arg);

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

		boolean root_element = parent_schema == null;

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (child.getNodeName().equals(option.xs_prefix_ + "element")) {

				Element child_e = (Element) child;

				String _abstract = child_e.getAttribute("abstract");

				if (_abstract != null && _abstract.equals("true"))
					continue;

				extractRootElement(child, root_element);

				if (parent_schema == null)
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

			if (parent_schema == null)
				throw new PgSchemaException("Not found any root element (/" + option.xs_prefix_ + "schema/" + option.xs_prefix_ + "element) or administrative elements (/" + option.xs_prefix_ + "schema/[" + option.xs_prefix_ + "complexType | " + option.xs_prefix_ + "simpleType]) in XML Schema: " + def_schema_location);

		}

		else {

			if (!option.rel_model_ext)
				tables.parallelStream().forEach(table -> table.classify());

			if (option.ddl_output && !root_schema.dup_schema_locations.containsKey(def_schema_location))
				root_schema.def_stat_msg.append("--  " + (parent_schema == null ? "Generated" : "Found") + " " + tables.parallelStream().filter(table -> option.rel_model_ext || !table.relational).count() + " tables (" + tables.parallelStream().map(table -> option.rel_model_ext || !table.relational ? table.fields.size() : 0).reduce((arg0, arg1) -> arg0 + arg1).get() + " fields), " + attr_groups.size() + " attr groups, " + model_groups.size() + " model groups " + (parent_schema == null ? "in total" : "in XML Schema: " + def_schema_location) + "\n");

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

				if (parent_schema == null)
					break;

			}

		}

		// append annotation of administrative tables if possible

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			else if (child.getNodeName().equals(option.xs_prefix_ + "complexType"))
				extractAdminElement(child, true, true);

			else if (child.getNodeName().equals(option.xs_prefix_ + "simpleType"))
				extractAdminElement(child, false, true);

		}

		if (parent_schema != null)
			return;

		// collect PostgreSQL named schemata

		if (option.pg_named_schema) {

			// decide whether writable table

			tables.parallelStream().filter(table -> table.required && (option.rel_model_ext || !table.relational)).forEach(table -> table.writable = true);

			pg_named_schemata = new HashSet<String>();

			tables.stream().filter(table -> table.writable).filter(table -> !table.pg_schema_name.equals(PgSchemaUtil.pg_public_schema_name)).forEach(table -> pg_named_schemata.add(table.pg_schema_name));

		}

		// apply pending attribute groups (lazy evaluation)

		if (pending_attr_groups.size() > 0) {

			pending_attr_groups.forEach(arg -> {

				try {

					PgTable attr_group  = getAttributeGroup(arg.ref_group, true);

					PgTable table = getPendingTable(arg);

					if (table != null && table.has_pending_group) {

						table.fields.addAll(arg.insert_position, attr_group.fields);

						table.removeProhibitedAttrs();
						table.removeBlockedSubstitutionGroups();
						table.countNestedFields();

					}

				} catch (PgSchemaException e) {
					e.printStackTrace();
				}

			});

			pending_attr_groups.clear();

		}

		// apply pending model groups (lazy evaluation)

		if (pending_model_groups.size() > 0) {

			pending_model_groups.forEach(arg -> {

				try {

					PgTable model_group = getModelGroup(arg.ref_group, true);

					PgTable table = getPendingTable(arg);

					if (table != null && table.has_pending_group) {

						table.fields.addAll(arg.insert_position, model_group.fields);

						table.removeProhibitedAttrs();
						table.removeBlockedSubstitutionGroups();
						table.countNestedFields();

					}

				} catch (PgSchemaException e) {
					e.printStackTrace();
				}

			});

			pending_model_groups.clear();

		}

		// resolved pending groups

		tables.parallelStream().filter(table -> table.has_pending_group).forEach(table -> table.has_pending_group = false);

		// classify type of table

		tables.parallelStream().forEach(table -> {

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

							// detect simple content as attribute

							if (field.nested_key_as_attr) {

								foreign_table.fields.stream().filter(foreign_field -> foreign_field.simple_content).forEach(foreign_field -> {

									foreign_field.simple_attribute = true;

									foreign_field.foreign_table_id = tables.indexOf(table);
									foreign_field.foreign_schema = table.pg_schema_name;
									foreign_field.foreign_table_xname = table.xname;

									table.has_nested_key_as_attr = true;
									foreign_table.has_simple_attribute = true;

								});

							}

						}

						else
							iterator.remove();

					}

				}

			}

		});

		// detect simple content as conditional attribute

		tables.stream().filter(foreign_table -> foreign_table.has_simple_attribute).forEach(foreign_table -> {

			if (tables.parallelStream().anyMatch(table -> table.nested_fields > 0 && table.fields.stream().anyMatch(field -> field.nested_key && !field.nested_key_as_attr && getForeignTable(field).equals(foreign_table)))) {

				foreign_table.fields.stream().filter(foreign_field -> foreign_field.simple_attribute).forEach(foreign_field -> {

					foreign_field.simple_attribute = false;
					foreign_field.simple_attr_cond = true;

				});

			}

		});

		// avoid virtual duplication of nested key

		tables.parallelStream().forEach(table -> {

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

		tables.parallelStream().filter(table -> table.virtual && table.anno != null).forEach(table -> {

			table.fields.stream().filter(field -> field.nested_key).forEach(field -> {

				PgTable nested_table = getForeignTable(field);

				if (nested_table != null && nested_table.anno == null) {
					nested_table.anno = "(quoted from " + table.pname + ")\n-- " + table.anno;
					nested_table.xanno_doc = "(quoted from " + table.pname + ")\n" + table.xanno_doc;
				}

			});

		});

		// cancel unique key constraint if parent table is list holder

		tables.parallelStream().filter(table -> table.list_holder).forEach(table -> table.fields.stream().filter(field -> field.nested_key).forEach(field -> getForeignTable(field).cancelUniqueKey()));

		// decide whether table has any unique child table

		tables.parallelStream().filter(table -> table.nested_fields > 0).forEach(table -> {

			table.fields.stream().filter(field -> field.nested_key).forEach(field -> {

				PgTable nested_table = getForeignTable(field);

				if (nested_table.fields.stream().anyMatch(nested_field -> nested_field.unique_key))
					table.has_unique_nested_key = true;

			});

		});

		// decide parent node name constraint

		tables.stream().filter(table -> table.nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && field.parent_node != null).forEach(field -> { // do not parallelize this stream which causes null pointer exception

			// tolerate parent node name constraint due to name collision

			if (table.name_collision)
				field.parent_node = null;

			else {

				String[] parent_nodes = field.parent_node.split(" ");

				field.parent_node = null;

				boolean infinite_loop = false;

				for (String parent_node : parent_nodes) {

					PgTable parent_table = getCanTable(field.foreign_schema, parent_node);

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
									field.parent_node = parent_field.foreign_table_xname;

								else {

									String[] _parent_nodes = field.parent_node.split(" ");

									boolean has_parent_node = false;

									for (String _parent_node : _parent_nodes) {

										if (_parent_node.equals(parent_field.foreign_table_xname)) {
											has_parent_node = true;
											break;
										}

									}

									if (!has_parent_node)
										field.parent_node += " " + parent_field.foreign_table_xname;

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

		// decide ancestor node name constraint

		tables.parallelStream().filter(table -> table.has_foreign_key && table.nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && !field.nested_key_as_attr && field.parent_node != null).forEach(field -> {

			Optional<PgField> opt = table.fields.stream().filter(foreign_field -> foreign_field.foreign_key && foreign_field.foreign_table_xname.equals(field.parent_node)).findFirst();

			if (opt.isPresent()) {

				Optional<PgField> opt2 = getForeignTable(opt.get()).fields.stream().filter(nested_field -> nested_field.nested_key && getForeignTable(nested_field).equals(table)).findFirst();

				if (opt2.isPresent())
					field.ancestor_node = opt2.get().parent_node;

			}

		}));

		tables.parallelStream().filter(table -> !table.has_foreign_key && table.nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && field.parent_node != null && field.ancestor_node == null).forEach(field -> {

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

		// update requirement flag due to foreign key

		foreign_keys.forEach(foreign_key -> {

			PgTable child_table = getChildTable(foreign_key);

			if (child_table != null) {

				PgTable parent_table = getParentTable(foreign_key);

				if (parent_table != null)
					child_table.required = parent_table.required = true;

			}

		});

		// decide whether writable table

		tables.parallelStream().filter(table -> table.required && (option.rel_model_ext || !table.relational)).forEach(table -> table.writable = true);

		// add serial key on demand in case that parent table is list holder

		if (option.serial_key)
			tables.parallelStream().filter(table -> table.writable && table.list_holder).forEach(table -> table.fields.stream().filter(field -> field.nested_key).forEach(field -> getForeignTable(field).addSerialKey(option)));

		// add XPath key on demand

		if (option.xpath_key)
			tables.parallelStream().filter(table -> table.writable).forEach(table -> table.addXPathKey(option));

		// remove nested key if relational model extension is disabled

		if (!option.rel_model_ext) {

			tables.parallelStream().filter(table -> table.writable && table.nested_fields > 0).forEach(table -> {

				table.fields.removeIf(field -> field.nested_key);
				table.countNestedFields();

			});

		}

		// retrieve document key if in-place document key no exists

		if (!option.document_key && option.inplace_document_key && option.document_key_if_no_in_place) {

			tables.parallelStream().filter(table -> table.writable && !table.fields.stream().anyMatch(field -> field.name.equals(option.document_key_name)) && !table.fields.stream().anyMatch(field -> (field.attribute || field.element) && (option.inplace_document_key_names.contains(field.name) || option.inplace_document_key_names.contains(table.name + "." + field.name)))).forEach(table -> {

				PgField field = new PgField();

				field.name = field.pname = field.xname = option.document_key_name;
				field.type = option.xs_prefix_ + "string";
				field.xs_type = XsDataType.xs_string;
				field.document_key = true;

				table.fields.add(0, field);

			});

		}

		// update system key, user key, omissible and jsonable flags

		tables.parallelStream().forEach(table -> table.fields.forEach(field -> {

			field.setSystemKey();
			field.setUserKey();
			field.setOmissible(table, option);
			field.setJsonable(table, option);


		}));

		// decide prefix of target namespace

		List<String> other_namespaces = new ArrayList<String>();

		tables.parallelStream().forEach(table -> {

			if (table.target_namespace != null)
				table.prefix = getPrefixOf(table.target_namespace.split(" ")[0], "");

			table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> {

				if (field.target_namespace != null) {

					field.prefix = getPrefixOf(field.target_namespace.split(" ")[0], "");
					field.is_xs_namespace = field.target_namespace.equals(PgSchemaUtil.xs_namespace_uri);

				}

				if (field.any || field.any_attribute) {

					String namespace = field.namespace.split(" ")[0]; // eval first item only

					switch (namespace) {
					case "##any":
					case "##targetNamespace":
						field.namespace = table.target_namespace;
						field.prefix = table.prefix;
						break;
					case "##other":
					case "##local":
						field.namespace = "";
						field.prefix = "";
						break;
					default:
						field.namespace = namespace;
						field.prefix = getPrefixOf(namespace, "");

						if (field.prefix.isEmpty()) {

							if (other_namespaces.contains(namespace))
								field.prefix = "ns" + (other_namespaces.indexOf(namespace) + 1);

							else {
								field.prefix = "ns" + (other_namespaces.size() + 1);
								other_namespaces.add(namespace);
							}

							if (field.prefix.equals("ns1"))
								field.prefix = "ns";

						}

					}

				}

			});

			table.has_required_field = table.fields.stream().anyMatch(field -> field.element && field.required);

		});

		other_namespaces.clear();

		// instance of message digest

		if (!option.hash_algorithm.isEmpty() && !option.hash_size.equals(PgHashSize.debug_string)) {

			try {
				md_hash_key = MessageDigest.getInstance(option.hash_algorithm);
			} catch (NoSuchAlgorithmException e) {
				throw new PgSchemaException(e);
			}

		}

		// instance of document builder, SAX parser for any content

		if (option.wild_card && tables.parallelStream().anyMatch(table -> table.has_any || table.has_any_attribute)) {

			try {

				any_doc_builder = doc_builder;

				TransformerFactory tf_factory = TransformerFactory.newInstance();
				any_transformer = tf_factory.newTransformer();

				any_transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
				any_transformer.setOutputProperty(OutputKeys.INDENT, "no");

				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setValidating(false);
				spf.setNamespaceAware(false);

				any_sax_parser = spf.newSAXParser();

			} catch (TransformerConfigurationException | IllegalArgumentException | TransformerFactoryConfigurationError | ParserConfigurationException | SAXException e) {
				throw new PgSchemaException(e);
			}

		}

		// statistics

		if (option.ddl_output) {

			StringBuilder sb = new StringBuilder();

			HashSet<String> namespace_uri = new HashSet<String>();

			def_namespaces.entrySet().stream().map(arg -> arg.getValue()).forEach(arg -> namespace_uri.add(arg));

			namespace_uri.forEach(arg -> sb.append(arg + " (" + getPrefixOf(arg, "default") + "), "));
			namespace_uri.clear();

			root_schema.def_stat_msg.append("--   Namespaces:\n");
			root_schema.def_stat_msg.append("--    " + sb.substring(0, sb.length() - 2) + "\n");

			sb.setLength(0);

			schema_locations.stream().filter(arg -> !dup_schema_locations.containsKey(arg)).forEach(arg -> sb.append(arg + ", "));

			root_schema.def_stat_msg.append("--   Schema locations:\n");
			root_schema.def_stat_msg.append("--    " + sb.substring(0, sb.length() - 2) + "\n");

			sb.setLength(0);

			root_schema.def_stat_msg.append("--   Table types:\n");
			root_schema.def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.xs_type.equals(XsTableType.xs_root) && table.writable).count() + " root, ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.xs_type.equals(XsTableType.xs_root_child) && table.writable).count() + " root children, ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_root) && table.writable).count() + " admin roots, ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_child) && table.writable).count() + " admin children\n");
			root_schema.def_stat_msg.append("--   System keys:\n");
			root_schema.def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.primary_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " primary keys ("
					+ tables.parallelStream().map(table -> table.fields.stream().filter(field -> field.unique_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " unique constraints), ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.foreign_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " foreign keys ("
					+ countForeignKeyReferences() + " key references), ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.nested_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " nested keys ("
					+ tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.nested_key_as_attr).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " as attribute)\n");
			root_schema.def_stat_msg.append("--   User keys:\n");
			root_schema.def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.document_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " document keys, ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.serial_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " serial keys, ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.xpath_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " xpath keys\n");
			root_schema.def_stat_msg.append("--   Contents:\n");
			root_schema.def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.attribute && !option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name)).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " attributes ("
					+ (option.document_key || !option.inplace_document_key ? 0 : tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.attribute && !option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name) && (option.inplace_document_key_names.contains(field.name) || option.inplace_document_key_names.contains(table.name + "." + field.name))).count()).reduce((arg0, arg1) -> arg0 + arg1).get()) + " in-place document keys), ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.element && !option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name)).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " elements ("
					+ (option.document_key || !option.inplace_document_key ? 0 : tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.element && !option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name) && (option.inplace_document_key_names.contains(field.name) || option.inplace_document_key_names.contains(table.name + "." + field.name))).count()).reduce((arg0, arg1) -> arg0 + arg1).get()) + " in-place document keys), ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.simple_content).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " simple contents ("
					+ tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.simple_attribute).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " as attribute, "
					+ tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.simple_attr_cond).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " as conditional attribute)\n");
			root_schema.def_stat_msg.append("--   Wild cards:\n");
			root_schema.def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.any).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " any elements, ");
			root_schema.def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.any_attribute).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " any attributes\n");

		}

		// update schema locations to unique ones

		tables.parallelStream().forEach(table -> table.schema_location = getUniqueSchemaLocations(table.schema_location));
		attr_groups.parallelStream().forEach(attr_group -> attr_group.schema_location = getUniqueSchemaLocations(attr_group.schema_location));
		model_groups.parallelStream().forEach(model_group -> model_group.schema_location = getUniqueSchemaLocations(model_group.schema_location));

		// realize PostgreSQL DDL

		realize();

		// check root table exists

		hasRootTable();

		// reset whether writable table in case of relational data extension

		if (!option.rel_data_ext)
			tables.parallelStream().filter(table -> table.writable && !(table.required && (option.rel_data_ext || !table.relational))).forEach(table -> table.writable = false);

		// decide primary table for questing document id

		setDocIdTable();

		// check in-place document keys

		if (!option.document_key && option.inplace_document_key) {

			tables.parallelStream().filter(table -> table.writable).forEach(table -> {

				try {
					getDocKeyName(table);
				} catch (PgSchemaException e) {
					e.printStackTrace();
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

		table.xname = option.getUnqualifiedName(name);
		table.name = table.pname = option.case_sense ? table.xname : table.xname.toLowerCase();

		if (table.pname.isEmpty())
			return;

		table.required = true;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		table.xs_type = root_element ? XsTableType.xs_root : XsTableType.xs_admin_root;

		table.fields = new ArrayList<PgField>();

		table.level = level = 0;

		table.addPrimaryKey(option, true);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name = child.getNodeName();

			if (child_name.equals(option.xs_prefix_ + "complexType") || child_name.equals(option.xs_prefix_ + "simpleType"))
				extractField(child, table, false);

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

			if (tables.parallelStream().anyMatch(_table -> _table.xs_type.equals(XsTableType.xs_root)))
				root_table = tables.parallelStream().filter(_table -> _table.xs_type.equals(XsTableType.xs_root)).findFirst().get();

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

					boolean unique_key = table.addNestedKey(option, child_table.pg_schema_name, option.getUnqualifiedName(name), dummy, node);

					Element child_e = (Element) node;

					String child_name = child_e.getAttribute("name");

					child_table.xname = option.getUnqualifiedName(child_name);
					child_table.name = child_table.pname = option.case_sense ? child_table.xname : child_table.xname.toLowerCase();

					table.required = child_table.required = true;

					if ((child_table.anno = option.extractAnnotation(node, true)) != null)
						child_table.xanno_doc = option.extractDocumentation(node, false);

					child_table.xs_type = XsTableType.xs_admin_root;

					child_table.fields = new ArrayList<PgField>();

					child_table.level = level;

					child_table.addPrimaryKey(option, unique_key);

					if (!child_table.addNestedKey(option, table.pg_schema_name, option.getUnqualifiedName(dummy.type), dummy, node))
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

		table.xname = option.getUnqualifiedName(name);
		table.name = table.pname = option.case_sense ? table.xname : table.xname.toLowerCase();

		if (table.pname.isEmpty())
			return;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		if (annotation) {

			if (table.anno != null && !table.anno.isEmpty()) {

				PgTable known_table = getCanTable(table.pg_schema_name, table.xname);

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

		table.addPrimaryKey(option, true);

		if (complex_type) {

			extractAttributeGroup(node, table); // default attribute group

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
				extractField(child, table, false);

		}

		else
			extractSimpleContent(node, table, false);

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

		table.xname = option.getUnqualifiedName(name);
		table.name = table.pname = option.case_sense ? table.xname : table.xname.toLowerCase();

		if (table.pname.isEmpty())
			return;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		table.xs_type = XsTableType.xs_attr_group;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table, false);

		if (avoidTableDuplication(root_schema.attr_groups, table)) {

			attr_groups.add(table);

			if (!this.equals(root_schema))
				root_schema.attr_groups.add(table);

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

		table.xname = option.getUnqualifiedName(name);
		table.name = table.pname = option.case_sense ? table.xname : table.xname.toLowerCase();

		if (table.pname.isEmpty())
			return;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		table.xs_type = XsTableType.xs_model_group;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table, false);

		if (avoidTableDuplication(root_schema.model_groups, table)) {

			model_groups.add(table);

			if (!this.equals(root_schema))
				root_schema.model_groups.add(table);

		}

	}

	/**
	 * Extract field of table.
	 *
	 * @param node current node
	 * @param table current table
	 * @param insert_complex_type whether this node has complex type parent node
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractField(Node node, PgTable table, boolean insert_complex_type) throws PgSchemaException {

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
			extractElement(node, table, insert_complex_type);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "group")) {
			extractModelGroup(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "simpleContent")) {
			extractSimpleContent(node, table, false);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "complexContent")) {
			extractComplexContent(node, table);
			return;
		}

		else if (node_name.equals(option.xs_prefix_ + "complexType"))
			insert_complex_type = true;

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
				extractElement(child, table, insert_complex_type);

			else if (child_name.equals(option.xs_prefix_ + "group"))
				extractModelGroup(child, table);

			else if (child_name.equals(option.xs_prefix_ + "simpleContent"))
				extractSimpleContent(child, table, false);

			else if (child_name.equals(option.xs_prefix_ + "complexContent"))
				extractComplexContent(child, table);

			else
				extractField(child, table, insert_complex_type);

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

		if (root_schema.foreign_keys.parallelStream().anyMatch(_foreign_key -> _foreign_key.equals(foreign_key)))
			return;

		root_schema.foreign_keys.add(foreign_key);

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

		field.xname = PgSchemaUtil.any_name;
		field.name = option.case_sense ? field.xname : field.xname.toLowerCase();
		field.pname = table.avoidFieldDuplication(option, field.xname);

		if ((field.anno = option.extractAnnotation(node, false)) != null)
			field.xanno_doc = option.extractDocumentation(node, false);

		field.xs_type = XsDataType.xs_any;
		field.type = field.xs_type.name();

		field.extractTargetNamespace(this, node); // require type definition
		field.extractNamespace(node);

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

		field.xname = PgSchemaUtil.any_attribute_name;
		field.name = option.case_sense ? field.xname : field.xname.toLowerCase();
		field.pname = table.avoidFieldDuplication(option, field.xname);

		if ((field.anno = option.extractAnnotation(node, false)) != null)
			field.xanno_doc = option.extractDocumentation(node, false);

		field.xs_type = XsDataType.xs_anyAttribute;
		field.type = field.xs_type.name();

		field.extractTargetNamespace(this, node); // require type definition
		field.extractNamespace(node);

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

		extractInfoItem(node, table, true, false);

	}

	/**
	 * Extract element.
	 *
	 * @param node current node
	 * @param table current table
	 * @param has_complex_type_parent whether this node has complex type parent node
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractElement(Node node, PgTable table, boolean has_complex_type_parent) throws PgSchemaException {

		extractInfoItem(node, table, false, has_complex_type_parent);

	}

	/**
	 * Concrete extractor for both attribute and element.
	 *
	 * @param node current node
	 * @param table current table
	 * @param attribute whether it is attribute or not (element)
	 * @param insert_complex_type whether this node has complex type parent node
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractInfoItem(Node node, PgTable table, boolean attribute, boolean insert_complex_type) throws PgSchemaException {

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
			field.name = option.case_sense ? field.xname : field.xname.toLowerCase();
			field.pname = table.avoidFieldDuplication(option, field.xname);

			if ((field.anno = option.extractAnnotation(node, false)) != null)
				field.xanno_doc = option.extractDocumentation(node, false);

			field.extractType(option, node);
			field.extractTargetNamespace(this, node); // require type definition
			field.extractRequired(node);
			field.extractFixedValue(node);
			field.extractDefaultValue(node);
			field.extractBlockValue(node);
			field.extractEnumeration(option, node);
			field.extractRestriction(option, node);

			if (field.substitution_group != null && !field.substitution_group.isEmpty())
				table.appendSubstitutionGroup(field);

			if (field.enumeration != null && field.enumeration.length > 0) {

				field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.pname) + "_" + PgSchemaUtil.avoidPgReservedOps(field.pname);

				if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
					field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

			}

			if (field.type == null || field.type.isEmpty()) {

				if (!table.addNestedKey(option, table.pg_schema_name, option.getUnqualifiedName(name), field, node))
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

					// list of primitive data type

					if (insert_complex_type && field.list_holder) {

						level++;

						PgTable child_table = new PgTable(table.pg_schema_name, table.target_namespace, def_schema_location);

						boolean unique_key = table.addNestedKey(option, child_table.pg_schema_name, option.getUnqualifiedName(name), field, node);

						if (!unique_key)
							table.cancelUniqueKey();

						Element child_e = (Element) node;

						String child_name = child_e.getAttribute("name");

						child_table.xname = option.getUnqualifiedName(child_name);
						child_table.name = child_table.pname = option.case_sense ? child_table.xname : child_table.xname.toLowerCase();

						table.required = child_table.required = true;

						if ((child_table.anno = option.extractAnnotation(node, true)) != null)
							child_table.xanno_doc = option.extractDocumentation(node, false);

						child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

						child_table.fields = new ArrayList<PgField>();

						child_table.level = level;

						child_table.addPrimaryKey(option, unique_key);

						extractSimpleContent(node, child_table, true);

						child_table.removeProhibitedAttrs();
						child_table.removeBlockedSubstitutionGroups();
						child_table.countNestedFields();

						if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
							tables.add(child_table);

						level--;

					}

					else {

						field.xs_type = XsDataType.valueOf("xs_" + type[1]);

						table.fields.add(field);

					}

				}

				// non-primitive data type

				else {

					level++;

					PgTable child_table = new PgTable(getPgSchemaOf(getNamespaceUriOfQName(field.type)), getNamespaceUriOfQName(field.type), def_schema_location);

					boolean unique_key = table.addNestedKey(option, child_table.pg_schema_name, option.getUnqualifiedName(name), field, node);

					if (!unique_key)
						table.cancelUniqueKey();

					Element child_e = (Element) node;

					String child_name = child_e.getAttribute("name");

					child_table.xname = option.getUnqualifiedName(child_name);
					child_table.name = child_table.pname = option.case_sense ? child_table.xname : child_table.xname.toLowerCase();

					table.required = child_table.required = true;

					if ((child_table.anno = option.extractAnnotation(node, true)) != null)
						child_table.xanno_doc = option.extractDocumentation(node, false);

					child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

					child_table.fields = new ArrayList<PgField>();

					child_table.level = level;

					child_table.addPrimaryKey(option, unique_key);

					if (!child_table.addNestedKey(option, table.pg_schema_name, option.getUnqualifiedName(field.type), field, node))
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

					String ref_xname = option.getUnqualifiedName(ref);

					Element child_e = (Element) child;

					String child_name = option.getUnqualifiedName(child_e.getAttribute("name"));

					if (child_name.equals(ref_xname) && (
							(table.target_namespace != null && table.target_namespace.equals(getNamespaceUriOfQName(ref))) ||
							(table.target_namespace == null && getNamespaceUriOfQName(ref) == null))) {

						field.xname = child_name;
						field.name = option.case_sense ? field.xname : field.xname.toLowerCase();
						field.pname = table.avoidFieldDuplication(option, field.xname);

						if ((field.anno = option.extractAnnotation(child, false)) != null)
							field.xanno_doc = option.extractDocumentation(child, false);

						field.extractType(option, child);
						field.extractTargetNamespace(this, child); // require type definition
						field.extractRequired(child);
						field.extractFixedValue(child);
						field.extractDefaultValue(child);
						field.extractBlockValue(child);
						field.extractEnumeration(option, child);
						field.extractRestriction(option, child);

						if (field.substitution_group != null && !field.substitution_group.isEmpty())
							table.appendSubstitutionGroup(field);

						if (field.enumeration != null && field.enumeration.length > 0) {

							field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.pname) + "_" + PgSchemaUtil.avoidPgReservedOps(field.pname);

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

								// list of primitive data type

								if (insert_complex_type && field.list_holder) {

									level++;

									PgTable child_table = new PgTable(table.pg_schema_name, table.target_namespace, def_schema_location);

									boolean unique_key = table.addNestedKey(option, child_table.pg_schema_name, child_name, field, child);

									if (!unique_key)
										table.cancelUniqueKey();

									child_table.xname = child_name;
									child_table.name = child_table.pname = option.case_sense ? child_table.xname : child_table.xname.toLowerCase();

									table.required = child_table.required = true;

									if ((child_table.anno = option.extractAnnotation(child, true)) != null)
										child_table.xanno_doc = option.extractDocumentation(child, false);

									child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

									child_table.fields = new ArrayList<PgField>();

									child_table.level = level;

									child_table.addPrimaryKey(option, unique_key);

									extractSimpleContent(node, child_table, true);

									child_table.removeProhibitedAttrs();
									child_table.removeBlockedSubstitutionGroups();
									child_table.countNestedFields();

									if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
										tables.add(child_table);

									level--;

								}

								else {

									field.xs_type = XsDataType.valueOf("xs_" + type[1]);

									table.fields.add(field);

								}

							}

							// non-primitive data type

							else {

								level++;

								PgTable child_table = new PgTable(getPgSchemaOf(getNamespaceUriOfQName(field.type)), getNamespaceUriOfQName(field.type), def_schema_location);

								boolean unique_key = table.addNestedKey(option, child_table.pg_schema_name, child_name, field, child);

								if (!unique_key)
									table.cancelUniqueKey();

								child_table.xname = child_name;
								child_table.name = child_table.pname = option.case_sense ? child_table.xname : child_table.xname.toLowerCase();

								table.required = child_table.required = true;

								if ((child_table.anno = option.extractAnnotation(child, true)) != null)
									child_table.xanno_doc = option.extractDocumentation(child, false);

								child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

								child_table.fields = new ArrayList<PgField>();

								child_table.level = level;

								child_table.addPrimaryKey(option, unique_key);

								if (!child_table.addNestedKey(option, table.pg_schema_name, option.getUnqualifiedName(field.type), field, child))
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

		PgTable table = new PgTable(getPgSchemaOf(foreign_table), foreign_table.target_namespace, def_schema_location);

		Element e = (Element) node;

		String type = e.getAttribute("type");

		if (type == null || type.isEmpty()) {

			String name = e.getAttribute("name");

			table.xname = option.getUnqualifiedName(name);
			table.name = table.pname = option.case_sense ? table.xname : table.xname.toLowerCase();
			table.xs_type = foreign_table.xs_type.equals(XsTableType.xs_root) || foreign_table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;


		}

		else {

			table.xname = option.getUnqualifiedName(type);
			table.name = table.pname = option.case_sense ? table.xname : table.xname.toLowerCase();
			table.xs_type = XsTableType.xs_admin_root;

		}

		if (table.pname.isEmpty())
			return;

		table.required = true;

		if ((table.anno = option.extractAnnotation(node, true)) != null)
			table.xanno_doc = option.extractDocumentation(node, false);

		table.fields = new ArrayList<PgField>();

		table.level = level;

		table.addPrimaryKey(option, true);
		table.addForeignKey(option, foreign_table);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			extractField(child, table, false);

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

		PgTable attr_group = getAttributeGroup(ref, false);

		if (attr_group == null) {

			root_schema.pending_attr_groups.add(new PgSchemaPendingGroup(ref, table.pg_schema_name, table.xname, table.fields.size()));
			table.has_pending_group = true;

			return;
		}

		table.fields.addAll(attr_group.fields);

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

		PgTable model_group = getModelGroup(ref, false);

		if (model_group == null) {

			root_schema.pending_model_groups.add(new PgSchemaPendingGroup(ref, table.pg_schema_name, table.xname, table.fields.size()));
			table.has_pending_group = true;

			return;
		}

		table.fields.addAll(model_group.fields);

	}

	/**
	 * Extract simple content.
	 *
	 * @param node current node
	 * @param table current table
	 * @param primitive_list whether primitive list
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractSimpleContent(Node node, PgTable table, boolean primitive_list) throws PgSchemaException {

		PgField field = new PgField();

		field.simple_content = true;
		field.simple_primitive_list = primitive_list;

		field.xname = PgSchemaUtil.simple_content_name; // anonymous simple content
		field.name = option.case_sense ? field.xname : field.xname.toLowerCase();
		field.pname = table.avoidFieldDuplication(option, field.xname);

		if ((field.anno = option.extractAnnotation(node, false)) != null)
			field.xanno_doc = option.extractDocumentation(node, false);

		field.extractType(option, node);
		field.extractTargetNamespace(this, node); // require type definition
		field.extractRequired(node);
		field.extractFixedValue(node);
		field.extractDefaultValue(node);
		field.extractBlockValue(node);
		field.extractEnumeration(option, node);
		field.extractRestriction(option, node);

		if (field.substitution_group != null && !field.substitution_group.isEmpty())
			table.appendSubstitutionGroup(field);

		if (field.enumeration != null && field.enumeration.length > 0) {

			field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.pname) + "_" + PgSchemaUtil.avoidPgReservedOps(field.pname);

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

				table.addNestedKey(option, table.pg_schema_name, option.getUnqualifiedName(child_e.getAttribute("base")));

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
			extractElement(node, table, false);
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
				extractElement(child, table, false);

			else if (child_name.equals(option.xs_prefix_ + "group"))
				extractModelGroup(child, table);

			else
				extractComplexContentExt(child, table);

		}

	}

	/**
	 * Avoid table duplication while merging equivalent tables.
	 *
	 * @param tables target table list
	 * @param table current table having table name at least
	 * @return boolean whether no name collision occurs
	 */
	private boolean avoidTableDuplication(List<PgTable> tables, PgTable table) {

		if (tables == null)
			return true;

		List<PgField> fields = table.fields;

		PgTable known_table = null;

		try {
			known_table = tables.equals(this.tables) ? getPgTable(table.pg_schema_name, table.pname) : tables.equals(root_schema.attr_groups) ? getAttributeGroup(table.xname, false) : tables.equals(root_schema.model_groups) ? getModelGroup(table.xname, false) : null;
		} catch (PgSchemaException e) {
			e.printStackTrace();
		}

		if (known_table == null)
			return true;

		// avoid table duplication (case insensitive)

		if (!option.case_sense && !known_table.xname.equals(table.xname)) {

			table.pname = "_" + table.pname;

			try {
				known_table = tables.equals(this.tables) ? getCanTable(table.pg_schema_name, table.xname) : tables.equals(root_schema.attr_groups) ? getAttributeGroup(table.xname, false) : tables.equals(root_schema.model_groups) ? getModelGroup(table.xname, false) : null;
			} catch (PgSchemaException e) {
				e.printStackTrace();
			}

			if (known_table == null)
				return true;

		}

		boolean changed = false;
		boolean name_collision = false;

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

				if (!known_table.schema_location.contains(root_schema.unq_schema_locations.get(table.target_namespace)))
					known_table.schema_location += " " + table.schema_location;
				else
					root_schema.dup_schema_locations.put(table.schema_location, root_schema.unq_schema_locations.get(table.target_namespace));

			}

		}

		List<PgField> known_fields = known_table.fields;

		for (PgField field : fields) {

			PgField known_field = known_table.getPgField(field.pname);

			// append new field to known table

			if (known_field == null) {

				changed = true;

				if (!field.primary_key && field.required && !table.xs_type.equals(XsTableType.xs_admin_root) && known_table.xs_type.equals(XsTableType.xs_admin_root))
					name_collision = true;

				known_fields.add(field);

			}

			// update field

			else {

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

					if (table.getPgField(known_field.pname) == null) {

						changed = true;

						if (!known_field.primary_key && known_field.required)
							name_collision = true;

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

			// avoid name collision

			if (name_collision) {

				known_fields.stream().filter(field -> !field.system_key && !field.user_key && field.required).forEach(field -> field.required = false);
				known_table.name_collision = true;

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

				if (root_schema.dup_schema_locations.containsKey(schema_location))
					sb.append(root_schema.dup_schema_locations.get(schema_location) + " ");
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

		String xname = option.getUnqualifiedName(qname);

		return getNamespaceUriForPrefix(xname.equals(qname) ? "" : qname.substring(0, qname.length() - xname.length() - 1));
	}

	/**
	 * Return prefix of namespace URI.
	 *
	 * @param namespace_uri namespace URI
	 * @param def_prefix prefix for default namespace URI
	 * @return String prefix of namespace URI
	 */
	private String getPrefixOf(String namespace_uri, String def_prefix) {
		return def_namespaces.entrySet().parallelStream().anyMatch(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()) ? def_namespaces.entrySet().parallelStream().filter(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()).findFirst().get().getKey() : def_prefix;
	}

	/**
	 * Return PostgreSQL schema name of namespace URI.
	 *
	 * @param namespace_uri namespace URI
	 * @return String PostgreSQL schema name of namespace URI
	 */
	private String getPgSchemaOf(String namespace_uri) {
		return option.pg_named_schema ? (def_namespaces.entrySet().parallelStream().anyMatch(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()) ? def_namespaces.entrySet().parallelStream().filter(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()).findFirst().get().getKey() : PgSchemaUtil.pg_public_schema_name) : PgSchemaUtil.pg_public_schema_name;
	}

	/**
	 * Return PostgreSQL schema name of namespace URI.
	 *
	 * @param table table
	 * @return String PostgreSQL schema name of namespace URI
	 */
	private String getPgSchemaOf(PgTable table) {
		return getPgSchemaOf(table.target_namespace);
	}

	/**
	 * Return PostgreSQL name of table.
	 *
	 * @param table table
	 * @return String PostgreSQL name of table
	 */
	protected String getPgNameOf(PgTable table) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(table.pname);
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
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(getDbTableName(db_conn, table.pname));
	}

	/**
	 * Return PostgreSQL name of parent table.
	 *
	 * @param foreign_key foreign key
	 * @return String PostgreSQL name of parent table
	 */
	private String getPgParentNameOf(PgForeignKey foreign_key) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(foreign_key.pg_schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(foreign_key.parent_table_pname);
	}

	/**
	 * Return PostgreSQL name of child table.
	 *
	 * @param foreign_key foreign key
	 * @return String PostgreSQL name of child table
	 */
	private String getPgChildNameOf(PgForeignKey foreign_key) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(foreign_key.pg_schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(foreign_key.child_table_pname);
	}

	/**
	 * Return PostgreSQL name of foreign table.
	 *
	 * @param field field of either nested key or foreign key
	 * @return String PostgreSQL name of foreign table
	 */
	private String getPgForeignNameOf(PgField field) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(field.foreign_schema) + "." : "") + PgSchemaUtil.avoidPgReservedWords(field.foreign_table_pname);
	}

	/**
	 * Return data (CSV/TSV) file name of table.
	 *
	 * @param table table
	 * @return String PostgreSQL name of table
	 */
	protected String getDataFileNameOf(PgTable table) {
		return (option.pg_named_schema ? table.pg_schema_name + "." : "") + table.pname + (option.pg_tab_delimiter ? ".tsv" : ".csv");
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

		if (!option.pg_named_schema)
			pg_schema_name = PgSchemaUtil.pg_public_schema_name;

		else if (pg_schema_name == null || pg_schema_name.isEmpty())
			pg_schema_name = root_table.pg_schema_name;

		String _pg_schema_name = pg_schema_name;

		Optional<PgTable> opt = tables.parallelStream().filter(table -> table.pg_schema_name.equals(_pg_schema_name) && table.name.equals(table_name)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Return table.
	 *
	 * @param pg_schema_name PostgreSQL schema name
	 * @param table_xname table name (canonical)
	 * @return PgTable table
	 */
	private PgTable getCanTable(String pg_schema_name, String table_xname) {

		if (!option.pg_named_schema)
			pg_schema_name = PgSchemaUtil.pg_public_schema_name;

		else if (pg_schema_name == null || pg_schema_name.isEmpty())
			pg_schema_name = root_table.pg_schema_name;

		String _pg_schema_name = pg_schema_name;

		Optional<PgTable> opt = tables.parallelStream().filter(table -> table.pg_schema_name.equals(_pg_schema_name) && table.xname.equals(table_xname)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Return table.
	 *
	 * @param pg_schema_name PostgreSQL schema name
	 * @param table_pname table name in PostgreSQL
	 * @return PgTable table
	 */
	private PgTable getPgTable(String pg_schema_name, String table_pname) {

		if (!option.pg_named_schema)
			pg_schema_name = PgSchemaUtil.pg_public_schema_name;

		else if (pg_schema_name == null || pg_schema_name.isEmpty())
			pg_schema_name = root_table.pg_schema_name;

		String _pg_schema_name = pg_schema_name;

		Optional<PgTable> opt = tables.parallelStream().filter(table -> table.pg_schema_name.equals(_pg_schema_name) && table.pname.equals(table_pname)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Return pending table.
	 *
	 * @param pending_group pending group
	 * @return PgTable pending table
	 */
	private PgTable getPendingTable(PgSchemaPendingGroup pending_group) {
		return getCanTable(pending_group.pg_schema_name, pending_group.xname);
	}

	/**
	 * Return parent table of foreign key.
	 *
	 * @param foreign_key foreign key
	 * @return PgTable parent table
	 */
	private PgTable getParentTable(PgForeignKey foreign_key) {
		return getCanTable(foreign_key.pg_schema_name, foreign_key.parent_table_xname);
	}

	/**
	 * Return child table of foreign key.
	 *
	 * @param foreign_key foreign key
	 * @return PgTable child table
	 */
	private PgTable getChildTable(PgForeignKey foreign_key) {
		return getCanTable(foreign_key.pg_schema_name, foreign_key.child_table_xname);
	}

	/**
	 * Return foreign table of either nested key or foreign key.
	 *
	 * @param field field of either nested key of foreign key
	 * @return PgTable foreign table
	 */
	protected PgTable getForeignTable(PgField field) {
		return field.foreign_table_id == -1 ? getCanTable(field.foreign_schema, field.foreign_table_xname) : getTable(field.foreign_table_id);
	}

	/**
	 * Return attribute group.
	 *
	 * @param attr_group_name attribute group name
	 * @param throwable throws exception if declaration does not exist
	 * @return PgTable attribute group
	 * @throws PgSchemaException the pg schema exception
	 */
	private PgTable getAttributeGroup(String attr_group_name, boolean throwable) throws PgSchemaException {

		Optional<PgTable> opt = root_schema.attr_groups.parallelStream().filter(attr_group -> attr_group.xname.equals(attr_group_name)).findFirst();

		if (opt.isPresent())
			return opt.get();

		if (throwable)
			throw new PgSchemaException("Not found attribute group declaration: " + attr_group_name + ".");

		return null;
	}

	/**
	 * Return model group.
	 *
	 * @param model_group_name model group name
	 * @param throwable throws exception if declaration does not exist
	 * @return PgTable model group
	 * @throws PgSchemaException the pg schema exception
	 */
	private PgTable getModelGroup(String model_group_name, boolean throwable) throws PgSchemaException {

		Optional<PgTable> opt = root_schema.model_groups.parallelStream().filter(model_group -> model_group.xname.equals(model_group_name)).findFirst();

		if (opt.isPresent())
			return opt.get();

		if (throwable)
			throw new PgSchemaException("Not found model group declaration: " + model_group_name + ".");

		return null;
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

		name_collision = tables.parallelStream().anyMatch(table -> table.name_collision);

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
		System.out.println("--  no name collision: " + !name_collision);
		System.out.println("--  append document key: " + option.document_key);
		System.out.println("--  append serial key: " + option.serial_key);
		System.out.println("--  append xpath key: " + option.xpath_key);
		System.out.println("--  retain constraint of primary/foreign key: " + option.retain_key);
		System.out.println("--  retrieve field annotation: " + !option.no_field_anno);
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

			if (!pg_named_schemata.isEmpty()) {

				// short of memory in case of huge database
				// pg_named_schemata.stream().forEach(named_schema -> System.out.println("DROP SCHEMA IF EXISTS " + PgSchemaUtil.avoidPgReservedWords(named_schema) + " CASCADE;"));
				// System.out.println("");

				pg_named_schemata.stream().forEach(named_schema -> System.out.println("CREATE SCHEMA IF NOT EXISTS " + PgSchemaUtil.avoidPgReservedWords(named_schema) + ";"));

				System.out.print("\nSET search_path TO ");

				pg_named_schemata.stream().forEach(named_schema -> System.out.print(PgSchemaUtil.avoidPgReservedWords(named_schema) + ", "));

				System.out.println(PgSchemaUtil.pg_public_schema_name + ";\n");

			}

		}

		if (def_anno != null) {

			System.out.println("--");
			System.out.println("-- " + def_anno);
			System.out.println("--\n");

		}

		tables.stream().filter(table -> table.writable).sorted(Comparator.comparingInt(table -> -table.order)).forEach(table -> {

			System.out.println("DROP TABLE IF EXISTS " + getPgNameOf(table) + " CASCADE;");

		});

		System.out.println("");

		tables.stream().filter(table -> !table.realized).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> realize(table, true));

		// add primary key/foreign key

		if (!option.retain_key) {

			tables.stream().filter(table -> !table.bridge).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

				table.fields.forEach(field -> {

					if (field.unique_key)
						System.out.println("--ALTER TABLE " + getPgNameOf(table) + " ADD PRIMARY KEY ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " );\n");

					else if (field.foreign_key) {

						if (!getForeignTable(field).bridge)
							System.out.println("--ALTER TABLE " + getPgNameOf(table) + " ADD FOREIGN KEY " + field.constraint_name + " REFERENCES " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_pname) + " );\n");

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

				if (foreign_key.parent_table_xname.equals(foreign_key2.parent_table_xname)) {
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

				PgField field = table.getCanField(foreign_key.parent_field_xnames);

				if (field != null) { // specified parent field

					if (field.primary_key)
						unique = false;

				}

				else
					continue;

			}

			if (!unique)
				continue;

			String constraint_name = "UNQ_" + foreign_key.parent_table_xname;

			if (constraint_name.length() > PgSchemaUtil.max_enum_len)
				constraint_name = constraint_name.substring(0, PgSchemaUtil.max_enum_len);

			System.out.println((option.retain_key ? "" : "--") + "ALTER TABLE " + getPgParentNameOf(foreign_key) + " ADD CONSTRAINT " + PgSchemaUtil.avoidPgReservedOps(constraint_name) + " UNIQUE ( " + PgSchemaUtil.avoidPgReservedWords(foreign_key.parent_field_pnames) + " );\n");

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

			String[] child_field_pnames = foreign_key.child_field_pnames.split(" ");
			String[] parent_field_pnames = foreign_key.parent_field_pnames.split(" ");

			if (child_field_pnames.length == parent_field_pnames.length) {

				for (int i = 0; i < child_field_pnames.length; i++) {

					child_field_pnames[i] = child_field_pnames[i].replaceFirst(",$", "");
					parent_field_pnames[i] = parent_field_pnames[i].replaceFirst(",$", "");

					String constraint_name = "KR_" + foreign_key.name + (child_field_pnames.length > 1 ? "_" + i : "");

					if (constraint_name.length() > PgSchemaUtil.max_enum_len)
						constraint_name = constraint_name.substring(0, PgSchemaUtil.max_enum_len);

					System.out.println((option.retain_key ? "" : "--") + "ALTER TABLE " + getPgChildNameOf(foreign_key) + " ADD CONSTRAINT " + PgSchemaUtil.avoidPgReservedOps(constraint_name) + " FOREIGN KEY ( " + PgSchemaUtil.avoidPgReservedWords(child_field_pnames[i]) + " ) REFERENCES " + getPgNameOf(getParentTable(foreign_key)) + " ( " + PgSchemaUtil.avoidPgReservedWords(parent_field_pnames[i]) + " ) ON DELETE CASCADE NOT VALID;\n");

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

		foreign_keys.stream().filter(foreign_key -> foreign_key.pg_schema_name.equals(table.pg_schema_name) && (foreign_key.child_table_xname.equals(table.xname))).map(foreign_key -> getParentTable(foreign_key)).filter(admin_table -> admin_table != null).forEach(admin_table -> {

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

		if (!table.pname.equals(table.xname))
			System.out.println("-- canonical name: " + table.xname);

		StringBuilder sb = new StringBuilder();

		if (table.target_namespace != null && !table.target_namespace.isEmpty()) {

			for (String namespace_uri : table.target_namespace.split(" "))
				sb.append(namespace_uri + " (" + getPrefixOf(namespace_uri, "default") + "), ");

		}

		else
			sb.append("no namespace, ");

		System.out.println("-- xmlns: " + sb.toString() + "schema location: " + table.schema_location);
		System.out.println("-- type: " + table.xs_type.toString().replaceFirst("^xs_", "").replace("_",  " ") + ", content: " + table.content_holder + ", list: " + table.list_holder + ", bridge: " + table.bridge + ", virtual: " + table.virtual + (name_collision ? ", name collision: " + table.name_collision : ""));
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
				System.out.println("-- FOREIGN KEY : " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_pname) + " )");

			else if (field.nested_key) {

				if (field.nested_key_as_attr)
					System.out.println("-- NESTED KEY AS ATTRIBUTE : " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_pname) + " )" + (field.parent_node != null ? ", PARENT NODE : " + field.parent_node : "") + (field.ancestor_node != null ? ", ANCESTOR NODE : " + field.ancestor_node : ""));
				else
					System.out.println("-- NESTED KEY : " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_pname) + " )" + (field.parent_node != null ? ", PARENT NODE : " + field.parent_node : "") + (field.ancestor_node != null ? ", ANCESTOR NODE : " + field.ancestor_node : ""));

			}

			else if (field.attribute) {

				if (option.discarded_document_key_names.contains(field.name) || option.discarded_document_key_names.contains(table.name + "." + field.name))
					continue;

				if (!option.document_key && option.inplace_document_key && (option.inplace_document_key_names.contains(field.name) || option.inplace_document_key_names.contains(table.name + "." + field.name)))
					System.out.println("-- ATTRIBUTE, IN-PLACE DOCUMENT KEY");
				else
					System.out.println("-- ATTRIBUTE");

			}

			else if (field.simple_content) {

				if (field.simple_primitive_list)
					System.out.println("-- SIMPLE CONTENT AS PRIMITIVE LIST");
				else if (field.simple_attribute)
					System.out.println("-- SIMPLE CONTENT AS ATTRIBUTE, ATTRIBUTE NODE: " + field.foreign_table_xname);
				else if (field.simple_attr_cond)
					System.out.println("-- SIMPLE CONTENT AS CONDITIONAL ATTRIBUTE, ATTRIBUTE NODE: " + field.foreign_table_xname);
				else
					System.out.println("-- SIMPLE CONTENT");

			}

			else if (field.any)
				System.out.println("-- ANY ELEMENT");

			else if (field.any_attribute)
				System.out.println("-- ANY ATTRIBUTE");

			else if (option.discarded_document_key_names.contains(field.name) || option.discarded_document_key_names.contains(table.name + "." + field.name))
				continue;

			else if (!option.document_key && option.inplace_document_key && (option.inplace_document_key_names.contains(field.name) || option.inplace_document_key_names.contains(table.name + "." + field.name)))
				System.out.println("-- IN-PLACE DOCUMENT KEY");

			if (!field.required && field.xrequired) {

				if (field.fixed_value == null || field.fixed_value.isEmpty())
					System.out.println("-- must not be NULL, but dismissed due to name collision");

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
						System.out.print("CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " = '" + field.fixed_value + "' ) ");
						break;
					default:
						System.out.print("CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " = " + field.fixed_value + " ) ");
					}

					System.out.println(", but dismissed due to name collision");
				}
			}

			if (field.enum_name == null || field.enum_name.isEmpty())
				System.out.print("\t" + PgSchemaUtil.avoidPgReservedWords(field.pname) + " " + field.getPgDataType());
			else
				System.out.print("\t" + PgSchemaUtil.avoidPgReservedWords(field.pname) + " " + (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + field.enum_name);

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
					System.out.print(" CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " = '" + field.fixed_value + "' )");
					break;
				default:
					System.out.print(" CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " = " + field.fixed_value + " )");
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

					PgField foreign_field = foreign_table.getPgField(field.foreign_field_pname);

					if (foreign_field != null) {

						if (foreign_field.unique_key)
							System.out.print(" CONSTRAINT " + field.constraint_name + " REFERENCES " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_pname) + " ) ON DELETE CASCADE");

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

			String[] child_field_pnames = foreign_key.child_field_pnames.split(" ");
			String[] parent_field_pnames = foreign_key.parent_field_pnames.split(" ");

			if (child_field_pnames.length == parent_field_pnames.length)
				key_references += child_field_pnames.length;

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

		option.fill_default_value = xml_post_editor.fill_default_value;

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

				if (table.getField(field_name) != null)
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

		tables.parallelStream().forEach(table -> {

			table.filt_out = !table.filt_out;

			if (table.filt_out)
				table.required = table.writable = false;

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

		tables.parallelStream().forEach(table -> table.fields.forEach(field -> field.setIndexable(table, option)));

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

		tables.parallelStream().forEach(table -> table.fields.stream().filter(field -> field.xs_type.equals(XsDataType.xs_ID)).forEach(field -> field.attr_sel = true));

		// type dependent attribute selection

		if (index_filter.attr_string || index_filter.attr_integer || index_filter.attr_float || index_filter.attr_date)
			tables.parallelStream().forEach(table -> table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> index_filter.appendAttrByType(table, field)));

		// select all attributes

		if (index_filter.attrs == null) {

			tables.parallelStream().forEach(table -> table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> field.attr_sel = true));

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

		tables.parallelStream().forEach(_table -> {

			if (!_table.fields.stream().anyMatch(_field -> !_field.system_key && !_field.user_key && (_field.attr_sel || _field.field_sel)))
				_table.required = _table.writable = false;

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

		if (!option.getUnqualifiedName(node.getNodeName()).equals(root_table.xname))
			throw new PgSchemaException("Not found root element (node_name: " + root_table.xname + ") in XML: " + document_id);

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
	 * Return document builder for any content.
	 *
	 * @return DocumentBuilder document builder for any content
	 */
	protected DocumentBuilder getAnyDocBuilder() {
		return any_doc_builder;
	}

	/**
	 * Return transformer for any content.
	 *
	 * @return Transformer transformer for any content
	 */
	protected Transformer getAnyTransformer() {
		return any_transformer;
	}

	/**
	 * Return SAX parser for any content.
	 *
	 * @return SAXParser SAX parser for any content
	 */
	protected SAXParser getAnySaxParser() {
		return any_sax_parser;
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

	/**
	 * Close prepared statement.
	 *
	 * @param primary whether close the primary prepared statement only
	 */
	private void closePreparedStatement(boolean primary) {

		tables.parallelStream().filter(table -> table.ps != null).forEach(table -> {

			try {

				if (!table.ps.isClosed())
					table.ps.close();

			} catch (SQLException e) {
				e.printStackTrace();
			}

			table.ps = null;

		});

		if (primary)
			return;

		tables.parallelStream().filter(table -> table.ps2 != null).forEach(table -> {

			try {

				if (!table.ps2.isClosed())
					table.ps2.close();

			} catch (SQLException e) {
				e.printStackTrace();
			}

			table.ps2 = null;

		});

	}

	// Data (CSV/TSV) conversion

	/**
	 * Data (CSV/TSV) conversion.
	 *
	 * @param xml_parser XML document
	 * @param work_dir working directory contains data (CSV/TSV) files
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgCsv(XmlParser xml_parser, Path work_dir) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(false);

		tables.parallelStream().forEach(table -> {

			if (table.writable) {

				if (table.buffw == null) {

					Path data_path = Paths.get(work_dir.toString(), getDataFileNameOf(table));

					try {

						table.buffw = Files.newBufferedWriter(data_path);

					} catch (IOException e) {
						e.printStackTrace();
					}

				}

			}

			else
				table.buffw = null;

		});

		// parse root node and write to data (CSV/TSV) file

		try {

			PgSchemaNode2PgCsv node2pgcsv = new PgSchemaNode2PgCsv(this, null, root_table);

			node2pgcsv.parseRootNode(node);

			node2pgcsv.invokeRootNestedNode();

			node2pgcsv.clear();

		} catch (IOException | TransformerException | SAXException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close xml2PgCsv.
	 */
	public void closeXml2PgCsv() {

		closeTableLock();

		tables.parallelStream().forEach(table -> {

			if (table.buffw != null ) {

				try {

					table.buffw.close();
					table.buffw = null;

				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		});

	}

	/**
	 * Parse current node and write to data (CSV/TSV) file.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2PgCsv(final Node parent_node, final PgTable parent_table, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		int table_id = nested_key.table_id;

		PgTable table = nested_key.table;

		PgSchemaNode2PgCsv node2pgcsv = null;

		try {

			node2pgcsv = new PgSchemaNode2PgCsv(this, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, nested_key);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				synchronized (table_lock[table_id]) {
					node2pgcsv.parseChildNode(node_test);
				}

				node2pgcsv.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2pgcsv.visited)
				return;

			synchronized (table_lock[table_id]) {
				node2pgcsv.parseChildNode(parent_node, nested_key);
			}

			node2pgcsv.invokeChildNestedNode();

		} catch (IOException | TransformerException | SAXException e) {
			throw new PgSchemaException(e);
		} finally {
			node2pgcsv.clear();
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

		initTableLock(false);

		// parse root node and send to PostgreSQL

		try {

			if (update) {

				deleteBeforeUpdate(db_conn, option.rel_data_ext && option.retain_key);

				if (!option.rel_data_ext || !option.retain_key)
					update = false;

			}

			PgSchemaNode2PgSql node2pgsql = new PgSchemaNode2PgSql(this, null, root_table, update, db_conn);

			node2pgsql.parseRootNode(node);

			node2pgsql.invokeRootNestedNode();

			node2pgsql.clear();

			db_conn.commit(); // transaction ends

		} catch (SQLException | TransformerException | IOException | SAXException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close xml2PgSql.
	 */
	public void closeXml2PgSql() {

		closeTableLock();
		closePreparedStatement(false);

	}

	/**
	 * Parse current node and send to PostgreSQL.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param nested_key nested key
	 * @param update whether update or insert
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2PgSql(final Node parent_node, final PgTable parent_table, final PgSchemaNestedKey nested_key, final boolean update, final Connection db_conn) throws PgSchemaException {

		int table_id = nested_key.table_id;

		PgTable table = nested_key.table;

		PgSchemaNode2PgSql node2pgsql = null;

		try {

			node2pgsql = new PgSchemaNode2PgSql(this, parent_table, table, update, db_conn);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, nested_key);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				synchronized (table_lock[table_id]) {
					node2pgsql.parseChildNode(node_test);
				}

				node2pgsql.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2pgsql.visited)
				return;

			synchronized (table_lock[table_id]) {
				node2pgsql.parseChildNode(parent_node, nested_key);
			}

			node2pgsql.invokeChildNestedNode();

		} catch (SQLException | IOException | TransformerException | SAXException e) {
			throw new PgSchemaException(e);
		} finally {
			node2pgsql.clear();
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

		if (!table.fields.stream().anyMatch(field -> (field.attribute || field.element) && (option.inplace_document_key_names.contains(field.name) || option.inplace_document_key_names.contains(table.name + "." + field.name)))) {

			if (option.document_key_if_no_in_place)
				return option.document_key_name;

			throw new PgSchemaException("Not found in-place document key in " + table.pname + ", or select --doc-key-if-no-inplace option.");
		}

		return table.fields.stream().filter(field -> (field.attribute || field.element) && (option.inplace_document_key_names.contains(field.name) || option.inplace_document_key_names.contains(table.name + "." + field.name))).findFirst().get().pname;
	}

	/**
	 * Decide primary table for questing document id.
	 */
	private void setDocIdTable() {

		if (doc_id_table != null)
			return;

		doc_id_table = root_table;

		if (!option.rel_data_ext) {

			Optional<PgTable> opt = tables.parallelStream().filter(table -> table.writable).min(Comparator.comparingInt(table -> table.order));

			if (opt.isPresent())
				doc_id_table = opt.get();

		}

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

				String doc_id_table_name = doc_id_table.pname;

				String sql1 = "SELECT EXISTS(SELECT 1 FROM " + getPgNameOf(db_conn, doc_id_table) + " LIMIT 1)";

				ResultSet rset1 = stat.executeQuery(sql1);

				if (rset1.next())
					has_db_rows.put(doc_id_table_name, rset1.getBoolean(1));

				rset1.close();

				boolean has_doc_id = has_db_rows.get(doc_id_table_name);

				tables.stream().filter(table -> table.writable && !table.equals(doc_id_table)).forEach(table -> {

					String table_name = table.pname;

					if (has_doc_id) {

						try {

							String sql2 = "SELECT EXISTS(SELECT 1 FROM " + getPgNameOf(db_conn, table) + " LIMIT 1)";

							ResultSet rset2 = stat.executeQuery(sql2);

							if (rset2.next())
								has_db_rows.put(table_name, rset2.getBoolean(1));

							rset2.close();

						} catch (SQLException | PgSchemaException e) {
							e.printStackTrace();
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
				ResultSet rset = meta.getTables(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, null, null);

				while (rset.next())
					db_tables.add(rset.getString("TABLE_NAME"));

				rset.close();

			} catch (SQLException e) {
				throw new PgSchemaException(e);
			}

		}

		Optional<String> opt = db_tables.parallelStream().filter(db_table_name -> db_table_name.equals(table_name)).findFirst();

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

			if (has_db_rows == null || (has_db_rows != null && has_db_rows.get(doc_id_table.pname))) {

				String sql = "DELETE FROM " + getPgNameOf(db_conn, doc_id_table) + " WHERE " + PgSchemaUtil.avoidPgReservedWords(getDocKeyName(doc_id_table)) + "='" + document_id + "'";

				has_doc_id = stat.executeUpdate(sql) > 0;

			}

			if (has_doc_id) {

				tables.stream().filter(table -> table.writable && !table.equals(doc_id_table) && ((no_pkey && !table.fields.stream().anyMatch(field -> field.primary_key && field.unique_key)) || !no_pkey)).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

					if (has_db_rows == null || (has_db_rows != null && has_db_rows.get(table.pname))) {

						try {

							String sql = "DELETE FROM " + getPgNameOf(db_conn, table) + " WHERE " + PgSchemaUtil.avoidPgReservedWords(getDocKeyName(table)) + "='" + document_id + "'";

							stat.executeUpdate(sql);

						} catch (PgSchemaException | SQLException e) {
							e.printStackTrace();
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
	public void pgCsv2PgSql(Connection db_conn, Path work_dir) throws PgSchemaException {

		try {

			CopyManager copy_man = new CopyManager((BaseConnection) db_conn);

			tables.stream().filter(table -> table.writable).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

				Path data_path = Paths.get(work_dir.toString(), getDataFileNameOf(table));

				try {

					if (Files.size(data_path) > 0) {

						String sql = "COPY " + getPgNameOf(db_conn, table) + " FROM STDIN" + (option.pg_tab_delimiter ? "" : " CSV");

						copy_man.copyIn(sql, Files.newInputStream(data_path));

					}

				} catch (SQLException | IOException | PgSchemaException e) {
					System.err.println("Exception occurred while processing " + (option.pg_tab_delimiter ? "TSV" : "CSV") + " document: " + data_path.toAbsolutePath().toString());
					e.printStackTrace();
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

			tables.stream().filter(table -> table.writable).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

				try {

					String pg_schema_name = getPgSchemaOf(table);
					String table_name = table.pname;
					String db_table_name = getDbTableName(db_conn, table_name);

					ResultSet rset_col = meta.getColumns(null, pg_schema_name, db_table_name, null);

					while (rset_col.next()) {

						String db_column_name = rset_col.getString("COLUMN_NAME");

						if (!table.fields.stream().filter(field -> !field.omissible).anyMatch(field -> field.pname.equals(db_column_name)))
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + db_column_name + " found without declaration in the data model."); // found without declaration in the data model

					}

					rset_col.close();

					for (PgField field : table.fields) {

						if (field.omissible)
							continue;

						rset_col = meta.getColumns(null, pg_schema_name, db_table_name, field.pname);

						if (!rset_col.next())
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + field.pname + " not found in the relation."); // not found in the relation

						rset_col.close();

					}

					if (strict) {

						rset_col = meta.getColumns(null, pg_schema_name, db_table_name, null);

						List<PgField> fields = table.fields.stream().filter(field -> !field.omissible).collect(Collectors.toList());

						int col_id = 0;

						while (rset_col.next()) {

							String db_column_name = rset_col.getString("COLUMN_NAME");
							int db_column_type = rset_col.getInt("DATA_TYPE");

							if (db_column_type == java.sql.Types.NUMERIC) // NUMERIC and DECIMAL are equivalent in PostgreSQL
								db_column_type = java.sql.Types.DECIMAL;

							PgField field = fields.get(col_id++);

							if (!field.pname.equals(db_column_name))
								throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + field.pname + " found in an incorrect order."); // found in an incorrect order

							if (field.getSqlDataType() != db_column_type)
								throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + field.pname + " column type " + JDBCType.valueOf(db_column_type) + " is incorrect with " + JDBCType.valueOf(field.getSqlDataType()) + "."); // column type is incorrect

						}

						fields.clear();

						rset_col.close();

					}

				} catch (SQLException | PgSchemaException e) {
					e.printStackTrace();
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

		tables.parallelStream().forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> field.attr_sel_rdy = true));

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

		tables.parallelStream().forEach(table -> table.lucene_doc = table.required ? lucene_doc : null);

		// parse root node and store into Lucene document

		try {

			lucene_doc.add(new StringField(option.document_key_name, document_id, Field.Store.YES));

			PgSchemaNode2LucIdx node2lucidx = new PgSchemaNode2LucIdx(this, null, root_table);

			node2lucidx.parseRootNode(node);

			node2lucidx.invokeRootNestedNode();

			node2lucidx.clear();

		} catch (TransformerException | IOException | SAXException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close xml2LucIdx.
	 */
	public void closeXml2LucIdx() {

		closeTableLock();

		tables.parallelStream().filter(table -> table.lucene_doc != null).forEach(table -> table.lucene_doc = null);

	}

	/**
	 * Parse current node and store into Lucene document.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2LucIdx(final Node parent_node, final PgTable parent_table, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		PgTable table = nested_key.table;

		PgSchemaNode2LucIdx node2lucidx = null;

		try {

			node2lucidx = new PgSchemaNode2LucIdx(this, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, nested_key);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				synchronized (table_lock[0]) {
					node2lucidx.parseChildNode(node_test);
				}

				node2lucidx.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2lucidx.visited)
				return;

			synchronized (table_lock[0]) {
				node2lucidx.parseChildNode(parent_node, nested_key);
			}

			node2lucidx.invokeChildNestedNode();

		} catch (IOException | TransformerException | SAXException e) {
			throw new PgSchemaException(e);
		} finally {
			node2lucidx.clear();
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
	 * @param sphinx_schema_path Sphinx xmlpipe2 file
	 * @param data_source whether it is data source or schema
	 * @throws PgSchemaException the pg schema exception
	 */
	public void writeSphSchema(Path sphinx_schema_path, boolean data_source) throws PgSchemaException {

		try {

			BufferedWriter buffw = Files.newBufferedWriter(sphinx_schema_path);

			buffw.write("<?xml version=\"" + PgSchemaUtil.def_xml_version + "\" encoding=\"" + PgSchemaUtil.def_encoding + "\"?>\n");

			if (data_source)
				buffw.write("<sphinx:docset xmlns:sphinx=\"" + PgSchemaUtil.sph_namespace_uri + "\">\n");

			buffw.write("<sphinx:schema>\n");

			if (data_source) {

				buffw.write("<sphinx:attr name=\"" + option.document_key_name + "\" type=\"string\"/>\n"); // default attr
				buffw.write("<sphinx:field name=\"" + PgSchemaUtil.simple_content_name + "\"/>\n"); // default field

			}

			tables.forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> {

				try {

					buffw.write("<sphinx:attr name=\"" + table.name + PgSchemaUtil.sph_member_op + field.name + "\"");

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
				}

			}));

			buffw.write("</sphinx:schema>\n");

			buffw.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Write Sphinx configuration file.
	 *
	 * @param sphinx_conf_path Sphinx configuration file path
	 * @param idx_name name of Sphinx index
	 * @param data_source_path Sphinx xmlpipe2 file path
	 * @throws PgSchemaException the pg schema exception
	 */
	public void writeSphConf(Path sphinx_conf_path, String idx_name, Path data_source_path) throws PgSchemaException {

		try {

			BufferedWriter buffw = Files.newBufferedWriter(sphinx_conf_path);

			buffw.write("#\n# Sphinx configuration file sample\n#\n# WARNING! While this sample file mentions all available options,\n#\n# it contains (very) short helper descriptions only. Please refer to\n# doc/sphinx.html for details.\n#\n\n#############################################################################\n## data source definition\n#############################################################################\n\n");

			buffw.write("source " + idx_name + "\n{\n");
			buffw.write("\ttype                    = xmlpipe2\n");
			buffw.write("\txmlpipe_command         = cat " + data_source_path.toAbsolutePath().toString() + "\n");
			buffw.write("\txmlpipe_attr_string     = " + option.document_key_name + "\n");
			buffw.write("\txmlpipe_field           = " + PgSchemaUtil.simple_content_name + "\n");

			tables.forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> {

				try {

					String attr_name = table.name + PgSchemaUtil.sph_member_op + field.name;

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
				}

			}));

			buffw.write("}\n\n");

			buffw.close();

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

		tables.parallelStream().forEach(table -> table.buffw = table.required ? buffw : null);

		// parse root node and write to Sphinx xmlpipe2 file

		try {

			buffw.write("<?xml version=\"" + PgSchemaUtil.def_xml_version + "\" encoding=\"" + PgSchemaUtil.def_encoding + "\"?>\n");
			buffw.write("<sphinx:document id=\"" + getHashKeyString(document_id) + "\" xmlns:sphinx=\"" + PgSchemaUtil.sph_namespace_uri + "\">\n");
			buffw.write("<" + option.document_key_name + ">" + StringEscapeUtils.escapeXml10(document_id) + "</" + option.document_key_name + ">\n");

			PgSchemaNode2SphDs node2sphds = new PgSchemaNode2SphDs(this, null, root_table);

			node2sphds.parseRootNode(node);

			node2sphds.invokeRootNestedNode();

			node2sphds.clear();

			buffw.write("</sphinx:document>\n");

		} catch (TransformerException | IOException | SAXException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close xml2SphDs.
	 */
	public void closeXml2SphDs() {

		closeTableLock();

		tables.parallelStream().filter(table -> table.buffw != null).forEach(table -> table.buffw = null);

	}

	/**
	 * Parse current node and store to Sphinx xmlpipe2 file.
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param nested_key nested_key
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2SphDs(final Node parent_node, final PgTable parent_table, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		PgTable table = nested_key.table;

		PgSchemaNode2SphDs node2sphds = null;

		try {

			node2sphds = new PgSchemaNode2SphDs(this, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, nested_key);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				synchronized (table_lock[0]) {
					node2sphds.parseChildNode(node_test);
				}

				node2sphds.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2sphds.visited)
				return;

			synchronized (table_lock[0]) {
				node2sphds.parseChildNode(parent_node, nested_key);
			}

			node2sphds.invokeChildNestedNode();

		} catch (IOException | TransformerException | SAXException e) {
			throw new PgSchemaException(e);
		} finally {
			node2sphds.clear();
		}

	}

	/**
	 * Return set of Sphinx attributes.
	 *
	 * @return HashSet set of Sphinx attributes
	 */
	public HashSet<String> getSphAttrs() {

		HashSet<String> sph_attrs = new HashSet<String>();

		tables.stream().forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> sph_attrs.add(table.name + PgSchemaUtil.sph_member_op + field.name)));

		return sph_attrs;
	}

	/**
	 * Return set of Sphinx multi-valued attributes.
	 *
	 * @return HashSet set of Sphinx multi-valued attributes
	 */
	public HashSet<String> getSphMVAs() {

		HashSet<String> sph_mvas = new HashSet<String>();

		tables.stream().forEach(table -> table.fields.stream().filter(field -> field.sph_mva).forEach(field -> sph_mvas.add(table.name + PgSchemaUtil.sph_member_op + field.name)));

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

		if (option.type.equals(JsonType.object) && tables.parallelStream().anyMatch(table -> !table.virtual && table.list_holder)) {

			option.type = JsonType.column;
			option.array_all = false;

		}

		if (option.array_all && option.type.equals(JsonType.object))
			option.array_all = false;

		jsonb = new JsonBuilder(option);

		tables.parallelStream().filter(table -> table.required && table.content_holder).forEach(table -> {

			table.jsonb_not_empty = false;

			table.fields.forEach(field -> {

				field.jsonb_not_empty = false;

				if (field.jsonb == null)
					field.jsonb = new StringBuilder();

				else
					field.jsonb.setLength(0);

				field.jsonb_col_size = field.jsonb_null_size = 0;

			});

		});

	}

	/**
	 * Clear JSON builder.
	 */
	private void clearJsonBuilder() {

		jsonb.clear(true);

		tables.parallelStream().filter(table -> table.required && table.content_holder).forEach(table -> {

			table.jsonb_not_empty = false;

			table.fields.forEach(field -> {

				field.jsonb_not_empty = false;

				if (field.jsonb == null)
					field.jsonb = new StringBuilder();

				else
					field.jsonb.setLength(0);

				field.jsonb_col_size = field.jsonb_null_size = 0;

			});

		});

	}

	/**
	 * Write JSON buffer
	 *
	 * @param bout buffered output stream
	 * @throws PgSchemaException the pg schema exception
	 */
	public void writeJsonBuilder(BufferedOutputStream bout) throws PgSchemaException {

		jsonb.write(bout);

	}

	// Object-oriented JSON conversion

	/**
	 * Realize Object-oriented JSON Schema.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void realizeObjJsonSchema() throws PgSchemaException {

		hasRootTable();

		jsonb.writeStartDocument(true);
		jsonb.writeStartSchema(def_namespaces, def_anno_appinfo, def_anno_doc);

		int json_indent_level = 2;

		if (!root_table.virtual) {

			jsonb.writeStartSchemaTable(root_table, json_indent_level);
			json_indent_level += 2;

		}

		int _json_indent_level = json_indent_level;

		List<PgField> fields = root_table.fields;

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaField(field, false, true, false, _json_indent_level));

		int[] list_id = { 0 };

		fields.stream().filter(field -> field.nested_key).forEach(field -> realizeObjJsonSchema(root_table, getForeignTable(field), field.nested_key_as_attr, list_id[0]++, root_table.nested_fields, _json_indent_level));

		if (!root_table.virtual)
			jsonb.writeEndSchemaTable(root_table, false);

		jsonb.writeEndSchema();
		jsonb.writeEndDocument();

		jsonb.print();

	}

	/**
	 * Realize Object-oriented JSON Schema (child).
	 *
	 * @param parent_table parent table
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param list_id the list id
	 * @param list_size the list size
	 * @param json_indent_level current indent level
	 */
	private void realizeObjJsonSchema(final PgTable parent_table, final PgTable table, final boolean as_attr, final int list_id, final int list_size, int json_indent_level) {

		if (!table.virtual) {

			jsonb.writeStartSchemaTable(table, json_indent_level);
			json_indent_level += 2;

		}

		int _json_indent_level = json_indent_level;

		List<PgField> fields = table.fields;

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaField(field, as_attr, true, false, _json_indent_level));

		if (table.nested_fields > 0) {

			int[] _list_id = { 0 };

			fields.stream().filter(field -> field.nested_key).forEach(field -> realizeObjJsonSchema(table, getForeignTable(field), field.nested_key_as_attr, _list_id[0]++, table.nested_fields, _json_indent_level));

		}

		if (!table.virtual)
			jsonb.writeEndSchemaTable(table, as_attr);

	}

	/**
	 * Object-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file_path JSON file path
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2ObjJson(XmlParser xml_parser, Path json_file_path) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(true);

		clearJsonBuilder();

		jsonb.writeStartDocument(true);

		// parse root node and store to JSON buffer

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

			node2json.parseRootNode(node);

			if (node2json.filled) {

				jsonb.writeStartTable(root_table, true, 1);
				jsonb.writeFields(root_table, false, 2);

				node2json.invokeRootNestedNodeObj(1);

				node2json.clear();

				jsonb.writeEndTable();

			}

		} catch (TransformerException | IOException | SAXException e) {
			throw new PgSchemaException(e);
		}

		jsonb.writeEndDocument();

		try {

			BufferedWriter buffw = Files.newBufferedWriter(json_file_path);

			jsonb.write(buffw);

			buffw.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close xml2Json.
	 */
	public void closeXml2Json() {

		closeTableLock();
		clearJsonBuilder();

	}

	/**
	 * Parse current node and store to JSON buffer (Object-oriented JSON format).
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param nested_key nested key
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2ObjJson(final Node parent_node, final PgTable parent_table, final PgSchemaNestedKey nested_key, int json_indent_level) throws PgSchemaException {

		PgTable table = nested_key.table;

		PgSchemaNode2Json node2json = null;

		try {

			node2json = new PgSchemaNode2Json(this, parent_table, table);

			if (!table.virtual && table.bridge) {
				jsonb.writeStartTable(table, true, ++json_indent_level);
				++json_indent_level;
			}

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, nested_key);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				synchronized (table_lock[0]) {

					node2json.parseChildNode(node_test);

					if (node2json.written) {

						if (!table.virtual) {

							jsonb.writeStartTable(table, true, json_indent_level);
							jsonb.writeFields(table, nested_key.as_attr, json_indent_level + 1);
							jsonb.writeEndTable();

						}

						else
							jsonb.writeFields(table, nested_key.as_attr, json_indent_level + 1);

					}

				}

				if (node_test.isLastNode()) {

					if (node2json.filled)
						node2json.invokeChildNestedNodeObj(node_test, json_indent_level);

					break;
				}

			}

			try {

				if (node2json.visited)
					return;

				synchronized (table_lock[0]) {

					node2json.parseChildNode(parent_node, nested_key);

					if (node2json.written) {

						if (!table.virtual)
							jsonb.writeStartTable(table, !parent_table.bridge, json_indent_level);

						jsonb.writeFields(table, nested_key.as_attr, json_indent_level + 1);

					}

				}

				if (node2json.filled) {

					node2json.invokeChildNestedNodeObj(json_indent_level);

					if (!table.virtual)
						jsonb.writeEndTable();

				}

			} finally {

				if (!table.virtual && table.bridge)
					jsonb.writeEndTable();

			}

		} catch (IOException | TransformerException | SAXException e) {
			throw new PgSchemaException(e);
		} finally {
			node2json.clear();
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

		jsonb.writeStartDocument(true);
		jsonb.writeStartSchema(def_namespaces, def_anno_appinfo, def_anno_doc);

		int json_indent_level = 2;

		if (!root_table.virtual) {

			jsonb.writeStartSchemaTable(root_table, json_indent_level);
			json_indent_level += 2;

		}

		int _json_indent_level = json_indent_level;

		List<PgField> fields = root_table.fields;

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaField(field, false, !field.list_holder, field.list_holder, _json_indent_level));

		int[] list_id = { 0 };

		fields.stream().filter(field -> field.nested_key).forEach(field -> realizeColJsonSchema(root_table, getForeignTable(field), field.nested_key_as_attr, list_id[0]++, root_table.nested_fields, _json_indent_level));

		if (!root_table.virtual)
			jsonb.writeEndSchemaTable(root_table, false);

		jsonb.writeEndSchema();
		jsonb.writeEndDocument();

		jsonb.print();

	}

	/**
	 * Realize Column-oriented JSON Schema (child).
	 *
	 * @param parent_table parent table
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param list_id the list id
	 * @param list_size the list size
	 * @param json_indent_level current indent level
	 */
	private void realizeColJsonSchema(final PgTable parent_table, final PgTable table, final boolean as_attr, final int list_id, final int list_size, int json_indent_level) {

		boolean obj_json = table.virtual || !jsonb.array_all;

		if (!table.virtual) {

			jsonb.writeStartSchemaTable(table, json_indent_level);
			json_indent_level += 2;

		}

		int _json_indent_level = json_indent_level;

		List<PgField> fields = table.fields;

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaField(field, as_attr, obj_json && !field.list_holder, !table.virtual || field.list_holder, _json_indent_level));

		if (table.nested_fields > 0) {

			int[] _list_id = { 0 };

			fields.stream().filter(field -> field.nested_key).forEach(field -> realizeColJsonSchema(table, getForeignTable(field), field.nested_key_as_attr, _list_id[0]++, table.nested_fields, _json_indent_level));

		}

		if (!table.virtual)
			jsonb.writeEndSchemaTable(table, as_attr);

	}

	/**
	 * Column-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file_path JSON file path
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2ColJson(XmlParser xml_parser, Path json_file_path) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		initTableLock(true);

		clearJsonBuilder();

		jsonb.writeStartDocument(true);

		// parse root node and write to JSON file

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

			node2json.parseRootNode(node);

			if (node2json.filled) {

				jsonb.writeStartTable(root_table, true, 1);
				jsonb.writeFields(root_table, false, 2);

				node2json.invokeRootNestedNodeCol(1);

				node2json.clear();

				jsonb.writeEndTable();

			}

		} catch (TransformerException | IOException | SAXException e) {
			throw new PgSchemaException(e);
		}

		jsonb.writeEndDocument();

		try {

			BufferedWriter buffw = Files.newBufferedWriter(json_file_path);

			jsonb.write(buffw);

			buffw.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Parse current node and store to JSON buffer (Column-oriented JSON format).
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param nested_key nested key
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2ColJson(final Node parent_node, final PgTable parent_table, final PgSchemaNestedKey nested_key, int json_indent_level) throws PgSchemaException {

		PgTable table = nested_key.table;

		PgSchemaNode2Json node2json = null;

		try {

			boolean list_and_bridge = table.list_holder && table.bridge;

			node2json = new PgSchemaNode2Json(this, parent_table, table);

			if (!table.virtual && !list_and_bridge)
				jsonb.writeStartTable(table, true, json_indent_level);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, nested_key);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				synchronized (table_lock[0]) {
					node2json.parseChildNode(node_test);
				}

				if (!table.virtual && list_and_bridge) {

					jsonb.writeStartTable(table, true, json_indent_level);
					jsonb.writeFields(table, nested_key.as_attr, json_indent_level + (table.virtual ? 0 : 1));

					node2json.invokeChildNestedNodeCol(node_test, json_indent_level);

					jsonb.writeEndTable();

				}

				else if (node_test.isLastNode()) {

					if (node2json.filled && table.jsonb_not_empty)
						jsonb.writeFields(table, nested_key.as_attr, json_indent_level + (table.virtual ? 0 : 1));

					node2json.invokeChildNestedNodeCol(node_test, json_indent_level);

					break;
				}

			}

			try {

				if (node2json.visited)
					return;

				synchronized (table_lock[0]) {

					node2json.parseChildNode(parent_node, nested_key);

					if (node2json.written)
						jsonb.writeFields(table, nested_key.as_attr, json_indent_level + (table.virtual ? 0 : 1));

				}

				node2json.invokeChildNestedNodeCol(json_indent_level);

			} finally {

				if (!table.virtual && !list_and_bridge)
					jsonb.writeEndTable();

			}

		} catch (IOException | TransformerException | SAXException e) {
			throw new PgSchemaException(e);
		} finally {
			node2json.clear();
		}

	}

	// Relational-oriented JSON conversion

	/**
	 * Realize Relational-oriented JSON Schema.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void realizeJsonSchema() throws PgSchemaException {

		switch (jsonb.type) {
		case object:
			realizeObjJsonSchema();
			return;
		case column:
			realizeColJsonSchema();
			return;
		default:
		}

		hasRootTable();

		jsonb.writeStartDocument(true);
		jsonb.writeStartSchema(def_namespaces, def_anno_appinfo, def_anno_doc);

		tables.stream().filter(table -> table.content_holder).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> realizeJsonSchema(table, 2));

		jsonb.writeEndSchema();
		jsonb.writeEndDocument();

		jsonb.print();

		// no support on conditional attribute

		tables.stream().filter(table -> table.content_holder).forEach(table -> {

			table.fields.stream().filter(field -> field.simple_attr_cond).forEach(field -> {

				String cont_name = table.name + "." + field.name;
				String attr_name = getForeignTable(field).fields.stream().filter(foreign_field -> foreign_field.nested_key_as_attr).findFirst().get().foreign_table_xname + "/@" + field.foreign_table_xname;

				System.err.println("[WARNING] Simple content \"" + (jsonb.case_sense ? cont_name : cont_name.toLowerCase()) + "\" may be confused with attribute \"" + (jsonb.case_sense ? attr_name : attr_name.toLowerCase()) + "\" in relational-oriented JSON format.");

			});

		});

	}

	/**
	 * Realize Relational-oriented JSON Schema (child).
	 *
	 * @param table current table
	 * @param json_indent_level current indent level
	 */
	private void realizeJsonSchema(final PgTable table, int json_indent_level) {

		jsonb.writeStartSchemaTable(table, json_indent_level);

		int _json_indent_level = json_indent_level + 2;
		boolean unique_table = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child);

		List<PgField> fields = table.fields;

		fields.stream().filter(field -> field.jsonable).forEach(field -> jsonb.writeSchemaField(field, false, !jsonb.array_all, jsonb.array_all || !unique_table, _json_indent_level));

		jsonb.writeEndSchemaTable(table, false);

	}

	/**
	 * Relational-oriented JSON conversion.
	 *
	 * @param xml_parser XML document
	 * @param json_file_path JSON file path
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2Json(XmlParser xml_parser, Path json_file_path) throws PgSchemaException {

		switch (jsonb.type) {
		case object:
			xml2ObjJson(xml_parser, json_file_path);
			return;
		case column:
			xml2ColJson(xml_parser, json_file_path);
			return;
		default:
		}

		Node node = getRootNode(xml_parser);

		initTableLock(false);

		clearJsonBuilder();

		jsonb.writeStartDocument(true);

		// parse root node and write to JSON file

		try {

			PgSchemaNode2Json node2json = new PgSchemaNode2Json(this, null, root_table);

			node2json.parseRootNode(node);

			node2json.invokeRootNestedNode();

			node2json.clear();

		} catch (TransformerException | IOException | SAXException e) {
			throw new PgSchemaException(e);
		}

		tables.stream().filter(_table -> _table.jsonb_not_empty).sorted(Comparator.comparingInt(table -> table.order)).forEach(_table -> {

			jsonb.writeStartTable(_table, true, 1);
			jsonb.writeFields(_table, 2);
			jsonb.writeEndTable();

		});

		jsonb.writeEndDocument();

		try {

			BufferedWriter buffw = Files.newBufferedWriter(json_file_path);

			jsonb.write(buffw);

			buffw.close();

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

		// no support on conditional attribute

		tables.stream().filter(table -> table.content_holder).forEach(table -> {

			table.fields.stream().filter(field -> field.simple_attr_cond).forEach(field -> {

				String cont_name = table.name + "." + field.name;
				String attr_name = getForeignTable(field).fields.stream().filter(foreign_field -> foreign_field.nested_key_as_attr).findFirst().get().foreign_table_xname + "/@" + field.foreign_table_xname;

				System.err.println("[WARNING] Simple content \"" + (jsonb.case_sense ? cont_name : cont_name.toLowerCase()) + "\" may be confused with attribute \"" + (jsonb.case_sense ? attr_name : attr_name.toLowerCase()) + "\" in relational-oriented JSON format.");

			});

		});

	}

	/**
	 * Parse current node and store to JSON buffer (Relational-oriented JSON format).
	 *
	 * @param parent_node parent node
	 * @param parent_table parent table
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode2Json(final Node parent_node, final PgTable parent_table, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		int table_id = nested_key.table_id;

		PgTable table = nested_key.table;

		PgSchemaNode2Json node2json = null;

		try {

			node2json = new PgSchemaNode2Json(this, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, nested_key);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				synchronized (table_lock[table_id]) {
					node2json.parseChildNode(node_test);
				}

				node2json.invokeChildNestedNode(node_test);

				if (node_test.isLastNode())
					break;

			}

			if (node2json.visited)
				return;

			synchronized (table_lock[table_id]) {
				node2json.parseChildNode(parent_node, nested_key);
			}

			node2json.invokeChildNestedNode();

		} catch (IOException | TransformerException | SAXException e) {
			throw new PgSchemaException(e);
		} finally {
			node2json.clear();
		}

	}

	// XPath query evaluation over PostgreSQL

	// XML composer over PostgreSQL

	/** The XML builder. */
	protected XmlBuilder xmlb = null;

	/**
	 * Initialize XML builder.
	 *
	 * @param xmlb XML builder
	 */
	public void initXmlBuilder(XmlBuilder xmlb) {

		this.xmlb = xmlb;
		this.xmlb.clear();

	}

	/**
	 * Compose XML fragment (field or text node)
	 *
	 * @param xpath_comp_list current XPath component list
	 * @param path_expr current XPath expression
	 * @param rset current result set
	 * @throws PgSchemaException the pg schema exception
	 */
	public void pgSql2XmlFrag(XPathCompList xpath_comp_list, XPathExpr path_expr, ResultSet rset) throws PgSchemaException {

		XPathCompType terminus = path_expr.terminus;

		if (terminus.equals(XPathCompType.table))
			return;

		PgTable table = path_expr.sql_subject.table;

		String table_name = table.xname;
		String table_ns = table.target_namespace;
		String table_prefix = table.prefix;

		PgField field = path_expr.sql_subject.field;

		String field_name = field.getCanName();
		String field_ns = field.getTagetNamespace();
		String field_prefix = field.getPrefix();

		boolean fill_default_value = option.fill_default_value;

		XMLStreamWriter xml_writer = xmlb.writer;

		try {

			while (rset.next()) {

				String content;

				switch (terminus) {
				case element:
					content = field.retrieveValue(rset, 1, fill_default_value);

					if (content != null && !content.isEmpty()) {

						if (field.is_xs_namespace) {

							xml_writer.writeStartElement(table_prefix, field_name, table_ns);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(table_prefix, table_ns);

						}

						else {

							xml_writer.writeStartElement(field_prefix, field_name, field_ns);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(field_prefix, field_ns);

						}

						xml_writer.writeCharacters(content);

						xml_writer.writeEndElement();

						xml_writer.writeCharacters(xmlb.getLineFeedCode());

					}

					else {

						if (field.is_xs_namespace) {

							xml_writer.writeEmptyElement(table_prefix, field_name, table_ns);

							if (xmlb.append_xmlns) {
								xml_writer.writeNamespace(table_prefix, table_ns);
								xml_writer.writeNamespace(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri);
							}

						}

						else {

							xml_writer.writeEmptyElement(field_prefix, field_name, field_ns);

							if (xmlb.append_xmlns) {
								xml_writer.writeNamespace(field_prefix, field_ns);
								xml_writer.writeNamespace(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri);
							}

						}

						xml_writer.writeAttribute(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri, "nil", "true");

						xml_writer.writeCharacters(xmlb.getLineFeedCode());

					}
					break;
				case simple_content:
					content = field.retrieveValue(rset, 1, fill_default_value);

					// simple content

					if (!field.simple_attribute) {

						if (content != null && !content.isEmpty()) {

							xml_writer.writeStartElement(table_prefix, table_name, table_ns);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(table_prefix, table_ns);

							xml_writer.writeCharacters(content);

							xml_writer.writeEndElement();

							xml_writer.writeCharacters(xmlb.getLineFeedCode());

						}

					}

					// simple attribute

					else {

						PgTable parent_table = xpath_comp_list.getParentTable(path_expr);

						if (content != null && !content.isEmpty()) {

							xml_writer.writeEmptyElement(parent_table.prefix, parent_table.xname, parent_table.target_namespace);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(parent_table.prefix, parent_table.target_namespace);

							if (field.is_xs_namespace)
								xmlb.writer.writeAttribute(field.foreign_table_xname, content);
							else	
								xmlb.writer.writeAttribute(field_prefix, field_ns, field.foreign_table_xname, content);

							xml_writer.writeCharacters(xmlb.getLineFeedCode());

						}

						else if (field.required) {

							xml_writer.writeEmptyElement(parent_table.prefix, parent_table.xname, parent_table.target_namespace);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(parent_table.prefix, parent_table.target_namespace);

							if (field.is_xs_namespace)
								xmlb.writer.writeAttribute(field.foreign_table_xname, "");
							else	
								xmlb.writer.writeAttribute(field_prefix, field_ns, field.foreign_table_xname, "");

							xml_writer.writeCharacters(xmlb.getLineFeedCode());

						}

					}
					break;
				case attribute:
					content = field.retrieveValue(rset, 1, fill_default_value);

					// attribute

					if (field.attribute) {

						if (content != null && !content.isEmpty()) {

							xml_writer.writeEmptyElement(table_prefix, table_name, table_ns);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(table_prefix, table_ns);

							if (field_ns.equals(PgSchemaUtil.xs_namespace_uri))
								xml_writer.writeAttribute(field_name, content);
							else
								xml_writer.writeAttribute(field_prefix, field_ns, field_name, content);

							xml_writer.writeCharacters(xmlb.getLineFeedCode());

						}

						else if (field.required) {

							xml_writer.writeEmptyElement(table_prefix, table_name, table_ns);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(table_prefix, table_ns);

							if (field_ns.equals(PgSchemaUtil.xs_namespace_uri))
								xml_writer.writeAttribute(field_name, "");
							else
								xml_writer.writeAttribute(field_prefix, field_ns, field_name, "");

							xml_writer.writeCharacters(xmlb.getLineFeedCode());

						}

					}

					// simple attribute

					else {

						PgTable parent_table = xpath_comp_list.getParentTable(path_expr);

						if (content != null && !content.isEmpty()) {

							xml_writer.writeEmptyElement(parent_table.prefix, parent_table.xname, parent_table.target_namespace);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(parent_table.prefix, parent_table.target_namespace);

							if (field.is_xs_namespace)
								xmlb.writer.writeAttribute(field.foreign_table_xname, content);
							else	
								xmlb.writer.writeAttribute(field_prefix, field_ns, field.foreign_table_xname, content);

							xml_writer.writeCharacters(xmlb.getLineFeedCode());

						}

						else if (field.required) {

							xml_writer.writeEmptyElement(parent_table.prefix, parent_table.xname, parent_table.target_namespace);

							if (xmlb.append_xmlns)
								xml_writer.writeNamespace(parent_table.prefix, parent_table.target_namespace);


							if (field.is_xs_namespace)
								xmlb.writer.writeAttribute(field.foreign_table_xname, "");
							else	
								xmlb.writer.writeAttribute(field_prefix, field_ns, field.foreign_table_xname, "");

							xml_writer.writeCharacters(xmlb.getLineFeedCode());

						}

					}
					break;
				case any_attribute:
				case any_element:
					Array arrayed_cont = rset.getArray(1);

					String[] contents = (String[]) arrayed_cont.getArray();

					for (String _content : contents) {

						if (!_content.isEmpty()) {

							String path = path_expr.getReadablePath();

							String target_path = xmlb.getLastNameOfPath(path);

							if (terminus.equals(XPathCompType.any_attribute) || target_path.startsWith("@")) {

								xml_writer.writeEmptyElement(field.prefix, xmlb.getLastNameOfPath(xmlb.getParentPath(path)), table_ns);

								if (xmlb.append_xmlns)
									xml_writer.writeNamespace(field.prefix, field.namespace);

								String attr_name = target_path.replace("@", "");

								if (field.prefix.isEmpty() || field.namespace.isEmpty())
									xml_writer.writeAttribute(attr_name, _content);
								else
									xml_writer.writeAttribute(field.prefix, field.namespace, attr_name, _content);

							}

							else {

								xml_writer.writeStartElement(field.prefix, target_path, field.namespace);

								if (xmlb.append_xmlns)
									xml_writer.writeNamespace(field.prefix, field.namespace);

								xml_writer.writeCharacters(_content);

								xml_writer.writeEndElement();

							}

							xml_writer.writeCharacters(xmlb.getLineFeedCode());

						}

					}
					break;
				case text:
					content = rset.getString(1);

					if (content != null && !content.isEmpty()) {

						String column_name = rset.getMetaData().getColumnName(1);

						PgField _field = table.getField(column_name);

						if (_field != null)
							content = field.retrieveValue(rset, 1, fill_default_value);

						xml_writer.writeCharacters(content);
						xml_writer.writeCharacters(xmlb.getLineFeedCode());

					}
					break;
				case comment:
					content = rset.getString(1);

					if (content != null && !content.isEmpty()) {

						xml_writer.writeComment(content);
						xml_writer.writeCharacters(xmlb.getLineFeedCode());

					}
					break;
				case processing_instruction:
					content = rset.getString(1);

					if (content != null && !content.isEmpty()) {

						xml_writer.writeProcessingInstruction(content);
						xml_writer.writeCharacters(xmlb.getLineFeedCode());

					}
					break;
				default:
					continue;
				}

			}

		} catch (SQLException | XMLStreamException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Compose XML document (table node)
	 *
	 * @param db_conn database connection
	 * @param path_expr current XPath expression
	 * @param rset current result set
	 * @throws PgSchemaException the pg schema exception
	 */
	public void pgSql2Xml(Connection db_conn, XPathExpr path_expr, ResultSet rset) throws PgSchemaException {

		XPathCompType terminus = path_expr.terminus;

		if (!terminus.equals(XPathCompType.table))
			return;

		PgTable table = path_expr.sql_subject.table;

		String table_ns = table.target_namespace;
		String table_prefix = table.prefix;

		boolean fill_default_value = option.fill_default_value;

		XMLStreamWriter xml_writer = xmlb.writer;

		try {

			XmlBuilderNestTester nest_test = new XmlBuilderNestTester(table, xmlb);

			xmlb.pending_elem.push(new XmlBuilderPendingElem(table, nest_test.current_indent_space, true));

			List<PgField> fields = table.fields;

			// attribute, any_attribute

			int param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.attribute) {

					String content = field.retrieveValue(rset, param_id, fill_default_value);

					if ((content != null && !content.isEmpty()) || field.required) {

						XmlBuilderPendingAttr attr = new XmlBuilderPendingAttr(field, content);

						XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

						if (elem != null)
							elem.appendPendingAttr(attr);
						else
							attr.write(xmlb, null);

						nest_test.has_content = true;

					}

				}

				else if (field.any_attribute) {

					SQLXML xml_object = rset.getSQLXML(param_id);

					if (xml_object != null) {

						InputStream in = xml_object.getBinaryStream();

						if (in != null) {

							XmlBuilderAnyAttrRetriever any_attr = new XmlBuilderAnyAttrRetriever(table.pname, field, nest_test, xmlb);

							any_sax_parser.parse(in, any_attr);

							any_sax_parser.reset();

							in.close();

						}

						xml_object.free();

					}

				}

				else if (field.nested_key_as_attr) {

					Object key = rset.getObject(param_id);

					if (key != null)
						nest_test.merge(nestChildNode2Xml(db_conn, getTable(field.foreign_table_id), key, true, nest_test));

				}

				if (!field.omissible)
					param_id++;

			}

			// simple_content, element, any

			param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (xmlb.insert_doc_key && field.document_key) {

					if (table.equals(root_table))
						throw new PgSchemaException("Not allowed to insert document key to root element.");

					XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

					if (elem != null)
						xmlb.writePendingElems(false);

					xmlb.writePendingSimpleCont();

					xml_writer.writeCharacters((nest_test.has_child_elem ? "" : xmlb.line_feed_code) + nest_test.child_indent_space);

					if (field.is_xs_namespace)
						xml_writer.writeStartElement(table_prefix, field.xname, table_ns);
					else
						xml_writer.writeStartElement(field.prefix, field.xname, field.target_namespace);

					xml_writer.writeCharacters(rset.getString(param_id));

					xml_writer.writeEndElement();

					nest_test.has_child_elem = nest_test.has_content = nest_test.has_insert_doc_key = true;

				}

				else if (field.simple_content && !field.simple_attribute) {

					String content = field.retrieveValue(rset, param_id, fill_default_value);

					if (content != null && !content.isEmpty()) {

						XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

						if (elem != null)
							xmlb.writePendingElems(false);

						if (field.simple_primitive_list) {

							if (xmlb.pending_simple_cont.length() > 0) {

								xmlb.writePendingSimpleCont();

								xml_writer.writeEndElement();

								elem = new XmlBuilderPendingElem(table, (xmlb.pending_elem.size() > 0 ? "" : xmlb.line_feed_code) + nest_test.current_indent_space, false);

								elem.write(xmlb);

							}

						}

						xmlb.appendSimpleCont(content);

						nest_test.has_simple_content = nest_test.has_open_simple_content = true;

					}

				}

				else if (field.element) {

					String content = field.retrieveValue(rset, param_id, fill_default_value);

					if ((content != null && !content.isEmpty()) || field.required) {

						XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

						if (elem != null)
							xmlb.writePendingElems(false);

						xmlb.writePendingSimpleCont();

						xml_writer.writeCharacters((nest_test.has_child_elem ? "" : xmlb.line_feed_code) + nest_test.child_indent_space);

						if (content != null && !content.isEmpty()) {

							if (field.is_xs_namespace)
								xml_writer.writeStartElement(table_prefix, field.xname, table_ns);
							else
								xml_writer.writeStartElement(field.prefix, field.xname, field.target_namespace);

							xml_writer.writeCharacters(content);

							xml_writer.writeEndElement();

						}

						else {

							if (field.is_xs_namespace)
								xml_writer.writeEmptyElement(table_prefix, field.xname, table_ns);
							else
								xml_writer.writeEmptyElement(field.prefix, field.xname, field.target_namespace);

							xml_writer.writeAttribute(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri, "nil", "true");

						}

						xml_writer.writeCharacters(xmlb.line_feed_code);

						nest_test.has_child_elem = nest_test.has_content = true;

						if (nest_test.has_insert_doc_key)
							nest_test.has_insert_doc_key = false;

					}

				}

				else if (field.any) {

					SQLXML xml_object = rset.getSQLXML(param_id);

					if (xml_object != null) {

						InputStream in = xml_object.getBinaryStream();

						if (in != null) {

							XmlBuilderAnyRetriever any = new XmlBuilderAnyRetriever(table.pname, field, nest_test, xmlb);

							any_sax_parser.parse(in, any);

							any_sax_parser.reset();

							if (nest_test.has_insert_doc_key)
								nest_test.has_insert_doc_key = false;

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

				if (field.nested_key && !field.nested_key_as_attr) {

					Object key = rset.getObject(param_id);

					if (key != null) {

						nest_test.has_child_elem |= n++ > 0;

						nest_test.merge(nestChildNode2Xml(db_conn, getTable(field.foreign_table_id), key, false, nest_test));

					}

				}

				if (!field.omissible)
					param_id++;

			}

			if (nest_test.has_content || nest_test.has_simple_content) {

				boolean attr_only = false;

				XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

				if (elem != null)
					xmlb.writePendingElems(attr_only = true);

				xmlb.writePendingSimpleCont();

				if (!nest_test.has_open_simple_content && !attr_only)
					xml_writer.writeCharacters(nest_test.current_indent_space);
				else if (nest_test.has_simple_content)
					nest_test.has_open_simple_content = false;

				if (!attr_only)
					xml_writer.writeEndElement();

				xml_writer.writeCharacters(xmlb.line_feed_code);

			}

			else
				xmlb.pending_elem.poll();

		} catch (XMLStreamException e) {
			if (xmlb.insert_doc_key)
				System.err.println("Not allowed insert document key to element has any child element.");
			throw new PgSchemaException(e);
		} catch (SQLException | SAXException | IOException e) {
			throw new PgSchemaException(e);
		}

		xmlb.clear();

	}

	/**
	 * Close pgSql2Xml.
	 */
	public void closePgSql2Xml() {

		closePreparedStatement(true);

	}

	/**
	 * Nest node and compose XML document.
	 *
	 * @param db_conn database connection
	 * @param table current table
	 * @param parent_key parent key
	 * @param as_attr whether parent key is simple attribute
	 * @param parent_nest_test nest test result of parent node
	 * @return XmlBuilderNestTester nest test of this node
	 * @throws PgSchemaException the pg schema exception
	 */	
	private XmlBuilderNestTester nestChildNode2Xml(final Connection db_conn, final PgTable table, final Object parent_key, final boolean as_attr, XmlBuilderNestTester parent_nest_test) throws PgSchemaException {

		boolean fill_default_value = option.fill_default_value;

		XMLStreamWriter xml_writer = xmlb.writer;

		try {

			XmlBuilderNestTester nest_test = new XmlBuilderNestTester(table, parent_nest_test);

			String table_ns = table.target_namespace;
			String table_prefix = table.prefix;

			boolean no_list_and_bridge = !table.list_holder && table.bridge;

			if (!table.virtual && no_list_and_bridge && !as_attr) {

				xmlb.pending_elem.push(new XmlBuilderPendingElem(table, (parent_nest_test.has_child_elem || xmlb.pending_elem.size() > 0 ? (parent_nest_test.has_insert_doc_key ? xmlb.line_feed_code : "") : xmlb.line_feed_code) + nest_test.current_indent_space, true));

				if (parent_nest_test.has_insert_doc_key)
					parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

			}

			if (table.ps == null || table.ps.isClosed()) {

				String sql = "SELECT * FROM " + getPgNameOf(db_conn, table) + " WHERE " + PgSchemaUtil.avoidPgReservedWords(table.fields.stream().filter(field -> field.primary_key).findFirst().get().pname) + " = ?";

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

			int list_id = 0;

			while (rset.next()) {

				if (!table.virtual && !no_list_and_bridge && !as_attr) {

					xmlb.pending_elem.push(new XmlBuilderPendingElem(table, (parent_nest_test.has_child_elem || xmlb.pending_elem.size() > 0 || list_id > 0 ? (parent_nest_test.has_insert_doc_key ? xmlb.line_feed_code : "") : xmlb.line_feed_code) + nest_test.current_indent_space, true));

					if (parent_nest_test.has_insert_doc_key)
						parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

					if (!table.bridge)
						nest_test.has_child_elem = false;

				}

				// attribute, simple attribute, any_attribute

				int param_id = 1;

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.attribute) {

						String content = field.retrieveValue(rset, param_id, fill_default_value);

						if ((content != null && !content.isEmpty()) || field.required) {

							XmlBuilderPendingAttr attr = new XmlBuilderPendingAttr(field, content);

							XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

							if (elem != null)
								elem.appendPendingAttr(attr);
							else
								attr.write(xmlb, null);

							nest_test.has_content = true;

						}

					}

					else if ((field.simple_attribute || field.simple_attr_cond) && as_attr) {

						String content = field.retrieveValue(rset, param_id, fill_default_value);

						if ((content != null && !content.isEmpty()) || field.required) {

							XmlBuilderPendingAttr attr = new XmlBuilderPendingAttr(field, getForeignTable(field), content);

							XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

							if (elem != null)
								elem.appendPendingAttr(attr);
							else
								attr.write(xmlb, null);

							nest_test.has_content = true;

						}

					}

					else if (field.any_attribute) {

						SQLXML xml_object = rset.getSQLXML(param_id);

						if (xml_object != null) {

							InputStream in = xml_object.getBinaryStream();

							if (in != null) {

								XmlBuilderAnyAttrRetriever any_attr = new XmlBuilderAnyAttrRetriever(table.pname, field, nest_test, xmlb);

								any_sax_parser.parse(in, any_attr);

								any_sax_parser.reset();

								in.close();

							}

							xml_object.free();

						}

					}

					else if (field.nested_key_as_attr) {

						Object key = rset.getObject(param_id);

						if (key != null)
							nest_test.merge(nestChildNode2Xml(db_conn, getTable(field.foreign_table_id), key, true, nest_test));

					}

					if (!field.omissible)
						param_id++;

				}

				// simple_content, element, any

				param_id = 1;

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.simple_content && !field.simple_attribute && !as_attr) {

						String content = field.retrieveValue(rset, param_id, fill_default_value);

						if (content != null && !content.isEmpty()) {

							XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

							if (elem != null)
								xmlb.writePendingElems(false);

							if (field.simple_primitive_list) {

								if (xmlb.pending_simple_cont.length() > 0) {

									xmlb.writePendingSimpleCont();

									xml_writer.writeEndElement();

									elem = new XmlBuilderPendingElem(table, (parent_nest_test.has_child_elem || xmlb.pending_elem.size() > 0 || list_id > 0 ? "" : xmlb.line_feed_code) + nest_test.current_indent_space, false);

									elem.write(xmlb);

								}

							}

							xmlb.appendSimpleCont(content);

							nest_test.has_simple_content = nest_test.has_open_simple_content = true;

						}

					}

					else if (field.element) {

						String content = field.retrieveValue(rset, param_id, fill_default_value);

						if ((content != null && !content.isEmpty()) || field.required) {

							XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

							if (elem != null)
								xmlb.writePendingElems(false);

							xmlb.writePendingSimpleCont();

							xml_writer.writeCharacters((nest_test.has_child_elem ? "" : xmlb.line_feed_code) + nest_test.child_indent_space);

							if (content != null && !content.isEmpty()) {

								if (field.is_xs_namespace)
									xml_writer.writeStartElement(table_prefix, field.xname, table_ns);
								else
									xml_writer.writeStartElement(field.prefix, field.xname, field.target_namespace);

								xml_writer.writeCharacters(content);

								xml_writer.writeEndElement();

							}

							else {

								if (field.is_xs_namespace)
									xml_writer.writeEmptyElement(table_prefix, field.xname, table_ns);
								else
									xml_writer.writeEmptyElement(field.prefix, field.xname, field.target_namespace);

								xml_writer.writeAttribute(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri, "nil", "true");

							}

							xml_writer.writeCharacters(xmlb.line_feed_code);

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

								XmlBuilderAnyRetriever any = new XmlBuilderAnyRetriever(table.pname, field, nest_test, xmlb);

								any_sax_parser.parse(in, any);

								any_sax_parser.reset();

								if (nest_test.has_insert_doc_key)
									parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

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

					if (field.nested_key && !field.nested_key_as_attr) {

						Object key = rset.getObject(param_id);

						if (key != null) {

							nest_test.has_child_elem |= n++ > 0;

							nest_test.merge(nestChildNode2Xml(db_conn, getTable(field.foreign_table_id), key, false, nest_test));

						}

					}

					if (!field.omissible)
						param_id++;

				}

				if (!table.virtual && !no_list_and_bridge && !as_attr) {

					if (nest_test.has_content || nest_test.has_simple_content) {

						boolean attr_only = false;

						XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

						if (elem != null)
							xmlb.writePendingElems(attr_only = true);

						xmlb.writePendingSimpleCont();

						if (!nest_test.has_open_simple_content && !attr_only)
							xml_writer.writeCharacters(nest_test.current_indent_space);
						else if (nest_test.has_simple_content)
							nest_test.has_open_simple_content = false;

						if (!attr_only)
							xml_writer.writeEndElement();

						xml_writer.writeCharacters(xmlb.line_feed_code);

					}

					else
						xmlb.pending_elem.poll();

					list_id++;

				}

			}

			rset.close();

			if (!table.virtual && no_list_and_bridge && !as_attr) {

				if (nest_test.has_content || nest_test.has_simple_content) {

					boolean attr_only = false;

					XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

					if (elem != null)
						xmlb.writePendingElems(attr_only = true);

					xmlb.writePendingSimpleCont();

					if (!nest_test.has_open_simple_content && !attr_only)
						xml_writer.writeCharacters(nest_test.current_indent_space);
					else if (nest_test.has_simple_content)
						nest_test.has_open_simple_content = false;

					if (!attr_only)
						xml_writer.writeEndElement();

					xml_writer.writeCharacters(xmlb.line_feed_code);

				}

				else
					xmlb.pending_elem.poll();

			}

			return nest_test;

		} catch (SQLException | XMLStreamException | SAXException | IOException e) {
			throw new PgSchemaException(e);
		}

	}

	// JSON composer over PostgreSQL

	/**
	 * Compose JSON fragment (field or text node)
	 *
	 * @param xpath_comp_list current XPath component list
	 * @param path_expr current XPath expression
	 * @param rset current result set
	 * @throws PgSchemaException the pg schema exception
	 */
	public void pgSql2JsonFrag(XPathCompList xpath_comp_list, XPathExpr path_expr, ResultSet rset) throws PgSchemaException {

		XPathCompType terminus = path_expr.terminus;

		if (terminus.equals(XPathCompType.table))
			return;

		jsonb.writeStartDocument(false);

		PgTable table = path_expr.sql_subject.table;

		PgField field = path_expr.sql_subject.field;

		boolean fill_default_value = option.fill_default_value;

		boolean as_attr = false;

		try {

			while (rset.next()) {

				String content;

				switch (terminus) {
				case element:
					content = field.retrieveValue(rset, 1, fill_default_value);
					jsonb.writeFieldFrag(field, as_attr, content);
					break;
				case simple_content:
					content = field.retrieveValue(rset, 1, fill_default_value);

					// simple content

					if (!field.simple_attribute) {

						if (content != null && !content.isEmpty())
							jsonb.writeFieldFrag(field, as_attr, content);

					}

					// simple attribute

					else if ((content != null && !content.isEmpty()) || field.required)
						jsonb.writeFieldFrag(field, as_attr = true, content);
					break;
				case attribute:
					content = field.retrieveValue(rset, 1, fill_default_value);

					// attribute

					if (field.attribute) {

						if ((content != null && !content.isEmpty()) || field.required)
							jsonb.writeFieldFrag(field, as_attr, content);

					}

					// simple attribute

					else if ((content != null && !content.isEmpty()) || field.required)
						jsonb.writeFieldFrag(field, as_attr = true, content);
					break;
				case any_attribute:
				case any_element:
					Array arrayed_cont = rset.getArray(1);

					String[] contents = (String[]) arrayed_cont.getArray();

					for (String _content : contents) {

						if (!_content.isEmpty()) {

							String path = path_expr.getReadablePath();

							String target_path = jsonb.getLastNameOfPath(path);

							if (terminus.equals(XPathCompType.any_attribute) || target_path.startsWith("@"))
								jsonb.writeAnyFieldFrag(field, target_path.replace("@", ""), true, _content, 1);

							else
								jsonb.writeAnyFieldFrag(field, target_path, false, _content, 1);

						}

					}
					break;
				case text:
					content = rset.getString(1);

					if (content != null && !content.isEmpty()) {

						String column_name = rset.getMetaData().getColumnName(1);

						PgField _field = table.getField(column_name);

						if (_field != null)
							content = field.retrieveValue(rset, 1, fill_default_value);

						jsonb.buffer.append(content + "," + jsonb.line_feed_code);

					}
					break;
				case comment:
				case processing_instruction:
					content = rset.getString(1);

					if (content != null && !content.isEmpty())
						jsonb.buffer.append(content + "," + jsonb.line_feed_code);
					break;
				default:
					continue;
				}

			}

			if (jsonb.array_all && terminus.isField()) {

				switch (terminus) {
				case any_attribute:
				case any_element:
					jsonb.writeAnyFieldFrag(field, path_expr.getReadablePath());
					break;
				default:
					jsonb.writeFieldFrag(field, as_attr);
				}

			}

			jsonb.writeEndDocument();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Compose JSON document (table node)
	 *
	 * @param db_conn database connection
	 * @param path_expr current XPath expression
	 * @param rset current result set
	 * @throws PgSchemaException the pg schema exception
	 */
	public void pgSql2Json(Connection db_conn, XPathExpr path_expr, ResultSet rset) throws PgSchemaException {

		XPathCompType terminus = path_expr.terminus;

		if (!terminus.equals(XPathCompType.table))
			return;

		jsonb.writeStartDocument(false);

		PgTable table = path_expr.sql_subject.table;

		boolean fill_default_value = option.fill_default_value;

		try {

			JsonBuilderNestTester nest_test = new JsonBuilderNestTester(table, jsonb);

			jsonb.pending_elem.push(new JsonBuilderPendingElem(table, nest_test.current_indent_level));

			List<PgField> fields = table.fields;

			// attribute, any_attribute

			int param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.attribute) {

					String content = field.retrieveValue(rset, param_id, fill_default_value);

					if ((content != null && !content.isEmpty()) || field.required) {

						JsonBuilderPendingAttr attr = new JsonBuilderPendingAttr(field, content, nest_test.child_indent_level);

						JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

						if (elem != null)
							elem.appendPendingAttr(attr);
						else
							attr.write(jsonb);

						nest_test.has_content = true;

					}

				}

				else if (field.any_attribute) {

					SQLXML xml_object = rset.getSQLXML(param_id);

					if (xml_object != null) {

						InputStream in = xml_object.getBinaryStream();

						if (in != null) {

							JsonBuilderAnyAttrRetriever any_attr = new JsonBuilderAnyAttrRetriever(table.pname, field, nest_test, false, jsonb);

							any_sax_parser.parse(in, any_attr);

							any_sax_parser.reset();

							in.close();

						}

						xml_object.free();

					}

				}

				else if (field.nested_key_as_attr) {

					Object key = rset.getObject(param_id);

					if (key != null)
						nest_test.merge(nestChildNode2Json(db_conn, getTable(field.foreign_table_id), key, true, nest_test));

				}

				if (!field.omissible)
					param_id++;

			}

			// simple_content, element, any

			param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (jsonb.insert_doc_key && field.document_key) {

					if (table.equals(root_table))
						throw new PgSchemaException("Not allowed to insert document key to root element.");

					JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

					if (elem != null)
						jsonb.writePendingElems(false);

					jsonb.writePendingSimpleCont();

					jsonb.writeField(table, field, false, rset.getString(param_id), nest_test.child_indent_level);

					nest_test.has_child_elem = nest_test.has_content = nest_test.has_insert_doc_key = true;

				}

				else if (field.simple_content && !field.simple_attribute) {

					String content = field.retrieveValue(rset, param_id, fill_default_value);

					if (content != null && !content.isEmpty()) {

						JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

						if (elem != null)
							jsonb.writePendingElems(false);

						jsonb.writePendingSimpleCont();

						jsonb.writeField(table, field, false, content, nest_test.child_indent_level);

						nest_test.has_simple_content = nest_test.has_open_simple_content = true;

					}

				}

				else if (field.element) {

					String content = field.retrieveValue(rset, param_id, fill_default_value);

					if ((content != null && !content.isEmpty()) || field.required) {

						JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

						if (elem != null)
							jsonb.writePendingElems(false);

						jsonb.writePendingSimpleCont();

						jsonb.writeField(table, field, false, content, nest_test.child_indent_level);

						nest_test.has_child_elem = nest_test.has_content = true;

						if (nest_test.has_insert_doc_key)
							nest_test.has_insert_doc_key = false;

					}

				}

				else if (field.any) {

					SQLXML xml_object = rset.getSQLXML(param_id);

					if (xml_object != null) {

						InputStream in = xml_object.getBinaryStream();

						if (in != null) {

							JsonBuilderAnyRetriever any = new JsonBuilderAnyRetriever(table.pname, field, nest_test, false, jsonb);

							any_sax_parser.parse(in, any);

							any_sax_parser.reset();

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

				if (field.nested_key && !field.nested_key_as_attr) {

					Object key = rset.getObject(param_id);

					if (key != null) {

						nest_test.has_child_elem |= n++ > 0;

						nest_test.merge(nestChildNode2Json(db_conn, getTable(field.foreign_table_id), key, false, nest_test));

					}

				}

				if (!field.omissible)
					param_id++;

			}

			if (nest_test.has_content || nest_test.has_simple_content) {

				boolean attr_only = false;

				JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

				if (elem != null)
					jsonb.writePendingElems(attr_only = true);

				jsonb.writePendingSimpleCont();

				if (!nest_test.has_open_simple_content && !attr_only) { }
				else if (nest_test.has_simple_content)
					nest_test.has_open_simple_content = false;

				jsonb.writeEndTable();

			}

			else
				jsonb.pending_elem.poll();

		} catch (SQLException | SAXException | IOException e) {
			throw new PgSchemaException(e);
		}

		jsonb.writeEndDocument();

		jsonb.clear(false);

	}

	/**
	 * Close pgSql2Json.
	 */
	public void closePgSql2Json() {

		closePreparedStatement(true);
		clearJsonBuilder();

	}

	/**
	 * Nest node and compose JSON document.
	 *
	 * @param db_conn database connection
	 * @param table current table
	 * @param parent_key parent key
	 * @param as_attr whether parent key is simple attribute
	 * @param parent_nest_test nest test result of parent node
	 * @return JsonBuilderNestTester nest test of this node
	 * @throws PgSchemaException the pg schema exception
	 */	
	private JsonBuilderNestTester nestChildNode2Json(final Connection db_conn, final PgTable table, final Object parent_key, final boolean as_attr, JsonBuilderNestTester parent_nest_test) throws PgSchemaException {

		boolean fill_default_value = option.fill_default_value;

		try {

			JsonBuilderNestTester nest_test = new JsonBuilderNestTester(table, parent_nest_test);

			boolean no_list_and_bridge = !table.list_holder && table.bridge;
			boolean array_field = !table.virtual && !no_list_and_bridge && table.nested_fields == 0 && !as_attr && jsonb.type.equals(JsonType.column);

			if (!table.virtual && (no_list_and_bridge || array_field) && !as_attr) {

				jsonb.pending_elem.push(new JsonBuilderPendingElem(table, nest_test.current_indent_level));

				if (parent_nest_test.has_insert_doc_key)
					parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

			}

			if (table.ps == null || table.ps.isClosed()) {

				String sql = "SELECT * FROM " + getPgNameOf(db_conn, table) + " WHERE " + PgSchemaUtil.avoidPgReservedWords(table.fields.stream().filter(field -> field.primary_key).findFirst().get().pname) + " = ?";

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

				if (!table.virtual && !(no_list_and_bridge || array_field) && !as_attr) {

					jsonb.pending_elem.push(new JsonBuilderPendingElem(table, nest_test.current_indent_level));

					if (parent_nest_test.has_insert_doc_key)
						parent_nest_test.has_insert_doc_key = nest_test.has_insert_doc_key = false;

					if (!table.bridge)
						nest_test.has_child_elem = false;

				}

				// attribute, simple attribute, any_attribute

				int param_id = 1;

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.attribute) {

						String content = field.retrieveValue(rset, param_id, fill_default_value);

						if ((content != null && !content.isEmpty()) || field.required) {

							if (array_field)
								field.writeValue2JsonBuf(jsonb.schema_ver, content, false, jsonb.key_value_space);

							else {

								JsonBuilderPendingAttr attr = new JsonBuilderPendingAttr(field, content, nest_test.child_indent_level);

								JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

								if (elem != null)
									elem.appendPendingAttr(attr);
								else
									attr.write(jsonb);

							}

							nest_test.has_content = true;

						}

					}

					else if ((field.simple_attribute || field.simple_attr_cond) && as_attr) {

						String content = field.retrieveValue(rset, param_id, fill_default_value);

						if ((content != null && !content.isEmpty()) || field.required) {

							if (array_field)
								field.writeValue2JsonBuf(jsonb.schema_ver, content, false, jsonb.key_value_space);

							else {

								JsonBuilderPendingAttr attr = new JsonBuilderPendingAttr(field, getForeignTable(field), content, nest_test.child_indent_level);

								JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

								if (elem != null)
									elem.appendPendingAttr(attr);
								else
									attr.write(jsonb);

							}

							nest_test.has_content = true;

						}

					}

					else if (field.any_attribute) {

						SQLXML xml_object = rset.getSQLXML(param_id);

						if (xml_object != null) {

							InputStream in = xml_object.getBinaryStream();

							if (in != null) {

								JsonBuilderAnyAttrRetriever any_attr = new JsonBuilderAnyAttrRetriever(table.pname, field, nest_test, array_field, jsonb);

								any_sax_parser.parse(in, any_attr);

								any_sax_parser.reset();

								in.close();

							}

							xml_object.free();

						}

					}

					else if (field.nested_key_as_attr) {

						Object key = rset.getObject(param_id);

						if (key != null)
							nest_test.merge(nestChildNode2Json(db_conn, getTable(field.foreign_table_id), key, true, nest_test));

					}

					if (!field.omissible)
						param_id++;

				}

				// simple_content, element, any

				param_id = 1;

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.simple_content && !field.simple_attribute && !as_attr) {

						String content = field.retrieveValue(rset, param_id, fill_default_value);

						if (content != null && !content.isEmpty()) {

							if (array_field)
								field.writeValue2JsonBuf(jsonb.schema_ver, content, false, jsonb.key_value_space);

							else {

								JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

								if (elem != null)
									jsonb.writePendingElems(false);

								jsonb.writePendingSimpleCont();

								jsonb.writeField(table, field, false, content, nest_test.child_indent_level);

							}

							nest_test.has_simple_content = nest_test.has_open_simple_content = true;

						}

					}

					else if (field.element) {

						String content = field.retrieveValue(rset, param_id, fill_default_value);

						if ((content != null && !content.isEmpty()) || field.required) {

							if (array_field)
								field.writeValue2JsonBuf(jsonb.schema_ver, content, false, jsonb.key_value_space);

							else {

								JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

								if (elem != null)
									jsonb.writePendingElems(false);

								jsonb.writePendingSimpleCont();

								jsonb.writeField(table, field, false, content, nest_test.child_indent_level);

							}

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

								JsonBuilderAnyRetriever any = new JsonBuilderAnyRetriever(table.pname, field, nest_test, array_field, jsonb);

								any_sax_parser.parse(in, any);

								any_sax_parser.reset();

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

					if (field.nested_key && !field.nested_key_as_attr) {

						Object key = rset.getObject(param_id);

						if (key != null) {

							nest_test.has_child_elem |= n++ > 0;

							nest_test.merge(nestChildNode2Json(db_conn, getTable(field.foreign_table_id), key, false, nest_test));

						}

					}

					if (!field.omissible)
						param_id++;

				}

				if (!table.virtual && !(no_list_and_bridge || array_field) && !as_attr) {

					if (nest_test.has_content || nest_test.has_simple_content) {

						boolean attr_only = false;

						JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

						if (elem != null)
							jsonb.writePendingElems(attr_only = true);

						jsonb.writePendingSimpleCont();

						if (!nest_test.has_open_simple_content && !attr_only) { }
						else if (nest_test.has_simple_content)
							nest_test.has_open_simple_content = false;

						jsonb.writeEndTable();

					}

					else
						jsonb.pending_elem.poll();

				}

			}

			rset.close();

			if (!table.virtual && (no_list_and_bridge || array_field) && !as_attr) {

				if (nest_test.has_content || nest_test.has_simple_content) {

					boolean attr_only = false;

					JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

					if (elem != null)
						jsonb.writePendingElems(attr_only = true);

					jsonb.writePendingSimpleCont();

					if (array_field)
						jsonb.writeFields(table, false, nest_test.child_indent_level);

					if (!nest_test.has_open_simple_content && !attr_only) { }
					else if (nest_test.has_simple_content)
						nest_test.has_open_simple_content = false;

					jsonb.writeEndTable();

				}

				else
					jsonb.pending_elem.poll();

			}

			return nest_test;

		} catch (SQLException | SAXException | IOException e) {
			throw new PgSchemaException(e);
		}

	}

}
