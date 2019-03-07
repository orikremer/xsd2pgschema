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
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
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
import java.util.Optional;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.nustaq.serialization.annotations.Flat;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.nodeparser.PgSchemaNodeParserBuilder;
import net.sf.xsd2pgschema.nodeparser.PgSchemaNodeParserType;
import net.sf.xsd2pgschema.option.IndexFilter;
import net.sf.xsd2pgschema.option.PgOption;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.option.XmlPostEditor;
import net.sf.xsd2pgschema.type.PgHashSize;
import net.sf.xsd2pgschema.type.XsFieldType;
import net.sf.xsd2pgschema.type.XsTableType;
import net.sf.xsd2pgschema.xmlutil.XmlParser;

/**
 * PostgreSQL data model.
 *
 * @author yokochi
 */
public class PgSchema implements Serializable {

	/** The default serial version ID. */
	private static final long serialVersionUID = 1L;

	/** The PostgreSQL data model option. */
	public PgSchemaOption option = null;

	/** The default schema location. */
	private String def_schema_location = null;

	/** The default namespace. */
	private String def_namespace = null;

	/** The list of namespace (key=prefix, value=namespace_uri). */
	private HashMap<String, String> def_namespaces = new HashMap<String, String>();

	/** The list of PostgreSQL table. */
	private List<PgTable> tables = null;

	/** The PostgreSQL root table. */
	private PgTable root_table = null;

	/** The PostgreSQL table for questing document id. */
	private PgTable doc_id_table = null;

	/** The total number of PostgreSQL named schema. */
	private int total_pg_named_schema = 1;

	/** Whether arbitrary table has any element. */
	private boolean has_any = false;

	/** Whether arbitrary table has any attribute. */
	private boolean has_any_attribute = false;

	/** The root schema (temporary). */
	@Flat
	private PgSchema root_schema = null;

	/** The root node (temporary). */
	@Flat
	private Node root_node = null;

	/** The node list of xs:key (temporary). */
	@Flat
	private NodeList key_nodes = null;

	/** The schema locations. */
	@Flat
	private HashSet<String> schema_locations = null;

	/** The PostgreSQL named schema. */
	@Flat
	private HashSet<String> pg_named_schema = null;

	/** The unique schema locations (value) with its target namespace (key). */
	@Flat
	private HashMap<String, String> unq_schema_locations = null;

	/** The duplicated schema locations (key=duplicated schema location, value=unique schema location). */
	@Flat
	private HashMap<String, String> dup_schema_locations = null;

	/** The dictionary of table name. */
	@Flat
	private HashMap<String, PgTable> table_name_dic = null;

	/** The dictionary of matched table path. */
	@Flat
	private HashMap<String, PgTable> table_path_dic = null;

	/** The attribute group definitions. */
	@Flat
	private List<PgTable> attr_groups = null;

	/** The model group definitions. */
	@Flat
	private List<PgTable> model_groups = null;

	/** The list of unique constraint corresponding to xs:unique. */
	@Flat
	private List<PgKey> unq_keys = null;

	/** The list of identification constraint corresponding to xs:key. */
	@Flat
	private List<PgKey> keys = null;

	/** The list of PostgreSQL foreign key corresponding to xs:keyref. */
	@Flat
	private List<PgForeignKey> foreign_keys = null;

	/** The pending list of attribute groups. */
	@Flat
	private List<PgPendingGroup> pending_attr_groups = null;

	/** The pending list of model groups. */
	@Flat
	private List<PgPendingGroup> pending_model_groups = null;

	/** The parent of default schema location. */
	@Flat
	private String def_schema_parent = null;

	/** The top level xs:annotation. */
	@Flat
	private String def_anno = null;

	/** The top level xs:annotation/xs:appinfo. */
	@Flat
	private String def_anno_appinfo = null;

	/** The top level xs:annotation/xs:documentation. */
	@Flat
	private String def_anno_doc = null;

	/** The top level xs:annotation/xs:documentation (as is). */
	@Flat
	private String def_xanno_doc = null;

	/** The default attributes. */
	@Flat
	private String def_attrs = null;

	/** The statistics message on schema. */
	@Flat
	private StringBuilder def_stat_msg = null;

	/** The current depth of table (internal use only). */
	@Flat
	private int level;

	/**
	 * Instance of PostgreSQL data model.
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

		boolean root = parent_schema == null;
		root_schema = root ? this : parent_schema;

		// detect entry point of XML schema

		def_schema_parent = root ? PgSchemaUtil.getSchemaParent(def_schema_location) : root_schema.def_schema_parent;

		// prepare dictionary of unique schema locations and duplicated schema locations

		if (root) {

			unq_schema_locations = new HashMap<String, String>();
			dup_schema_locations = new HashMap<String, String>();

		}

		// extract default namespace and default attributes

		NamedNodeMap root_attrs = root_node.getAttributes();

		Node root_attr;
		String node_name, target_namespace;

		for (int i = 0; i < root_attrs.getLength(); i++) {

			root_attr = root_attrs.item(i);

			if (root_attr != null) {

				node_name = root_attr.getNodeName();

				if (node_name.equals("targetNamespace")) {

					target_namespace = root_attr.getNodeValue().split(" ")[0];

					root_schema.unq_schema_locations.putIfAbsent(target_namespace, def_schema_location);

					def_namespaces.putIfAbsent("", target_namespace);

				}

				else if (node_name.startsWith("xmlns")) {

					target_namespace = root_attr.getNodeValue().split(" ")[0];

					if (!target_namespace.equals(PgSchemaUtil.xsi_namespace_uri))
						def_namespaces.putIfAbsent(node_name.replaceFirst("^xmlns:?", ""), target_namespace);
					else
						def_namespaces.putIfAbsent(PgSchemaUtil.xsi_prefix, target_namespace);

				}

				else if (node_name.equals("defaultAttributes"))
					def_attrs = root_attr.getNodeValue();

			}

		}

		// set default namespace

		def_namespace = def_namespaces.get("");

		// retrieve top level schema annotation

		def_anno = PgSchemaUtil.extractAnnotation(root_node, true);
		def_anno_appinfo = PgSchemaUtil.extractAppinfo(root_node);

		if ((def_anno_doc = PgSchemaUtil.extractDocumentation(root_node, true)) != null)
			def_xanno_doc = PgSchemaUtil.extractDocumentation(root_node, false);

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

		if (root) {

			unq_keys = new ArrayList<PgKey>();

			keys = new ArrayList<PgKey>();
			foreign_keys = new ArrayList<PgForeignKey>();

			pending_attr_groups = new ArrayList<PgPendingGroup>();
			pending_model_groups = new ArrayList<PgPendingGroup>();

		}

		tables = new ArrayList<PgTable>();

		// include or import namespace

		Element child_elem;
		String child_name, schema_location;

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_elem = (Element) child;

			child_name = child_elem.getLocalName();

			if (!child_name.equals("include") && !child_name.equals("import"))
				continue;

			// reset prefix of XSD because import or include process may override

			option.setPrefixOfXmlSchema(doc, def_schema_location);

			schema_location = child_elem.getAttribute("schemaLocation");

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

					if ((schema2.tables == null || schema2.tables.size() == 0) && (schema2.attr_groups == null || schema2.attr_groups.size() == 0) && (schema2.model_groups == null || schema2.model_groups.size() == 0))
						continue;

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

		// reset prefix of XSD because import or include process may override

		option.setPrefixOfXmlSchema(doc, def_schema_location);

		// extract attribute group elements

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			if (((Element) child).getLocalName().equals("attributeGroup"))
				extractAdminAttributeGroup(child);

		}

		// extract model group elements

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			if (((Element) child).getLocalName().equals("group"))
				extractAdminModelGroup(child);

		}

		// extract identity constraint restraints

		key_nodes = doc.getElementsByTagNameNS(PgSchemaUtil.xs_namespace_uri, "key");

		if (key_nodes != null) {

			Node key_node;
			String name;

			for (int i = 0; i < key_nodes.getLength(); i++) {

				key_node = key_nodes.item(i);

				name = ((Element) key_node).getAttribute("name");

				if (name == null || name.isEmpty())
					continue;

				PgKey key = new PgKey(getPgSchemaOf(def_namespace), key_node, name, option.case_sense);

				if (key.isEmpty())
					continue;

				if (root_schema.keys.stream().anyMatch(_key -> _key.equals(key)))
					continue;

				root_schema.keys.add(key);

			}

		}

		NodeList unq_nodes = doc.getElementsByTagNameNS(PgSchemaUtil.xs_namespace_uri, "unique");

		if (unq_nodes != null) {

			Node unq_node;
			String name;

			for (int i = 0; i < unq_nodes.getLength(); i++) {

				unq_node = unq_nodes.item(i);

				name = ((Element) unq_node).getAttribute("name");

				if (name == null || name.isEmpty())
					continue;

				PgKey key = new PgKey(getPgSchemaOf(def_namespace), unq_node, name, option.case_sense);

				if (key.isEmpty())
					continue;

				if (root_schema.unq_keys.stream().anyMatch(_key -> _key.equals(key)))
					continue;

				root_schema.unq_keys.add(key);

			}

		}

		// create table for root element

		boolean root_element = root;

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			if (((Element) child).getLocalName().equals("element")) {

				String _abstract = ((Element) child).getAttribute("abstract");

				if (_abstract != null && _abstract.equals("true"))
					continue;

				// test whether element is referred by sibling nodes

				if (root_element && isReferredBySibling(child))
					continue;

				extractRootElement(child, root_element);

				if (root)
					break;

				root_element = false;

			}

		}

		// create table for administrative elements

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_name = ((Element) child).getLocalName();

			if (child_name.equals("complexType"))
				extractAdminElement(child, true, false);

			else if (child_name.equals("simpleType"))
				extractAdminElement(child, false, false);

		}

		if (tables.size() == 0) {

			if (root)
				throw new PgSchemaException("Not found any root element (/" + option.xs_prefix_ + "schema/" + option.xs_prefix_ + "element) or administrative elements (/" + option.xs_prefix_ + "schema/[" + option.xs_prefix_ + "complexType | " + option.xs_prefix_ + "simpleType]) in XML Schema: " + def_schema_location);

		}

		else {

			if (!option.rel_model_ext)
				tables.parallelStream().forEach(table -> table.classify());

			if (!root && option.ddl_output && !root_schema.dup_schema_locations.containsKey(def_schema_location))
				root_schema.def_stat_msg.append("--  Found " + tables.parallelStream().filter(table -> option.rel_model_ext || !table.relational).count() + " tables (" + tables.parallelStream().map(table -> option.rel_model_ext || !table.relational ? table.fields.size() : 0).reduce((arg0, arg1) -> arg0 + arg1).get() + " fields), " + attr_groups.size() + " attr groups, " + model_groups.size() + " model groups in XML Schema: " + def_schema_location + "\n");

		}

		// append annotation of root table if possible

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_elem = (Element) child;

			if (child_elem.getLocalName().equals("element")) {

				String _abstract = child_elem.getAttribute("abstract");

				if (_abstract != null && _abstract.equals("true"))
					continue;

				extractAdminElement(child, false, true);

				if (root)
					break;

			}

		}

		// append annotation of administrative tables if possible

		for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_name = ((Element) child).getLocalName();

			if (child_name.equals("complexType"))
				extractAdminElement(child, true, true);

			else if (child_name.equals("simpleType"))
				extractAdminElement(child, false, true);

		}

		if (!root)
			return;

		// collect PostgreSQL named schema

		if (option.pg_named_schema) {

			// decide whether writable table

			tables.parallelStream().filter(table -> table.required && (option.rel_model_ext || !table.relational)).forEach(table -> table.writable = true);

			pg_named_schema = new HashSet<String>();

			tables.stream().filter(table -> table.writable).filter(table -> !table.schema_name.equals(PgSchemaUtil.pg_public_schema_name)).forEach(table -> pg_named_schema.add(table.schema_name)); // do not parallelize this because of order change

			total_pg_named_schema = pg_named_schema.size();

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

			pending_attr_groups = null; // never chance to access, set null for serialization

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

			pending_model_groups = null; // never chance to access, set null for serialization

		}

		// resolved pending groups

		tables.parallelStream().filter(table -> table.has_pending_group).forEach(table -> table.has_pending_group = false);

		// update DTD data holder, content holder, any content holder

		tables.parallelStream().forEach(table -> table.fields.forEach(field -> {

			field.setDtdDataHolder();
			field.setContentHolder();
			field.setAnyContentHolder();

		}));

		// classify type of table

		tables.parallelStream().forEach(table -> {

			table.classify();

			// set foreign_table_id as table pointer otherwise remove orphan nested key

			if (table.required) {

				Iterator<PgField> iterator = table.fields.iterator();

				PgField field;

				while (iterator.hasNext()) {

					field = iterator.next();

					if (field.nested_key) {

						PgTable foreign_table = getForeignTable(field);

						if (foreign_table != null) {

							field.foreign_table_id = tables.indexOf(foreign_table);
							foreign_table.required = true;

							// detect simple content as attribute

							if (field.nested_key_as_attr) { // && foreign_table.has_simple_content) { // do not test table.has_simple_content because table.classify() has not been completed

								foreign_table.fields.stream().filter(foreign_field -> foreign_field.simple_content).forEach(foreign_field -> {

									foreign_field.simple_attribute = true;
									foreign_field.foreign_table_id = tables.indexOf(table);
									foreign_field.foreign_schema = table.schema_name;
									foreign_field.foreign_table_xname = table.xname;

									table.has_nested_key_to_simple_attr = foreign_table.has_simple_attribute = true;

								});

							}

						}

						else
							iterator.remove();

					}

				}

			}

		});

		// detect wild card contents

		if (option.wild_card) {

			has_any = tables.parallelStream().anyMatch(table -> table.has_any);
			has_any_attribute = tables.parallelStream().anyMatch(table -> table.has_any_attribute);

		}

		// update table has attributes

		tables.parallelStream().filter(table -> !table.has_attrs && (table.has_simple_attribute || table.has_nested_key_to_simple_attr)).forEach(table -> table.has_attrs = true);

		// update table has nested key as attribute

		tables.parallelStream().filter(table -> table.total_nested_fields > 0 && table.fields.stream().anyMatch(field -> field.nested_key_as_attr)).forEach(table -> table.has_nested_key_as_attr = true);

		// detect simple content as conditional attribute

		tables.stream().filter(foreign_table -> foreign_table.has_simple_attribute).forEach(foreign_table -> {

			if (tables.parallelStream().anyMatch(table -> table.total_nested_fields > 0 && table.fields.stream().anyMatch(field -> field.nested_key && !field.nested_key_as_attr && getForeignTable(field).equals(foreign_table)))) {

				foreign_table.fields.stream().filter(foreign_field -> foreign_field.simple_attribute).forEach(foreign_field -> {

					foreign_field.simple_attribute = false;
					foreign_field.simple_attr_cond = true;

				});

			}

		});

		// avoid virtual duplication of nested key

		tables.parallelStream().forEach(table -> {

			Iterator<PgField> iterator = table.fields.iterator(), _iterator;

			PgField field, _field;
			PgTable nested_table, _nested_table;
			boolean changed;

			while (iterator.hasNext()) {

				field = iterator.next();

				if (!field.nested_key)
					continue;

				nested_table = getForeignTable(field);

				if (nested_table.virtual)
					continue;

				changed = false;

				_iterator = table.fields.iterator();

				while (_iterator.hasNext()) {

					_field = _iterator.next();

					if (!_field.nested_key)
						continue;

					if (_field.equals(field))
						continue;

					_nested_table = getForeignTable(_field);

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
					nested_table.anno = "(quoted from " + table.pname + ")\n-- " + table.anno;
					nested_table.xanno_doc = "(quoted from " + table.pname + ")\n" + table.xanno_doc;
				}

			});

		});

		// cancel unique key constraint if parent table is list holder

		tables.parallelStream().filter(table -> table.list_holder).forEach(table -> table.fields.stream().filter(field -> field.nested_key).forEach(field -> getForeignTable(field).cancelUniqueKey()));

		// decide primary field and table has unique primary key

		tables.parallelStream().forEach(table -> {

			table.schema_pgname = (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.schema_name) + "." : "");

			Optional<PgField> opt = table.fields.stream().filter(field -> field.primary_key).findFirst();

			if (opt.isPresent()) {

				PgField primary_field = opt.get();

				table.primary_key_pgname = PgSchemaUtil.avoidPgReservedWords(primary_field.pname);
				table.has_unique_primary_key = primary_field.unique_key;

			}

		});

		// decide parent node name constraint

		tables.stream().filter(table -> table.total_nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && field.parent_node != null).forEach(field -> { // do not parallelize this stream which causes null pointer exception

			// tolerate parent node name constraint due to name collision

			if (table.name_collision)
				field.parent_node = null;

			else {

				String[] parent_nodes = field.parent_node.split(" ");

				field.parent_node = null;

				boolean infinite_loop = false, has_content, has_foreign_key;

				PgTable parent_table, _parent_table;

				for (String parent_node : parent_nodes) {

					parent_table = getCanTable(field.foreign_schema, parent_node);

					if (parent_table == null)
						continue;

					has_content = false;
					has_foreign_key = false;

					do {

						_parent_table = parent_table;

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

							if (ancestor_table.total_nested_fields == 0)
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

		tables.parallelStream().filter(table -> table.total_foreign_fields > 0 && table.total_nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && !field.nested_key_as_attr && field.parent_node != null).forEach(field -> {

			Optional<PgField> opt = table.fields.stream().filter(foreign_field -> foreign_field.foreign_key && foreign_field.foreign_table_xname.equals(field.parent_node)).findFirst();

			if (opt.isPresent()) {

				Optional<PgField> opt2 = getForeignTable(opt.get()).fields.stream().filter(nested_field -> nested_field.nested_key && getForeignTable(nested_field).equals(table)).findFirst();

				if (opt2.isPresent())
					field.ancestor_node = opt2.get().parent_node;

			}

		}));

		tables.parallelStream().filter(table -> table.total_foreign_fields == 0 && table.total_nested_fields > 0).forEach(table -> table.fields.stream().filter(field -> field.nested_key && field.parent_node != null && field.ancestor_node == null).forEach(field -> {

			StringBuilder sb = new StringBuilder();

			tables.stream().filter(ancestor_table -> ancestor_table.total_nested_fields > 0).forEach(ancestor_table -> {

				Optional<PgField> opt = ancestor_table.fields.stream().filter(ancestor_field -> ancestor_field.nested_key && getForeignTable(ancestor_field).equals(table) && ancestor_field.xtype.equals(field.xtype) && ancestor_field.ancestor_node != null).findFirst();

				if (opt.isPresent()) {

					String[] ancestor_nodes = opt.get().ancestor_node.split(" ");

					String[] _ancestor_nodes = sb.toString().split(" ");

					boolean has_ancestor_node;

					for (String ancestor_node : ancestor_nodes) {

						has_ancestor_node = false;

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

		tables.parallelStream().filter(table -> table.total_nested_fields > 0).forEach(table -> {

			// decide whether table has nested key with parent/ancestor path restriction

			if (table.fields.stream().anyMatch(field -> field.nested_key && (field.parent_node != null || field.ancestor_node != null))) {

				table.has_path_restriction = true;

				// split parent node name

				table.fields.stream().filter(field -> field.nested_key && field.parent_node != null).forEach(field -> field.parent_nodes = field.parent_node.split(" "));

				// split ancestor node name

				table.fields.stream().filter(field -> field.nested_key && field.ancestor_node != null).forEach(field -> field.ancestor_nodes = field.ancestor_node.split(" "));

			}

		});

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

		tables.parallelStream().filter(table -> table.writable).forEach(table -> {

			// add serial key on demand in case that parent table is list holder

			if (option.serial_key && table.list_holder)
				table.fields.stream().filter(field -> field.nested_key).forEach(field -> getForeignTable(field).addSerialKey(option));

			// add XPath key on demand

			if (option.xpath_key)
				table.addXPathKey(option);

			// remove nested key if relational model extension is disabled

			if (!option.rel_model_ext && table.total_nested_fields > 0) {

				table.fields.removeIf(field -> field.nested_key);
				table.countNestedFields();

			}

			// retrieve document key if no in-place document key exists

			if (!option.document_key && option.in_place_document_key && (option.rel_model_ext || option.document_key_if_no_in_place)) {

				if (!table.fields.stream().anyMatch(field -> field.name.equals(option.document_key_name)) && !table.fields.stream().anyMatch(field -> (field.dtd_data_holder && option.in_place_document_key_names.contains(field.name)) || ((field.dtd_data_holder || field.simple_content) && option.in_place_document_key_names.contains(table.name + "." + field.name)))) {

					PgField field = new PgField();

					field.name = field.pname = field.xname = option.document_key_name;
					field.type = option.xs_prefix_ + "string";
					field.xs_type = XsFieldType.xs_string;
					field.document_key = true;

					table.fields.add(0, field);

				}

			}

			// update system key, user key, omissible and jsonable flags

			table.fields.forEach(field -> {

				field.setSystemKey();
				field.setUserKey();
				field.setOmissible(table, option);
				field.setJsonable(table, option);


			});

		});

		// decide prefix of target namespace

		List<String> other_namespaces = new ArrayList<String>();

		tables.parallelStream().forEach(table -> {

			if (table.target_namespace == null)
				table.target_namespace = "";

			table.prefix = getPrefixOf(table.target_namespace.split(" ")[0], "");

			table.fields.stream().filter(field -> !field.system_key && !field.user_key).forEach(field -> {

				if (field.target_namespace != null) {

					field.prefix = getPrefixOf(field.target_namespace.split(" ")[0], "");
					field.is_same_namespace_of_table = field.target_namespace.equals(table.target_namespace);

				}

				if (field.any_content_holder) {

					String any_namespace = field.any_namespace.split(" ")[0]; // eval first item only

					switch (any_namespace) {
					case "##any":
					case "##targetNamespace":
						field.any_namespace = table.target_namespace;
						field.prefix = table.prefix;
						break;
					case "##other":
					case "##local":
						field.any_namespace = "";
						field.prefix = "";
						break;
					default:
						field.any_namespace = any_namespace;
						field.prefix = getPrefixOf(any_namespace, "");

						if (field.prefix.isEmpty()) {

							if (other_namespaces.contains(any_namespace))
								field.prefix = "ns" + (other_namespaces.indexOf(any_namespace) + 1);

							else {

								field.prefix = "ns" + (other_namespaces.size() + 1);
								other_namespaces.add(any_namespace);

							}

							if (field.prefix.equals("ns1"))
								field.prefix = "ns";

						}

					}

					field.is_same_namespace_of_table = field.any_namespace.equals(table.target_namespace);

				}

			});

			table.has_nillable_element = table.fields.stream().anyMatch(field -> field.element && field.nillable);

		});

		other_namespaces.clear();

		// statistics

		if (option.ddl_output) {

			def_stat_msg.insert(0, "--  Generated " + tables.parallelStream().filter(table -> (option.rel_model_ext || !table.relational) && (table.writable || option.show_orphan_table)).count() + " tables (" + tables.parallelStream().map(table -> (option.rel_model_ext || !table.relational) && table.writable ? table.fields.size() : 0).reduce((arg0, arg1) -> arg0 + arg1).get() + " fields), " + attr_groups.size() + " attr groups, " + model_groups.size() + " model groups in total\n");

			if (!option.show_orphan_table && tables.parallelStream().filter(table -> (option.rel_model_ext || !table.relational) && !table.writable).count() > 0) {

				def_stat_msg.append("--   Orphan tables:\n--    ");
				tables.stream().filter(table -> (option.rel_model_ext || !table.relational) && !table.writable).forEach(table -> def_stat_msg.append(table.pname + ", "));
				def_stat_msg.setLength(def_stat_msg.length() - 2);
				def_stat_msg.append("\n");

			}

			StringBuilder sb = new StringBuilder();

			HashSet<String> namespace_uri = new HashSet<String>();

			def_namespaces.entrySet().stream().map(arg -> arg.getValue()).forEach(arg -> namespace_uri.add(arg));

			namespace_uri.forEach(arg -> sb.append(arg + " (" + getPrefixOf(arg, "default") + "), "));
			namespace_uri.clear();

			def_stat_msg.append("--   Namespaces:\n");
			def_stat_msg.append("--    " + sb.substring(0, sb.length() - 2) + "\n");

			sb.setLength(0);

			schema_locations.stream().filter(arg -> !dup_schema_locations.containsKey(arg)).forEach(arg -> sb.append(arg + ", "));

			def_stat_msg.append("--   Schema locations:\n");
			def_stat_msg.append("--    " + sb.substring(0, sb.length() - 2) + "\n");

			sb.setLength(0);

			def_stat_msg.append("--   Table types:\n");
			def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.xs_type.equals(XsTableType.xs_root) && table.writable).count() + " root, ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.xs_type.equals(XsTableType.xs_root_child) && table.writable).count() + " root children, ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_root) && table.writable).count() + " admin roots, ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.xs_type.equals(XsTableType.xs_admin_child) && table.writable).count() + " admin children\n");
			def_stat_msg.append("--   System keys:\n");
			def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.primary_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " primary keys ("
					+ tables.parallelStream().map(table -> table.fields.stream().filter(field -> field.unique_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " unique constraints), ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.foreign_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " foreign keys, ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.nested_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " nested keys ("
					+ tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.nested_key_as_attr).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " as attribute)\n");
			def_stat_msg.append("--   User keys:\n");
			def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.document_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " document keys, ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.serial_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " serial keys, ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.xpath_key).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " xpath keys\n");
			def_stat_msg.append("--   Contents:\n");
			def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.attribute && !option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name)).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " attributes ("
					+ (option.document_key || !option.in_place_document_key ? 0 : tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.attribute && !option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name) && (option.in_place_document_key_names.contains(field.name) || option.in_place_document_key_names.contains(table.name + "." + field.name))).count()).reduce((arg0, arg1) -> arg0 + arg1).get()) + " in-place document keys), ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.element && !option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name)).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " elements ("
					+ (option.document_key || !option.in_place_document_key ? 0 : tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.element && !option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name) && (option.in_place_document_key_names.contains(field.name) || option.in_place_document_key_names.contains(table.name + "." + field.name))).count()).reduce((arg0, arg1) -> arg0 + arg1).get()) + " in-place document keys), ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.simple_content && !option.discarded_document_key_names.contains(table.name + "." + field.name)).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " simple contents ("
					+ (option.document_key || !option.in_place_document_key ? 0 : tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.simple_content && option.in_place_document_key_names.contains(table.name + "." + field.name)).count()).reduce((arg0, arg1) -> arg0 + arg1).get()) + " in-place document keys, "
					+ tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.simple_attribute).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " as attribute, "
					+ tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.simple_attr_cond).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " as conditional attribute)\n");
			def_stat_msg.append("--   Wild cards:\n");
			def_stat_msg.append("--    " + tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.any).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " any elements, ");
			def_stat_msg.append(tables.parallelStream().filter(table -> table.writable).map(table -> table.fields.stream().filter(field -> field.any_attribute).count()).reduce((arg0, arg1) -> arg0 + arg1).get() + " any attributes\n");
			def_stat_msg.append("--   Constraints:\n");
			def_stat_msg.append("--    " + countKeys(unq_keys) + " unique constraints from " + option.xs_prefix_ + "unique, "
					+ countKeys(keys) + " unique constraints from " + option.xs_prefix_ + "key, "
					+ countKeyReferences() + " foreign key constraints from " + option.xs_prefix_ + "keyref\n");

		}

		// update schema locations to unique ones

		tables.parallelStream().forEach(table -> table.schema_location = getUniqueSchemaLocations(table.schema_location));
		attr_groups.stream().forEach(attr_group -> attr_group.schema_location = getUniqueSchemaLocations(attr_group.schema_location));
		model_groups.stream().forEach(model_group -> model_group.schema_location = getUniqueSchemaLocations(model_group.schema_location));

		// realize PostgreSQL DDL

		realize();

		// never change to access, set null for serialization

		root_schema = null;

		root_node = null;

		key_nodes = null;

		schema_locations.clear();
		schema_locations = null;

		if (option.pg_named_schema) {

			pg_named_schema.clear();
			pg_named_schema = null;

		}

		unq_schema_locations.clear();
		unq_schema_locations = null;

		dup_schema_locations.clear();
		dup_schema_locations = null;

		attr_groups.clear();
		attr_groups = null;

		model_groups.clear();
		model_groups = null;

		unq_keys.clear();
		unq_keys = null;

		keys.clear();
		keys = null;

		foreign_keys.clear();
		foreign_keys = null;

		// check root table exists

		if (root_table == null)
			throw new PgSchemaException("Not found root table in XML Schema: " + def_schema_location);

		// update writable table flags if relational data extension is disabled

		if (!option.rel_data_ext)
			tables.parallelStream().filter(table -> table.writable && !(table.required && (option.rel_data_ext || !table.relational))).forEach(table -> table.writable = false);

		tables.parallelStream().filter(table -> table.writable).forEach(table -> {

			// preset Latin-1 encoded field

			table.fields.forEach(field -> field.latin_1_encoded = field.xs_type.isLatin1Encoded());

			// preset XML start/end element tag template for document key

			if (option.document_key || option.in_place_document_key) {

				Optional<PgField> opt = table.fields.stream().filter(field -> field.document_key).findFirst();

				if (opt.isPresent()) {

					PgField field = opt.get();

					field.start_end_elem_tag = new String("<<" + (table.prefix.isEmpty() ? "" : table.prefix + ":") + field.xname + ">").getBytes(PgSchemaUtil.def_charset);

				}

			}

			// preset XML start/end/empty element tag template for element

			if (table.has_element) {

				table.fields.stream().filter(field -> field.element).forEach(field -> {

					String prefix = field.is_same_namespace_of_table ? table.prefix : field.prefix;

					field.start_end_elem_tag = new String("<<" + (prefix.isEmpty() ? "" : prefix + ":") + field.xname + ">\n").getBytes(PgSchemaUtil.def_charset);
					field.empty_elem_tag = new String("<" + (prefix.isEmpty() ? "" : prefix + ":") + field.xname + " " + PgSchemaUtil.xsi_prefix + ":nil=\"true\"/>\n").getBytes(PgSchemaUtil.def_charset);

				});

			}

			// preset SQL parameter id

			int param_id = 0;

			for (PgField field : table.fields) {

				if (field.omissible)
					continue;

				field.sql_param_id = ++param_id;

			}

			// preset SQL upsert id

			table.total_sql_params = param_id;

			for (PgField field : table.fields) {

				if (field.omissible)
					continue;

				else if (field.primary_key)
					field.sql_upsert_id = table.total_sql_params * 2;

				else
					field.sql_upsert_id = ++param_id;

			}

			// check in-place document keys

			if (option.document_key || option.in_place_document_key) {

				try {

					table.doc_key_pgname = PgSchemaUtil.avoidPgReservedWords(table.doc_key_pname = getDocKeyName(table));

				} catch (PgSchemaException e) {
					e.printStackTrace();
				}

			}

		});

		tables.parallelStream().forEach(table -> {

			// preset list of attribute fields

			if (table.has_attrs)
				table.attr_fields = table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond || field.any_attribute || field.nested_key_as_attr) && !option.discarded_document_key_names.contains(field.name)).collect(Collectors.toList());

			// preset list of element fields

			if (table.has_elems)
				table.elem_fields = table.fields.stream().filter(field -> ((field.simple_content && !field.simple_attribute) || field.element || field.any) && !option.discarded_document_key_names.contains(field.name)).collect(Collectors.toList());

			// preset list of nested fields and array of foreign table id

			if (table.total_nested_fields > 0) {

				table.nested_fields = table.fields.stream().filter(field -> field.nested_key).collect(Collectors.toList());
				table.ft_ids = table.nested_fields.stream().map(field -> field.foreign_table_id).collect(Collectors.toList()).stream().mapToInt(Integer::intValue).toArray();

			}

		});

		// decide primary table for questing document id

		doc_id_table = root_table;

		if (!option.rel_data_ext) {

			Optional<PgTable> opt = tables.parallelStream().filter(table -> table.writable).min(Comparator.comparingInt(table -> table.order));

			if (opt.isPresent())
				doc_id_table = opt.get();

		}

	}

	/**
	 * Weather a given node is referred by sibling nodes.
	 *
	 * @param node element node
	 * @return boolean whether the node is referred by sibling nodes
	 */
	private boolean isReferredBySibling(Node node) {

		String name = ((Element) node).getAttribute("name");

		for (Node prev_node = node.getPreviousSibling(); prev_node != null; prev_node = prev_node.getPreviousSibling()) {

			if (prev_node.getNodeType() != Node.ELEMENT_NODE || !prev_node.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			if (isReferredByOffspring(name, prev_node))
				return true;

		}

		for (Node next_node = node.getNextSibling(); next_node != null; next_node = next_node.getNextSibling()) {

			if (next_node.getNodeType() != Node.ELEMENT_NODE || !next_node.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			if (isReferredByOffspring(name, next_node))
				return true;

		}

		return false;
	}

	/** Whether a given name is referred by offspring nodes.
	 *
	 * @param name element name
	 * @param node current node
	 * @return boolean whether the name is referred by offspring nodes
	 */
	private boolean isReferredByOffspring(String name, Node node) {

		if (node.getNodeType() == Node.ELEMENT_NODE && node.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri)) {

			if (((Element) node).getLocalName().equals("element")) {

				String ref = ((Element) node).getAttribute("ref");

				if (ref != null && ref.equals(name))
					return true;

				String _name = ((Element) node).getAttribute("name");

				if (_name != null && _name.equals(name))
					return true;

			}

		}

		if (node.hasChildNodes()) {

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (isReferredByOffspring(name, child))
					return true;

			}

		}

		return false;
	}

	/**
	 * Extract root element of XML Schema.
	 *
	 * @param node current node
	 * @param root_element whether root element
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractRootElement(Node node, boolean root_element) throws PgSchemaException {

		PgTable table = new PgTable(getPgSchemaOf(def_namespace), def_namespace, def_schema_location);

		String name = ((Element) node).getAttribute("name");

		table.xname = PgSchemaUtil.getUnqualifiedName(name);
		table.name = table.pname = option.case_sense ? table.xname : PgSchemaUtil.toCaseInsensitive(table.xname);

		if (table.pname.isEmpty())
			return;

		table.required = true;

		if ((table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
			table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

		table.xs_type = root_element ? XsTableType.xs_root : XsTableType.xs_admin_root;

		table.fields = new ArrayList<PgField>();

		table.level = level = 0;

		table.addPrimaryKey(option, true);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			extractField(child, table, false);

		}

		table.removeProhibitedAttrs();
		table.removeBlockedSubstitutionGroups();
		table.countNestedFields();

		if (!table.has_pending_group && table.fields.size() < option.getMinimumSizeOfField())
			return;

		tables.add(table);

		extractRootElementType(node, table);

		if (root_element) {

			if (tables.parallelStream().anyMatch(_table -> _table.xs_type.equals(XsTableType.xs_root)))
				root_table = tables.parallelStream().filter(_table -> _table.xs_type.equals(XsTableType.xs_root)).findFirst().get();

		}

	}

	/**
	 * Extract root element type.
	 *
	 * @param node current node
	 * @param table root table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractRootElementType(Node node, PgTable table) throws PgSchemaException {

		PgField dummy = new PgField();

		String name = ((Element) node).getAttribute("name");

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

					boolean unique_key = table.addNestedKey(option, child_table.schema_name, PgSchemaUtil.getUnqualifiedName(name), dummy, node);

					String child_name = name;

					child_table.xname = PgSchemaUtil.getUnqualifiedName(child_name);
					child_table.name = child_table.pname = option.case_sense ? child_table.xname : PgSchemaUtil.toCaseInsensitive(child_table.xname);

					table.required = child_table.required = true;

					if ((child_table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
						child_table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

					child_table.xs_type = XsTableType.xs_admin_root;

					child_table.fields = new ArrayList<PgField>();

					child_table.level = level;

					child_table.addPrimaryKey(option, unique_key);

					if (!child_table.addNestedKey(option, table.schema_name, PgSchemaUtil.getUnqualifiedName(dummy.type), dummy, node))
						child_table.cancelUniqueKey();

					child_table.removeProhibitedAttrs();
					child_table.removeBlockedSubstitutionGroups();
					child_table.countNestedFields();

					if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
						tables.add(child_table);

					extractChildElement(node, child_table);

					level--;

				}

			}

		}

	}

	/**
	 * Extract administrative element of XML Schema.
	 *
	 * @param node current node
	 * @param complex_type whether complexType
	 * @param annotation whether to collect annotation only
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractAdminElement(Node node, boolean complex_type, boolean annotation) throws PgSchemaException {

		PgTable table = new PgTable(getPgSchemaOf(def_namespace), def_namespace, def_schema_location);

		String name = ((Element) node).getAttribute("name");

		table.xname = PgSchemaUtil.getUnqualifiedName(name);
		table.name = table.pname = option.case_sense ? table.xname : PgSchemaUtil.toCaseInsensitive(table.xname);

		if (table.pname.isEmpty())
			return;

		if ((table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
			table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

		if (annotation) {

			if (table.anno != null && !table.anno.isEmpty()) {

				PgTable known_table = getCanTable(table.schema_name, table.xname);

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

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
					continue;

				extractField(child, table, false);

			}

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

		PgTable table = new PgTable(getPgSchemaOf(def_namespace), def_namespace, def_schema_location);

		String name = ((Element) node).getAttribute("name");

		table.xname = PgSchemaUtil.getUnqualifiedName(name);
		table.name = table.pname = option.case_sense ? table.xname : PgSchemaUtil.toCaseInsensitive(table.xname);

		if (table.pname.isEmpty())
			return;

		if ((table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
			table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

		table.xs_type = XsTableType.xs_attr_group;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			extractField(child, table, false);

		}

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

		PgTable table = new PgTable(getPgSchemaOf(def_namespace), def_namespace, def_schema_location);

		String name = ((Element) node).getAttribute("name");

		table.xname = PgSchemaUtil.getUnqualifiedName(name);
		table.name = table.pname = option.case_sense ? table.xname : PgSchemaUtil.toCaseInsensitive(table.xname);

		if (table.pname.isEmpty())
			return;

		if ((table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
			table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

		table.xs_type = XsTableType.xs_model_group;

		table.fields = new ArrayList<PgField>();

		table.level = 0;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			extractField(child, table, false);

		}

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
	 * @param has_complex_parent whether this node has parent node of complex type
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractField(Node node, PgTable table, boolean has_complex_parent) throws PgSchemaException {

		String node_name = ((Element) node).getLocalName();

		if (node_name.equals("any")) {
			extractAny(node, table);
			return;
		}

		else if (node_name.equals("anyAttribute")) {
			extractAnyAttribute(node, table);
			return;
		}

		else if (node_name.equals("attribute")) {
			extractAttribute(node, table);
			return;
		}

		else if (node_name.equals("attributeGroup")) {
			extractAttributeGroup(node, table);
			return;
		}

		else if (node_name.equals("element")) {
			extractElement(node, table, has_complex_parent);
			return;
		}

		else if (node_name.equals("group")) {
			extractModelGroup(node, table);
			return;
		}

		else if (node_name.equals("simpleContent")) {
			extractSimpleContent(node, table, false);
			return;
		}

		else if (node_name.equals("complexContent")) {
			extractComplexContent(node, table);
			return;
		}

		else if (node_name.equals("keyref")) {
			extractForeignKeyRef(node);
			return;
		}

		else if (node_name.equals("complexType"))
			has_complex_parent = true;

		String child_name;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_name = child.getLocalName();

			if (child_name.equals("annotation"))
				continue;

			if (child_name.equals("any"))
				extractAny(child, table);

			else if (child_name.equals("anyAttribute"))
				extractAnyAttribute(child, table);

			else if (child_name.equals("attribute"))
				extractAttribute(child, table);

			else if (child_name.equals("attributeGroup"))
				extractAttributeGroup(child, table);

			else if (child_name.equals("element"))
				extractElement(child, table, has_complex_parent);

			else if (child_name.equals("group"))
				extractModelGroup(child, table);

			else if (child_name.equals("simpleContent"))
				extractSimpleContent(child, table, false);

			else if (child_name.equals("complexContent"))
				extractComplexContent(child, table);

			else if (child_name.equals("keyref"))
				extractForeignKeyRef(child);

			else
				extractField(child, table, has_complex_parent);

		}

	}

	/**
	 * Extract foreign key from xs:keyref.
	 *
	 * @param node current node
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractForeignKeyRef(Node node) throws PgSchemaException {

		Element elem = (Element) node;

		String name = elem.getAttribute("name");
		String refer = elem.getAttribute("refer");

		if (name == null || name.isEmpty() || refer == null || refer.isEmpty())
			return;

		PgForeignKey foreign_key = new PgForeignKey(getPgSchemaOf(def_namespace), key_nodes, node, name, PgSchemaUtil.getUnqualifiedName(refer), option.case_sense);

		if (foreign_key.isEmpty())
			return;

		if (root_schema.foreign_keys.stream().anyMatch(_foreign_key -> _foreign_key.equals(foreign_key)))
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

		field.extractMaxOccurs(node);
		field.extractMinOccurs(node);

		field.xname = PgSchemaUtil.any_name;
		field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
		field.pname = table.avoidFieldDuplication(option, field.xname);

		if ((field.anno = PgSchemaUtil.extractAnnotation(node, false)) != null)
			field.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

		field.xs_type = XsFieldType.xs_any;
		field.type = field.xs_type.name();

		field.extractTargetNamespace(this, node, null);
		field.extractAnyNamespace(node);

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
		field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
		field.pname = table.avoidFieldDuplication(option, field.xname);

		if ((field.anno = PgSchemaUtil.extractAnnotation(node, false)) != null)
			field.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

		field.xs_type = XsFieldType.xs_anyAttribute;
		field.type = field.xs_type.name();

		field.extractTargetNamespace(this, node, null);
		field.extractAnyNamespace(node);

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

		extractDtdInfoItem(node, table, true, false);

	}

	/**
	 * Extract element.
	 *
	 * @param node current node
	 * @param table current table
	 * @param has_complex_parent whether this node has parent node of complex type
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractElement(Node node, PgTable table, boolean has_complex_parent) throws PgSchemaException {

		extractDtdInfoItem(node, table, false, has_complex_parent);

	}

	/**
	 * Concrete extractor for both attribute and element (information item of DTD data holder).
	 *
	 * @param node current node
	 * @param table current table
	 * @param attribute whether attribute or element (false)
	 * @param has_complex_parent whether this node has parent node of complex type
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractDtdInfoItem(Node node, PgTable table, boolean attribute, boolean has_complex_parent) throws PgSchemaException {

		PgField field = new PgField();

		Element elem = (Element) node;

		String name = elem.getAttribute("name");
		String ref = elem.getAttribute("ref");

		if (attribute)
			field.attribute = true;

		else {

			field.element = true;

			field.extractMaxOccurs(node);
			field.extractMinOccurs(node);

		}

		if (name != null && !name.isEmpty()) {

			field.xname = PgSchemaUtil.getUnqualifiedName(name);
			field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
			field.pname = table.avoidFieldDuplication(option, field.xname);

			if ((field.anno = PgSchemaUtil.extractAnnotation(node, false)) != null)
				field.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

			field.extractType(option, node);
			field.extractTargetNamespace(this, node, getNamespaceUriOfFieldQName(table, name));
			field.extractRequired(node);
			field.extractFixedValue(node);
			field.extractDefaultValue(option, node);
			field.extractBlockValue(node);
			field.extractEnumeration(option, node);
			field.extractRestriction(node);

			if (field.substitution_group != null && !field.substitution_group.isEmpty())
				table.appendSubstitutionGroup(field);

			if (field.enumeration != null && field.enumeration.length > 0) {

				field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.pname) + "_" + PgSchemaUtil.avoidPgReservedOps(field.pname);

				if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
					field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

			}

			if (field.type == null || field.type.isEmpty()) {

				if (!table.addNestedKey(option, table.schema_name, PgSchemaUtil.getUnqualifiedName(name), field, node))
					table.cancelUniqueKey();

				level++;

				table.required = true;

				extractChildElement(node, table);

				level--;

			}

			else {

				String[] type = field.type.contains(" ") ? field.type.split(" ")[0].split(":") : field.type.split(":");

				// primitive data type

				if (type.length != 0 && type[0].equals(option.xs_prefix)) {

					boolean has_complex_child = false;

					for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

						if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
							continue;

						if (((Element) child).getLocalName().equals("complexType")) {

							has_complex_child = true;

							// in-line complex type extension of primitive data type

							level++;

							PgTable child_table = new PgTable(table.schema_name, table.target_namespace, def_schema_location);

							boolean unique_key = table.addNestedKey(option, child_table.schema_name, PgSchemaUtil.getUnqualifiedName(name), field, node);

							if (!unique_key)
								table.cancelUniqueKey();

							child_table.xname = PgSchemaUtil.getUnqualifiedName(name);
							child_table.name = child_table.pname = option.case_sense ? child_table.xname : PgSchemaUtil.toCaseInsensitive(child_table.xname);

							table.required = child_table.required = true;

							if ((child_table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
								child_table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

							child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

							child_table.fields = new ArrayList<PgField>();

							child_table.level = level;

							child_table.addPrimaryKey(option, unique_key);

							child_table.addForeignKey(option, table);

							for (Node grandchild = child.getFirstChild(); grandchild != null; grandchild = grandchild.getNextSibling()) {

								if (grandchild.getNodeType() != Node.ELEMENT_NODE || !grandchild.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
									continue;

								extractField(grandchild, child_table, true);

							}

							child_table.removeProhibitedAttrs();
							child_table.removeBlockedSubstitutionGroups();
							child_table.countNestedFields();

							if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
								tables.add(child_table);

							extractChildElement(child, child_table);

							level--;

						}

					}

					// list of primitive data type or complex type extension

					if (has_complex_parent && field.list_holder) {

						level++;

						PgTable child_table = new PgTable(table.schema_name, table.target_namespace, def_schema_location);

						boolean unique_key = table.addNestedKey(option, child_table.schema_name, PgSchemaUtil.getUnqualifiedName(name), field, node);

						if (!unique_key)
							table.cancelUniqueKey();

						child_table.xname = PgSchemaUtil.getUnqualifiedName(name);
						child_table.name = child_table.pname = option.case_sense ? child_table.xname : PgSchemaUtil.toCaseInsensitive(child_table.xname);

						table.required = child_table.required = true;

						if ((child_table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
							child_table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

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

					else if (!has_complex_child) {

						field.xs_type = XsFieldType.valueOf("xs_" + type[1]);

						table.fields.add(field);

					}

				}

				// non-primitive data type

				else {

					level++;

					PgTable child_table = new PgTable(getPgSchemaOf(getNamespaceUriOfQName(field.type)), getNamespaceUriOfQName(field.type), def_schema_location);

					boolean unique_key = table.addNestedKey(option, child_table.schema_name, PgSchemaUtil.getUnqualifiedName(name), field, node);

					if (!unique_key)
						table.cancelUniqueKey();

					child_table.xname = PgSchemaUtil.getUnqualifiedName(name);
					child_table.name = child_table.pname = option.case_sense ? child_table.xname : PgSchemaUtil.toCaseInsensitive(child_table.xname);

					table.required = child_table.required = true;

					if ((child_table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
						child_table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

					child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

					child_table.fields = new ArrayList<PgField>();

					child_table.level = level;

					child_table.addPrimaryKey(option, unique_key);

					if (!child_table.addNestedKey(option, table.schema_name, PgSchemaUtil.getUnqualifiedName(field.type), field, node))
						child_table.cancelUniqueKey();

					child_table.removeProhibitedAttrs();
					child_table.removeBlockedSubstitutionGroups();
					child_table.countNestedFields();

					if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
						tables.add(child_table);

					extractChildElement(node, child_table);

					level--;

				}

			}

		}

		else if (ref != null && !ref.isEmpty()) {

			Element child_elem;
			String child_name;

			for (Node child = root_node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
					continue;

				child_elem = ((Element) child);
				child_name = child_elem.getLocalName();

				if (child_name.equals(attribute ? "attribute" : "element")) {

					child_name = PgSchemaUtil.getUnqualifiedName(child_elem.getAttribute("name"));

					if (child_name.equals(PgSchemaUtil.getUnqualifiedName(ref)) /* && (
							(table.target_namespace != null && table.target_namespace.equals(getNamespaceUriOfQName(ref))) ||
							(table.target_namespace == null && getNamespaceUriOfQName(ref) == null)) */) {

						field.xname = child_name;
						field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
						field.pname = table.avoidFieldDuplication(option, field.xname);

						if ((field.anno = PgSchemaUtil.extractAnnotation(child, false)) != null)
							field.xanno_doc = PgSchemaUtil.extractDocumentation(child, false);

						field.extractType(option, child);
						field.extractTargetNamespace(this, child, getNamespaceUriOfFieldQName(table, ref));
						field.extractRequired(child);
						field.extractFixedValue(child);
						field.extractDefaultValue(option, child);
						field.extractBlockValue(child);
						field.extractEnumeration(option, child);
						field.extractRestriction(child);

						if (field.substitution_group != null && !field.substitution_group.isEmpty())
							table.appendSubstitutionGroup(field);

						if (field.enumeration != null && field.enumeration.length > 0) {

							field.enum_name = "ENUM_" + PgSchemaUtil.avoidPgReservedOps(table.pname) + "_" + PgSchemaUtil.avoidPgReservedOps(field.pname);

							if (field.enum_name.length() > PgSchemaUtil.max_enum_len)
								field.enum_name = field.enum_name.substring(0, PgSchemaUtil.max_enum_len);

						}

						if (field.type == null || field.type.isEmpty()) {

							if (!table.addNestedKey(option, table.schema_name, child_name, field, child))
								table.cancelUniqueKey();

							level++;

							table.required = true;

							extractChildElement(child, table);

							level--;

						}

						else {

							String[] type = field.type.contains(" ") ? field.type.split(" ")[0].split(":") : field.type.split(":");

							// primitive data type

							if (type.length != 0 && type[0].equals(option.xs_prefix)) {

								field.xs_type = XsFieldType.valueOf("xs_" + type[1]);

								table.fields.add(field);

							}

							// non-primitive data type

							else {

								level++;

								PgTable child_table = new PgTable(getPgSchemaOf(getNamespaceUriOfQName(field.type)), getNamespaceUriOfQName(field.type), def_schema_location);

								boolean unique_key = table.addNestedKey(option, child_table.schema_name, child_name, field, child);

								if (!unique_key)
									table.cancelUniqueKey();

								child_table.xname = child_name;
								child_table.name = child_table.pname = option.case_sense ? child_table.xname : PgSchemaUtil.toCaseInsensitive(child_table.xname);

								table.required = child_table.required = true;

								if ((child_table.anno = PgSchemaUtil.extractAnnotation(child, true)) != null)
									child_table.xanno_doc = PgSchemaUtil.extractDocumentation(child, false);

								child_table.xs_type = table.xs_type.equals(XsTableType.xs_root) || table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;

								child_table.fields = new ArrayList<PgField>();

								child_table.level = level;

								child_table.addPrimaryKey(option, unique_key);

								if (!child_table.addNestedKey(option, table.schema_name, PgSchemaUtil.getUnqualifiedName(field.type), field, child))
									child_table.cancelUniqueKey();

								child_table.removeProhibitedAttrs();
								child_table.removeBlockedSubstitutionGroups();
								child_table.countNestedFields();

								if (!child_table.has_pending_group && child_table.fields.size() > 1 && avoidTableDuplication(tables, child_table))
									tables.add(child_table);

								extractChildElement(child, child_table);

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
	 * Extract child element.
	 *
	 * @param node current node
	 * @param foreign_table foreign table
	 * @throws PgSchemaException the pg schema exception
	 */
	private void extractChildElement(Node node, PgTable foreign_table) throws PgSchemaException {

		PgTable table = new PgTable(getPgSchemaOf(foreign_table), foreign_table.target_namespace, def_schema_location);

		Element elem = (Element) node;

		String type = elem.getAttribute("type");

		if (type == null || type.isEmpty()) {

			String name = elem.getAttribute("name");

			table.xname = PgSchemaUtil.getUnqualifiedName(name);
			table.name = table.pname = option.case_sense ? table.xname : PgSchemaUtil.toCaseInsensitive(table.xname);
			table.xs_type = foreign_table.xs_type.equals(XsTableType.xs_root) || foreign_table.xs_type.equals(XsTableType.xs_root_child) ? XsTableType.xs_root_child : XsTableType.xs_admin_child;


		}

		else {

			table.xname = PgSchemaUtil.getUnqualifiedName(type);
			table.name = table.pname = option.case_sense ? table.xname : PgSchemaUtil.toCaseInsensitive(table.xname);
			table.xs_type = XsTableType.xs_admin_root;

		}

		if (table.pname.isEmpty())
			return;

		table.required = true;

		if ((table.anno = PgSchemaUtil.extractAnnotation(node, true)) != null)
			table.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

		table.fields = new ArrayList<PgField>();

		table.level = level;

		table.addPrimaryKey(option, true);
		table.addForeignKey(option, foreign_table);

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			extractField(child, table, false);

		}

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

		Element elem = (Element) node;

		String ref = elem.getAttribute("ref");

		if (ref == null || ref.isEmpty()) {

			ref = elem.getAttribute("defaultAttributesApply");

			if (ref == null || !ref.equals("true"))
				return;

			if (def_attrs == null || def_attrs.isEmpty())
				return;

			ref = def_attrs;

			return;
		}

		ref = PgSchemaUtil.getUnqualifiedName(ref);

		PgTable attr_group = getAttributeGroup(ref, false);

		if (attr_group == null) {

			root_schema.pending_attr_groups.add(new PgPendingGroup(ref, table.schema_name, table.xname, table.fields.size()));
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

		String ref = ((Element) node).getAttribute("ref");

		if (ref == null || ref.isEmpty())
			return;

		ref = PgSchemaUtil.getUnqualifiedName(ref);

		PgTable model_group = getModelGroup(ref, false);

		if (model_group == null) {

			root_schema.pending_model_groups.add(new PgPendingGroup(ref, table.schema_name, table.xname, table.fields.size()));
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
		field.name = option.case_sense ? field.xname : PgSchemaUtil.toCaseInsensitive(field.xname);
		field.pname = table.avoidFieldDuplication(option, field.xname);

		if ((field.anno = PgSchemaUtil.extractAnnotation(node, false)) != null)
			field.xanno_doc = PgSchemaUtil.extractDocumentation(node, false);

		field.extractType(option, node);
		field.extractTargetNamespace(this, node, table.target_namespace);
		field.extractRequired(node);
		field.extractFixedValue(node);
		field.extractDefaultValue(option, node);
		field.extractBlockValue(node);
		field.extractEnumeration(option, node);
		field.extractRestriction(node);

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

			field.xs_type = XsFieldType.valueOf("xs_" + type[1]);

			table.fields.add(field);

			String grandchild_name;

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
					continue;

				if (((Element) child).getLocalName().equals("extension")) {

					for (Node grandchild = child.getFirstChild(); grandchild != null; grandchild = grandchild.getNextSibling()) {

						if (grandchild.getNodeType() != Node.ELEMENT_NODE || !grandchild.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
							continue;

						grandchild_name = ((Element) grandchild).getLocalName();

						if (grandchild_name.equals("attribute"))
							extractAttribute(grandchild, table);

						else if (grandchild_name.equals("attributeGroup"))
							extractAttributeGroup(grandchild, table);

					}

					break;
				}

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

		Element child_elem;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_elem = (Element) child;

			if (child_elem.getLocalName().equals("extension")) {

				table.addNestedKey(option, table.schema_name, PgSchemaUtil.getUnqualifiedName(child_elem.getAttribute("base")));

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

		String node_name = node.getLocalName();

		if (node_name.equals("any")) {
			extractAny(node, table);
			return;
		}

		else if (node_name.equals("anyAttribute")) {
			extractAnyAttribute(node, table);
			return;
		}

		else if (node_name.equals("attribute")) {
			extractAttribute(node, table);
			return;
		}

		else if (node_name.equals("attributeGroup")) {
			extractAttributeGroup(node, table);
			return;
		}

		else if (node_name.equals("element")) {
			extractElement(node, table, false);
			return;
		}

		else if (node_name.equals("group")) {
			extractModelGroup(node, table);
			return;
		}

		String child_name;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(PgSchemaUtil.xs_namespace_uri))
				continue;

			child_name = ((Element) child).getLocalName();

			if (child_name.equals("annotation"))
				continue;

			if (child_name.equals("any"))
				extractAny(child, table);

			else if (child_name.equals("anyAttribute"))
				extractAnyAttribute(child, table);

			else if (child_name.equals("attribute"))
				extractAttribute(child, table);

			else if (child_name.equals("attributeGroup"))
				extractAttributeGroup(child, table);

			else if (child_name.equals("element"))
				extractElement(child, table, false);

			else if (child_name.equals("group"))
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
			known_table = tables.equals(this.tables) ? getPgTable(table.schema_name, table.pname) : tables.equals(root_schema.attr_groups) ? getAttributeGroup(table.xname, false) : tables.equals(root_schema.model_groups) ? getModelGroup(table.xname, false) : null;
		} catch (PgSchemaException e) {
			e.printStackTrace();
		}

		if (known_table == null)
			return true;

		// avoid table duplication (case insensitive)

		if (!option.case_sense && !known_table.xname.equals(table.xname)) {

			table.pname = "_" + table.pname;

			try {
				known_table = tables.equals(this.tables) ? getCanTable(table.schema_name, table.xname) : tables.equals(root_schema.attr_groups) ? getAttributeGroup(table.xname, false) : tables.equals(root_schema.model_groups) ? getModelGroup(table.xname, false) : null;
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

		PgField known_field;

		for (PgField field : fields) {

			known_field = known_table.getPgField(field.pname);

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

				for (PgField _known_field : known_fields) {

					if (table.getPgField(_known_field.pname) == null) {

						changed = true;

						if (!_known_field.primary_key && _known_field.required)
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
	 * Return total number of PostgreSQL named schema.
	 *
	 * @return int total number of PostgreSQL named schema
	 */
	public int getTotalPgNamedSchema() {
		return total_pg_named_schema;
	}

	/**
	 * Return whether arbitrary table has any element.
	 *
	 * @return boolean whether arbitrary table has any element
	 */
	public boolean hasAny() {
		return has_any;
	}

	/**
	 * Return whether arbitrary table has any attribute.
	 *
	 * @return boolean whether arbitrary table has any attribute
	 */
	public boolean hasAnyAttribute() {
		return has_any_attribute;
	}

	/**
	 * Return whether arbitrary table has any wild card.
	 *
	 * @return boolean whether arbitrary table has any wild card
	 */
	public boolean hasWildCard() {
		return has_any | has_any_attribute;
	}

	/**
	 * Return default namespace.
	 *
	 * @return String default namespace
	 */
	public String getDefaultNamespace() {
		return def_namespace;
	}

	/**
	 * Return default schema location.
	 *
	 * @return String default schema location
	 */
	public String getDefaultSchemaLocation() {
		return def_schema_location;
	}

	/**
	 * Return top level xs:annotation/xs:appinfo.
	 *
	 * @return String top level xs:annotation/xs:appinfo
	 */
	public String getDefaultAppinfo() {
		return def_anno_appinfo;
	}

	/**
	 * Return top level xs:annotation/xs:documentation.
	 *
	 * @return String top level xs:annotation/xs:documentation
	 */
	public String getDefaultDocumentation() {
		return def_anno_doc;
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

				if (dup_schema_locations.containsKey(schema_location))
					sb.append(dup_schema_locations.get(schema_location) + " ");
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
	public String getNamespaceUriForPrefix(String prefix) {
		return def_namespaces.get(prefix);
	}

	/**
	 * Return namespace URI of qualified name.
	 *
	 * @param qname qualified name
	 * @return String namespace URI
	 */
	private String getNamespaceUriOfQName(String qname) {

		String xname = PgSchemaUtil.getUnqualifiedName(qname);

		return getNamespaceUriForPrefix(xname.equals(qname) ? "" : qname.substring(0, qname.length() - xname.length() - 1));
	}

	/**
	 * Return namespace URI of field's qualified name.
	 *
	 * @param table current table
	 * @param qname qualified name of field
	 * @return String namespace URI
	 */
	private String getNamespaceUriOfFieldQName(PgTable table, String qname) {

		String xname = PgSchemaUtil.getUnqualifiedName(qname);

		return xname.equals(qname) ? table.target_namespace : getNamespaceUriForPrefix(qname.substring(0, qname.length() - xname.length() - 1));
	}

	/**
	 * Return prefix of namespace URI.
	 *
	 * @param namespace_uri namespace URI
	 * @param def_prefix prefix for default namespace URI
	 * @return String prefix of namespace URI
	 */
	private String getPrefixOf(String namespace_uri, String def_prefix) {
		return def_namespaces.entrySet().stream().anyMatch(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()) ? def_namespaces.entrySet().stream().filter(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()).findFirst().get().getKey() : def_prefix;
	}

	/**
	 * Return PostgreSQL schema name of namespace URI.
	 *
	 * @param namespace_uri namespace URI
	 * @return String PostgreSQL schema name of namespace URI
	 */
	private String getPgSchemaOf(String namespace_uri) {
		return option.pg_named_schema ? (def_namespaces.entrySet().stream().anyMatch(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()) ? def_namespaces.entrySet().stream().filter(arg -> arg.getValue().equals(namespace_uri) && !arg.getKey().isEmpty()).findFirst().get().getKey() : PgSchemaUtil.pg_public_schema_name) : PgSchemaUtil.pg_public_schema_name;
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
	private String getPgNameOf(PgTable table) {
		return table.schema_pgname + PgSchemaUtil.avoidPgReservedWords(table.pname);
	}

	/**
	 * Return PostgreSQL name of child table.
	 *
	 * @param foreign_key foreign key
	 * @return String PostgreSQL name of child table
	 */
	private String getPgChildNameOf(PgForeignKey foreign_key) {
		return (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(foreign_key.schema_name) + "." : "") + PgSchemaUtil.avoidPgReservedWords(foreign_key.child_table_pname);
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
	private String getDataFileNameOf(PgTable table) {
		return (option.pg_named_schema ? table.schema_name + "." : "") + table.pname + (option.pg_tab_delimiter ? ".tsv" : ".csv");
	}

	/**
	 * Prepare dictionaries for XPath parser.
	 */
	public void prepDicForXPath() {

		if (table_name_dic == null) {

			table_name_dic = new HashMap<String, PgTable>();

			tables.parallelStream().filter(table -> table.writable).forEach(table -> {

				table_name_dic.put(table.xname, table);

				if (table.has_attrs) {

					table.attr_name_dic = new HashMap<String, PgField>();
					table.attr_fields.stream().filter(field -> field.attribute || field.simple_attribute || field.simple_attr_cond).forEach(field -> table.attr_name_dic.put(field.simple_attribute || field.simple_attr_cond ? field.foreign_table_xname : field.xname, field));

				}

				if (table.has_elems) {

					table.elem_name_dic = new HashMap<String, PgField>();
					table.elem_fields.stream().filter(field -> field.element).forEach(field -> table.elem_name_dic.put(field.xname, field));

				}

			});

		}

		if (table_path_dic == null)
			table_path_dic = new HashMap<String, PgTable>();

	}

	/**
	 * Return dictionary of table name.
	 *
	 * @return HashMap dictionary of table name
	 */
	public HashMap<String, PgTable> getTableNameDictionary() {
		return table_name_dic;
	}

	/**
	 * Return dictionary of matched table path.
	 *
	 * @return HashMap dictionary of matched table path
	 */
	public HashMap<String, PgTable> getTablePathDictionary() {
		return table_path_dic;
	}

	/**
	 * Return table list.
	 *
	 * @return List table list
	 */
	public List<PgTable> getTableList() {
		return tables;
	}

	/**
	 * Return root table.
	 *
	 * @return PgTable root table
	 */
	public PgTable getRootTable() {
		return root_table;
	}

	/**
	 * Return table.
	 *
	 * @param table_id table id
	 * @return PgTable table
	 */
	public PgTable getTable(int table_id) {
		return table_id < 0 || table_id >= tables.size() ? null : tables.get(table_id);
	}

	/**
	 * Return table.
	 *
	 * @param schema_name PostgreSQL schema name
	 * @param table_name table name
	 * @return PgTable table
	 */
	private PgTable getTable(String schema_name, String table_name) {

		if (!option.pg_named_schema)
			schema_name = PgSchemaUtil.pg_public_schema_name;

		else if (schema_name == null || schema_name.isEmpty())
			schema_name = root_table.schema_name;

		String _schema_name = schema_name;

		Optional<PgTable> opt = tables.parallelStream().filter(table -> table.schema_name.equals(_schema_name) && table.name.equals(table_name)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Return table.
	 *
	 * @param schema_name PostgreSQL schema name
	 * @param table_xname table name (canonical)
	 * @return PgTable table
	 */
	private PgTable getCanTable(String schema_name, String table_xname) {

		if (!option.pg_named_schema)
			schema_name = PgSchemaUtil.pg_public_schema_name;

		else if (schema_name == null || schema_name.isEmpty())
			schema_name = root_table.schema_name;

		String _schema_name = schema_name;

		Optional<PgTable> opt = tables.parallelStream().filter(table -> table.schema_name.equals(_schema_name) && table.xname.equals(table_xname)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Return table.
	 *
	 * @param schema_name PostgreSQL schema name
	 * @param table_pname table name in PostgreSQL
	 * @return PgTable table
	 */
	private PgTable getPgTable(String schema_name, String table_pname) {

		if (!option.pg_named_schema)
			schema_name = PgSchemaUtil.pg_public_schema_name;

		else if (schema_name == null || schema_name.isEmpty())
			schema_name = root_table.schema_name;

		String _schema_name = schema_name;

		Optional<PgTable> opt = tables.parallelStream().filter(table -> table.schema_name.equals(_schema_name) && table.pname.equals(table_pname)).findFirst();

		return opt.isPresent() ? opt.get() : null;
	}

	/**
	 * Return pending table.
	 *
	 * @param pending_group pending group
	 * @return PgTable pending table
	 */
	private PgTable getPendingTable(PgPendingGroup pending_group) {
		return getCanTable(pending_group.schema_name, pending_group.xname);
	}

	/**
	 * Return table of key.
	 *
	 * @param key key
	 * @return PgTable table
	 */
	private PgTable getTable(PgKey key) {
		return getCanTable(key.schema_name, key.table_xname);
	}

	/**
	 * Return parent table of foreign key.
	 *
	 * @param foreign_key foreign key
	 * @return PgTable parent table
	 */
	private PgTable getParentTable(PgForeignKey foreign_key) {
		return getCanTable(foreign_key.schema_name, foreign_key.parent_table_xname);
	}

	/**
	 * Return child table of foreign key.
	 *
	 * @param foreign_key foreign key
	 * @return PgTable child table
	 */
	private PgTable getChildTable(PgForeignKey foreign_key) {
		return getCanTable(foreign_key.schema_name, foreign_key.child_table_xname);
	}

	/**
	 * Return foreign table of either nested key or foreign key.
	 *
	 * @param field field of either nested key of foreign key
	 * @return PgTable foreign table
	 */
	public PgTable getForeignTable(PgField field) {
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

		Optional<PgTable> opt = root_schema.attr_groups.stream().filter(attr_group -> attr_group.xname.equals(attr_group_name)).findFirst();

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

		Optional<PgTable> opt = root_schema.model_groups.stream().filter(model_group -> model_group.xname.equals(model_group_name)).findFirst();

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

		// set table names with schema specification

		tables.parallelStream().forEach(table -> table.pgname = getPgNameOf(table));

		if (!option.ddl_output)
			return;

		boolean name_collision = tables.parallelStream().anyMatch(table -> table.name_collision);

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
		System.out.println("--  append document key: " + (option.document_key || option.in_place_document_key) + (option.in_place_document_key ? " (in-place)" : ""));
		System.out.println("--  append serial key: " + option.serial_key);
		System.out.println("--  append xpath key: " + option.xpath_key);
		System.out.println("--  retain constraint: " + option.pg_retain_key);
		System.out.println("--  retrieve field annotation: " + !option.no_field_anno);
		System.out.println("--  map integer numbers to: " + option.pg_integer.getName());
		System.out.println("--  map decimal numbers to: " + option.pg_decimal.getName());
		if (option.rel_model_ext || option.serial_key) {
			if (!option.hash_algorithm.isEmpty() && !option.hash_size.equals(PgHashSize.debug_string))
				System.out.println("--  hash algorithm: " + option.hash_algorithm);
			else
				System.out.println("--  assumed hash algorithm: " + PgSchemaUtil.def_hash_algorithm);
		}
		if (option.rel_model_ext)
			System.out.println("--  hash key type: " + option.hash_size.getName());
		if (option.serial_key)
			System.out.println("--  searial key type: " + option.ser_size.getName());
		System.out.println("--");
		System.out.println("-- Statistics of schema:");
		System.out.print(def_stat_msg.toString());
		System.out.println("--\n");

		if (option.pg_named_schema) {

			if (!pg_named_schema.isEmpty()) {

				// short of memory in case of huge database
				// pg_named_schema.forEach(named_schema -> System.out.println("DROP SCHEMA IF EXISTS " + PgSchemaUtil.avoidPgReservedWords(named_schema) + " CASCADE;"));
				// System.out.println("");

				pg_named_schema.forEach(named_schema -> System.out.println("CREATE SCHEMA IF NOT EXISTS " + PgSchemaUtil.avoidPgReservedWords(named_schema) + ";"));

				System.out.print("\nSET search_path TO ");

				pg_named_schema.forEach(named_schema -> System.out.print(PgSchemaUtil.avoidPgReservedWords(named_schema) + ", "));

				System.out.println(PgSchemaUtil.pg_public_schema_name + ";\n");

			}

		}

		if (def_anno != null) {

			System.out.println("--");
			System.out.println("-- " + def_anno);
			System.out.println("--\n");

		}

		tables.stream().filter(table -> table.writable).sorted(Comparator.comparingInt(table -> -table.order)).forEach(table -> {

			System.out.println("DROP TABLE IF EXISTS " + table.pgname + " CASCADE;");

		});

		System.out.println("");

		tables.stream().sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> realize(table, true));

		// add primary key/foreign key

		if (!option.pg_retain_key) {

			tables.stream().filter(table -> !table.bridge).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

				table.fields.forEach(field -> {

					if (field.unique_key)
						System.out.println("--ALTER TABLE " + table.pgname + " ADD PRIMARY KEY ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " );\n");

					else if (field.foreign_key) {

						if (!getForeignTable(field).bridge)
							System.out.println("--ALTER TABLE " + table.pgname + " ADD FOREIGN KEY " + field.constraint_name + " REFERENCES " + getPgForeignNameOf(field) + " ( " + PgSchemaUtil.avoidPgReservedWords(field.foreign_field_pname) + " );\n");

					}

				});

			});

		}

		if (option.document_key || option.in_place_document_key) {

			// append unique key constraint from xs:unique

			unq_keys.forEach(key -> {

				PgTable table = getTable(key);

				if (table != null) {

					try {

						if (table.writable)
							appendUniqueKeyConstraint(table, key, true);

					} catch (PgSchemaException e) {
						e.printStackTrace();
					}

				}

				else {

					for (String table_name : key.table_pname.split(" ")) {

						table_name = table_name.replaceFirst(",$", "");

						// wild card

						if (table_name.equals("*")) {

							tables.parallelStream().filter(_table -> _table.writable).forEach(_table -> {

								try {
									appendUniqueKeyConstraint(_table, key, true);
								} catch (PgSchemaException e) {
									e.printStackTrace();
								}

							});


						}

						else {

							table = getTable(key.schema_name, table_name);

							try {

								if (table != null && table.writable)
									appendUniqueKeyConstraint(table, key, true);

							} catch (PgSchemaException e) {
								e.printStackTrace();
							}

						}

					}

				}

			});

			// append unique key constraint from xs:key

			keys.forEach(key -> {

				PgTable table = getTable(key);

				if (table != null) {

					try {

						if (table.writable)
							appendUniqueKeyConstraint(table, key, false);

					} catch (PgSchemaException e) {
						e.printStackTrace();
					}

				}

				else {

					for (String table_name : key.table_pname.split(" ")) {

						table_name = table_name.replaceFirst(",$", "");

						// wild card

						if (table_name.equals("*")) {

							tables.parallelStream().filter(_table -> _table.writable).forEach(_table -> {

								try {
									appendUniqueKeyConstraint(_table, key, false);
								} catch (PgSchemaException e) {
									e.printStackTrace();
								}

							});


						}

						else {

							table = getTable(key.schema_name, table_name);

							try {

								if (table != null && table.writable)
									appendUniqueKeyConstraint(table, key, false);

							} catch (PgSchemaException e) {
								e.printStackTrace();
							}

						}

					}

				}

			});

		}

		// append foreign key constraint

		boolean relational;
		PgTable child_table, parent_table;
		String constraint_name;
		String[] child_field_pnames, parent_field_pnames;

		for (PgForeignKey foreign_key : foreign_keys) {

			relational = false;

			child_table = getChildTable(foreign_key);

			if (child_table != null)
				relational = child_table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			parent_table = getParentTable(foreign_key);

			if (parent_table != null)
				relational = parent_table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			child_field_pnames = foreign_key.child_field_pnames.split(" ");
			parent_field_pnames = foreign_key.parent_field_pnames.split(" ");

			if (child_field_pnames.length == parent_field_pnames.length) {

				for (int i = 0; i < child_field_pnames.length; i++) {

					child_field_pnames[i] = child_field_pnames[i].replaceFirst(",$", "");
					parent_field_pnames[i] = parent_field_pnames[i].replaceFirst(",$", "");

					constraint_name = "KR_" + PgSchemaUtil.avoidPgReservedOps(foreign_key.name) + (child_field_pnames.length > 1 ? "_" + i : "");

					if (constraint_name.length() > PgSchemaUtil.max_enum_len)
						constraint_name = constraint_name.substring(0, PgSchemaUtil.max_enum_len);

					System.out.println("-- (derived from " + option.xs_prefix_ + "keyref[@name='" + foreign_key.name + "'])");
					System.out.println((option.pg_retain_key ? "" : "--") + "ALTER TABLE " + getPgChildNameOf(foreign_key) + " ADD CONSTRAINT " + PgSchemaUtil.avoidPgReservedOps(constraint_name) + " FOREIGN KEY ( " + PgSchemaUtil.avoidPgReservedWords(child_field_pnames[i]) + " ) REFERENCES " + getParentTable(foreign_key).pgname + " ( " + PgSchemaUtil.avoidPgReservedWords(parent_field_pnames[i]) + " ) ON DELETE CASCADE NOT VALID DEFERRABLE INITIALLY DEFERRED;\n");

				}

			}

		}

	}

	/**
	 * Realize PostgreSQL DDL of administrative table.
	 *
	 * @param table current table
	 * @param output whether to output PostgreSQL DDL via standard output
	 */
	private void realizeAdmin(PgTable table, boolean output) {

		// realize parent table at first

		foreign_keys.stream().filter(foreign_key -> foreign_key.schema_name.equals(table.schema_name) && (foreign_key.child_table_xname.equals(table.xname))).map(foreign_key -> getParentTable(foreign_key)).filter(admin_table -> admin_table != null).forEach(admin_table -> {

			realizeAdmin(admin_table, output);

			realize(admin_table, output);

		});

		// set foreign_table_id as table pointer otherwise remove foreign key

		Iterator<PgField> iterator = table.fields.iterator();

		PgField field;
		PgTable admin_table;

		while (iterator.hasNext()) {

			field = iterator.next();

			if (field.foreign_key) {

				admin_table = getForeignTable(field);

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
	 * @param output whether to output PostgreSQL DDL via standard output
	 */
	private void realize(PgTable table, boolean output) {

		if (table == null)
			return;

		if (!output) {
			table.order--;
			return;
		}

		if (!((option.rel_model_ext || !table.relational) && (table.writable || option.show_orphan_table)))
			return;

		System.out.println("--");
		System.out.println("-- " + (table.anno != null && !table.anno.isEmpty() ? table.anno : "No annotation is available"));

		if (!table.pname.equals(table.xname))
			System.out.println("-- canonical name: " + table.xname);

		StringBuilder sb = new StringBuilder();

		if (!table.target_namespace.isEmpty()) {

			for (String namespace_uri : table.target_namespace.split(" "))
				sb.append(namespace_uri + " (" + getPrefixOf(namespace_uri, "default") + "), ");

		}

		else
			sb.append("no namespace, ");

		System.out.println("-- xmlns: " + sb.toString() + "schema location: " + table.schema_location);
		System.out.println("-- type: " + table.xs_type.toString().replaceFirst("^xs_", "").replace('_', ' ') + (!table.writable && option.show_orphan_table ? " (orphan)" : "") + ", content: " + table.content_holder + ", list: " + table.list_holder + ", bridge: " + table.bridge + ", virtual: " + table.virtual + (table.name_collision ? ", name collision: " + table.name_collision : ""));
		System.out.println("--");

		sb.setLength(0);

		List<PgField> fields = table.fields;

		fields.stream().filter(field -> field.enum_name != null && !field.enum_name.isEmpty()).forEach(field -> {

			System.out.println("DROP TYPE IF EXISTS " + table.schema_pgname + field.enum_name + " CASCADE;");

			System.out.print("CREATE TYPE " + table.schema_pgname + field.enum_name + " AS ENUM (");

			for (int i = 0; i < field.enumeration.length; i++) {

				System.out.print(" '" + field.enumeration[i] + "'");

				if (i < field.enumeration.length - 1)
					System.out.print(",");

			}

			System.out.println(" );");

		});

		System.out.println("CREATE TABLE " + table.pgname + " (");

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (option.discarded_document_key_names.contains(field.name) || option.discarded_document_key_names.contains(table.name + "." + field.name))
				continue;

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

				if (!option.document_key && option.in_place_document_key && (option.in_place_document_key_names.contains(field.name) || option.in_place_document_key_names.contains(table.name + "." + field.name)))
					System.out.println("-- ATTRIBUTE, IN-PLACE DOCUMENT KEY");
				else
					System.out.println("-- ATTRIBUTE");

			}

			else if (field.simple_content) {

				if (option.discarded_document_key_names.contains(table.name + "." + field.name))
					continue;

				if (field.simple_primitive_list)
					System.out.print("-- SIMPLE CONTENT AS PRIMITIVE LIST");
				else if (field.simple_attribute)
					System.out.print("-- SIMPLE CONTENT AS ATTRIBUTE, ATTRIBUTE NODE: " + field.foreign_table_xname);
				else if (field.simple_attr_cond)
					System.out.print("-- SIMPLE CONTENT AS CONDITIONAL ATTRIBUTE, ATTRIBUTE NODE: " + field.foreign_table_xname);
				else
					System.out.print("-- SIMPLE CONTENT");

				if (!option.document_key && option.in_place_document_key && option.in_place_document_key_names.contains(table.name + "." + field.name))
					System.out.println(", IN-PLACE DOCUMENT KEY");
				else
					System.out.println("");

			}

			else if (field.any)
				System.out.println("-- ANY ELEMENT");

			else if (field.any_attribute)
				System.out.println("-- ANY ATTRIBUTE");

			else if (!field.primary_key && !option.document_key && option.in_place_document_key && (option.in_place_document_key_names.contains(field.name) || option.in_place_document_key_names.contains(table.name + "." + field.name)))
				System.out.println("-- IN-PLACE DOCUMENT KEY");

			if (!field.system_key && !field.user_key) {

				switch (option.pg_integer) {
				case signed_int_32:
					if (field.getSqlDataType() == java.sql.Types.INTEGER && !field.xs_type.equals(XsFieldType.xs_int) && !field.xs_type.equals(XsFieldType.xs_unsignedInt))
						System.out.println("-- map mathematical concept of integer numbers (" + field.type + ") to " + option.pg_integer.getName());
					break;
				case signed_long_64:
					if (field.getSqlDataType() == java.sql.Types.BIGINT && !field.xs_type.equals(XsFieldType.xs_long) && !field.xs_type.equals(XsFieldType.xs_unsignedLong))
						System.out.println("-- map mathematical concept of integer numbers (" + field.type + ") to " + option.pg_integer.getName());
					break;
				case big_integer:
					if (field.getSqlDataType() == java.sql.Types.DECIMAL && !field.xs_type.equals(XsFieldType.xs_decimal))
						System.out.println("-- must be treated as a BigInteger outside of JDBC");
					break;
				}

				if (field.xs_type.equals(XsFieldType.xs_decimal)) {

					switch (option.pg_decimal) {
					case double_precision_64:
						if (field.getSqlDataType() == java.sql.Types.DOUBLE)
							System.out.println("-- map mathematical concept of decimal numbers (" + field.type + ") to " + option.pg_decimal.getName());
						break;
					case single_precision_32:
						if (field.getSqlDataType() == java.sql.Types.FLOAT)
							System.out.println("-- map mathematical concept of decimal numbers (" + field.type + ") to " + option.pg_decimal.getName());
						break;
					default:
					}

				}

			}

			if (!field.required && field.xrequired) {

				if (field.fixed_value == null || field.fixed_value.isEmpty())
					System.out.println("-- must not be NULL, but dismissed due to name collision");

				else {

					System.out.print("-- must have a constraint ");

					switch (field.xs_type.getJsonSchemaType()) {
					case "\"number\"":
						System.out.print("CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " = " + field.fixed_value + " ) ");
						break;
					default:
						System.out.print("CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " = '" + field.fixed_value + "' ) ");
					}

					System.out.println(", but dismissed due to name collision");

				}
			}

			if (!table.target_namespace.isEmpty() && !field.system_key && !field.user_key && !field.any_content_holder && !field.is_same_namespace_of_table)
				System.out.println("-- xmlns: " + field.target_namespace + " (" + getPrefixOf(field.target_namespace, "n.d.") + ")");

			if (field.enum_name == null || field.enum_name.isEmpty())
				System.out.print("\t" + PgSchemaUtil.avoidPgReservedWords(field.pname) + " " + field.getPgDataType());
			else
				System.out.print("\t" + PgSchemaUtil.avoidPgReservedWords(field.pname) + " " + table.schema_pgname + field.enum_name);

			if ((field.required || !field.xrequired) && field.fixed_value != null && !field.fixed_value.isEmpty()) {

				switch (field.xs_type.getJsonSchemaType()) {
				case "\"number\"":
					System.out.print(" CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " = " + field.fixed_value + " )");
					break;
				default:
					System.out.print(" CHECK ( " + PgSchemaUtil.avoidPgReservedWords(field.pname) + " = '" + field.fixed_value + "' )");
				}

			}

			if (field.required)
				System.out.print(" NOT NULL");

			if (option.pg_retain_key) {

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
	 * Append PostgreSQL unique key.
	 *
	 * @param table current table
	 * @param key PostgreSQL key
	 * @param unique whether identity constraint derived from xs:unique or not (xs:key)
	 * @throws PgSchemaException the pg schema exception
	 */
	private void appendUniqueKeyConstraint(PgTable table, PgKey key, boolean unique) throws PgSchemaException {

		String constraint_name = "UNQ_" + PgSchemaUtil.avoidPgReservedOps(key.table_xname);

		if (constraint_name.length() > PgSchemaUtil.max_enum_len)
			constraint_name = constraint_name.substring(0, PgSchemaUtil.max_enum_len);

		List<String> field_names = new ArrayList<String>();

		try {

			field_names.add(getDocKeyName(table)); // document key should be included

			PgField field = table.getPgField(key.field_pnames);

			if (field != null) {

				if (!field_names.contains(key.field_pnames))
					field_names.add(key.field_pnames);

			}

			else {

				for (String field_name : key.field_pnames.split(" ")) {

					field_name = field_name.replaceFirst(",$", "");

					if (table.getPgField(field_name) == null)
						return;

					if (!field_names.contains(field_name))
						field_names.add(field_name);

				}

			}

			if (field_names.size() == 1 && !option.in_place_document_key)
				return;

			StringBuilder sb = new StringBuilder();

			field_names.forEach(field_name -> sb.append(PgSchemaUtil.avoidPgReservedWords(field_name) + ", "));

			sb.setLength(sb.length() - 2);

			System.out.println("-- (derived from " + option.xs_prefix_ + (unique ? "unique" : "key") + "[@name='" + key.name + "'])");
			System.out.println(((option.pg_retain_key && (unique || option.pg_max_uniq_tuple_size <= 0 || field_names.size() <= option.pg_max_uniq_tuple_size)) ? "" : "--") + "ALTER TABLE " + table.pgname + " ADD CONSTRAINT " + PgSchemaUtil.avoidPgReservedOps(constraint_name) + " UNIQUE ( " + sb.toString() + " );\n");

			sb.setLength(0);

		} finally {
			field_names.clear();
		}

	}

	/**
	 * Count the total number of effective keys.
	 *
	 * @param keys list of identification constraint
	 * @return int the total number of effective keys
	 */
	private int countKeys(List<PgKey> keys) {

		if (!option.document_key && !option.in_place_document_key)
			return 0;

		int total = 0;

		boolean relational;
		PgTable table;

		for (PgKey key : keys) {

			relational = false;

			table = getTable(key);

			if (table != null)
				relational = table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			if (table != null) {

				if (table.writable)
					total++;

			}

			else {

				for (String table_name : key.table_pname.split(" ")) {

					table_name = table_name.replaceFirst(",$", "");

					// wild card

					if (table_name.equals("*"))
						total += tables.parallelStream().filter(_table -> _table.writable).count();

					else {

						table = getTable(key.schema_name, table_name);

						if (table != null && table.writable)
							total++;

					}

				}

			}

		}

		return total;
	}

	/**
	 * Count the total number of effective key references.
	 *
	 * @return int the total number of effective key references
	 */
	private int countKeyReferences() {

		int total = 0;

		boolean relational;
		PgTable child_table, parent_table;
		String[] child_field_pnames, parent_field_pnames;

		for (PgForeignKey foreign_key : foreign_keys) {

			relational = false;

			child_table = getChildTable(foreign_key);

			if (child_table != null)
				relational = child_table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			parent_table = getParentTable(foreign_key);

			if (parent_table != null)
				relational = parent_table.relational;

			if (!option.rel_model_ext && relational)
				continue;

			child_field_pnames = foreign_key.child_field_pnames.split(" ");
			parent_field_pnames = foreign_key.parent_field_pnames.split(" ");

			if (child_field_pnames.length == parent_field_pnames.length)
				total += child_field_pnames.length;

		}

		return total;
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

		if (xml_post_editor.filt_ins != null)
			applyFiltIn(xml_post_editor);

		if (xml_post_editor.filt_outs != null)
			applyFiltOut(xml_post_editor);

		if (xml_post_editor.fill_these != null)
			applyFillThis(xml_post_editor);

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

		option.post_editor_resolved = true;

		String[] key_val, key;
		String schema_name, table_name, field_name;
		PgTable table;

		for (String filt_in : xml_post_editor.filt_ins) {

			key_val = filt_in.split(":");
			key = key_val[0].split("\\.");

			schema_name = null;
			table_name = null;
			field_name = null;

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

			table = getTable(schema_name, table_name);

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
		PgTable nested_table;

		do {

			append_table = false;

			for (PgTable _table : tables) {

				if (_table.filt_out && _table.total_nested_fields > 0) {

					for (PgField field : _table.nested_fields) {

						nested_table = getForeignTable(field);

						if (nested_table.filt_out)
							continue;

						nested_table.filt_out = true;

						append_table = true;

					}

				}

			}

		} while (append_table);

		// inverse filt_out flag and update requirement

		tables.parallelStream().forEach(_table -> {

			_table.filt_out = !_table.filt_out;

			if (_table.filt_out)
				_table.required = _table.writable = false;

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

		option.post_editor_resolved = true;

		String[] key_val, key;
		String schema_name, table_name, field_name;
		PgTable table;

		for (String filt_out : xml_post_editor.filt_outs) {

			key_val = filt_out.split(":");
			key = key_val[0].split("\\.");

			schema_name = null;
			table_name = null;
			field_name = null;

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

			table = getTable(schema_name, table_name);

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

		option.post_editor_resolved = true;

		String[] key_val, key;
		String schema_name, table_name, field_name, filled_text;
		PgTable table;
		PgField field;

		for (String fill_this : xml_post_editor.fill_these) {

			key_val = fill_this.split(":");
			key = key_val[0].split("\\.");

			schema_name = null;
			table_name = null;
			field_name = null;

			filled_text = key_val.length > 1 ? key_val[1] : "";

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

			table = getTable(schema_name, table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " is unselectable (root table).");

			field = table.getField(field_name);

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
	 * @param include_system_keys whether to include system keys in full-text indexing (Lucene specific)
	 * @throws PgSchemaException the pg schema exception
	 */
	public void applyIndexFilter(IndexFilter index_filter, boolean include_system_keys) throws PgSchemaException {

		applyAttr(index_filter);

		if (index_filter.fields != null)
			applyField(index_filter);

		// update indexable flag

		tables.parallelStream().filter(table -> table.required).forEach(table -> table.fields.forEach(field -> field.setIndexable(table, option)));

		tables.parallelStream().filter(table -> table.required).forEach(table -> table.indexable = table.fields.stream().anyMatch(field -> (include_system_keys && field.system_key) || field.indexable));

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

		tables.parallelStream().forEach(table -> table.fields.stream().filter(field -> field.xs_type.equals(XsFieldType.xs_ID)).forEach(field -> field.attr_sel = true));

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

		String[] key;
		String schema_name = PgSchemaUtil.pg_public_schema_name, table_name, field_name;
		PgTable table;

		for (String attr : index_filter.attrs) {

			key = attr.split("\\.");

			table_name = null;
			field_name = null;

			switch (key.length) {
			case 2:
				field_name = key[1];
			case 1:
				table_name = key[0];
				break;
			default:
				throw new PgSchemaException(attr + ": argument should be expressed by \"table_name.column_name\".");
			}

			table = getTable(schema_name, table_name);

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

		String[] key;
		String schema_name = PgSchemaUtil.pg_public_schema_name, table_name, field_name;
		PgTable table;
		PgField field;

		for (String sph_mva : index_filter.sph_mvas) {

			key = sph_mva.split("\\.");

			table_name = null;
			field_name = null;

			switch (key.length) {
			case 2:
				field_name = key[1];
			case 1:
				table_name = key[0];
				break;
			default:
				throw new PgSchemaException(sph_mva + ": argument should be expressed by \"table_name.column_name\".");
			}

			table = getTable(schema_name, table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " is unselectable (root table).");

			field = table.getField(field_name);

			if (field != null) {

				if (field.system_key || field.user_key)
					throw new PgSchemaException(table_name + "." + field_name + " is administrative key.");

				switch (field.xs_type) {
				case xs_integer:
				case xs_nonNegativeInteger:
				case xs_nonPositiveInteger:
				case xs_positiveInteger:
				case xs_negativeInteger:
				case xs_long:
				case xs_unsignedLong:
				case xs_int:
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

		String[] key;
		String schema_name, table_name, field_name;
		PgTable table;
		PgField field;

		for (String index_field : index_filter.fields) {

			key = index_field.split("\\.");

			schema_name = null;
			table_name = null;
			field_name = null;

			if (option.pg_named_schema) {

				switch (key.length) {
				case 3:
					field_name = key[2];
				case 2:
					table_name = key[1];
					schema_name = key[0];
					break;
				default:
					throw new PgSchemaException(index_field + ": argument should be expressed by \"schema_name.table_name.column_name\".");
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
					throw new PgSchemaException(index_field + ": argument should be expressed by \"table_name.column_name\".");
				}

			}

			table = getTable(schema_name, table_name);

			if (table == null)
				throw new PgSchemaException("Not found " + table_name + ".");

			if (table.xs_type.equals(XsTableType.xs_root))
				throw new PgSchemaException(table_name + " is unselectable (root table).");

			if (field_name != null && !field_name.isEmpty()) {

				field = table.getField(field_name);

				if (field != null) {

					if (field.system_key || field.user_key)
						throw new PgSchemaException(table_name + "." + field_name + " is administrative key.");

					field.field_sel = true;

				}

				else
					throw new PgSchemaException("Not found " + table_name + "." + field_name + ".");

			}

			else
				table.fields.stream().filter(_field -> !_field.system_key && !_field.user_key).forEach(_field -> _field.field_sel = true);

		}

		tables.parallelStream().filter(_table -> !_table.fields.stream().anyMatch(_field -> !_field.system_key && !_field.user_key && (_field.attr_sel || _field.field_sel))).forEach(_table -> _table.required = _table.writable = false);

	}

	/** The instance of message digest. */
	@Flat
	public MessageDigest md_hash_key = null;

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return String hash key
	 */
	private String getHashKeyString(String key_name) {

		if (md_hash_key == null) // debug mode
			return key_name;

		try {

			byte[] bytes = md_hash_key.digest(key_name.getBytes(PgSchemaUtil.def_charset));

			switch (option.hash_size) {
			case native_default:
				return "E'\\\\x" + DatatypeConverter.printHexBinary(bytes) + "'"; // PostgreSQL hex format
			case unsigned_long_64:
				BigInteger blong = new BigInteger(bytes);
				return Long.toString(Math.abs(blong.longValue())); // use lower order 64 bits
			case unsigned_int_32:
				BigInteger bint = new BigInteger(bytes);
				return Integer.toString(Math.abs(bint.intValue())); // use lower order 32 bits
			default:
				return key_name;
			}

		} finally {
			md_hash_key.reset();
		}

	}

	/** The current document id. */
	@Flat
	public String document_id = null;

	/**
	 * Return root node of document.
	 *
	 * @param xml_parser XML parser
	 * @return Node root node of document
	 * @throws PgSchemaException the pg schema exception
	 */
	public Node getRootNode(XmlParser xml_parser) throws PgSchemaException {

		if (root_table == null)
			throw new PgSchemaException("Not found root table in XML Schema: " + def_schema_location);

		Node node = xml_parser.document.getDocumentElement();

		// check root element name

		if (!PgSchemaUtil.getUnqualifiedName(node.getNodeName()).equals(root_table.xname))
			throw new PgSchemaException("Not found root element (node_name: " + root_table.xname + ") in XML: " + document_id);

		document_id = xml_parser.document_id;

		return node;
	}

	/**
	 * Close prepared statement.
	 *
	 * @param primary whether to close the primary prepared statement only
	 */
	public void closePreparedStatement(boolean primary) {

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
	 * @param xml_parser XML parser
	 * @param md_hash_key instance of message digest
	 * @param work_dir working directory contains data (CSV/TSV) files
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgCsv(XmlParser xml_parser, MessageDigest md_hash_key, Path work_dir) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		this.md_hash_key = md_hash_key;

		tables.parallelStream().filter(table -> table.writable).forEach(table -> {

			if (table.pathw == null)
				table.pathw = Paths.get(work_dir.toString(), getDataFileNameOf(table));

		});

		// parse root node and write to data (CSV/TSV) file

		PgSchemaNodeParserBuilder npb = new PgSchemaNodeParserBuilder(this, PgSchemaNodeParserType.pg_data_migration);

		npb.xml2PgCsv(root_table, node);

	}

	/**
	 * Close xml2PgCsv.
	 */
	public void closeXml2PgCsv() {

		tables.parallelStream().filter(table -> table.writable).forEach(table -> {

			try {

				if (table.buffw != null)
					table.buffw.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

			table.pathw = null;

		});

	}

	// PostgreSQL data migration via COPY command

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

					if (Files.exists(data_path)) {

						String sql = "COPY " + table.pgname + " FROM STDIN" + (option.pg_tab_delimiter ? "" : " WITH CSV");

						copy_man.copyIn(sql, Files.newInputStream(data_path), PgSchemaUtil.def_buffered_output_stream_buffer_size);

					}

				} catch (SQLException | IOException e) {
					System.err.println("Exception occurred while processing " + (option.pg_tab_delimiter ? "TSV" : "CSV") + " document: " + data_path.toAbsolutePath().toString());
					e.printStackTrace();
				}

			});

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	// PostgreSQL data migration via prepared statement

	/** The database connection. */
	@Flat
	public Connection db_conn = null;

	/** Whether to set all constraints deferred. */
	@Flat
	private boolean pg_deferred = false;

	/**
	 * PostgreSQL data migration.
	 *
	 * @param xml_parser XML parser
	 * @param md_hash_key instance of message digest
	 * @param update whether update or insertion
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgSql(XmlParser xml_parser, MessageDigest md_hash_key, boolean update, Connection db_conn) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		this.md_hash_key = md_hash_key;
		this.db_conn = db_conn;

		boolean sync_rescue = option.sync_rescue;

		if (sync_rescue && !pg_deferred) {

			try {

				Statement stat = db_conn.createStatement();

				stat.execute("SET CONSTRAINTS ALL DEFERRED");

				stat.close();

			} catch (SQLException e) {
				throw new PgSchemaException(e);
			}

			pg_deferred = true;

		}

		if (update || sync_rescue) {

			deleteBeforeUpdate(option.rel_data_ext && option.pg_retain_key);

			if (!option.rel_data_ext || !option.pg_retain_key || sync_rescue)
				update = false;

		}

		// parse root node and send to PostgreSQL

		PgSchemaNodeParserBuilder npb = new PgSchemaNodeParserBuilder(this, PgSchemaNodeParserType.pg_data_migration);

		npb.xml2PgSql(root_table, node, update);

		try {
			db_conn.commit(); // transaction ends
		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Close xml2PgSql.
	 */
	public void closeXml2PgSql() {

		closePreparedStatement(false);

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

			String sql = "SELECT " + doc_id_table.doc_key_pgname + " FROM " + doc_id_table.pgname;

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

	/**
	 * Return document key name.
	 *
	 * @param table current table
	 * @return String document key name
	 * @throws PgSchemaException the pg schema exception
	 */
	private String getDocKeyName(PgTable table) throws PgSchemaException {

		if (option.document_key)
			return option.document_key_name;

		if (!option.in_place_document_key)
			throw new PgSchemaException("Not defined document key, or select either --doc-key or --doc-key-if-no-inplace option.");

		List<PgField> fields = table.fields;

		if (fields.stream().anyMatch(field -> field.document_key))
			return option.document_key_name;

		if (!fields.stream().anyMatch(field -> (field.dtd_data_holder && option.in_place_document_key_names.contains(field.name)) || ((field.dtd_data_holder || field.simple_content) && option.in_place_document_key_names.contains(table.name + "." + field.name)))) {

			if (option.document_key_if_no_in_place)
				return option.document_key_name;

			throw new PgSchemaException("Not found in-place document key in " + table.pname + ", or select --doc-key-if-no-inplace option.");
		}

		return fields.stream().filter(field -> (field.dtd_data_holder && option.in_place_document_key_names.contains(field.name)) || ((field.dtd_data_holder || field.simple_content) && option.in_place_document_key_names.contains(table.name + "." + field.name))).findFirst().get().pname;
	}

	/**
	 * Execute PostgreSQL DELETE command for strict synchronization.
	 *
	 * @param db_conn database connection
	 * @param set set of target document ids
	 * @throws PgSchemaException the pg schema exception
	 */
	public void deleteRows(Connection db_conn, HashSet<String> set) throws PgSchemaException {

		if (!option.rel_data_ext) {

			this.db_conn = db_conn;

			set.forEach(id -> {

				try {

					document_id = id;

					deleteBeforeUpdate(false);

				} catch (PgSchemaException e) {
					e.printStackTrace();
				}

			});

		}

	}

	/**
	 * Execute PostgreSQL DELETE command before INSERT for all tables of current document.
	 *
	 * @param no_pkey delete whether relations not having primary key or uniformly (false)
	 * @throws PgSchemaException the pg schema exception
	 */
	private void deleteBeforeUpdate(boolean no_pkey) throws PgSchemaException {

		try {

			if (has_db_rows.size() == 0)
				initHasDbRows();

			Statement stat = db_conn.createStatement();

			boolean has_doc_id = false;
			boolean sync_rescue = option.sync_rescue;

			if (has_db_rows.get(doc_id_table.pname)) {

				String sql = "DELETE FROM " + doc_id_table.pgname + " WHERE " + doc_id_table.doc_key_pgname + "='" + document_id + "'";

				has_doc_id = stat.executeUpdate(sql) > 0;

			}

			if (has_doc_id || sync_rescue) {

				tables.stream().filter(table -> table.writable && !table.equals(doc_id_table) && ((no_pkey && !table.has_unique_primary_key) || !no_pkey || sync_rescue)).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

					if (has_db_rows.get(table.pname)) {

						try {

							String sql = "DELETE FROM " + table.pgname + " WHERE " + table.doc_key_pgname + "='" + document_id + "'";

							stat.executeUpdate(sql);

						} catch (SQLException e) {
							e.printStackTrace();
						}

					}

				});

			}

			stat.close();

			if (has_doc_id || sync_rescue)
				db_conn.commit(); // transaction ends

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Create PostgreSQL index on document key if not exists.
	 *
	 * @param db_conn database connection
	 * @param pg_option PostgreSQL option
	 * @throws PgSchemaException the pg schema exception
	 */
	public void createDocKeyIndex(Connection db_conn, PgOption pg_option) throws PgSchemaException {

		this.db_conn = db_conn;

		if (has_db_rows.size() == 0)
			initHasDbRows();

		try {

			Statement stat = db_conn.createStatement();

			DatabaseMetaData meta = db_conn.getMetaData();

			has_db_rows.entrySet().stream().filter(arg -> arg.getValue()).map(arg -> arg.getKey()).forEach(table_name -> {

				PgTable table = getPgTable(option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name);

				try {

					ResultSet rset = meta.getIndexInfo(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name, false, true);

					boolean has_index = false;

					String column_name;

					while (rset.next()) {

						column_name = rset.getString("COLUMN_NAME");

						if (column_name != null && column_name.contains(table.doc_key_pname)) {

							has_index = true;

							break;
						}

					}

					rset.close();

					if (!has_index) {

						String sql = "SELECT COUNT( id ) FROM ( SELECT 1 AS id FROM " + table.pgname + " LIMIT " + pg_option.min_rows_for_index + " ) AS trunc";

						rset = stat.executeQuery(sql);

						while (rset.next()) {

							if (rset.getInt(1) == pg_option.min_rows_for_index) {

								sql = "CREATE INDEX IDX_" + PgSchemaUtil.avoidPgReservedOps(table_name) + "_" + PgSchemaUtil.avoidPgReservedOps(table.doc_key_pname) + " ON " + table.pgname + " ( " + table.doc_key_pgname + " )";

								System.out.println(sql);

								stat.execute(sql);

							}

							break;
						}

						rset.close();

					}

				} catch (SQLException e) {
					e.printStackTrace();
				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Drop PostgreSQL index on document key if exists.
	 *
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	public void dropDocKeyIndex(Connection db_conn) throws PgSchemaException {

		this.db_conn = db_conn;

		if (has_db_rows.size() == 0)
			initHasDbRows();

		try {

			Statement stat = db_conn.createStatement();

			DatabaseMetaData meta = db_conn.getMetaData();

			has_db_rows.entrySet().stream().filter(arg -> arg.getValue()).map(arg -> arg.getKey()).forEach(table_name -> {

				PgTable table = getPgTable(option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name);

				try {

					ResultSet rset = meta.getIndexInfo(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name, false, true);

					String index_name, column_name, sql;

					while (rset.next()) {

						index_name = rset.getString("INDEX_NAME");
						column_name = rset.getString("COLUMN_NAME");

						if (index_name != null && index_name.equalsIgnoreCase("IDX_" + PgSchemaUtil.avoidPgReservedOps(table_name) + "_" + PgSchemaUtil.avoidPgReservedOps(table.doc_key_pname)) && column_name != null && column_name.equals(table.doc_key_pname)) {

							sql = "DROP INDEX " + PgSchemaUtil.avoidPgReservedWords(index_name);

							System.out.println(sql);

							stat.execute(sql);

							break;
						}

					}

					rset.close();

				} catch (SQLException e) {
					e.printStackTrace();
				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Create PostgreSQL index on attribute if not exists.
	 *
	 * @param db_conn database connection
	 * @param pg_option PostgreSQL option
	 * @throws PgSchemaException the pg schema exception
	 */
	public void createAttrIndex(Connection db_conn, PgOption pg_option) throws PgSchemaException {

		if (pg_option.max_attr_cols_for_index < 1)
			return;

		this.db_conn = db_conn;

		if (has_db_rows.size() == 0)
			initHasDbRows();

		try {

			Statement stat = db_conn.createStatement();

			DatabaseMetaData meta = db_conn.getMetaData();

			has_db_rows.entrySet().stream().filter(arg -> arg.getValue()).map(arg -> arg.getKey()).forEach(table_name -> {

				PgTable table = getPgTable(option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name);

				if (table.has_attribute) {

					List<PgField> attrs = table.attr_fields.stream().filter(field -> field.attribute &&
							!option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name) &&
							(option.document_key || !option.in_place_document_key || (!option.in_place_document_key_names.contains(field.name) && !option.in_place_document_key_names.contains(table.name + "." + field.name)))).collect(Collectors.toList());

					int attr_count = attrs.size();

					if (attr_count > 0 && attr_count <= pg_option.max_attr_cols_for_index) {

						try {

							ResultSet rset = meta.getIndexInfo(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name, false, true);

							boolean[] has_index = new boolean[attr_count];

							Arrays.fill(has_index, false);

							while (rset.next()) {

								String column_name = rset.getString("COLUMN_NAME");

								if (column_name != null)
									attrs.stream().filter(attr -> column_name.contains(attr.pname)).forEach(attr -> has_index[attrs.indexOf(attr)] = true);

							}

							rset.close();

							boolean has_no_index = false;

							for (boolean _has_index : has_index)
								has_no_index |= !_has_index;

							if (has_no_index) {

								String sql = "SELECT COUNT( id ) FROM ( SELECT 1 AS id FROM " + table.pgname + " LIMIT " + pg_option.min_rows_for_index + " ) AS trunc";

								rset = stat.executeQuery(sql);

								String attr_pname;

								while (rset.next()) {

									if (rset.getInt(1) == pg_option.min_rows_for_index) {

										for (int attr_id = 0; attr_id < attr_count; attr_id++) {

											if (has_index[attr_id])
												continue;

											attr_pname = attrs.get(attr_id).pname;

											sql = "CREATE INDEX IDX_" + PgSchemaUtil.avoidPgReservedOps(table_name) + "_" + PgSchemaUtil.avoidPgReservedOps(attr_pname) + " ON " + table.pgname + " ( " + PgSchemaUtil.avoidPgReservedWords(attr_pname) + " )";

											System.out.println(sql);

											stat.execute(sql);

										}

										break;
									}

								}

								rset.close();

							}

						} catch (SQLException e) {
							e.printStackTrace();
						}

					}

				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Drop PostgreSQL index on attributes if exists.
	 *
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	public void dropAttrIndex(Connection db_conn) throws PgSchemaException {

		this.db_conn = db_conn;

		if (has_db_rows.size() == 0)
			initHasDbRows();

		try {

			Statement stat = db_conn.createStatement();

			DatabaseMetaData meta = db_conn.getMetaData();

			has_db_rows.entrySet().stream().filter(arg -> arg.getValue()).map(arg -> arg.getKey()).forEach(table_name -> {

				PgTable table = getPgTable(option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name);

				if (table.has_attribute) {

					List<PgField> attrs = table.attr_fields.stream().filter(field -> field.attribute &&
							!option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name) &&
							(option.document_key || !option.in_place_document_key || (!option.in_place_document_key_names.contains(field.name) && !option.in_place_document_key_names.contains(table.name + "." + field.name)))).collect(Collectors.toList());

					int attr_count = attrs.size();

					if (attr_count > 0) {

						try {

							ResultSet rset = meta.getIndexInfo(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name, false, true);

							String sql;

							while (rset.next()) {

								String index_name = rset.getString("INDEX_NAME");
								String column_name = rset.getString("COLUMN_NAME");

								if (index_name != null && attrs.stream().anyMatch(attr -> index_name.equalsIgnoreCase("IDX_" + PgSchemaUtil.avoidPgReservedOps(table_name) + "_" + PgSchemaUtil.avoidPgReservedOps(attr.pname))) && column_name != null && attrs.stream().anyMatch(attr -> column_name.equals(attr.pname))) {

									sql = "DROP INDEX " + PgSchemaUtil.avoidPgReservedWords(index_name);

									System.out.println(sql);

									stat.execute(sql);

									break;
								}

							}

							rset.close();

						} catch (SQLException e) {
							e.printStackTrace();
						}

					}

				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Create PostgreSQL index on element if not exists.
	 *
	 * @param db_conn database connection
	 * @param pg_option PostgreSQL option
	 * @throws PgSchemaException the pg schema exception
	 */
	public void createElemIndex(Connection db_conn, PgOption pg_option) throws PgSchemaException {

		if (pg_option.max_elem_cols_for_index < 1)
			return;

		this.db_conn = db_conn;

		if (has_db_rows.size() == 0)
			initHasDbRows();

		try {

			Statement stat = db_conn.createStatement();

			DatabaseMetaData meta = db_conn.getMetaData();

			has_db_rows.entrySet().stream().filter(arg -> arg.getValue()).map(arg -> arg.getKey()).forEach(table_name -> {

				PgTable table = getPgTable(option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name);

				if (table.has_element) {

					List<PgField> elems = table.elem_fields.stream().filter(field -> field.element &&
							!option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name) &&
							(option.document_key || !option.in_place_document_key || (!option.in_place_document_key_names.contains(field.name) && !option.in_place_document_key_names.contains(table.name + "." + field.name)))).collect(Collectors.toList());

					int elem_count = elems.size();

					if (elem_count > 0 && elem_count <= pg_option.max_elem_cols_for_index) {

						try {

							ResultSet rset = meta.getIndexInfo(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name, false, true);

							boolean[] has_index = new boolean[elem_count];

							Arrays.fill(has_index, false);

							while (rset.next()) {

								String column_name = rset.getString("COLUMN_NAME");

								if (column_name != null)
									elems.stream().filter(elem -> column_name.contains(elem.pname)).forEach(elem -> has_index[elems.indexOf(elem)] = true);

							}

							rset.close();

							boolean has_no_index = false;

							for (boolean _has_index : has_index)
								has_no_index |= !_has_index;

							if (has_no_index) {

								String sql = "SELECT COUNT( id ) FROM ( SELECT 1 AS id FROM " + table.pgname + " LIMIT " + pg_option.min_rows_for_index + " ) AS trunc";

								rset = stat.executeQuery(sql);

								String elem_pname;

								while (rset.next()) {

									if (rset.getInt(1) == pg_option.min_rows_for_index) {

										for (int elem_id = 0; elem_id < elem_count; elem_id++) {

											if (has_index[elem_id])
												continue;

											elem_pname = elems.get(elem_id).pname;

											sql = "CREATE INDEX IDX_" + PgSchemaUtil.avoidPgReservedOps(table_name) + "_" + PgSchemaUtil.avoidPgReservedOps(elem_pname) + " ON " + table.pgname + " ( " + PgSchemaUtil.avoidPgReservedWords(elem_pname) + " )";

											System.out.println(sql);

											stat.execute(sql);

										}

										break;
									}

								}

								rset.close();

							}

						} catch (SQLException e) {
							e.printStackTrace();
						}

					}

				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Drop PostgreSQL index on elements if exists.
	 *
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	public void dropElemIndex(Connection db_conn) throws PgSchemaException {

		this.db_conn = db_conn;

		if (has_db_rows.size() == 0)
			initHasDbRows();

		try {

			Statement stat = db_conn.createStatement();

			DatabaseMetaData meta = db_conn.getMetaData();

			has_db_rows.entrySet().stream().filter(arg -> arg.getValue()).map(arg -> arg.getKey()).forEach(table_name -> {

				PgTable table = getPgTable(option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name);

				if (table.has_element) {

					List<PgField> elems = table.elem_fields.stream().filter(field -> field.element &&
							!option.discarded_document_key_names.contains(field.name) && !option.discarded_document_key_names.contains(table.name + "." + field.name) &&
							(option.document_key || !option.in_place_document_key || (!option.in_place_document_key_names.contains(field.name) && !option.in_place_document_key_names.contains(table.name + "." + field.name)))).collect(Collectors.toList());

					int elem_count = elems.size();

					if (elem_count > 0) {

						try {

							ResultSet rset = meta.getIndexInfo(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name, false, true);

							String sql;

							while (rset.next()) {

								String index_name = rset.getString("INDEX_NAME");
								String column_name = rset.getString("COLUMN_NAME");

								if (index_name != null && elems.stream().anyMatch(elem -> index_name.equalsIgnoreCase("IDX_" + PgSchemaUtil.avoidPgReservedOps(table_name) + "_" + PgSchemaUtil.avoidPgReservedOps(elem.pname))) && column_name != null && elems.stream().anyMatch(elem -> column_name.equals(elem.pname))) {

									sql = "DROP INDEX " + PgSchemaUtil.avoidPgReservedWords(index_name);

									System.out.println(sql);

									stat.execute(sql);

									break;
								}

							}

							rset.close();

						} catch (SQLException e) {
							e.printStackTrace();
						}

					}

				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Create PostgreSQL index on simple content if not exists.
	 *
	 * @param db_conn database connection
	 * @param pg_option PostgreSQL option
	 * @throws PgSchemaException the pg schema exception
	 */
	public void createSimpleContIndex(Connection db_conn, PgOption pg_option) throws PgSchemaException {

		this.db_conn = db_conn;

		if (has_db_rows.size() == 0)
			initHasDbRows();

		try {

			Statement stat = db_conn.createStatement();

			DatabaseMetaData meta = db_conn.getMetaData();

			has_db_rows.entrySet().stream().filter(arg -> arg.getValue()).map(arg -> arg.getKey()).forEach(table_name -> {

				PgTable table = getPgTable(option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name);

				if (table.has_simple_content && table.fields.stream().filter(field -> field.foreign_key).count() <= pg_option.max_fks_for_simple_cont_index) {

					List<PgField> simple_conts = table.elem_fields.stream().filter(field -> field.simple_content &&
							!option.discarded_document_key_names.contains(table.name + "." + field.name) &&
							(option.document_key || !option.in_place_document_key || !option.in_place_document_key_names.contains(table.name + "." + field.name))).collect(Collectors.toList());

					int simple_cont_count = simple_conts.size();

					if (simple_cont_count > 0) {

						try {

							ResultSet rset = meta.getIndexInfo(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name, false, true);

							boolean[] has_index = new boolean[simple_cont_count];

							Arrays.fill(has_index, false);

							while (rset.next()) {

								String column_name = rset.getString("COLUMN_NAME");

								if (column_name != null)
									simple_conts.stream().filter(simple_cont -> column_name.contains(simple_cont.pname)).forEach(simple_cont -> has_index[simple_conts.indexOf(simple_cont)] = true);

							}

							rset.close();

							boolean has_no_index = false;

							for (boolean _has_index : has_index)
								has_no_index |= !_has_index;

							if (has_no_index) {

								String sql = "SELECT COUNT( id ) FROM ( SELECT 1 AS id FROM " + table.pgname + " LIMIT " + pg_option.min_rows_for_index + " ) AS trunc";

								rset = stat.executeQuery(sql);

								String simple_cont_pname;

								while (rset.next()) {

									if (rset.getInt(1) == pg_option.min_rows_for_index) {

										for (int simple_cont_id = 0; simple_cont_id < simple_cont_count; simple_cont_id++) {

											if (has_index[simple_cont_id])
												continue;

											simple_cont_pname = simple_conts.get(simple_cont_id).pname;

											sql = "CREATE INDEX IDX_" + PgSchemaUtil.avoidPgReservedOps(table_name) + "_" + PgSchemaUtil.avoidPgReservedOps(simple_cont_pname) + " ON " + table.pgname + " ( " + PgSchemaUtil.avoidPgReservedWords(simple_cont_pname) + " )";

											System.out.println(sql);

											stat.execute(sql);

										}

										break;
									}

								}

								rset.close();

							}

						} catch (SQLException e) {
							e.printStackTrace();
						}

					}

				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Drop PostgreSQL index on simple contents if exists.
	 *
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	public void dropSimpleContIndex(Connection db_conn) throws PgSchemaException {

		this.db_conn = db_conn;

		if (has_db_rows.size() == 0)
			initHasDbRows();

		try {

			Statement stat = db_conn.createStatement();

			DatabaseMetaData meta = db_conn.getMetaData();

			has_db_rows.entrySet().stream().filter(arg -> arg.getValue()).map(arg -> arg.getKey()).forEach(table_name -> {

				PgTable table = getPgTable(option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name);

				if (table.has_simple_content) {

					List<PgField> simple_conts = table.elem_fields.stream().filter(field -> field.simple_content &&
							!option.discarded_document_key_names.contains(table.name + "." + field.name) &&
							(option.document_key || !option.in_place_document_key || !option.in_place_document_key_names.contains(table.name + "." + field.name))).collect(Collectors.toList());

					int simple_cont_count = simple_conts.size();

					if (simple_cont_count > 0) {

						try {

							ResultSet rset = meta.getIndexInfo(null, option.pg_named_schema ? null : PgSchemaUtil.pg_public_schema_name, table_name, false, true);

							String sql;

							while (rset.next()) {

								String index_name = rset.getString("INDEX_NAME");
								String column_name = rset.getString("COLUMN_NAME");

								if (index_name != null && simple_conts.stream().anyMatch(simple_cont -> index_name.equalsIgnoreCase("IDX_" + PgSchemaUtil.avoidPgReservedOps(table_name) + "_" + PgSchemaUtil.avoidPgReservedOps(simple_cont.pname))) && column_name != null && simple_conts.stream().anyMatch(simple_cont -> column_name.equals(simple_cont.pname))) {

									sql = "DROP INDEX " + PgSchemaUtil.avoidPgReservedWords(index_name);

									System.out.println(sql);

									stat.execute(sql);

									break;
								}

							}

							rset.close();

						} catch (SQLException e) {
							e.printStackTrace();
						}

					}

				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/** Whether PostgreSQL table has any rows. */
	private HashMap<String, Boolean> has_db_rows = new HashMap<String, Boolean>();

	/**
	 * Initialize whether PostgreSQL table has any rows.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	private void initHasDbRows() throws PgSchemaException {

		try {

			Statement stat = db_conn.createStatement();

			String sql1 = "SELECT EXISTS( SELECT 1 FROM " + doc_id_table.pgname + " LIMIT 1 )";

			ResultSet rset1 = stat.executeQuery(sql1);

			if (rset1.next())
				has_db_rows.put(doc_id_table.pname, rset1.getBoolean(1));

			rset1.close();

			boolean has_doc_id = has_db_rows.get(doc_id_table.pname);

			tables.stream().filter(table -> table.writable && !table.equals(doc_id_table)).forEach(table -> {

				String table_name = table.pname;

				if (has_doc_id) {

					try {

						String sql2 = "SELECT EXISTS( SELECT 1 FROM " + table.pgname + " LIMIT 1 )";

						ResultSet rset2 = stat.executeQuery(sql2);

						if (rset2.next())
							has_db_rows.put(table_name, rset2.getBoolean(1));

						rset2.close();

					} catch (SQLException e) {
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

	/** The set of PostgreSQL table. */
	private HashSet<String> db_tables = new HashSet<String>();

	/**
	 * Return exact table name in PostgreSQL
	 *
	 * @param table_name table name
	 * @throws PgSchemaException the pg schema exception
	 */
	private String getDbTableName(String table_name) throws PgSchemaException {

		if (db_tables.size() == 0) {

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
	 * Perform consistency test on PostgreSQL DDL.
	 *
	 * @param db_conn database connection
	 * @param strict whether to perform strict consistency test
	 * @throws PgSchemaException the pg schema exception
	 */
	public void testPgSql(Connection db_conn, boolean strict) throws PgSchemaException {

		this.db_conn = db_conn;

		try {

			DatabaseMetaData meta = db_conn.getMetaData();

			Statement stat = db_conn.createStatement();

			tables.stream().filter(table -> table.writable).sorted(Comparator.comparingInt(table -> table.order)).forEach(table -> {

				try {

					String schema_name = getPgSchemaOf(table);
					String table_name = table.pname;
					String db_table_name = getDbTableName(table_name);

					ResultSet rset_col = meta.getColumns(null, schema_name, db_table_name, null);

					while (rset_col.next()) {

						String db_column_name = rset_col.getString("COLUMN_NAME");

						if (!table.fields.stream().filter(field -> !field.omissible).anyMatch(field -> field.pname.equals(db_column_name)))
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + db_column_name + " found without declaration in the data model."); // found without declaration in the data model

					}

					rset_col.close();

					for (PgField field : table.fields) {

						if (field.omissible)
							continue;

						rset_col = meta.getColumns(null, schema_name, db_table_name, field.pname);

						if (!rset_col.next())
							throw new PgSchemaException(db_conn.toString() + " : " + table_name + "." + field.pname + " not found in the relation."); // not found in the relation

						rset_col.close();

					}

					if (strict) {

						rset_col = meta.getColumns(null, schema_name, db_table_name, null);

						List<PgField> fields = table.fields.stream().filter(field -> !field.omissible).collect(Collectors.toList());

						int col_id = 0, db_column_type;
						String db_column_name;
						PgField field;

						while (rset_col.next()) {

							db_column_name = rset_col.getString("COLUMN_NAME");
							db_column_type = rset_col.getInt("DATA_TYPE");

							if (db_column_type == java.sql.Types.NUMERIC) // NUMERIC and DECIMAL are equivalent in PostgreSQL
								db_column_type = java.sql.Types.DECIMAL;

							field = fields.get(col_id++);

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

	// Full-text indexing

	/**
	 * Reset attr_sel_rdy flag.
	 */
	private void resetAttrSelRdy() {

		tables.parallelStream().filter(table -> table.indexable).forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> field.attr_sel_rdy = true));

	}

	// Lucene full-text indexing

	/** The Lucene document. */
	@Flat
	public org.apache.lucene.document.Document lucene_doc = null;

	/**
	 * Lucene document conversion.
	 *
	 * @param xml_parser XML parser
	 * @param md_hash_key instance of message digest
	 * @param index_filter index filter
	 * @param lucene_doc Lucene document
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2LucIdx(XmlParser xml_parser, MessageDigest md_hash_key, IndexFilter index_filter, org.apache.lucene.document.Document lucene_doc) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		this.md_hash_key = md_hash_key;
		this.lucene_doc = lucene_doc;

		resetAttrSelRdy();

		// parse root node and store to Lucene document

		lucene_doc.add(new StringField(option.document_key_name, document_id, Field.Store.YES));

		PgSchemaNodeParserBuilder npb = new PgSchemaNodeParserBuilder(this, PgSchemaNodeParserType.full_text_indexing);

		npb.xml2LucIdx(root_table, node, index_filter);

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

		String child_name;
		boolean sph_field, sph_attr;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			child_name = child.getNodeName();

			sph_field = child_name.equals("sphinx:field");
			sph_attr = child_name.equals("sphinx:attr");

			if (sph_field || sph_attr) {

				if (child.hasAttributes()) {

					Element child_elem = (Element) child;

					String[] name_attr = child_elem.getAttribute("name").replaceFirst(PgSchemaUtil.sph_member_op, "\\.").split("\\.");

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

					String type_attr = child_elem.getAttribute("type");

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
	 * @param data_source whether data source or schema (false)
	 * @throws PgSchemaException the pg schema exception
	 */
	public void writeSphSchema(Path sphinx_schema_path, boolean data_source) throws PgSchemaException {

		try {

			BufferedWriter buffw = Files.newBufferedWriter(sphinx_schema_path);

			buffw.write("<?xml version=\"" + PgSchemaUtil.def_xml_version + "\" encoding=\"" + PgSchemaUtil.def_encoding + "\"?>\n");

			if (data_source) {

				buffw.write("<sphinx:docset xmlns:sphinx=\"" + PgSchemaUtil.sph_namespace_uri + "\">\n");
				buffw.write("<sphinx:schema>\n");

			}

			else
				buffw.write("<sphinx:schema xmlns:sphinx=\"" + PgSchemaUtil.sph_namespace_uri + "\">\n");

			if (data_source) {

				buffw.write("<sphinx:attr name=\"" + option.document_key_name + "\" type=\"string\"/>\n"); // default attr
				buffw.write("<sphinx:field name=\"" + PgSchemaUtil.simple_content_name + "\"/>\n"); // default field

			}

			tables.stream().filter(table -> table.indexable).forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> {

				try {

					buffw.write("<sphinx:attr name=\"" + table.name + PgSchemaUtil.sph_member_op + field.name + "\"");

					String attrs = null;

					switch (field.xs_type) {
					case xs_boolean:
						attrs = " type=\"bool\"";
						break;
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
					case xs_long:
					case xs_unsignedLong:
					case xs_int:
					case xs_unsignedInt:
						attrs = " type=\"int\" bits=\"32\"";
						break;
					case xs_float:
					case xs_double:
					case xs_decimal:
						attrs = " type=\"float\"";
						break;
					case xs_short:
					case xs_unsignedShort:
						attrs = " type=\"int\" bits=\"16\"";
						break;
					case xs_byte:
					case xs_unsignedByte:
						attrs = " type=\"int\" bits=\"8\"";
						break;
					case xs_dateTime:
					case xs_dateTimeStamp:
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

			tables.stream().filter(table -> table.indexable).forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> {

				try {

					String attr_name = table.name + PgSchemaUtil.sph_member_op + field.name;

					switch (field.xs_type) {
					case xs_boolean:
						buffw.write("\txmlpipe_attr_bool       = " + attr_name + "\n");
						break;
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
					case xs_long:
					case xs_unsignedLong:
					case xs_int:
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
					case xs_dateTimeStamp:
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

	/** The buffered writer of Sphinx xmlpipe2 file. */
	@Flat
	public BufferedWriter sph_ds_buffw = null;

	/**
	 * Sphinx xmlpipe2 conversion.
	 *
	 * @param xml_parser XML parser
	 * @param md_hash_key instance of message digest
	 * @param index_filter index filter
	 * @param sph_ds_buffw buffered writer of Sphinx xmlpipe2 file
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2SphDs(XmlParser xml_parser, MessageDigest md_hash_key, IndexFilter index_filter, BufferedWriter sph_ds_buffw) throws PgSchemaException {

		Node node = getRootNode(xml_parser);

		this.md_hash_key = md_hash_key;
		this.sph_ds_buffw = sph_ds_buffw;

		resetAttrSelRdy();

		try {

			sph_ds_buffw.write("<?xml version=\"" + PgSchemaUtil.def_xml_version + "\" encoding=\"" + PgSchemaUtil.def_encoding + "\"?>\n");
			sph_ds_buffw.write("<sphinx:document id=\"" + getHashKeyString(document_id) + "\" xmlns:sphinx=\"" + PgSchemaUtil.sph_namespace_uri + "\">\n");
			sph_ds_buffw.write("<" + option.document_key_name + ">" + StringEscapeUtils.escapeXml10(document_id) + "</" + option.document_key_name + ">\n");

			// parse root node and write to Sphinx xmlpipe2 file

			PgSchemaNodeParserBuilder npb = new PgSchemaNodeParserBuilder(this, PgSchemaNodeParserType.full_text_indexing);

			npb.xml2SphDs(root_table, node, index_filter);

			sph_ds_buffw.write("</sphinx:document>\n");

		} catch (IOException e) {
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

		tables.stream().filter(table -> table.indexable).forEach(table -> table.fields.stream().filter(field -> field.attr_sel).forEach(field -> sph_attrs.add(table.name + PgSchemaUtil.sph_member_op + field.name)));

		return sph_attrs;
	}

	/**
	 * Return set of Sphinx multi-valued attributes.
	 *
	 * @return HashSet set of Sphinx multi-valued attributes
	 */
	public HashSet<String> getSphMVAs() {

		HashSet<String> sph_mvas = new HashSet<String>();

		tables.stream().filter(table -> table.indexable).forEach(table -> table.fields.stream().filter(field -> field.sph_mva).forEach(field -> sph_mvas.add(table.name + PgSchemaUtil.sph_member_op + field.name)));

		return sph_mvas;
	}

}
