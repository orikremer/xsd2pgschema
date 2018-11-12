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

package net.sf.xsd2pgschema.nodeparser;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;

/**
 * Node parser for PostgreSQL data migration.
 *
 * @author yokochi
 */
public class PgSchemaNode2PgSql extends PgSchemaNodeParser {

	/** Whether to update. */
	private boolean update;

	/** The prepared statement. */
	private PreparedStatement ps = null;

	/** Whether to upsert. */
	private boolean upsert = false;

	/** Whether any content was written. */
	private boolean written = false;

	/** Whether field is occupied. */
	private boolean[] occupied;

	/**
	 * Parse root node and send to PostgreSQL.
	 *
	 * @param npb node parser builder
	 * @param table current table
	 * @param root_node root node
	 * @param update whether update or insertion
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2PgSql(final PgSchemaNodeParserBuilder npb, final PgTable table, final Node root_node, final boolean update) throws PgSchemaException {

		super(npb, null, table);

		this.update = update;

		if (table.writable) {

			as_attr = false;

			try {

				PgField field;

				if (update && npb.rel_data_ext)
					upsert = table.has_unique_primary_key;

				// upsert

				if (upsert) {

					if (table.ps2 == null) {

						StringBuilder sql = new StringBuilder();

						sql.append("INSERT INTO " + table.pgname + " VALUES ( ");

						for (int f = 0; f < fields_size; f++) {

							field = fields.get(f);

							if (field.omissible)
								continue;

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + table.schema_pgname + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);
						sql.append(" )");

						sql.append(" ON CONFLICT ( " + table.primary_key_pgname + " ) DO UPDATE SET ");

						for (int f = 0; f < fields_size; f++) {

							field = fields.get(f);

							if (field.omissible || field.primary_key)
								continue;

							sql.append(PgSchemaUtil.avoidPgReservedWords(field.pname) + " = ");

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + table.schema_pgname + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);

						sql.append(" WHERE EXCLUDED." + table.primary_key_pgname + " = ?");

						table.ps2 = npb.db_conn.prepareStatement(sql.toString());

						sql.setLength(0);

					}

					ps = table.ps2;

				}

				// insert

				else {

					if (table.ps == null) {

						StringBuilder sql = new StringBuilder();

						sql.append("INSERT INTO " + table.pgname + " VALUES ( ");

						for (int f = 0; f < fields_size; f++) {

							field = fields.get(f);

							if (field.omissible)
								continue;

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + table.schema_pgname + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);
						sql.append(" )");

						table.ps = npb.db_conn.prepareStatement(sql.toString());

						sql.setLength(0);

					}

					ps = table.ps;

				}

				occupied = new boolean[fields_size];

			} catch (SQLException e) {
				throw new PgSchemaException(e);
			}

		}

		parseRootNode(root_node);

		clear();

	}

	/**
	 * Node parser for PostgreSQL data migration.
	 *
	 * @param npb node parser builder
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param update whether update or insertion
	 * @throws PgSchemaException the pg schema exception
	 */
	protected PgSchemaNode2PgSql(final PgSchemaNodeParserBuilder npb, final PgTable parent_table, final PgTable table, final boolean as_attr, final boolean update) throws PgSchemaException {

		super(npb, parent_table, table);

		this.update = update;

		if (table.writable) {

			this.as_attr = as_attr;

			try {

				PgField field;

				if (update && npb.rel_data_ext)
					upsert = table.has_unique_primary_key;

				// upsert

				if (upsert) {

					if (table.ps2 == null) {

						StringBuilder sql = new StringBuilder();

						sql.append("INSERT INTO " + table.pgname + " VALUES ( ");

						for (int f = 0; f < fields_size; f++) {

							field = fields.get(f);

							if (field.omissible)
								continue;

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + table.schema_pgname + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);
						sql.append(" )");

						sql.append(" ON CONFLICT ( " + table.primary_key_pgname + " ) DO UPDATE SET ");

						for (int f = 0; f < fields_size; f++) {

							field = fields.get(f);

							if (field.omissible || field.primary_key)
								continue;

							sql.append(PgSchemaUtil.avoidPgReservedWords(field.pname) + " = ");

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + table.schema_pgname + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);

						sql.append(" WHERE EXCLUDED." + table.primary_key_pgname + " = ?");

						table.ps2 = npb.db_conn.prepareStatement(sql.toString());

						sql.setLength(0);

					}

					ps = table.ps2;

				}

				// insert

				else {

					if (table.ps == null) {

						StringBuilder sql = new StringBuilder();

						sql.append("INSERT INTO " + table.pgname + " VALUES ( ");

						for (int f = 0; f < fields_size; f++) {

							field = fields.get(f);

							if (field.omissible)
								continue;

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + table.schema_pgname + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);
						sql.append(" )");

						table.ps = npb.db_conn.prepareStatement(sql.toString());

						sql.setLength(0);

					}

					ps = table.ps;

				}

				occupied = new boolean[fields_size];

			} catch (SQLException e) {
				throw new PgSchemaException(e);
			}

		}

	}

	/**
	 * Traverse nested node.
	 *
	 * @param parent_node parent node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void traverseNestedNode(final Node parent_node, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		PgSchemaNode2PgSql node_parser = new PgSchemaNode2PgSql(npb, table, nested_key.table, nested_key.as_attr, update);
		PgSchemaNodeTester node_test = node_parser.node_test;

		node_test.prepForTraversal(table, parent_node, nested_key);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (node_test.isOmissibleNode(node))
					continue;

				if (node_parser.parseProcNode())
					break;

			}

			if (node_parser.visited)
				return;

			node_parser.parseNode(parent_node);

		} finally {
			node_parser.clear();
		}

	}

	/**
	 * Parse processing node.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void parse() throws PgSchemaException {

		if (table.visited_key.equals(current_key = node_test.proc_key))
			return;

		if (table.has_path_restriction)
			extractParentAncestorNodeName();

		Node proc_node = node_test.proc_node;

		super.clear();

		if (!table.writable) {

			if (total_nested_fields > 0)
				table.nested_fields.forEach(field -> setNestedKey(proc_node, field));

			return;
		}

		if (visited) {

			not_complete = null_simple_list = false;

			Arrays.fill(occupied, false);

		}

		try {

			PgField field;

			if (npb.rel_data_ext) {

				for (int f = 0; f < fields_size; f++) {

					field = fields.get(f);

					if (field.omissible)
						continue;

					// document_key

					else if (field.document_key)
						field.write(ps, upsert, npb.document_id);

					// primary_key

					else if (field.primary_key)
						npb.writeHashKey(ps, upsert, field, node_test.primary_key);

					// foreign_key

					else if (field.foreign_key) {

						if (parent_table.xname.equals(field.foreign_table_xname)) {

							npb.writeHashKey(ps, upsert, field, node_test.parent_key);
							occupied[f] = true;

						}

					}

					// nested_key

					else if (field.nested_key) {

						String nested_key;

						if ((nested_key = setNestedKey(proc_node, field)) != null) {

							npb.writeHashKey(ps, upsert, field, nested_key);
							occupied[f] = true;

						}

					}

					// attribute, simple_content, element

					else if (field.content_holder) {

						if (setContent(proc_node, field, true)) {

							if (!content.isEmpty()) {

								field.write(ps, upsert, content);
								occupied[f] = true;

							}

						} else if (field.required) {

							not_complete = true;

							return;
						}

					}

					// any, any_attribute

					else if (field.any_content_holder) {

						try {

							if (npb.setAnyContent(proc_node, table, field)) {

								SQLXML xml_object = npb.db_conn.createSQLXML();

								xml_object.setString(npb.content);

								field.write(ps, upsert, xml_object);

								xml_object.free();

								occupied[f] = true;

							}

						} catch (TransformerException | IOException | SAXException e) {
							throw new PgSchemaException(e);
						}

					}

					// serial_key

					else if (field.serial_key)
						npb.writeSerKey(ps, upsert, field, node_test.node_ordinal);

					// xpath_key

					else if (field.xpath_key)
						npb.writeHashKey(ps, upsert, field, current_key.substring(npb.document_id_len));

				}

			}

			else {

				for (int f = 0; f < fields_size; f++) {

					field = fields.get(f);

					// nested_key should be processed

					if (field.nested_key)
						setNestedKey(proc_node, field);

					if (field.omissible)
						continue;

					// document_key

					else if (field.document_key)
						field.write(ps, upsert, npb.document_id);

					// attribute, simple_content, element

					else if (field.content_holder) {

						if (setContent(proc_node, field, true)) {

							if (!content.isEmpty()) {

								field.write(ps, upsert, content);
								occupied[f] = true;

							}

						} else if (field.required) {

							not_complete = true;

							return;
						}

					}

					// any, any_attribute

					else if (field.any_content_holder) {

						try {

							if (npb.setAnyContent(proc_node, table, field)) {

								SQLXML xml_object = npb.db_conn.createSQLXML();

								xml_object.setString(npb.content);

								field.write(ps, upsert, xml_object);

								xml_object.free();

								occupied[f] = true;

							}

						} catch (TransformerException | IOException | SAXException e) {
							throw new PgSchemaException(e);
						}

					}

					// serial_key

					else if (field.serial_key)
						npb.writeSerKey(ps, upsert, field, node_test.node_ordinal);

					// xpath_key

					else if (field.xpath_key)
						npb.writeHashKey(ps, upsert, field, current_key.substring(npb.document_id_len));

				}

			}

			if (null_simple_list && (total_nested_fields == 0 || nested_keys.size() == 0))
				return;

			written = true;

			for (int f = 0; f < fields_size; f++) {

				field = fields.get(f);

				if (field.omissible || field.primary_key || field.user_key)
					continue;

				if (!occupied[f])
					ps.setNull(field.sql_param_id, field.getSqlDataType());

			}

			if (upsert) {

				for (int f = 0; f < fields_size; f++) {

					field = fields.get(f);

					if (field.omissible || field.primary_key || field.user_key)
						continue;

					if (!occupied[f])
						ps.setNull(field.sql_upsert_id, field.getSqlDataType());

				}

			}

			ps.addBatch();

			if (npb.rel_data_ext)
				ps.executeBatch();

			table.visited_key = current_key;

		} catch (SQLException e) {
			System.err.println("Exception occurred while processing table: " + table.xname);
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Clear node parser.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void clear() throws PgSchemaException {

		if (written && !npb.rel_data_ext) {

			try {
				ps.executeBatch();
			} catch (SQLException e) {
				throw new PgSchemaException(e);
			}

		}

		super.clear();

	}

}
