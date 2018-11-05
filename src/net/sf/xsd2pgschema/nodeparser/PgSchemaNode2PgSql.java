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
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;
import net.sf.xsd2pgschema.type.PgSerSize;

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

	/** Whether default serial key size (unsigned int 32 bit). */
	private boolean is_def_ser_size;

	/** Whether field is occupied. */
	private boolean[] occupied;

	/**
	 * Node parser for PostgreSQL data migration.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param update whether update or insertion
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2PgSql(final PgSchema schema, final PgTable parent_table, final PgTable table, final boolean as_attr, final boolean update) throws PgSchemaException {

		super(schema, parent_table, table, PgSchemaNodeParserType.pg_data_migration);

		this.update = update;

		if (table.writable) {

			this.as_attr = as_attr;

			if (rel_data_ext || schema.option.xpath_key) {

				md_hash_key = schema.md_hash_key;
				hash_size = schema.option.hash_size;

			}

			try {

				PgField field;

				if (update && rel_data_ext)
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

						table.ps2 = schema.db_conn.prepareStatement(sql.toString());

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

						table.ps = schema.db_conn.prepareStatement(sql.toString());

						sql.setLength(0);

					}

					ps = table.ps;

				}

				if (schema.option.serial_key)
					is_def_ser_size = schema.option.ser_size.equals(PgSerSize.defaultSize());

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

		PgSchemaNode2PgSql node_parser = new PgSchemaNode2PgSql(schema, table, nested_key.table, nested_key.as_attr, update);
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

			if (rel_data_ext) {

				for (int f = 0; f < fields_size; f++) {

					field = fields.get(f);

					if (field.omissible)
						continue;

					// document_key

					else if (field.document_key)
						field.write(ps, upsert, document_id);

					// primary_key

					else if (field.primary_key)
						writeHashKey(field, node_test.primary_key);

					// foreign_key

					else if (field.foreign_key) {

						if (parent_table.xname.equals(field.foreign_table_xname)) {

							writeHashKey(field, node_test.parent_key);
							occupied[f] = true;

						}

					}

					// nested_key

					else if (field.nested_key) {

						String nested_key;

						if ((nested_key = setNestedKey(proc_node, field)) != null) {

							writeHashKey(field, nested_key);
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

							if (setAnyContent(proc_node, field) && !content.isEmpty()) {

								SQLXML xml_object = schema.db_conn.createSQLXML();

								xml_object.setString(content);

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
						writeSerKey(field, node_test.node_ordinal);

					// xpath_key

					else if (field.xpath_key)
						writeHashKey(field, current_key.substring(document_id.length()));

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
						field.write(ps, upsert, document_id);

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

							if (setAnyContent(proc_node, field) && !content.isEmpty()) {

								SQLXML xml_object = schema.db_conn.createSQLXML();

								xml_object.setString(content);

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
						writeSerKey(field, node_test.node_ordinal);

					// xpath_key

					else if (field.xpath_key)
						writeHashKey(field, current_key.substring(document_id.length()));

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

			if (rel_data_ext)
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
	public void clear() throws PgSchemaException {

		if (written && !rel_data_ext) {

			try {
				ps.executeBatch();
			} catch (SQLException e) {
				throw new PgSchemaException(e);
			}

		}

		super.clear();

	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return bytes[] hash key
	 */
	private byte[] getHashKeyBytes(String key_name) {

		try {

			return md_hash_key.digest(key_name.getBytes(PgSchemaUtil.def_charset));

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
	private int getHashKeyInt(String key_name) {

		try {

			byte[] hash = md_hash_key.digest(key_name.getBytes(PgSchemaUtil.def_charset));

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
	private long getHashKeyLong(String key_name) {

		try {

			byte[] hash = md_hash_key.digest(key_name.getBytes(PgSchemaUtil.def_charset));

			BigInteger bint = new BigInteger(hash);

			return Math.abs(bint.longValue()); // use lower order 64bit

		} finally {
			md_hash_key.reset();
		}

	}

	/**
	 * Write hash key via prepared statement.
	 *
	 * @param fieldd current field
	 * @param current_key current key
	 * @throws SQLException the SQL exception
	 */
	private void writeHashKey(PgField field, String current_key) throws SQLException {

		switch (hash_size) {
		case native_default:
			byte[] bytes = getHashKeyBytes(current_key);
			ps.setBytes(field.sql_param_id, bytes);
			if (upsert)
				ps.setBytes(field.sql_upsert_id, bytes);
			break;
		case unsigned_int_32:
			int int_key = getHashKeyInt(current_key);
			ps.setInt(field.sql_param_id, int_key);
			if (upsert)
				ps.setInt(field.sql_upsert_id, int_key);
			break;
		case unsigned_long_64:
			long long_key = getHashKeyLong(current_key);
			ps.setLong(field.sql_param_id, long_key);
			if (upsert)
				ps.setLong(field.sql_upsert_id, long_key);
			break;
		default:
			ps.setString(field.sql_param_id, current_key);
			if (upsert)
				ps.setString(field.sql_upsert_id, current_key);
		}

	}

	/**
	 * Write serial key via prepared statement.
	 *
	 * @param field current field
	 * @param ordinal serial id
	 * @throws SQLException the SQL exception
	 */
	private void writeSerKey(PgField field, int ordinal) throws SQLException {

		if (is_def_ser_size) {
			ps.setInt(field.sql_param_id, ordinal);
			if (upsert)
				ps.setInt(field.sql_upsert_id, ordinal);
		}

		else {
			ps.setShort(field.sql_param_id, (short) ordinal);
			if (upsert)
				ps.setInt(field.sql_upsert_id, ordinal);
		}

	}

}
