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

	/** The size of parameters. */
	private int param_size = 1;

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

				for (int f = 0; f < fields_size; f++) {

					field = fields.get(f);

					if (field.omissible)
						continue;

					param_size++;

				}

				if (update && rel_data_ext)
					upsert = table.has_unique_primary_key;

				// upsert

				if (upsert) {

					if (table.ps2 == null || table.ps2.isClosed()) {

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

					if (table.ps == null || table.ps.isClosed()) {

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

	/** The parameter index. */
	private int par_idx;

	/** The parameter index for upsert. */
	private int ins_idx;

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

			fields.stream().filter(field -> field.nested_key).forEach(field -> setNestedKey(proc_node, field));

			return;
		}

		if (visited) {

			not_complete = null_simple_list = false;

			Arrays.fill(occupied, false);

		}

		par_idx = 1;
		ins_idx = upsert ? param_size : -1;

		try {

			PgField field;

			if (rel_data_ext) {

				for (int f = 0; f < fields_size; f++) {

					field = fields.get(f);

					if (field.omissible)
						continue;

					// document_key

					else if (field.document_key) {

						field.write(ps, par_idx, ins_idx, document_id);
						occupied[f] = true;

					}

					// primary_key

					else if (field.primary_key)
						writePriKey(f, node_test.primary_key);

					// foreign_key

					else if (field.foreign_key) {

						if (parent_table.xname.equals(field.foreign_table_xname))
							writeHashKey(f, node_test.parent_key);

					}

					// nested_key

					else if (field.nested_key) {

						String nested_key;

						if ((nested_key = setNestedKey(proc_node, field)) != null)
							writeHashKey(f, nested_key);

					}

					// attribute, simple_content, element

					else if (field.content_holder) {

						if (setContent(proc_node, field, true)) {

							if (!content.isEmpty()) {

								field.write(ps, par_idx, ins_idx, content);
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

								field.write(ps, par_idx, ins_idx, xml_object);

								xml_object.free();

								occupied[f] = true;

							}

						} catch (TransformerException | IOException | SAXException e) {
							throw new PgSchemaException(e);
						}

					}

					// serial_key

					else if (field.serial_key)
						writeSerKey(f, node_test.node_ordinal);

					// xpath_key

					else if (field.xpath_key)
						writeHashKey(f, current_key.substring(document_id.length()));

					par_idx++;

					if (upsert && !field.primary_key)
						ins_idx++;

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

					else if (field.document_key) {

						field.write(ps, par_idx, ins_idx, document_id);
						occupied[f] = true;

					}

					// attribute, simple_content, element

					else if (field.content_holder) {

						if (setContent(proc_node, field, true)) {

							if (!content.isEmpty()) {

								field.write(ps, par_idx, ins_idx, content);
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

								field.write(ps, par_idx, ins_idx, xml_object);

								xml_object.free();

								occupied[f] = true;

							}

						} catch (TransformerException | IOException | SAXException e) {
							throw new PgSchemaException(e);
						}

					}

					// serial_key

					else if (field.serial_key)
						writeSerKey(f, node_test.node_ordinal);

					// xpath_key

					else if (field.xpath_key)
						writeHashKey(f, current_key.substring(document_id.length()));

					par_idx++;

					if (upsert && !field.primary_key)
						ins_idx++;

				}

			}

			if (null_simple_list && (nested_keys == null || nested_keys.size() == 0))
				return;

			written = true;

			par_idx = 1;

			for (int f = 0; f < fields_size; f++) {

				field = fields.get(f);

				if (field.omissible)
					continue;

				if (!occupied[f])
					ps.setNull(par_idx, field.getSqlDataType());

				par_idx++;

			}

			if (upsert) {

				ins_idx = param_size;

				for (int f = 0; f < fields_size; f++) {

					field = fields.get(f);

					if (field.omissible || field.primary_key)
						continue;

					if (!occupied[f])
						ps.setNull(ins_idx, field.getSqlDataType());

					ins_idx++;

				}

			}

			ps.addBatch();

			if (rel_data_ext)
				ps.executeBatch();

			table.visited_key = current_key;

		} catch (SQLException e) {
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
	private int getHashKeyInt(String key_name) {

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
	private long getHashKeyLong(String key_name) {

		try {

			byte[] hash = md_hash_key.digest(key_name.getBytes());

			BigInteger bint = new BigInteger(hash);

			return Math.abs(bint.longValue()); // use lower order 64bit

		} finally {
			md_hash_key.reset();
		}

	}

	/**
	 * Write hash key via prepared statement.
	 *
	 * @param field_id field id
	 * @param current_key current key
	 * @throws SQLException the SQL exception
	 */
	private void writeHashKey(int field_id, String current_key) throws SQLException {

		switch (hash_size) {
		case native_default:
			byte[] bytes = getHashKeyBytes(current_key);
			ps.setBytes(par_idx, bytes);
			if (upsert)
				ps.setBytes(ins_idx, bytes);
			break;
		case unsigned_int_32:
			int int_key = getHashKeyInt(current_key);
			ps.setInt(par_idx, int_key);
			if (upsert)
				ps.setInt(ins_idx, int_key);
			break;
		case unsigned_long_64:
			long long_key = getHashKeyLong(current_key);
			ps.setLong(par_idx, long_key);
			if (upsert)
				ps.setLong(ins_idx, long_key);
			break;
		default:
			ps.setString(par_idx, current_key);
			if (upsert)
				ps.setString(ins_idx, current_key);
		}

		occupied[field_id] = true;

	}

	/**
	 * Write primary hash key via prepared statement.
	 *
	 * @param field_id field id
	 * @param current_key current key
	 * @throws SQLException the SQL exception
	 */
	private void writePriKey(int field_id, String current_key) throws SQLException {

		int ins_idx = (param_size - 1) * 2;

		switch (hash_size) {
		case native_default:
			byte[] bytes = getHashKeyBytes(current_key);
			ps.setBytes(par_idx, bytes);
			if (upsert)
				ps.setBytes(ins_idx, bytes);
			break;
		case unsigned_int_32:
			int int_key = getHashKeyInt(current_key);
			ps.setInt(par_idx, int_key);
			if (upsert)
				ps.setInt(ins_idx, int_key);
			break;
		case unsigned_long_64:
			long long_key = getHashKeyLong(current_key);
			ps.setLong(par_idx, long_key);
			if (upsert)
				ps.setLong(ins_idx, long_key);
			break;
		default:
			ps.setString(par_idx, current_key);
			if (upsert)
				ps.setString(ins_idx, current_key);
		}

		occupied[field_id] = true;

	}

	/**
	 * Write serial key via prepared statement.
	 *
	 * @param field_id field id
	 * @param ordinal serial id
	 * @throws SQLException the SQL exception
	 */
	private void writeSerKey(int field_id, int ordinal) throws SQLException {

		if (is_def_ser_size) {
			ps.setInt(par_idx, ordinal);
			if (upsert)
				ps.setInt(ins_idx, ordinal);
		}

		else {
			ps.setShort(par_idx, (short) ordinal);
			if (upsert)
				ps.setInt(ins_idx, ordinal);
		}

		occupied[field_id] = true;

	}

}
