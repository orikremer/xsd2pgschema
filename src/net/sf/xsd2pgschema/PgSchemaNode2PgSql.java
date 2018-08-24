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

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * Node parser for PostgreSQL data migration.
 *
 * @author yokochi
 */
public class PgSchemaNode2PgSql extends PgSchemaNodeParser {

	/** The prepared statement. */
	private PreparedStatement ps;

	/** Whether table could have writer. */
	private boolean writable;

	/** Whether field is occupied. */
	private boolean[] occupied;

	/** Whether to update. */
	private boolean update;

	/** Whether to upsert. */
	private boolean upsert = false;

	/** Whether any content was written. */
	protected boolean written = false;

	/** The size of parameters. */
	private int param_size = 1;

	/** The database connection. */
	private Connection db_conn;

	/** The size of hash key. */
	private PgHashSize hash_size;

	/** Whether default serial key size (unsigned int 32 bit). */
	private boolean def_ser_size;

	/**
	 * Node parser for PostgreSQL data migration.
	 *
	 * @param schema PostgreSQL data model
	 * @param md_hash_key instance of message digest
	 * @param parent_table parent table
	 * @param table current table
	 * @param update whether update or insertion
	 * @param db_conn database connection
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2PgSql(final PgSchema schema, final MessageDigest md_hash_key, final PgTable parent_table, final PgTable table, final boolean update, final Connection db_conn) throws PgSchemaException {

		super(schema, md_hash_key, parent_table, table, PgSchemaNodeParserType.pg_data_migration);

		occupied = new boolean[fields.size()];

		this.update = update;

		this.db_conn = db_conn;

		try {

			if (writable = table.writable) {

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.omissible)
						continue;

					param_size++;

				}

				if (update && rel_data_ext)
					upsert = fields.parallelStream().filter(field -> field.primary_key).findFirst().get().unique_key;

				// upsert

				if (upsert) {

					if (table.ps2 == null || table.ps2.isClosed()) {

						StringBuilder sql = new StringBuilder();

						sql.append("INSERT INTO " + schema.getPgNameOf(table) + " VALUES ( ");

						for (int f = 0; f < fields.size(); f++) {

							PgField field = fields.get(f);

							if (field.omissible)
								continue;

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);
						sql.append(" )");

						String pkey_name = PgSchemaUtil.avoidPgReservedWords(fields.parallelStream().filter(field -> field.primary_key).findFirst().get().pname);

						sql.append(" ON CONFLICT ( " + pkey_name + " ) DO UPDATE SET ");

						for (int f = 0; f < fields.size(); f++) {

							PgField field = fields.get(f);

							if (field.omissible || field.primary_key)
								continue;

							sql.append(PgSchemaUtil.avoidPgReservedWords(field.pname) + " = ");

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);

						sql.append(" WHERE EXCLUDED." + pkey_name + " = ?");

						table.ps2 = db_conn.prepareStatement(sql.toString());

						sql.setLength(0);

					}

					ps = table.ps2;

				}

				// insert

				else {

					if (table.ps == null || table.ps.isClosed()) {

						StringBuilder sql = new StringBuilder();

						sql.append("INSERT INTO " + schema.getPgNameOf(table) + " VALUES ( ");

						for (int f = 0; f < fields.size(); f++) {

							PgField field = fields.get(f);

							if (field.omissible)
								continue;

							if (field.enum_name == null)
								sql.append("?");
							else
								sql.append("?::" + (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + field.enum_name);

							sql.append(", ");

						}

						sql.setLength(sql.length() - 2);
						sql.append(" )");

						table.ps = db_conn.prepareStatement(sql.toString());

						sql.setLength(0);

					}

					ps = table.ps;

				}

			}

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

		hash_size = option.hash_size;

		def_ser_size = option.ser_size.equals(PgSerSize.defaultSize());

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

		PgSchemaNode2PgSql node2pgsql = new PgSchemaNode2PgSql(schema, md_hash_key, table, nested_key.table, update, db_conn);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, table, nested_key, node2pgsql.node_count, node2pgsql.node_ordinal);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				if (node2pgsql.parseChildNode(node_test, nested_key))
					break;

			}

			if (node2pgsql.visited)
				return;

			node2pgsql.parseChildNode(parent_node, nested_key);

		} finally {
			node2pgsql.clear();
		}

	}

	/**
	 * Parse processing node.
	 *
	 * @param node_test node tester
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void parse(final PgSchemaNodeTester node_test) throws PgSchemaException {

		proc_node = node_test.proc_node;
		current_key = node_test.current_key;
		indirect = node_test.indirect;

		Arrays.fill(occupied, false);

		filled = true;
		null_simple_primitive_list = false;

		if (nested_keys != null)
			nested_keys.clear();

		int par_idx = 1;
		int ins_idx = upsert ? param_size : -1;

		try {

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				// document_key

				if (field.document_key) {

					if (writable) {

						field.writeValue2PgSql(ps, par_idx, ins_idx, document_id);
						occupied[f] = true;

					}

				}

				// serial_key

				else if (field.serial_key) {

					if (writable)
						writeSerKey(f, par_idx, ins_idx, node_test.node_ordinal);

				}

				// xpath_key

				else if (field.xpath_key) {

					if (writable)
						writeHashKey(f, par_idx, ins_idx, current_key.substring(document_id_len));

				}

				// primary_key

				else if (field.primary_key) {

					if (writable && rel_data_ext)
						writeHashKey(f, par_idx, (param_size - 1) * 2, node_test.primary_key);

				}

				// foreign_key

				else if (field.foreign_key) {

					if (parent_table.xname.equals(field.foreign_table_xname)) {

						if (writable && rel_data_ext)
							writeHashKey(f, par_idx, ins_idx, node_test.parent_key);

					}

				}

				// nested_key

				else if (field.nested_key) {

					String nested_key;

					if ((nested_key = setNestedKey(proc_node, field, current_key)) != null) {

						if (writable && rel_data_ext)
							writeHashKey(f, par_idx, ins_idx, nested_key);

					}

				}

				// attribute, simple_content, element

				else if (field.attribute || field.simple_content || field.element) {

					if (setContent(proc_node, field, current_key, node_test.as_attr, true)) {

						if (writable && !content.isEmpty()) {

							field.writeValue2PgSql(ps, par_idx, ins_idx, content);
							occupied[f] = true;

						}

					} else if (field.required) {
						filled = false;
						break;
					}

				}

				// any, any_attribute

				else if (field.any || field.any_attribute) {

					try {

						if (writable && setAnyContent(proc_node, field) && !content.isEmpty()) {

							SQLXML xml_object = db_conn.createSQLXML();

							xml_object.setString(content);

							field.writeValue2PgSql(ps, par_idx, ins_idx, xml_object);

							xml_object.free();

							occupied[f] = true;

						}

					} catch (TransformerException | IOException | SAXException e) {
						throw new PgSchemaException(e);
					}

				}

				if (!filled)
					break;

				if (!field.omissible) {

					par_idx++;

					if (upsert && !field.primary_key)
						ins_idx++;

				}

			}

			if (!writable || !filled || (null_simple_primitive_list && (nested_keys == null || nested_keys.size() == 0)))
				return;

			written = true;

			par_idx = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.omissible)
					continue;

				if (!occupied[f])
					ps.setNull(par_idx, field.getSqlDataType());

				par_idx++;

			}

			if (upsert) {

				ins_idx = param_size;

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

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
	 * @param par_idx parameter index
	 * @param ins_idx parameter index for upsert
	 * @param current_key current key
	 * @throws SQLException the SQL exception
	 */
	private void writeHashKey(int field_id, int par_idx, int ins_idx, String current_key) throws SQLException {

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
	 * @param par_idx parameter index
	 * @param ins_idx parameter index for upsert
	 * @param ordinal serial id
	 * @throws SQLException the SQL exception
	 */
	private void writeSerKey(int field_id, int par_idx, int ins_idx, int ordinal) throws SQLException {

		if (def_ser_size) {
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
