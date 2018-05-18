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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;

/**
 * Node parser for PostgreSQL data migration.
 *
 * @author yokochi
 */
public class PgSchemaNode2PgSql extends PgSchemaNodeParser {

	/** The prepared statement. */
	private PreparedStatement ps = null;

	/** Whether field is occupied or not. */
	private boolean[] occupied = null;

	/** Whether update or insert. */
	private boolean update;

	/** Whether upsert or insert. */
	private boolean upsert = false;

	/** The size of parameters. */
	private int param_size = 1;

	/** The database connection. */
	private Connection db_conn;

	/** The size of hash key. */
	private PgHashSize hash_size = null;

	/** Whether use default serial key size (unsigned int 32 bit). */
	private boolean def_ser_size = true;

	/**
	 * Node parser for PostgreSQL data migration.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 * @param update whether update or insert
	 * @param db_conn database connection
	 * @throws SQLException the SQL exception
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerConfigurationException the transformer configuration exception
	 */
	public PgSchemaNode2PgSql(final PgSchema schema, final PgTable parent_table, final PgTable table, final boolean update, final Connection db_conn) throws SQLException, ParserConfigurationException, TransformerConfigurationException {

		super(schema, parent_table, table);

		occupied = new boolean[fields.size()];

		this.update = update;

		this.db_conn = db_conn;

		if (table.required && (rel_data_ext || !table.relational)) {

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

				param_size++;

			}

			sql.setLength(sql.length() - 2);
			sql.append(" )");

			if (update && rel_data_ext) {

				PgField pkey = fields.parallelStream().filter(field -> field.primary_key).findFirst().get();

				String pkey_name = PgSchemaUtil.avoidPgReservedWords(pkey.pname);

				if (upsert = pkey.unique_key) {

					sql.append(" ON CONFLICT ( " + pkey_name + " ) DO UPDATE SET ");

					for (int f = 0; f < fields.size(); f++) {

						PgField field = fields.get(f);

						if (field.omissible || field.primary_key)
							continue;

						sql.append(PgSchemaUtil.avoidPgReservedWords(field.pname) + "=");

						if (field.enum_name == null)
							sql.append("?");
						else
							sql.append("?::" + (option.pg_named_schema ? PgSchemaUtil.avoidPgReservedWords(table.pg_schema_name) + "." : "") + field.enum_name);

						sql.append(", ");

					}

					sql.setLength(sql.length() - 2);

					sql.append(" WHERE EXCLUDED." + pkey_name + "=?");

				}

			}

			ps = db_conn.prepareStatement(sql.toString());

			sql.setLength(0);

		}

		hash_size = option.hash_size;

		def_ser_size = option.ser_size.equals(PgSerSize.defaultSize());

	}

	/**
	 * Parse processing node (root).
	 *
	 * @param proc_node processing node
	 * @throws SQLException the SQL exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void parseRootNode(final Node proc_node) throws SQLException, TransformerException, IOException {

		current_key = document_id + "/" + table.xname;

		parse(proc_node, null, current_key, current_key, nested, 1);

	}

	/**
	 * Parse processing node (child).
	 *
	 * @param node_test node tester
	 * @throws SQLException the SQL exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws TransformerException the transformer exception
	 */
	@Override
	public void parseChildNode(final PgSchemaNodeTester node_test) throws IOException, TransformerException, SQLException {

		parse(node_test.proc_node, node_test.parent_key, node_test.primary_key, node_test.current_key, node_test.nested, node_test.key_id);

	}

	/**
	 * Parse processing node (child).
	 *
	 * @param proc_node processing node
	 * @param parent_key key name of parent node
	 * @param proc_key processing key name
	 * @param nested whether it is nested
	 * @throws SQLException the SQL exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void parseChildNode(final Node proc_node, final String parent_key, final String proc_key, final boolean nested) throws SQLException, TransformerException, IOException {

		parse(proc_node, parent_key, proc_key, proc_key, nested, 1);

	}

	/**
	 * Parse processing node.
	 *
	 * @param proc_node processing node
	 * @param parent_key name of parent node
	 * @param primary_key name of primary key
	 * @param current_key name of current node
	 * @param nested whether it is nested
	 * @param key_id ordinal number of current node
	 * @throws SQLException the SQL exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void parse(final Node proc_node, final String parent_key, final String primary_key, final String current_key, final boolean nested, final int key_id) throws SQLException, TransformerException, IOException {

		Arrays.fill(occupied, false);

		filled = true;

		nested_fields = 0;

		int param_id = 1;

		int _param_id = param_size;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			// document_key

			if (field.document_key) {

				if (ps != null) {

					field.writeValue2PgSql(ps, param_id, document_id);

					if (upsert)
						field.writeValue2PgSql(ps, _param_id, document_id);

					occupied[f] = true;

				}

			}

			// serial_key

			else if (field.serial_key) {

				if (ps != null)
					writeSerKey(f, param_id, upsert ? _param_id : -1, key_id);

			}

			// xpath_key

			else if (field.xpath_key) {

				if (ps != null)
					writeHashKey(f, param_id, upsert ? _param_id : -1, current_key.substring(document_id_len));

			}

			// primary_key

			else if (field.primary_key) {

				if (ps != null && rel_data_ext)
					writeHashKey(f, param_id, upsert ? (param_size - 1) * 2 : -1, primary_key);

			}

			// foreign_key

			else if (field.foreign_key) {

				if (parent_table.xname.equals(field.foreign_table_xname)) {

					if (ps != null && rel_data_ext)
						writeHashKey(f, param_id, upsert ? _param_id : -1, parent_key);

				}

			}

			// nested_key

			else if (field.nested_key) {

				if (setNestedKey(field, current_key, key_id)) {

					if (ps != null && rel_data_ext)
						writeHashKey(f, param_id, upsert ? _param_id : -1, nested_key[nested_fields]);

					nested_fields++;

				}

			}

			// attribute, simple_content, element

			else if (field.attribute || field.simple_content || field.element) {

				if (setContent(proc_node, field, true) && !content.isEmpty()) {

					if (ps != null) {

						field.writeValue2PgSql(ps, param_id, content);

						if (upsert)
							field.writeValue2PgSql(ps, _param_id, content);

						occupied[f] = true;

					}

				} else if (field.required) {
					filled = false;
					break;
				}

			}

			// any, any_attribute

			else if ((field.any || field.any_attribute) && ps != null) {

				if (setAnyContent(proc_node, field)) {

					SQLXML xml_object = db_conn.createSQLXML();

					xml_object.setString(content);

					field.writeValue2PgSql(ps, param_id, xml_object);

					if (upsert)
						field.writeValue2PgSql(ps, _param_id, xml_object);

					xml_object.free();

					occupied[f] = true;

				}

			}

			if (!filled)
				break;

			if (!field.omissible) {

				param_id++;

				if (upsert && !field.primary_key)
					_param_id++;

			}

		}

		if (filled) {

			writeNull();

			this.proc_node = proc_node;
			this.current_key = current_key;
			this.nested = nested;

		}

	}

	/**
	 * Null writer of processing node.
	 *
	 * @throws SQLException the SQL exception
	 */
	private void writeNull() throws SQLException {

		written = false;

		if (ps != null) {

			written = true;

			int param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.omissible)
					continue;

				if (!occupied[f])
					ps.setNull(param_id, field.getSqlDataType());

				param_id++;

			}

			if (upsert) {

				int _param_id = param_size;

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.omissible || field.primary_key)
						continue;

					if (!occupied[f])
						ps.setNull(_param_id, field.getSqlDataType());

					_param_id++;

				}

			}

			ps.addBatch();

		}

	}

	/**
	 * Invoke nested node (root).
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeRootNestedNode() throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++)
			schema.parseChildNode2PgSql(proc_node, table, schema.getTable(nested_table_id[n]), current_key, nested_key[n], list_holder[n], table.bridge, 0, update, db_conn);

	}

	/**
	 * Invoke nested node (child).
	 *
	 * @param node_test node tester
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeChildNestedNode(PgSchemaNodeTester node_test) throws PgSchemaException {

		if (!filled)
			return;

		visited = true;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			boolean exists = existsNestedNode(nested_table, node_test.proc_node);

			schema.parseChildNode2PgSql(exists || nested ? node_test.proc_node : proc_node, table, nested_table, node_test.primary_key, nested_key[n], list_holder[n], !exists, exists ? 0 : node_test.key_id, update, db_conn);

		}

	}

	/**
	 * Invoke nested node (child).
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeChildNestedNode() throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			if (existsNestedNode(nested_table, proc_node))
				schema.parseChildNode2PgSql(proc_node, table, nested_table, current_key, nested_key[n], list_holder[n], false, 0, update, db_conn);

		}

	}

	/**
	 * Write hash key via prepared statement.
	 *
	 * @param field_id field id
	 * @param param_id parameter index
	 * @param _param_id parameter index for upsert
	 * @param proc_key source string
	 * @throws SQLException the SQL exception
	 */
	private void writeHashKey(int field_id, int param_id, int _param_id, String key_name) throws SQLException {

		switch (hash_size) {
		case native_default:
			byte[] bytes = schema.getHashKeyBytes(key_name);
			ps.setBytes(param_id, bytes);
			if (_param_id != -1)
				ps.setBytes(_param_id, bytes);
			break;
		case unsigned_int_32:
			int int_key = schema.getHashKeyInt(key_name);
			ps.setInt(param_id, int_key);
			if (_param_id != -1)
				ps.setInt(_param_id, int_key);
			break;
		case unsigned_long_64:
			long long_key = schema.getHashKeyLong(key_name);
			ps.setLong(param_id, long_key);
			if (_param_id != -1)
				ps.setLong(_param_id, long_key);
			break;
		default:
			ps.setString(param_id, key_name);
			if (_param_id != -1)
				ps.setString(_param_id, key_name);
		}

		occupied[field_id] = true;

	}

	/**
	 * Write serial key via prepared statement.
	 *
	 * @param field_id field id
	 * @param param_id parameter index
	 * @param _param_id parameter index for upsert
	 * @param key_id serial id
	 * @throws SQLException the SQL exception
	 */
	private void writeSerKey(int field_id, int param_id, int _param_id, int key_id) throws SQLException {

		if (def_ser_size ) {

			ps.setInt(param_id, key_id);

			if (_param_id != -1)
				ps.setInt(_param_id,  key_id);

		}

		else {

			ps.setShort(param_id, (short) key_id);

			if (_param_id != -1)
				ps.setInt(_param_id,  key_id);

		}

		occupied[field_id] = true;

	}

	/**
	 * Execute batch SQL.
	 *
	 * @throws SQLException the SQL exception
	 */
	public void executeBatch() throws SQLException {

		if (ps != null && !ps.isClosed()) {

			ps.executeBatch();
			ps.close();

		}

	}

}
