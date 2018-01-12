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

	/** The database connection. */
	private Connection db_conn;

	/**
	 * Node parser for PostgreSQL data migration.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 * @param db_conn database connection
	 * @throws SQLException the SQL exception
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerConfigurationException the transformer configuration exception
	 */
	public PgSchemaNode2PgSql(final PgSchema schema, final PgTable parent_table, final PgTable table, final Connection db_conn) throws SQLException, ParserConfigurationException, TransformerConfigurationException {

		super(schema, parent_table, table);

		occupied = new boolean[fields.size()];

		this.db_conn = db_conn;

		if (table.required && (rel_data_ext || !table.relational)) {

			StringBuilder sql = new StringBuilder();

			sql.append("INSERT INTO " + PgSchemaUtil.avoidPgReservedWords(table.name) + " VALUES ( ");

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.omissible)
					continue;

				if (field.enum_name == null)
					sql.append("?");
				else
					sql.append("?::" + field.enum_name);

				sql.append(", ");

			}

			sql.setLength(sql.length() - 2);
			sql.append(" )");

			ps = db_conn.prepareStatement(sql.toString());

			sql.setLength(0);

		}

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

		current_key = document_id + "/" + table.name;

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

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			// document_key

			if (field.document_key) {

				if (ps != null)
					setValue(f, param_id, document_id);

			}

			// serial_key

			else if (field.serial_key) {

				if (ps != null)
					setSerKey(f, param_id, key_id);

			}

			// xpath_key

			else if (field.xpath_key) {

				if (ps != null)
					setHashKey(f, param_id, current_key.substring(document_id_len));

			}

			// primary_key

			else if (field.primary_key) {

				if (ps != null && rel_data_ext)
					setHashKey(f, param_id, primary_key);

			}

			// foreign_key

			else if (field.foreign_key) {

				if (parent_table.name.equals(field.foreign_table)) {

					if (ps != null && rel_data_ext)
						setHashKey(f, param_id, parent_key);

				}

			}

			// nested_key

			else if (field.nested_key) {

				if (setNestedKey(field, current_key, key_id)) {

					if (ps != null && rel_data_ext)
						setHashKey(f, param_id, nested_key[nested_fields]);

					nested_fields++;

				}

			}

			// attribute, simple_content, element

			else if (field.attribute || field.simple_content || field.element) {

				if (setContent(proc_node, field, true)) {

					if (ps != null)
						setValue(f, param_id, XsDataType.normalize(field, content));

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

					setValue(f, param_id, xml_object);

				}

			}

			if (!filled)
				break;

			if (!field.omissible)
				param_id++;

		}

		if (filled) {

			write();

			this.proc_node = proc_node;
			this.current_key = current_key;
			this.nested = nested;

		}

	}

	/**
	 * Writer of processing node.
	 *
	 * @throws SQLException the SQL exception
	 */
	private void write() throws SQLException {

		written = false;

		if (ps != null) {

			written = true;

			int param_id = 1;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.omissible)
					continue;

				if (!occupied[f])
					ps.setNull(param_id, XsDataType.getSqlDataType(field));

				param_id++;

			}

			ps.addBatch();

		}

	}

	/**
	 * Invoke nested node (root).
	 *
	 * @throws SQLException the SQL exception
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void invokeRootNestedNode() throws SQLException, ParserConfigurationException, TransformerException, IOException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++)
			schema.parseChildNode2PgSql(proc_node, table, schema.getTable(nested_table_id[n]), current_key, nested_key[n], list_holder[n], table.bridge, 0, db_conn);

	}

	/**
	 * Invoke nested node (child).
	 *
	 * @param node_test node tester
	 * @throws SQLException the SQL exception
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void invokeChildNestedNode(PgSchemaNodeTester node_test) throws SQLException, ParserConfigurationException, TransformerException, IOException {

		if (!filled)
			return;

		invoked = true;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			boolean exists = existsNestedNode(nested_table, node_test.proc_node);

			schema.parseChildNode2PgSql(exists || nested ? node_test.proc_node : proc_node, table, nested_table, node_test.primary_key, nested_key[n], list_holder[n], !exists, exists ? 0 : node_test.key_id, db_conn);

		}

	}

	/**
	 * Invoke nested node (child).
	 *
	 * @throws SQLException the SQL exception
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void invokeChildNestedNode() throws SQLException, ParserConfigurationException, TransformerException, IOException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			if (existsNestedNode(nested_table, proc_node))
				schema.parseChildNode2PgSql(proc_node, table, nested_table, current_key, nested_key[n], list_holder[n], false, 0, db_conn);

		}

	}

	/**
	 * Set value via prepared statement.
	 *
	 * @param field_id field id
	 * @param param_id parameter index
	 * @param value data string
	 * @throws SQLException the SQL exception
	 */
	private void setValue(int field_id, int param_id, String value) throws SQLException {

		XsDataType.setValue(fields.get(field_id), ps, param_id, value);

		occupied[field_id] = true;

	}

	/**
	 * Set XML object via prepared statement.
	 *
	 * @param field_id field id
	 * @param param_id parameter index
	 * @param xml_object XML object
	 * @throws SQLException the SQL exception
	 */
	private void setValue(int field_id, int param_id, SQLXML xml_object) throws SQLException {

		XsDataType.setValue(fields.get(field_id), ps, param_id, xml_object);

		occupied[field_id] = true;

	}

	/**
	 * Set hash key via prepared statement.
	 *
	 * @param field_id field id
	 * @param param_id parameter index
	 * @param proc_key source string
	 * @throws SQLException the SQL exception
	 */
	private void setHashKey(int field_id, int param_id, String key_name) throws SQLException {

		switch (option.hash_size) {
		case native_default:
			ps.setBytes(param_id, schema.getHashKeyBytes(key_name));
			break;
		case unsigned_int_32:
			ps.setInt(param_id, schema.getHashKeyInt(key_name));
			break;
		case unsigned_long_64:
			ps.setLong(param_id, schema.getHashKeyLong(key_name));
			break;
		default:
			ps.setString(param_id, key_name);
		}

		occupied[field_id] = true;

	}

	/**
	 * Set serial key via prepared statement.
	 *
	 * @param field_id field id
	 * @param param_id parameter index
	 * @param key_id serial id
	 * @throws SQLException the SQL exception
	 */
	private void setSerKey(int field_id, int param_id, int key_id) throws SQLException {

		switch (option.ser_size) {
		case unsigned_int_32:
			ps.setInt(param_id, key_id);
			break;
		case unsigned_short_16:
			ps.setShort(param_id, (short) key_id);
			break;
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
