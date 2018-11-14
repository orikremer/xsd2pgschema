/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

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
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.text.StringEscapeUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;
import net.sf.xsd2pgschema.docbuilder.JsonBuilder;
import net.sf.xsd2pgschema.option.IndexFilter;
import net.sf.xsd2pgschema.type.PgHashSize;
import net.sf.xsd2pgschema.type.PgSerSize;

/**
 * Node parser builder.
 *
 * @author yokochi
 */
public class PgSchemaNodeParserBuilder {

	/** The node parser type. */
	protected PgSchemaNodeParserType parser_type;

	/** The PostgreSQL data model. */
	protected PgSchema schema;

	/** The JSON builder. */
	protected JsonBuilder jsonb = null;

	/** The relational data extension. */
	protected boolean rel_data_ext;

	/** Whether to fill @default value. */
	protected boolean fill_default_value;

	/** Whether default serial key size (unsigned int 32 bit). */
	protected boolean is_def_ser_size;

	/** The size of hash key. */
	protected PgHashSize hash_size;

	/** The database connection. */
	protected Connection db_conn;

	/** The instance of message digest. */
	protected MessageDigest md_hash_key;

	/** The document id. */
	protected String document_id;

	/** The length of document id. */
	protected int document_id_len;

	/** The common content holder for element, simple_content and attribute. */
	protected String content;

	/** The common content holder for xs:any and xs:anyAttribute. */
	protected StringBuilder any_content = null;

	/** The document builder for any content. */
	private DocumentBuilder any_doc_builder = null;

	/** The instance of transformer for any content. */
	private Transformer any_transformer = null;

	/** SAX parser for any content. */
	private SAXParser any_sax_parser = null;

	/**
	 * Node parser builder.
	 *
	 * @param schema PostgreSQL data model
	 * @param parser_type node parser type
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNodeParserBuilder(final PgSchema schema, final PgSchemaNodeParserType parser_type) throws PgSchemaException {

		this.parser_type = parser_type;
		this.schema = schema;

		rel_data_ext = schema.option.rel_data_ext;
		fill_default_value = schema.option.fill_default_value;

		document_id = schema.document_id;

		switch (parser_type) {
		case pg_data_migration:
			db_conn = schema.db_conn;
			if (schema.option.serial_key)
				is_def_ser_size = schema.option.ser_size.equals(PgSerSize.defaultSize());
			if (schema.option.xpath_key)
				document_id_len = document_id.length();
		case full_text_indexing:
			hash_size = schema.option.hash_size;
			md_hash_key = schema.md_hash_key;
			break;
		case json_conversion:
			throw new PgSchemaException("Use another instance for JSON conversion: PgSchemaNodeParserBuilder(JsonBuilder)");
		}

		if (schema.hasWildCard()) {

			try {

				DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
				doc_builder_fac.setValidating(false);
				doc_builder_fac.setNamespaceAware(true);
				doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
				doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				any_doc_builder = doc_builder_fac.newDocumentBuilder();

				TransformerFactory tf_factory = TransformerFactory.newInstance();
				any_transformer = tf_factory.newTransformer();
				any_transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
				any_transformer.setOutputProperty(OutputKeys.INDENT, "no");

				if (schema.hasAny()) {

					SAXParserFactory spf = SAXParserFactory.newInstance();
					spf.setValidating(false);
					spf.setNamespaceAware(false);

					any_sax_parser = spf.newSAXParser();

				}

			} catch (ParserConfigurationException | TransformerConfigurationException | SAXException e) {
				throw new PgSchemaException(e);
			}

			if (parser_type.equals(PgSchemaNodeParserType.full_text_indexing))
				any_content = new StringBuilder();

		}

	}

	/**
	 * Node parser builder for JSON conversion.
	 *
	 * @param jsonb JSON builder
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNodeParserBuilder(JsonBuilder jsonb) throws PgSchemaException {

		parser_type = PgSchemaNodeParserType.json_conversion;

		this.jsonb = jsonb;

		schema = jsonb.schema;

		fill_default_value = schema.option.fill_default_value;

		document_id = schema.document_id;

		if (schema.hasWildCard()) {

			try {

				DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
				doc_builder_fac.setValidating(false);
				doc_builder_fac.setNamespaceAware(true);
				doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
				doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				any_doc_builder = doc_builder_fac.newDocumentBuilder();

				TransformerFactory tf_factory = TransformerFactory.newInstance();
				any_transformer = tf_factory.newTransformer();
				any_transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
				any_transformer.setOutputProperty(OutputKeys.INDENT, "no");

				if (schema.hasAny()) {

					SAXParserFactory spf = SAXParserFactory.newInstance();
					spf.setValidating(false);
					spf.setNamespaceAware(false);

					any_sax_parser = spf.newSAXParser();

				}

			} catch (ParserConfigurationException | TransformerConfigurationException | SAXException e) {
				throw new PgSchemaException(e);
			}

			any_content = new StringBuilder();

		}

	}

	/**
	 * Set any content.
	 *
	 * @param node current node
	 * @param table current table
	 * @param field current field
	 * @return boolean whether content has value
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred
	 * @throws SAXException the SAX exception
	 */
	protected boolean setAnyContent(final Node node, final PgTable table, final PgField field) throws TransformerException, IOException, SAXException {
		return field.any ? setAny(node, table): setAnyAttribute(node, table);
	}

	/**
	 * Set any.
	 *
	 * @param node current node
	 * @param table current table
	 * @return boolean whether any element exists
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SAXException the SAX exception
	 */
	private boolean setAny(Node node, PgTable table) throws TransformerException, IOException, SAXException {

		boolean has_any = false;

		Document doc = null;
		Element doc_root = null;
		Node _child;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name = ((Element) child).getLocalName();

			if (table.has_element && table.elem_fields.stream().filter(field -> field.element).anyMatch(field -> child_name.equals(field.xname)))
				continue;

			if (!has_any) { // initial instance of new document

				doc = any_doc_builder.newDocument();
				doc_root = doc.createElementNS(schema.getNamespaceUriForPrefix(""), table.pname);

				any_doc_builder.reset();

			}

			_child = doc.importNode(child, true);

			removeWhiteSpace(_child);
			removePrefixOfElement(_child);

			doc_root.appendChild(_child);

			has_any = true;

		}

		if (has_any) {

			doc.appendChild(doc_root);

			DOMSource source = new DOMSource(doc);
			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);

			any_transformer.transform(source, result);

			content = writer.toString().replace(" xmlns=\"\"", "");

			writer.close();

			any_transformer.reset();

			switch (parser_type) {
			case full_text_indexing:
			case json_conversion:
				PgSchemaNodeAnyExtractor any = new PgSchemaNodeAnyExtractor(parser_type, table.pname, any_content);

				any_sax_parser.parse(new InputSource(new StringReader(content)), any);

				any_sax_parser.reset();
				break;
			default:
			}

		}

		return has_any;
	}

	/**
	 * Remove white space.
	 *
	 * @param node current node
	 */
	private void removeWhiteSpace(Node node) {

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() == Node.TEXT_NODE) {

				if (PgSchemaUtil.null_simple_cont_pattern.matcher(child.getNodeValue()).matches()) {

					node.removeChild(child);
					child = node.getFirstChild();

				}

			}

			if (child.hasChildNodes())
				removeWhiteSpace(child);

		}

	}

	/**
	 * Remove prefix of element.
	 *
	 * @param node current node
	 */
	private void removePrefixOfElement(Node node) {

		if (node.getNodeType() == Node.ELEMENT_NODE) {

			Document owner_doc = node.getOwnerDocument();
			owner_doc.renameNode(node, null, node.getLocalName());

		}

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() == Node.ELEMENT_NODE)
				removePrefixOfElement(child);

		}

	}

	/**
	 * Set any attribute.
	 *
	 * @param node current node
	 * @param table current table
	 * @return boolean whether any attribute exists
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private boolean setAnyAttribute(Node node, PgTable table) throws TransformerException, IOException {

		boolean has_any_attr = false;

		Document doc = null;
		Element doc_root = null;

		if (node.hasAttributes()) {

			NamedNodeMap attrs = node.getAttributes();

			HashSet<String> prefixes = new HashSet<String>();

			Node attr;

			for (int i = 0; i < attrs.getLength(); i++) {

				attr = attrs.item(i);

				if (attr != null) {

					String attr_name = attr.getNodeName();

					if (attr_name.startsWith("xmlns:"))
						prefixes.add(attr_name.substring(6));

				}

			}

			for (int i = 0; i < attrs.getLength(); i++) {

				attr = attrs.item(i);

				if (attr != null) {

					String attr_name = attr.getNodeName();

					if (attr_name.startsWith("xmlns"))
						continue;

					if (prefixes.size() > 0 && attr_name.contains(":") && prefixes.contains(attr_name.substring(0, attr_name.indexOf(':'))))
						continue;

					if (table.has_attribute && table.attr_fields.stream().filter(field -> field.attribute).anyMatch(field -> attr_name.equals(field.xname)))
						continue;

					String attr_value = attr.getNodeValue();

					if (attr_value != null && !attr_value.isEmpty()) {

						switch (parser_type) {
						case pg_data_migration:
							if (!has_any_attr) { // initial instance of new document

								doc = any_doc_builder.newDocument();
								doc_root = doc.createElementNS(schema.getNamespaceUriForPrefix(""), table.pname);

								any_doc_builder.reset();

							}

							doc_root.setAttribute(attr_name, attr_value);
							break;
						case full_text_indexing:
							any_content.append(attr_value + " ");
							break;
						case json_conversion:
							attr_value = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(attr_value));

							if (!attr_value.startsWith("\""))
								attr_value = "\"" + attr_value + "\"";

							any_content.append("/@" + attr_name + ":" + attr_value + "\n");
							break;
						}

						has_any_attr = true;

					}

				}

			}

			prefixes.clear();

		}

		if (has_any_attr && parser_type.equals(PgSchemaNodeParserType.pg_data_migration)) {

			doc.appendChild(doc_root);

			DOMSource source = new DOMSource(doc);
			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);

			any_transformer.transform(source, result);

			content = writer.toString();

			writer.close();

			any_transformer.reset();

		}

		return has_any_attr;
	}

	/**
	 * Determine hash key of source string.
	 *
	 * @param key_name source string
	 * @return String hash key
	 */
	protected String getHashKeyString(String key_name) {

		if (md_hash_key == null) // debug mode
			return key_name;

		try {

			byte[] bytes = md_hash_key.digest(key_name.getBytes(PgSchemaUtil.def_charset));

			switch (hash_size) {
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
	 * @param ps prepared statement
	 * @param upsert whether to upsert
	 * @param field current field
	 * @param current_key current key
	 * @throws SQLException the SQL exception
	 */
	protected void writeHashKey(PreparedStatement ps, boolean upsert, PgField field, String current_key) throws SQLException {

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
	 * @param ps prepared statement
	 * @param upsert whether to upsert
	 * @param field current field
	 * @param ordinal serial id
	 * @throws SQLException the SQL exception
	 */
	protected void writeSerKey(PreparedStatement ps, boolean upsert, PgField field, int ordinal) throws SQLException {

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

	/**
	 * Parse root node and write to data (CSV/TSV) file.
	 *
	 * @param root_table root table
	 * @param root_node root node
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgCsv(PgTable root_table, Node root_node) throws PgSchemaException {

		PgSchemaNode2PgCsv np = new PgSchemaNode2PgCsv(this, null, root_table, false);

		np.parseRootNode(root_node);

		np.clear();

	}

	/**
	 * Parse root node and send to PostgreSQL.
	 *
	 * @param root_table root table
	 * @param root_node root node
	 * @param update whether update or insertion
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2PgSql(PgTable root_table, Node root_node, boolean update) throws PgSchemaException {

		PgSchemaNode2PgSql np = new PgSchemaNode2PgSql(this, null, root_table, false, update);

		np.parseRootNode(root_node);

		np.clear();

	}

	/**
	 * Parse root node and store to Lucene document.
	 *
	 * @param root_table root table
	 * @param root_node root node
	 * @param index_filter index filter
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2LucIdx(PgTable root_table, Node root_node, IndexFilter index_filter) throws PgSchemaException {

		PgSchemaNode2LucIdx np = new PgSchemaNode2LucIdx(this, null, root_table, false, index_filter.min_word_len, index_filter.lucene_numeric_index);

		np.parseRootNode(root_node);

		np.clear();

	}

	/**
	 * Parse root node and write to Sphinx xmlpipe2 file.
	 *
	 * @param root_table root table
	 * @param root_node root node
	 * @param index_filter index filter
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2SphDs(PgTable root_table, Node root_node, IndexFilter index_filter) throws PgSchemaException {

		PgSchemaNode2SphDs np = new PgSchemaNode2SphDs(this, null, root_table, false, index_filter.min_word_len);

		np.parseRootNode(root_node);

		np.clear();

	}

	/**
	 * Parser root node and store to JSON buffer.
	 *
	 * @param root_table root table
	 * @param root_node root node
	 * @throws PgSchemaException the pg schema exception
	 */
	public void xml2Json(PgTable root_table, Node root_node) throws PgSchemaException {

		PgSchemaNode2Json np = new PgSchemaNode2Json(this, null, root_table, false);

		switch (jsonb.type) {
		case column:
		case object:
			np.parseRootNode(root_node, 1);
			break;
		case relational:
			np.parseRootNode(root_node);
			break;
		}

		np.clear();

	}

}
