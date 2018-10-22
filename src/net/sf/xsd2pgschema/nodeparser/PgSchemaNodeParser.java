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
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
import net.sf.xsd2pgschema.type.PgHashSize;

/**
 * Abstract node parser.
 *
 * @author yokochi
 */
public abstract class PgSchemaNodeParser {

	/** The PostgreSQL data model. */
	protected PgSchema schema;

	/** The relational data extension. */
	protected boolean rel_data_ext;

	/** The parent table. */
	protected PgTable parent_table;

	/** The current table. */
	protected PgTable table;

	/** The node parser type. */
	private PgSchemaNodeParserType parser_type;

	/** The field list. */
	protected List<PgField> fields;

	/** The size of field list. */
	protected int fields_size;

	/** The array of nested key. */
	protected List<PgSchemaNestedKey> nested_keys = null;

	/** The node tester. */
	protected PgSchemaNodeTester node_test = new PgSchemaNodeTester();

	/** Whether virtual table. */
	protected boolean virtual;

	/** Whether parent node as attribute. */
	protected boolean as_attr;

	/** Whether values are not complete. */
	protected boolean not_complete = false;

	/** Whether simple content as primitive list is null. */
	protected boolean null_simple_list = false;

	/** Whether any nested node has been visited. */
	protected boolean visited;

	/** The current key name. */
	protected String current_key;

	/** The parent node name. */
	protected String parent_node_name;

	/** The ancestor node name. */
	protected String ancestor_node_name;

	/** The document id. */
	protected String document_id;

	/** The common content holder for element, simple_content and attribute. */
	protected String content;

	/** The size of hash key. */
	protected PgHashSize hash_size;

	/** The common content holder for xs:any and xs:anyAttribute. */
	protected StringBuilder any_content = null;

	/** The instance of message digest. */
	protected MessageDigest md_hash_key = null;

	/** The document builder for any content. */
	private DocumentBuilder any_doc_builder = null;

	/** The instance of transformer for any content. */
	private Transformer any_transformer = null;

	/** SAX parser for any content. */
	private SAXParser any_sax_parser = null;

	/**
	 * Node parser.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 * @param parser_type node parser type
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNodeParser(final PgSchema schema, final PgTable parent_table, final PgTable table, final PgSchemaNodeParserType parser_type) throws PgSchemaException {

		this.schema = schema;
		this.parent_table = parent_table;
		this.table = table;
		this.parser_type = parser_type;

		document_id = schema.document_id;
		rel_data_ext = schema.option.rel_data_ext;

		fields = table.fields;
		fields_size = fields.size();

		if (table.nested_fields > 0)
			nested_keys = new ArrayList<PgSchemaNestedKey>();

		virtual = table.virtual;
		visited = !virtual;

		if (table.has_any || table.has_any_attribute) {

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

				if (table.has_any) {

					SAXParserFactory spf = SAXParserFactory.newInstance();
					spf.setValidating(false);
					spf.setNamespaceAware(false);

					any_sax_parser = spf.newSAXParser();

				}

			} catch (ParserConfigurationException | TransformerConfigurationException | SAXException e) {
				throw new PgSchemaException(e);
			}

			switch (parser_type) {
			case full_text_indexing:
			case json_conversion:
				any_content = new StringBuilder();
				break;
			default:
			}

		}

	}

	/**
	 * Parse root node.
	 *
	 * @param root_node root node
	 * @throws PgSchemaException the pg schema exception
	 */
	public void parseRootNode(final Node root_node) throws PgSchemaException {

		node_test.setRootNode(root_node, document_id + "/" + table.xname);

		parse();

		if (not_complete || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys)
			traverseNestedNode(root_node, nested_key.asIs(this));

	}

	/**
	 * Parse processing node.
	 * prepForTraversal() should be called beforehand.
	 *
	 * @return boolean whether the node is the last one
	 * @throws PgSchemaException the pg schema exception
	 */
	protected boolean parseProcNode() throws PgSchemaException {

		parse();

		if (!not_complete) {

			visited = true;

			if (nested_keys != null) {

				Node proc_node = node_test.proc_node;

				for (PgSchemaNestedKey nested_key : nested_keys)
					traverseNestedNode(proc_node, nested_key.asOfChild(this));

			}

		}

		return node_test.isLastNode();
	}

	/**
	 * Parse current node.
	 *
	 * @param node current node
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseNode(final Node node) throws PgSchemaException {

		node_test.setProcNode(node);

		parse();

		if (not_complete || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			if (existsNestedNode(node, nested_key.table))
				traverseNestedNode(node, nested_key.asIs(this));

		}

	}

	/**
	 * Abstract traverser of nested node.
	 *
	 * @param parent_node parent node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	abstract protected void traverseNestedNode(final Node parent_node, final PgSchemaNestedKey nested_key) throws PgSchemaException;

	/**
	 * Abstract parser of processing node.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	abstract protected void parse() throws PgSchemaException;

	/**
	 * Clear node parser.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void clear() throws PgSchemaException {

		if (visited && nested_keys != null && nested_keys.size() > 0)
			nested_keys.clear();

	}

	/**
	 * Set nested key.
	 *
	 * @param node current node
	 * @param field current field
	 * @return String nested key name, null if invalid
	 */
	protected String setNestedKey(final Node node, final PgField field) {

		if (table.has_path_restriction) {

			if (!field.matchesParentNodeName(parent_node_name))
				return null;

			if (!field.matchesAncestorNodeName(ancestor_node_name))
				return null;

		}

		if (field.nested_key_as_attr) {

			if (!node.getParentNode().hasAttributes())
				return null;

		}

		else if (table.has_nested_key_as_attr && current_key.contains("@"))
			return null;

		PgTable nested_table = schema.getTable(field.foreign_table_id);

		if (!nested_table.virtual && !field.nested_key_as_attr && !existsNestedNode(node, nested_table))
			return null;

		PgSchemaNestedKey nested_key = new PgSchemaNestedKey(nested_table, field, current_key);

		nested_keys.add(nested_key);

		return nested_key.current_key;
	}

	/**
	 * Return whether nested node exists.
	 *
	 * @param node current node
	 * @param nested_table nested table
	 * @return boolean whether nested node exists
	 */
	protected boolean existsNestedNode(final Node node, final PgTable nested_table) {

		if (nested_table.virtual)
			return nested_table.content_holder;

		String nested_table_xname = nested_table.xname;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (((Element) child).getLocalName().equals(nested_table_xname))
				return true;

		}

		return false;
	}

	/**
	 * Extract parent/ancestor node name.
	 */
	protected void extractParentAncestorNodeName() {

		String[] path = current_key.split("\\/"); // XPath notation

		int path_len = path.length;

		parent_node_name = path[path_len - (virtual ? 1 : 2)];

		if (parent_node_name.contains("[")) // list case
			parent_node_name = parent_node_name.substring(0, parent_node_name.lastIndexOf('['));

		ancestor_node_name = path[path_len - (virtual ? 2 : 3)];

		if (ancestor_node_name.contains("[")) // list case
			ancestor_node_name = ancestor_node_name.substring(0, ancestor_node_name.lastIndexOf('['));

	}

	/**
	 * Set content.
	 *
	 * @param node current node
	 * @param field current field
	 * @param pg_enum_limit whether PostgreSQL enumeration length limit is applied
	 * @return boolean whether content is valid
	 */
	protected boolean setContent(final Node node, final PgField field, final boolean pg_enum_limit) {

		content = null;

		if (field.attribute || field.simple_attribute || (field.simple_attr_cond && as_attr))
			setAttribute(node, field);

		else if (field.simple_content)
			setSimpleContent(node, field);

		else if (field.element)
			setElement(node, field);

		if (applyContentFilter(field, pg_enum_limit)) {

			if (content != null && !content.isEmpty()) { // && field.validate(content)) { skip validation while data migration for performance

				if (pg_enum_limit) // normalize data for PostgreSQL
					content = field.normalize(content);

				return true;
			}

		}

		return false;
	}

	/**
	 * Set any content.
	 *
	 * @param node current node
	 * @param field current field
	 * @return boolean whether content has value
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred
	 * @throws SAXException the SAX exception
	 */
	protected boolean setAnyContent(final Node node, final PgField field) throws TransformerException, IOException, SAXException {

		content = null;

		return (field.any ? setAny(node) : setAnyAttribute(node));
	}

	/**
	 * Set attribute.
	 *
	 * @param node current node
	 * @param field current field
	 */
	private void setAttribute(final Node node, final PgField field) {

		// attribute

		if (field.attribute) {

			if (!node.hasAttributes())
				return;

			content = ((Element) node).getAttribute(field.xname);

		}

		// simple attribute

		else {

			Node parent_node = node.getParentNode();

			if (!parent_node.hasAttributes())
				return;

			content = ((Element) parent_node).getAttribute(field.foreign_table_xname);

		}

	}

	/**
	 * Set simple content.
	 *
	 * @param node current node
	 * @param field current field
	 * @return boolean whether simple content has value
	 */
	private void setSimpleContent(final Node node, final PgField field) {

		try {

			Node child = node.getFirstChild();

			if (child == null || child.getNodeType() != Node.TEXT_NODE)
				return;

			content = child.getNodeValue();

			if (PgSchemaUtil.null_simple_cont_pattern.matcher(content).matches())
				content = null;

		} finally {

			if (field.simple_primitive_list) {

				if (content != null && fields.parallelStream().anyMatch(_field -> _field.nested_key && _field.matchesParentNodeName(parent_node_name)))
					content = null;

				null_simple_list = content == null;

			}

		}

	}

	/**
	 * Set element.
	 *
	 * @param node current node
	 * @param field current field
	 * @return boolean whether element has value
	 */
	private void setElement(final Node node, final PgField field) {

		String field_xname = field.xname;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if (((Element) child).getLocalName().equals(field_xname)) {

				content = child.getTextContent();

				return;
			}

		}

	}

	/**
	 * Apply content filter.
	 *
	 * @param field current field
	 * @param pg_enum_limit whether PostgreSQL enumeration length limit is applied
	 * @return boolean whether content is valid
	 */
	private boolean applyContentFilter(final PgField field, boolean pg_enum_limit) {

		if (field.default_value != null && (content == null || content.isEmpty()) && schema.option.fill_default_value)
			content = field.default_value;

		if (field.fill_this)
			content = field.filled_text;

		if (field.required && (content == null || content.isEmpty()))
			return false;

		if (field.pattern != null && (content == null || !content.matches(field.pattern)))
			return false;

		if (field.filt_out && field.matchesFilterPattern(content))
			return false;

		if (field.enum_name != null) {

			if (pg_enum_limit) {

				if (content != null && content.length() > PgSchemaUtil.max_enum_len)
					content = content.substring(0, PgSchemaUtil.max_enum_len);

				if (!field.matchesEnumeration(content))
					return false;

			}

			else if (!field.matchesXEnumeration(content))
				return false;

		}

		return true;
	}

	/**
	 * Set any.
	 *
	 * @param node current node
	 * @return boolean whether any element exists
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SAXException the SAX exception
	 */
	private boolean setAny(Node node) throws TransformerException, IOException, SAXException {

		boolean has_any = false;

		Document doc = null;
		Element doc_root = null;
		Node _child;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name = ((Element) child).getLocalName();

			if (table.has_element && fields.parallelStream().filter(field -> field.element).anyMatch(field -> child_name.equals(field.xname)))
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
	 * @return boolean whether any attribute exists
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private boolean setAnyAttribute(Node node) throws TransformerException, IOException {

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

					if (table.has_attribute && fields.parallelStream().filter(field -> field.attribute).anyMatch(field -> attr_name.equals(field.xname)))
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

			byte[] bytes = md_hash_key.digest(key_name.getBytes());

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

}
