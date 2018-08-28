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

	/** Whether to fill @default value. */
	protected boolean fill_default_value;

	/** The size of hash key. */
	protected PgHashSize hash_size;

	/** The parent table. */
	protected PgTable parent_table;

	/** The current table. */
	protected PgTable table;

	/** The node parser type. */
	protected PgSchemaNodeParserType parser_type;

	/** The field list. */
	protected List<PgField> fields;

	/** The content of fields. */
	protected String[] values;

	/** The array of nested key. */
	protected List<PgSchemaNestedKey> nested_keys = null;

	/** The count of sibling nodes. */
	protected int node_count = 0;

	/** The ordinal number of sibling node. */
	protected int node_ordinal = 1;

	/** Whether values were adequately filled. */
	protected boolean filled = true;

	/** Whether simple content as primitive list was null. */
	protected boolean null_simple_primitive_list = false;

	/** Whether any nested node has been visited. */
	protected boolean visited;

	/** Whether child node is not nested node (indirect). */
	protected boolean indirect = false;

	/** The processing node. */
	protected Node proc_node;

	/** The current key name. */
	protected String current_key;

	/** The document id. */
	protected String document_id;

	/** The length of document id. */
	protected int document_id_len;

	/** The common content holder for element, simple_content and attribute. */
	protected String content;

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
	 * @param md_hash_key instance of message digest
	 * @param parent_table parent table
	 * @param table current table
	 * @param parser_type node parser type
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNodeParser(final PgSchema schema, final MessageDigest md_hash_key, final PgTable parent_table, final PgTable table, final PgSchemaNodeParserType parser_type) throws PgSchemaException {

		this.schema = schema;
		this.md_hash_key = md_hash_key;
		this.parent_table = parent_table;
		this.table = table;
		this.parser_type = parser_type;

		rel_data_ext = schema.option.rel_data_ext;
		fill_default_value = schema.option.fill_default_value;
		hash_size = schema.option.hash_size;

		document_id = schema.getDocumentId();
		document_id_len = document_id.length();

		fields = table.fields;

		values = new String[fields.size()];

		if (table.nested_fields > 0)
			nested_keys = new ArrayList<PgSchemaNestedKey>();

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

		visited = !table.virtual;

	}

	/**
	 * Parse root node.
	 *
	 * @param proc_node processing node
	 * @throws PgSchemaException the pg schema exception
	 */
	public void parseRootNode(final Node proc_node) throws PgSchemaException {

		parse(new PgSchemaNodeTester(proc_node, current_key = document_id + "/" + table.xname));

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys)
			traverseNestedNode(proc_node, nested_key.asOfRoot(this));

	}

	/**
	 * Parse child node.
	 *
	 * @param node_test node tester
	 * @param nested_key nested key
	 * @return boolean whether current node is the last one
	 * @throws PgSchemaException the pg schema exception
	 */
	protected boolean parseChildNode(final PgSchemaNodeTester node_test, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		parse(node_test);

		if (filled) {

			visited = true;

			if (nested_keys != null) {

				for (PgSchemaNestedKey _nested_key : nested_keys) {

					boolean exists = existsNestedNode(_nested_key.table, node_test.proc_node);

					traverseNestedNode(exists || indirect ? node_test.proc_node : proc_node, _nested_key.asOfChild(node_test, exists));

				}

			}

		}

		return isLastNode(nested_key, node_test.node_count);
	}

	/**
	 * Parse child node.
	 *
	 * @param proc_node processing node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode(final Node proc_node, PgSchemaNestedKey nested_key) throws PgSchemaException {

		parse(new PgSchemaNodeTester(proc_node, nested_key));

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey _nested_key : nested_keys) {

			if (existsNestedNode(_nested_key.table, proc_node))
				traverseNestedNode(proc_node, _nested_key.asOfChild(this));

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
	 * @param node_test node tester
	 * @throws PgSchemaException the pg schema exception
	 */
	abstract protected void parse(final PgSchemaNodeTester node_test) throws PgSchemaException;

	/**
	 * Clear node parser.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void clear() throws PgSchemaException {

		if (nested_keys != null)
			nested_keys.clear();

	}

	/**
	 * Set nested key.
	 *
	 * @param node current node
	 * @param field current field
	 * @param current_key current key
	 * @return String nested key name, null if invalid
	 */
	protected String setNestedKey(final Node node, final PgField field, final String current_key) {

		if (!matchesParentNode(current_key, field.parent_node))
			return null;

		if (field.nested_key_as_attr) {

			if (!node.getParentNode().hasAttributes())
				return null;

		}

		else {

			if (!matchesAncestorNode(current_key, field.ancestor_node))
				return null;

			if (table.has_nested_key_as_attr && current_key.contains("@"))
				return null;

		}

		PgTable nested_table = schema.getTable(field.foreign_table_id);

		PgSchemaNestedKey nested_key = new PgSchemaNestedKey(nested_table, field, current_key);

		nested_keys.add(nested_key);

		return nested_key.current_key;
	}

	/**
	 * Return whether parent node name matches.
	 *
	 * @param current_key current key
	 * @param parent_node the parent node
	 * @return boolean whether parent node's name matches
	 */
	private boolean matchesParentNode(final String current_key, final String parent_node) {

		if (parent_node == null)
			return true;

		String[] path = current_key.substring(document_id_len).split("\\/"); // XPath notation

		String node_name = path[path.length - (table.virtual ? 1 : 2)];

		if (node_name.contains("[")) // list case
			node_name = node_name.substring(0, node_name.lastIndexOf('['));

		for (String _parent_node : parent_node.split(" ")) {

			if (_parent_node.equals(node_name))
				return true;

		}

		return false;
	}

	/**
	 * Return whether ancestor node name matches.
	 *
	 * @param current_key current key
	 * @param ancestor_node the ancestor node
	 * @return boolean whether parent node's name matches
	 */
	private boolean matchesAncestorNode(final String current_key, final String ancestor_node) {

		if (ancestor_node == null)
			return true;

		String[] path = current_key.substring(document_id_len).split("\\/"); // XPath notation

		String node_name = path[path.length - (table.virtual ? 2 : 3)];

		if (node_name.contains("[")) // list case
			node_name = node_name.substring(0, node_name.lastIndexOf('['));

		for (String _ancestor_node : ancestor_node.split(" ")) {

			if (_ancestor_node.equals(node_name))
				return true;

		}

		return false;
	}

	/**
	 * Set content.
	 *
	 * @param node current node
	 * @param field current field
	 * @param current_key current key
	 * @param as_attr whether nested key as attribute
	 * @param pg_enum_limit whether PostgreSQL enumeration length limit is applied
	 * @return boolean whether content is valid
	 */
	protected boolean setContent(final Node node, final PgField field, final String current_key, final boolean as_attr, final boolean pg_enum_limit) {

		content = null;

		if (field.attribute || field.simple_attribute || (field.simple_attr_cond && as_attr))
			setAttribute(node, field);

		else if (field.simple_content)
			setSimpleContent(node, field, current_key);

		else if (field.element)
			setElement(node, field);

		if (applyContentFilter(field, pg_enum_limit)) {

			if (content != null && field.validate(content)) {

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

			Element e = (Element) node;

			content = e.getAttribute(field.xname);

		}

		// simple attribute

		else {

			Node parent_node = node.getParentNode();

			if (!parent_node.hasAttributes())
				return;

			Element e = (Element) parent_node;

			content = e.getAttribute(field.foreign_table_xname);

		}

	}

	/**
	 * Set simple content.
	 *
	 * @param node current node
	 * @param field current field
	 * @param current_key current key
	 * @return boolean whether simple content has value
	 */
	private void setSimpleContent(final Node node, final PgField field, final String current_key) {

		try {

			Node child = node.getFirstChild();

			if (child == null || child.getNodeType() != Node.TEXT_NODE)
				return;

			content = child.getNodeValue();

			if (PgSchemaUtil.null_simple_cont_pattern.matcher(content).matches())
				content = null;

		} finally {

			if (field.simple_primitive_list) {

				if (content != null && fields.parallelStream().anyMatch(_field -> _field.nested_key && matchesParentNode(current_key, _field.parent_node)))
					content = null;

				null_simple_primitive_list = content == null;

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

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name;

			if ((child_name = child.getLocalName()) == null)
				child_name = PgSchemaUtil.getUnqualifiedName(child.getNodeName());

			if (!child_name.equals(field.xname))
				continue;

			content = child.getTextContent();

			return;
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

		if (field.default_value != null && fill_default_value && (content == null || content.isEmpty()))
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

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name;

			if ((child_name = child.getLocalName()) == null)
				child_name = PgSchemaUtil.getUnqualifiedName(child.getNodeName());

			String _child_name = child_name;

			if (fields.parallelStream().filter(field -> field.element).anyMatch(field -> _child_name.equals(field.xname)))
				continue;

			if (!has_any) { // initial instance of new document

				doc = any_doc_builder.newDocument();
				doc_root = doc.createElementNS(schema.getNamespaceUriForPrefix(""), table.pname);

				any_doc_builder.reset();

			}

			Node _child = doc.importNode(child, true);

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

			for (int i = 0; i < attrs.getLength(); i++) {

				Node attr = attrs.item(i);

				if (attr != null) {

					String attr_name = attr.getNodeName();

					if (attr_name.startsWith("xmlns:"))
						prefixes.add(attr_name.substring(6));

				}

			}

			for (int i = 0; i < attrs.getLength(); i++) {

				Node attr = attrs.item(i);

				if (attr != null) {

					String attr_name = attr.getNodeName();

					if (attr_name.startsWith("xmlns"))
						continue;

					if (prefixes.size() > 0 && attr_name.contains(":") && prefixes.contains(attr_name.substring(0, attr_name.indexOf(':'))))
						continue;

					if (fields.parallelStream().filter(field -> field.attribute).anyMatch(field -> attr_name.equals(field.xname)))
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
	 * Return whether nested node exists.
	 *
	 * @param nested_table nested table
	 * @param node current node
	 * @return boolean whether nested node exists
	 */
	protected boolean existsNestedNode(final PgTable nested_table, final Node node) {

		if (nested_table.virtual)
			return nested_table.content_holder;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name;

			if ((child_name = child.getLocalName()) == null)
				child_name = PgSchemaUtil.getUnqualifiedName(child.getNodeName());

			if (!child_name.equals(nested_table.xname))
				continue;

			return true;
		}

		return false;
	}

	/**
	 * Return whether current node is the last one.
	 *
	 * @param nested_key nested key
	 * @param node_count count of sibling nodes
	 * @return boolean whether current node is the last one
	 */
	protected boolean isLastNode(PgSchemaNestedKey nested_key, int node_count) {

		try {
			return node_count <= 1 || (node_ordinal == node_count) || (nested_key.indirect && node_ordinal == nested_key.target_ordinal);
		} finally {

			if (this.node_count == 0)
				this.node_count = node_count;

			node_ordinal++;

		}

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
