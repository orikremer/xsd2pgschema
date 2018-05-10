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
import java.io.StringWriter;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * Abstract node parser.
 *
 * @author yokochi
 */
public abstract class PgSchemaNodeParser {

	/** The PostgreSQL data model. */
	protected PgSchema schema = null;

	/** The PostgreSQL data model option. */
	protected PgSchemaOption option = null;

	/** The relational data extension. */
	protected boolean rel_data_ext;

	/** The parent table. */
	protected PgTable parent_table = null;

	/** The current table. */
	protected PgTable table = null;

	/** The field list. */
	protected List<PgField> fields = null;

	/** The content of fields. */
	protected String[] values = null;

	/** The number of nested fields. */
	protected int nested_fields = 0;

	/** Whether list holder. */
	protected boolean[] list_holder = null;

	/** The nested table id. */
	protected int[] nested_table_id = null;

	/** The nested key name. */
	protected String[] nested_key = null;

	/** Whether values were filled. */
	protected boolean filled = true;

	/** Whether any content was written. */
	protected boolean written = false;

	/** Whether any nested node was invoked. */
	protected boolean invoked = false;

	/** Whether nested node. */
	protected boolean nested = false;

	/** The processing node. */
	protected Node proc_node;

	/** The current key name. */
	protected String current_key;

	/** The document id. */
	protected String document_id = null;

	/** The length of document id. */
	protected int document_id_len;

	/** The common content holder for element, simple_content and attribute. */
	protected String content;

	/** The document builder for xs:any and xs:anyAttribute. */
	private DocumentBuilder doc_builder = null;

	/** The XML content of xs:any and xs:anyAttribute. */
	private Document doc = null;

	/** The root element of the XML content. */
	private Element doc_root = null;

	/** The transformer of the XML content. */
	private Transformer transformer = null;

	/**
	 * Node parser.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerConfigurationException the transformer configuration exception
	 */
	public PgSchemaNodeParser(final PgSchema schema, final PgTable parent_table, final PgTable table) throws ParserConfigurationException, TransformerConfigurationException {

		this.schema = schema;
		option = schema.option;

		rel_data_ext = option.rel_data_ext;

		this.parent_table = parent_table;
		this.table = table;

		document_id = schema.getDocumentId();
		document_id_len = document_id.length();

		fields = table.fields;

		values = new String[table.fields.size()];

		if (table.nested_fields > 0) {

			list_holder = new boolean[table.nested_fields];
			nested_table_id = new int[table.nested_fields];
			nested_key = new String[table.nested_fields];

		}

		if (table.has_any || table.has_any_attribute) {

			DocumentBuilderFactory doc_factory = DocumentBuilderFactory.newInstance();
			doc_builder = doc_factory.newDocumentBuilder();

			TransformerFactory tf_factory = TransformerFactory.newInstance();
			transformer = tf_factory.newTransformer();

			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			transformer.setOutputProperty(OutputKeys.INDENT, "no");

		}

	}

	/**
	 * Abstract parser of processing node (root).
	 *
	 * @param node processing node
	 * @throws Exception the exception
	 */
	abstract public void parseRootNode(final Node node) throws Exception;

	/**
	 * Abstract parser of processing node (child).
	 *
	 * @param node_test node tester
	 * @throws Exception the exception
	 */
	abstract public void parseChildNode(final PgSchemaNodeTester node_test) throws Exception;

	/**
	 * Abstract parser of processing node (child).
	 *
	 * @param node processing node
	 * @param parent_key key name of parent node
	 * @param proc_key processing key name
	 * @param nested whether it is nested
	 * @throws Exception the exception
	 */
	abstract public void parseChildNode(final Node node, final String parent_key, final String proc_key, final boolean nested) throws Exception;

	/**
	 * Abstract invoker nested node (root).
	 *
	 * @throws Exception the exception
	 */
	abstract public void invokeRootNestedNode() throws Exception;

	/**
	 * Abstract invoker nested node (child).
	 *
	 * @param node_test node tester
	 * @throws Exception the exception
	 */
	abstract public void invokeChildNestedNode(PgSchemaNodeTester node_test) throws Exception;

	/**
	 * Abstract invoker nested node (child).
	 *
	 * @throws Exception the exception
	 */
	abstract public void invokeChildNestedNode() throws Exception;

	/**
	 * Set nested key.
	 *
	 * @param field current field
	 * @param proc_key processing key name
	 * @param key_id ordinal number of current node
	 * @return boolean whether success or not
	 */
	public boolean setNestedKey(final PgField field, final String proc_key, final int key_id) {

		if (!matchesParentNode(proc_key, field.parent_node))
			return false;

		if (!matchesAncestorNode(proc_key, field.ancestor_node))
			return false;

		PgTable nested_table = schema.getTable(field.foreign_table_id);

		list_holder[nested_fields] = field.list_holder;
		nested_table_id[nested_fields] = field.foreign_table_id;
		nested_key[nested_fields] = proc_key;

		if (!nested_table.virtual)
			nested_key[nested_fields] += "/" + field.foreign_table_name; // XPath child

		return true;
	}

	/**
	 * Return whether parent node's name matches.
	 *
	 * @param proc_key processing key name
	 * @param parent_node the parent node
	 * @return boolean whether parent node's name matches
	 */
	private boolean matchesParentNode(final String proc_key, final String parent_node) {

		if (parent_node == null)
			return true;

		String[] path = proc_key.substring(document_id_len).split("\\/"); // XPath notation

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
	 * Return whether ancestor node's name matches.
	 *
	 * @param proc_key processing key name
	 * @param ancestor_node the ancestor node
	 * @return boolean whether parent node's name matches
	 */
	private boolean matchesAncestorNode(final String proc_key, final String ancestor_node) {

		if (ancestor_node == null)
			return true;

		String[] path = proc_key.substring(document_id_len).split("\\/"); // XPath notation

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
	 * @param pg_enum_limit whether PostgreSQL enumeration length limit is applied
	 * @return boolean whether content has value
	 */
	public boolean setContent(final Node node, final PgField field, final boolean pg_enum_limit) {

		content = null;

		if (field.attribute)
			setAttribute(node, field);

		else if (field.simple_content)
			setSimpleContent(node);

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
	 */
	public boolean setAnyContent(final Node node, final PgField field) throws TransformerException, IOException {

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

		if (!node.hasAttributes())
			return;

		Element e = (Element) node;

		content = e.getAttribute(field.xname);

	}

	/**
	 * Set simple content.
	 *
	 * @param node current node
	 * @return boolean whether simple content has value
	 */
	private void setSimpleContent(final Node node) {

		Node child = node.getFirstChild();

		if (child == null || child.getNodeType() != Node.TEXT_NODE)
			return;

		content = child.getNodeValue();

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
				child_name = option.getUnqualifiedName(child.getNodeName());

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
	 * @return boolean whether content passes filter
	 */
	private boolean applyContentFilter(final PgField field, boolean pg_enum_limit) {

		if (field.default_value != null && (content == null || content.isEmpty()))
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
	 */
	private boolean setAny(Node node) throws TransformerException, IOException {

		boolean has_any = false;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name;

			if ((child_name = child.getLocalName()) == null)
				child_name = option.getUnqualifiedName(child.getNodeName());

			String _child_name = child_name;

			if (fields.stream().filter(field -> field.element).anyMatch(field -> _child_name.equals(field.xname)))
				continue;

			if (!has_any) { // initial instance of new document

				doc = doc_builder.newDocument();
				doc_root = doc.createElement(node.getParentNode().getNodeName());

			}

			doc_root.appendChild(child);

			has_any = true;

		}

		if (has_any) {

			doc.appendChild(doc_root);

			DOMSource source = new DOMSource(doc);
			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);

			transformer.transform(source, result);

			content = writer.toString();

			writer.close();

		}

		return has_any;
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

		if (node.hasAttributes()) {

			NamedNodeMap attrs = node.getAttributes();

			for (int i = 0; i < attrs.getLength(); i++) {

				Node attr = attrs.item(i);

				if (attr != null) {

					if (fields.stream().filter(field -> field.attribute).anyMatch(field -> attr.getNodeName().equals(field.xname)))
						continue;

					if (!has_any_attr) { // initial instance of new document

						doc = doc_builder.newDocument();
						doc_root = doc.createElement(node.getParentNode().getNodeName());

					}

					doc_root.setAttribute(attr.getNodeName(), attr.getNodeValue());

					has_any_attr = true;

				}

			}

		}

		if (has_any_attr) {

			doc.appendChild(doc_root);

			DOMSource source = new DOMSource(doc);
			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);

			transformer.transform(source, result);

			content = writer.toString();

			writer.close();

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
	public boolean existsNestedNode(final PgTable nested_table, final Node node) {

		if (nested_table.virtual)
			return nested_table.content_holder;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name;

			if ((child_name = child.getLocalName()) == null)
				child_name = option.getUnqualifiedName(child.getNodeName());

			if (!child_name.equals(nested_table.name))
				continue;

			return true;
		}

		return false;
	}

}
