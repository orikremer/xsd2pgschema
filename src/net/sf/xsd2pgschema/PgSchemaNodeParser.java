/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2017 Masashi Yokochi

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

import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;

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
	PgSchema schema = null;

	/** The relational data extension. */
	boolean rel_data_ext;

	/** The parent table. */
	PgTable parent_table = null;

	/** The current table. */
	PgTable table = null;

	/** The field list. */
	List<PgField> fields = null;

	/** The content of fields. */
	String[] values = null;

	/** The number of nested fields. */
	int nested_fields = 0;

	/** Whether list holder. */
	boolean[] list_holder = null;

	/** The nested table id. */
	int[] nested_table_id = null;

	/** The nested key name. */
	String[] nested_key = null;

	/** Whether values were filled. */
	boolean filled = true;

	/** Whether any content was written. */
	boolean written = false;

	/** Whether any nested node was invoked. */
	boolean invoked = false;

	/** Whether nested node. */
	boolean nested = false;

	/** The processing node. */
	Node proc_node;

	/** The current key name. */
	String current_key;

	/** The document id. */
	String document_id = null;

	/** The length of document id. */
	int document_id_len;

	/** The common content holder for attribute, simple_cont and element. */
	String content;

	/** The document builder for xs:any and xs:anyAttribute. */
	DocumentBuilder doc_builder = null;

	/** The XML content of xs:any and xs:anyAttribute. */
	Document doc = null;

	/** The root element of the XML content. */
	Element doc_root = null;

	/** The transformer of the XML content. */
	Transformer transformer = null;

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

		rel_data_ext = schema.option.rel_data_ext;

		this.parent_table = parent_table;
		this.table = table;

		document_id = schema.getDocumentId();
		document_id_len = document_id.length();

		fields = table.fields;

		values = new String[table.fields.size()];

		list_holder = new boolean[table.nested_fields];
		nested_table_id = new int[table.nested_fields];
		nested_key = new String[table.nested_fields];

		if (table.fields.stream().anyMatch(field -> field.any || field.any_attribute)) {

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

		PgTable nested_table = schema.getTable(field.foreign_table_id);

		list_holder[nested_fields] = field.list_holder;
		nested_table_id[nested_fields] = field.foreign_table_id;
		nested_key[nested_fields] = proc_key;

		if (!nested_table.virtual)
			nested_key[nested_fields] += "/" + field.foreign_table; // XPath child

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
			node_name = node_name.substring(0, node_name.lastIndexOf("["));

		for (String _parent_node : parent_node.split(" ")) {

			if (_parent_node.equals(node_name))
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
	public boolean setCont(final Node node, final PgField field, final boolean pg_enum_limit) {

		content = null;

		if (field.attribute)
			setAttr(node, field);

		else if (field.simple_cont)
			setSimpleCont(node);

		else if (field.element)
			setElement(node, field);

		if (applyContFilter(field, pg_enum_limit)) {

			if (content != null && XsDataType.isValid(field, content))
				return true;

		}

		return false;
	}

	/**
	 * Set attribute.
	 *
	 * @param node current node
	 * @param field current field
	 */
	private void setAttr(final Node node, final PgField field) {

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
	private void setSimpleCont(final Node node) {

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
				child_name = schema.getUnqualifiedName(child.getNodeName());

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
	private boolean applyContFilter(final PgField field, boolean pg_enum_limit) {

		if (field.default_value != null && (content == null || content.isEmpty()))
			content = field.default_value;

		if (field.fill_this)
			content = field.filled_text;

		if (field.required && (content == null || content.isEmpty()))
			return false;

		if (field.pattern != null && (content == null || !content.matches(field.pattern)))
			return false;

		if (field.filt_out && field.matchesOutPattern(content))
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
	 * Set any element.
	 *
	 * @param node current node
	 * @return boolean whether any element exists
	 */
	public boolean setAnyElement(Node node) {

		boolean has_any = false;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;

			String child_name;

			if ((child_name = child.getLocalName()) == null)
				child_name = schema.getUnqualifiedName(child.getNodeName());

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

		return has_any;
	}

	/**
	 * Set any attribute.
	 *
	 * @param node current node
	 * @return boolean whether any attribute exists
	 */
	public boolean setAnyAttr(Node node) {

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

		return has_any_attr;
	}

	/**
	 * Return whether nested node exists.
	 *
	 * @param schema PostgreSQL data model
	 * @param nested_table nested table
	 * @param node current node
	 * @return boolean whether nested node exists
	 */
	public boolean existsNestedNode(final PgSchema schema, final PgTable nested_table, final Node node) {

		if (!nested_table.virtual) {

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				String child_name;

				if ((child_name = child.getLocalName()) == null)
					child_name = schema.getUnqualifiedName(child.getNodeName());

				if (!child_name.equals(nested_table.name))
					continue;

				return true;
			}

		}

		return false;
	}

}
