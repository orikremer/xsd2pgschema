/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2019 Masashi Yokochi

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

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;

/**
 * Abstract node parser.
 *
 * @author yokochi
 */
public abstract class PgSchemaNodeParser {

	/** The node parser builder. */
	protected PgSchemaNodeParserBuilder npb;

	/** The parent table. */
	protected PgTable parent_table;

	/** The current table. */
	protected PgTable table;

	/** The field list. */
	protected List<PgField> _fields = null;

	/** The size of field list. */
	protected int _fields_size = 0;

	/** The total number of field as nested key. */
	protected int total_nested_fields;

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

	/** The common content holder for element, simple_content and attribute. */
	protected String content;

	/**
	 * Node parser.
	 *
	 * @param npb node parser builder
	 * @param parent_table parent table
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNodeParser(final PgSchemaNodeParserBuilder npb, final PgTable parent_table, final PgTable table) throws PgSchemaException {

		this.npb = npb;
		this.parent_table = parent_table;
		this.table = table;

		switch (npb.parser_type) {
		case pg_data_migration:
			if (table.writable) {
				_fields = table.custom_fields;
				_fields_size = _fields.size();
			}
			break;			
		case json_conversion:
			if (table.jsonable) {
				_fields = table.custom_fields;
				_fields_size = _fields.size();
			}
			break;
		case full_text_indexing:
			if (table.indexable) {
				_fields = table.custom_fields;
				_fields_size = _fields.size();
			}
			break;
		}

		if ((total_nested_fields = table.total_nested_fields) > 0)
			nested_keys = new ArrayList<PgSchemaNestedKey>();

		virtual = table.virtual;
		visited = !virtual;

	}

	/**
	 * Abstract initializer of nested node.
	 *
	 * @param as_attr whether parent node as attribute
	 * @throws PgSchemaException the pg schema exception
	 */
	abstract protected void init(boolean as_attr) throws PgSchemaException;

	/**
	 * Parse root node.
	 *
	 * @param root_node root node
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseRootNode(final Node root_node) throws PgSchemaException {

		node_test.setRootNode(root_node, npb.document_id + "/" + table.xname);

		parse();

		if (not_complete || total_nested_fields == 0)
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

			if (total_nested_fields > 0) {

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

		if (not_complete || total_nested_fields == 0)
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
	protected void clear() throws PgSchemaException {

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

			if (!field.matchesParentNodeNameConstraint(parent_node_name))
				return null;

			if (!field.matchesAncestorNodeNameConstraint(ancestor_node_name))
				return null;

		}

		if (field.nested_key_as_attr_group && !node.hasAttributes())
			return null;

		if (field.nested_key_as_attr) {

			if (!node.getParentNode().hasAttributes())
				return null;

			if (table.has_simple_content && !node.hasAttributes())
				return null;

			if (field.delegated_field_pname != null && ((Element) node).getAttribute(field.foreign_table_xname).isEmpty())
				return null;

		}

		else {

			if (table.has_nested_key_to_simple_attr && current_key.contains("@"))
				return null;

			if (field.delegated_sibling_key_name != null) {

				PgField _field = table.getField(field.delegated_sibling_key_name);

				if (_field != null && !testNestedKey(node, _field))
					return null;

			}

			else if (!field.matchesChildNodeNameConstraint(node))
				return null;

		}

		PgSchemaNestedKey nested_key = new PgSchemaNestedKey(npb.schema.getTable(field.foreign_table_id), field, current_key);

		nested_keys.add(nested_key);

		return nested_key.current_key;
	}

	/**
	 * Test nested key.
	 *
	 * @param node current node
	 * @param field current field
	 * @return boolean whether nested key is valid
	 */
	private boolean testNestedKey(final Node node, final PgField field) {

		if (table.has_path_restriction) {

			if (!field.matchesParentNodeNameConstraint(parent_node_name))
				return false;

			if (!field.matchesAncestorNodeNameConstraint(ancestor_node_name))
				return false;

		}

		if (field.nested_key_as_attr_group && !node.hasAttributes())
			return false;

		if (field.nested_key_as_attr) {

			if (field.delegated_field_pname != null && ((Element) node).getAttribute(field.foreign_table_xname).isEmpty())
				return false;

		}

		else {

			if (field.delegated_sibling_key_name != null) {

				PgField _field = table.getField(field.delegated_sibling_key_name);

				if (_field != null && !testNestedKey(node, _field))
					return false;

			}

			else if (!field.matchesChildNodeNameConstraint(node))
				return false;

		}

		return true;
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
	 * @return boolean whether content is valid
	 */
	protected boolean setContent(final Node node, final PgField field) {

		content = null;

		if (field.attribute || field.simple_attribute || (field.simple_attr_cond && as_attr))
			setAttribute(node, field);

		else if (field.simple_content)
			setSimpleContent(node, field);

		else if (field.element)
			setElement(node, field);

		if (applyContentFilter(field)) {

			if (content != null && !content.isEmpty() && ((npb.type_check && field.validate(content)) || !npb.type_check)) {

				if (npb.pg_enum_limit) // normalize data for PostgreSQL
					content = field.normalize(content);

				return true;
			}

		}

		return false;
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

			NamedNodeMap attrs = parent_node.getAttributes();

			for (int i = 0; i < attrs.getLength(); i++) {

				Node attr = attrs.item(i);

				if (attr != null && field.matchesParentNodeNameConstraint(attr.getNodeName())) {

					content = attr.getNodeValue();

					return;
				}

			}

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

			if (field.simple_primitive_list && total_nested_fields > 0) {

				if (content != null && table.nested_fields.stream().filter(_field -> _field.parent_node != null).anyMatch(_field -> _field.matchesParentNodeNameConstraint(parent_node_name)))
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
	 * @return boolean whether content is valid
	 */
	private boolean applyContentFilter(final PgField field) {

		if (field.default_value != null && (content == null || content.isEmpty()) && npb.fill_default_value)
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

			if (npb.pg_enum_limit) {

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

}
