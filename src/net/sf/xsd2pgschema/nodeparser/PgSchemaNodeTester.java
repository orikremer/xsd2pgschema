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

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;

/**
 * Node tester.
 *
 * @author yokochi
 */
public class PgSchemaNodeTester {

	/** The parent key name. */
	protected String parent_key;

	/** The primary key name. */
	protected String primary_key;

	/** The processing key name. */
	protected String proc_key;

	/** The processing node. */
	protected Node proc_node;

	/** The ordinal number of sibling node. */
	protected int node_ordinal = 1;

	/** The target ordinal number of sibling node. */
	private int target_ordinal;

	/** The integer value of @maxOccurs except for @maxOccurs="unbounded", which gives -1. */
	private int maxoccurs = -1;

	/** The original processing key name. */
	private String _proc_key;

	/** The parent node. */
	private Node parent_node;

	/** The last node. */
	private Node last_node = null;

	/** The parent table. */
	private PgTable parent_table;

	/** The current table. */
	private PgTable table;

	/** The canonical table name in XML Schema. */
	private String table_xname;

	/** Whether virtual table. */
	private boolean virtual;

	/** Whether parent node as attribute. */
	private boolean as_attr;

	/** Whether child node is not nested node (indirect). */
	private boolean indirect;

	/** Whether nested key is list holder. */
	private boolean list_holder;

	/**
	 * Set root node as processing node.
	 *
	 * @param root_node root node
	 * @param root_key root key
	 */
	protected void setRootNode(Node root_node, String root_key) {

		proc_node = root_node;

		primary_key = proc_key = root_key;

	}

	/**
	 * Prepare node tester for traversal of nested node.
	 *
	 * @param parent_table parent table
	 * @param parent_node parent node
	 * @param nested_key nested_key
	 */
	protected void prepForTraversal(final PgTable parent_table, final Node parent_node, final PgSchemaNestedKey nested_key) {

		this.parent_table = parent_table;
		this.parent_node = parent_node;

		table = nested_key.table;

		table_xname = table.xname;
		virtual = table.virtual;

		target_ordinal = nested_key.target_ordinal;
		maxoccurs = nested_key.maxoccurs;

		parent_key = nested_key.parent_key;
		primary_key = proc_key = _proc_key = nested_key.current_key;
		as_attr = nested_key.as_attr;
		indirect = nested_key.indirect;
		list_holder = nested_key.list_holder;

	}

	/**
	 * Set current node as processing node if the node is not omissible.
	 * prepForTraversal() should be called beforehand.
	 *
	 * @param node current node
	 * @return boolean whether current node is omissible
	 */
	protected boolean isOmissibleNode(final Node node) {

		String qname = node.getNodeName();
		String xname = PgSchemaUtil.getUnqualifiedName(qname);

		if (!virtual && !xname.equals(table_xname) && (!indirect || (indirect && (parent_table.virtual || !xname.equals(parent_table.xname))))) {

			if (!as_attr || (as_attr && !parent_node.hasAttributes()))
				return true;

		}

		// processing key

		if (list_holder) {

			if (indirect && node_ordinal < target_ordinal)
				return true;

			if (maxoccurs >= 0 && node_ordinal > maxoccurs)
				return true;

			if (!virtual)
				proc_key = _proc_key + "[" + node_ordinal + "]"; // XPath predicate

			if (last_node == null) {

				for (Node child = parent_node.getLastChild(); child != null; child = child.getPreviousSibling()) {

					if (child.getNodeType() != Node.ELEMENT_NODE)
						continue;

					if (qname.equals(child.getNodeName())) {

						last_node = child;

						break;
					}

				}

			}

		}

		if (table.visited_key.equals(proc_key))
			return true;

		// processing node

		proc_node = virtual ? parent_node : node;

		if (!virtual && indirect) {

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (((Element) child).getLocalName().equals(table_xname)) {

					proc_node = child;

					break;
				}

			}

		}

		return false;
	}

	/**
	 * Return whether current node is the last one.
	 * prepForTraversal() and isOmissibleNode() should be called beforehand.
	 *
	 * @return boolean whether current node is the last one
	 */
	protected boolean isLastNode() {

		try {
			return last_node == null || proc_node.equals(last_node) || (indirect && node_ordinal == target_ordinal) || (maxoccurs >= 0 && node_ordinal == maxoccurs);
		} finally {
			++node_ordinal;
		}

	}

	/**
	 * Set current node as processing node.
	 * prepForTraversal() should be called beforehand.
	 *
	 * @param node current node
	 */
	protected void setProcNode(Node node) {

		proc_node = node;

		proc_key = _proc_key;

	}

}
