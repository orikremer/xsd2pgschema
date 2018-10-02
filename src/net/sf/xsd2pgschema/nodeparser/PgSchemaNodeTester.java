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

	/** The current primary key name. */
	protected String primary_key;

	/** The current key name. */
	protected String current_key;

	/** The processing node. */
	protected Node proc_node;

	/** The last node. */
	protected Node last_node = null;

	/** Whether parent node as attribute. */
	protected boolean as_attr;

	/** Whether child node is not nested node (indirect). */
	protected boolean indirect;

	/** The ordinal number of sibling node. */
	protected int node_ordinal = 1;

	/** The target ordinal number of sibling node (internal use only). */
	protected int target_ordinal;

	/** The original current key name (internal use only). */
	private String _current_key;

	/** The parent table (internal use only). */
	private PgTable parent_table;

	/** The current table (internal use only). */
	private PgTable table;

	/** The canonical table name in XML Schema (internal use only). */
	private String table_xname;

	/** Whether the table is virtual (internal use only). */
	private boolean virtual;

	/** Whether nested key is list holder (internal use only). */
	private boolean list_holder;

	/**
	 * Set root node as processing node.
	 *
	 * @param root_node root node
	 * @param root_key root key
	 */
	public void setRootNode(Node root_node, String root_key) {

		proc_node = root_node;

		parent_key = null;
		primary_key = current_key = _current_key = root_key;
		as_attr = indirect = false;

	}

	/**
	 * Prepare node tester.
	 *
	 * @param parent_table parent_table
	 * @param nested_key nested_key
	 */
	public void prepare(final PgTable parent_table, final PgSchemaNestedKey nested_key) {

		this.parent_table = parent_table;
		table = nested_key.table;

		table_xname = table.xname;
		virtual = table.virtual;

		target_ordinal = nested_key.target_ordinal;
		parent_key = nested_key.parent_key;
		primary_key = current_key = _current_key = nested_key.current_key;
		as_attr = nested_key.as_attr;
		indirect = nested_key.indirect;
		list_holder = nested_key.list_holder;

	}

	/**
	 * Return whether current node is omissible.
	 *
	 * @param parent_node parent node
	 * @param node current node
	 * @param node_ordinal the ordinal number of sibling node
	 * @param last_node the last node
	 * @return boolean whether current node is omissible
	 */
	public boolean isOmissibleNode(final Node parent_node, final Node node, final int node_ordinal, final Node last_node) {

		String qname = node.getNodeName();
		String xname = PgSchemaUtil.getUnqualifiedName(qname);

		if (!virtual && !xname.equals(table_xname) && (!indirect || (indirect && (parent_table.virtual || !xname.equals(parent_table.xname))))) {

			if (!as_attr || (as_attr && !parent_node.hasAttributes()))
				return true;

		}

		// processing key

		if (list_holder) {

			this.node_ordinal = node_ordinal;

			if (indirect && node_ordinal < target_ordinal)
				return true;

			if (!virtual)
				current_key = _current_key + "[" + node_ordinal + "]"; // XPath predicate

			if (this.last_node == null && (this.last_node = last_node) == null) {

				for (Node child = parent_node.getLastChild(); child != null; child = child.getPreviousSibling()) {

					if (child.getNodeType() != Node.ELEMENT_NODE)
						continue;

					if (qname.equals(child.getNodeName())) {

						this.last_node = child;

						break;
					}

				}

			}

		}

		if (table.visited_key.equals(current_key))
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
	 * Set current node as processing node.
	 *
	 * @param node current node
	 */
	public void setProcNode(Node node) {

		proc_node = node;

		current_key = _current_key;

	}

}
