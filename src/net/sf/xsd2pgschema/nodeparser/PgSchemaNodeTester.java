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

	/**
	 * Set root node as processing node.
	 *
	 * @param root_node root node
	 * @param root_key root key
	 */
	public void setRootNode(Node root_node, String root_key) {

		proc_node = root_node;

		parent_key = null;
		primary_key = current_key = root_key;
		as_attr = indirect = false;

	}

	/**
	 * Return whether current node is omissible.
	 *
	 * @param node_parser node parser
	 * @param parent_node parent node
	 * @param node current node
	 * @param nested_key nested key
	 * @return boolean whether current node is omissible
	 */
	public boolean isOmissible(final PgSchemaNodeParser node_parser, final Node parent_node, final Node node, final PgSchemaNestedKey nested_key) {

		PgTable parent_table = node_parser.parent_table;
		PgTable table = nested_key.table;

		String qname = node.getNodeName();
		String xname = PgSchemaUtil.getUnqualifiedName(qname);
		String table_xname = table.xname;

		boolean virtual = table.virtual;

		if (!virtual && !xname.equals(table_xname) && (!indirect || (indirect && (parent_table.virtual || !xname.equals(parent_table.xname))))) {

			if (!as_attr || (as_attr && !parent_node.hasAttributes()))
				return true;

		}

		parent_key = nested_key.parent_key;
		primary_key = current_key = nested_key.current_key;
		as_attr = nested_key.as_attr;
		indirect = nested_key.indirect;

		// processing key

		if (nested_key.list_holder) {

			node_ordinal = node_parser.node_ordinal;

			if (indirect && node_ordinal < nested_key.target_ordinal)
				return true;

			if (!virtual)
				current_key += "[" + node_ordinal + "]"; // XPath predicate

			if (last_node == null && (last_node = node_parser.last_node) == null) {

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
	 * @param nested_key nested key
	 */
	public void setNode(Node node, PgSchemaNestedKey nested_key) {

		proc_node = node;

		parent_key = nested_key.parent_key;
		primary_key = current_key = nested_key.current_key;
		as_attr = nested_key.as_attr;
		indirect = nested_key.indirect;

	}

}
