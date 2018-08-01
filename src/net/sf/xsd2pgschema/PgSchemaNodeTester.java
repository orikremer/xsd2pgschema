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

import org.w3c.dom.Node;

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

	/** Whether this node is omissible. */
	protected boolean omissible = false;

	/** Whether this node has been already visited. */
	protected boolean visited = false;

	/** Whether parent node as attribute. */
	protected boolean as_attr;

	/** Whether child node is not nested node (indirect). */
	protected boolean indirect;

	/** The count of sibling nodes. */
	protected int node_count;

	/** The ordinal number of sibling node. */
	protected int node_ordinal;

	/**
	 * Decide whether to process this node.
	 *
	 * @param option PostgreSQL data model option
	 * @param parent_node parent node
	 * @param node current node
	 * @param parent_table parent table
	 * @param table current table
	 * @param nested_key nested key
	 * @param node_count count of sibling nodes
	 * @param node_ordinal ordinal number of sibling node
	 */
	public PgSchemaNodeTester(final PgSchemaOption option, final Node parent_node, final Node node, final PgTable parent_table, final PgTable table, final PgSchemaNestedKey nested_key, final int node_count, final int node_ordinal) {

		boolean virtual = table.virtual;

		String qname = node.getNodeName();

		String xname = option.getUnqualifiedName(qname);

		String table_xname = table.xname;

		as_attr = nested_key.as_attr;
		indirect = nested_key.indirect;

		if (!virtual) {

			boolean parent_virtual = parent_table.virtual;

			if ((!indirect && !xname.equals(table_xname)) ||
					(indirect && (parent_virtual || (!parent_virtual && !xname.equals(parent_table.xname))) && !xname.equals(table_xname))) {

				if (!as_attr || (as_attr && !parent_node.hasAttributes())) {
					omissible = true;
					return;
				}

			}

		}

		// processing key name

		primary_key = current_key = nested_key.current_key;

		if (nested_key.list_holder) {

			if (indirect && node_ordinal < nested_key.target_ordinal) {
				omissible = true;
				return;
			}

			if (!virtual)
				current_key += "[" + node_ordinal + "]"; // XPath predicate

		}

		if (table.visited_key.equals(current_key)) {
			visited = true;
			return;	
		}

		table.visited_key = current_key;

		parent_key = nested_key.parent_key;

		// processing node

		proc_node = virtual ? node.getParentNode() : node;

		if (!virtual && indirect) {

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				String child_name;

				if ((child_name = child.getLocalName()) == null)
					child_name = option.getUnqualifiedName(child.getNodeName());

				if (!child_name.equals(table_xname))
					continue;

				proc_node = child;

				break;
			}

		}

		this.node_count = node_count > 0 ? node_count : nested_key.getNodeCount(parent_node, qname);
		this.node_ordinal = node_ordinal;

	}

}
