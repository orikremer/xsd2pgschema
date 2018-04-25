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

	/** The ordinal number in sibling node. */
	protected int key_id = 1;

	/** The parent key name. */
	protected String parent_key = null;

	/** The current primary key name. */
	protected String primary_key = null;

	/** The current key name. */
	protected String current_key = null;

	/** The processing node. */
	protected Node proc_node = null;

	/** Whether this node is omissible. */
	protected boolean omissible = false;

	/** Whether nested node. */
	protected boolean nested;

	/** The ordinal number of current node in nested case. */
	private int nest_id;

	/** The total number of sibling node. */
	private int node_count = 0;

	/**
	 * Decide to process this node or not.
	 *
	 * @param option PostgreSQL data model option
	 * @param parent_node parent node
	 * @param node current node
	 * @param parent_table parent table
	 * @param table current table
	 * @param parent_key name of parent node
	 * @param proc_key name of processing node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node in nested case
	 */
	public PgSchemaNodeTester(final PgSchemaOption option, final Node parent_node, final Node node, final PgTable parent_table, final PgTable table, final String parent_key, final String proc_key, final boolean list_holder, final boolean nested, final int nest_id) {

		boolean virtual = table.virtual;

		String node_name = node.getNodeName();
		String node_uname;

		if ((node_uname = node.getLocalName()) == null)
			node_uname = option.getUnqualifiedName(node.getNodeName());

		if (!virtual) {

			boolean parent_virtual = parent_table.virtual;

			if ((!nested && !node_uname.equals(table.name)) ||
					(nested && (parent_virtual || (!parent_virtual && !node_uname.equals(parent_table.name))) && !node_uname.equals(table.name))) {
				omissible = true;
				return;
			}

		}

		this.parent_key = parent_key;

		// processing key name

		primary_key = current_key = proc_key;

		if (list_holder) {

			boolean node_test = false;

			for (Node child = parent_node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (node_name.equals(child.getNodeName())) {

					node_count++;

					if (child.isSameNode(node))
						node_test = true;

					if (!node_test)
						key_id++;

				}

			}

			if (key_id < nest_id) {
				omissible = true;
				return;
			}

			if (!virtual)
				current_key += "[" + key_id + "]"; // XPath predicate

		}

		// processing node

		proc_node = virtual ? node.getParentNode() : node;

		if (!virtual && nested) {

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				String child_name;

				if ((child_name = child.getLocalName()) == null)
					child_name = option.getUnqualifiedName(child.getNodeName());

				if (!child_name.equals(table.name))
					continue;

				proc_node = child;

				break;
			}

		}

		this.nested = nested;
		this.nest_id = nest_id;

	}

	/**
	 * Return whether current node is the last one.
	 *
	 * @return boolean whether current node is the last one
	 */
	public boolean isLastNode() {
		return node_count == 0 || (key_id == node_count) || (nested && key_id == nest_id);
	}

}
