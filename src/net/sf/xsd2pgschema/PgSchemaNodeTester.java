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

import org.w3c.dom.Node;

/**
 * Node tester.
 *
 * @author yokochi
 */
public class PgSchemaNodeTester {

	/** The ordinal number in sibling node. */
	public int key_id = 1;

	/** The current key name. */
	public String key_name = null;

	/** The processing node. */
	public Node proc_node = null;

	/** Whether if this node is omitted or not. */
	public boolean omitted = false;

	/** Whether if nested node. */
	boolean nested;

	/** The ordinal number of current node in nested case. */
	int nest_id;

	/** The total number of sibling node. */
	int node_count = 0;

	/**
	 * Decide to process this node or not (see result at omitted).
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_node parent node
	 * @param node current node
	 * @param parent_table parent table
	 * @param table current table
	 * @param key_base basename of current node
	 * @param list_holder whether parent field is list holder
	 * @param nested whether it is nested
	 * @param nest_id ordinal number of current node in nested case
	 */
	public PgSchemaNodeTester(final PgSchema schema, final Node parent_node, final Node node, final PgTable parent_table, final PgTable table, final String key_base, final boolean list_holder, final boolean nested, final int nest_id) {

		boolean virtual = table.virtual;

		String node_name = node.getNodeName();
		String node_uname;

		if ((node_uname = node.getLocalName()) == null)
			node_uname = schema.getUnqualifiedName(node.getNodeName());

		if (!virtual) {

			boolean parent_virtual = parent_table.virtual;

			if (!nested && !node_uname.equals(table.name)) {
				omitted = true;
				return;
			}

			else if (nested && (parent_virtual || (!parent_virtual && !node_uname.equals(parent_table.name))) && !node_uname.equals(table.name)) {
				omitted = true;
				return;
			}

		}

		// processing key name

		key_name = key_base;

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
				omitted = true;
				return;
			}

			if (!virtual) {

				// relabeling ordinal number in sibling node

				if (key_name.endsWith("]"))
					key_name = key_name.substring(0, key_name.lastIndexOf("["));

				key_name += "[" + key_id + "]"; // XPath predicate

			}

		}

		// processing node

		proc_node = virtual ? node.getParentNode() : node;

		if (!virtual && nested) {

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				String child_name;

				if ((child_name = child.getLocalName()) == null)
					child_name = schema.getUnqualifiedName(child.getNodeName());

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
	 * Return whether if current node is the last one.
	 *
	 * @return boolean whether if current node is the last one
	 */
	public boolean isLastNode() {
		return node_count == 0 || (key_id == node_count) || (nested && key_id == nest_id);
	}

}
