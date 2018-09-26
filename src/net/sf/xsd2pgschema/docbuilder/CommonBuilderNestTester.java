/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

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

package net.sf.xsd2pgschema.docbuilder;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgTable;

/**
 * Common nest tester.
 *
 * @author yokochi
 */
public abstract class CommonBuilderNestTester {

	/** The ancestor node name. */
	protected String ancestor_node;

	/** The parent node name. */
	protected String parent_node;

	/** Whether this node has child element. */
	public boolean has_child_elem = false;

	/** Whether this node has content. */
	public boolean has_content = false;

	/** Whether this node has simple content. */
	public boolean has_simple_content = false;

	/** Whether this node has opened simple content. */
	public boolean has_open_simple_content = false;

	/** Whether this node has inserted document key. */
	public boolean has_insert_doc_key = false;

	/** SAX parser for any content. */
	public SAXParser any_sax_parser = null;

	/**
	 * Instance of nest tester from root node.
	 *
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	public CommonBuilderNestTester(PgTable table) throws PgSchemaException {

		ancestor_node = "";
		parent_node = table.xname;

		if (!table.virtual) {

			ancestor_node = parent_node;
			parent_node = table.xname;

		}

		if (table.has_any || table.has_any_attribute) {

			try {

				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setValidating(false);
				spf.setNamespaceAware(false);

				any_sax_parser = spf.newSAXParser();

			} catch (ParserConfigurationException | SAXException e) {
				throw new PgSchemaException(e);
			}

		}

	}

	/**
	 * Instance of nest tester from parent node.
	 *
	 * @param table current table
	 * @param parent_test nest test of parent node
	 * @throws PgSchemaException the pg schema exception
	 */
	public CommonBuilderNestTester(PgTable table, CommonBuilderNestTester parent_test) throws PgSchemaException {

		ancestor_node = parent_test.ancestor_node;
		parent_node = parent_test.parent_node;

		if (!table.virtual) {

			ancestor_node = parent_node;
			parent_node = table.xname;

		}

		has_insert_doc_key = parent_test.has_insert_doc_key;

		if (table.has_any || table.has_any_attribute) {

			try {

				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setValidating(false);
				spf.setNamespaceAware(false);

				any_sax_parser = spf.newSAXParser();

			} catch (ParserConfigurationException | SAXException e) {
				throw new PgSchemaException(e);
			}

		}

	}

	/**
	 * Merge test result.
	 *
	 * @param test nest test of child node
	 */
	public void merge(CommonBuilderNestTester test) {

		has_child_elem |= test.has_child_elem;
		has_content |= test.has_content;
		has_simple_content |= test.has_simple_content;
		has_open_simple_content |= test.has_open_simple_content;

	}

}
