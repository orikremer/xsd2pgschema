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

package net.sf.xsd2pgschema;

import org.xml.sax.helpers.DefaultHandler;

/**
 * Common any attrivute retriever.
 *
 * @author yokochi
 */
public abstract class CommonBuilderAnyAttrRetriever extends DefaultHandler {

	/** The root node name. */
	protected String root_node_name;

	/** The current field. */
	protected PgField field;

	/** The current state for root node. */
	protected boolean root_node = false;

	/**
	 * Instance of any retriever.
	 *
	 * @param root_node_name root node name
	 * @param field current field
	 */
	public CommonBuilderAnyAttrRetriever(String root_node_name, PgField field) {

		this.root_node_name = root_node_name;
		this.field = field;

	}

}
