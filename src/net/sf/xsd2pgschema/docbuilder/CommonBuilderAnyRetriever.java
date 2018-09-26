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

import java.util.HashMap;

import org.xml.sax.helpers.DefaultHandler;

/**
 * Common any retriever.
 *
 * @author yokochi
 */
public abstract class CommonBuilderAnyRetriever extends DefaultHandler {

	/** The root node name. */
	protected String root_node_name;

	/** The simple content holder of any element. */
	protected HashMap<String, StringBuilder> simple_contents = new HashMap<String, StringBuilder>();

	/** The current state for root node. */
	protected boolean root_node = false;

	/** The current path. */
	protected StringBuilder cur_path = new StringBuilder();

	/** The offset value of current path. */
	protected int cur_path_offset;

	/**
	 * Instance of any retriever.
	 *
	 * @param root_node_name root node name
	 */
	public CommonBuilderAnyRetriever(String root_node_name) {

		this.root_node_name = root_node_name;
		cur_path_offset = root_node_name.length() + 1;

	}

}
