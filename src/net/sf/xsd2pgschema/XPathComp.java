/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017-2018 Masashi Yokochi

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

import org.antlr.v4.runtime.tree.ParseTree;

/**
 * XPath component.
 *
 * @author yokochi
 */
public class XPathComp {

	/** The union id. */
	protected int union_id;

	/** The step id. */
	protected int step_id;

	/** The XPath parse tree. */
	protected ParseTree tree;

	/**
	 * Instance of XPathComp.
	 *
	 * @param union_id union id
	 * @param step_id step id
	 * @param tree XPath parse tree
	 */
	public XPathComp(int union_id, int step_id, ParseTree tree) {

		this.union_id = union_id;
		this.step_id = step_id;
		this.tree = tree;

	}

}
