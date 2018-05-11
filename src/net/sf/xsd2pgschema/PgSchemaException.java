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

import org.antlr.v4.runtime.tree.ParseTree;

/**
 * PostgreSQL schema contractor exception.
 *
 * @author yokochi
 */
public class PgSchemaException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/**
	 * Instantiates a new pg schema exception.
	 */
	public PgSchemaException() {

		super();

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param message the message
	 */
	public PgSchemaException(String message) {

		super(message);

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param cause the cause
	 */
	public PgSchemaException(Throwable cause) {

		super(cause);

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param tree the parse tree
	 */
	public PgSchemaException(ParseTree tree) {

		super("Unexpceted XPath expression (" + tree.getSourceInterval().toString() + ": " + tree.getClass().getSimpleName() + " '" + tree.getText() + "').");

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param tree the current parse tree
	 * @param prev_tree the previous parse tree
	 */
	public PgSchemaException(ParseTree tree, ParseTree prev_tree) {

		super("Unexpceted XPath expression (" + tree.getSourceInterval().toString() + ": " + tree.getClass().getSimpleName() + " '" + tree.getText() + "' after " + prev_tree.getClass().getSimpleName() + " '" + prev_tree.getText() + "').");

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param tree the parse tree
	 * @param key_name the concerned key name
	 * @param status the status of the key
	 */
	public PgSchemaException(ParseTree tree, String key_name, boolean status) {

		super("Unexpected XPath expression (" + tree.getSourceInterval().toString() + ": " + tree.getClass().getSimpleName() + " '" + tree.getText() + "', but " + key_name + " has been turned " + (status ? "on" : "off") + ").");

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param tree the parse tree
	 * @param schema_location default schema location
	 */
	public PgSchemaException(ParseTree tree, String schema_location) {

		super("Not found node for XPath expression (" + tree.getSourceInterval().toString() + ": " + tree.getClass().getSimpleName() + " '" + tree.getText() + "') in XML Schema: " + schema_location);

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param tree the parse tree
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param schema_location default schema location
	 */
	public PgSchemaException(ParseTree tree, boolean wild_card, String composite_text, String schema_location) {

		super("Not found node for XPath expression (" + tree.getSourceInterval().toString() + ": " + tree.getClass().getSimpleName() + " '" + (wild_card ? composite_text.replace(".*", "*") : tree.getText()) + "') in XML Schema: " + schema_location);

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param tree the parse tree
	 * @param schema_location default schema location
	 * @param prefix_ns_uri prefix of target namespace URI
	 */
	public PgSchemaException(ParseTree tree, String schema_location, String prefix_ns_uri) {

		super("Not found prefix (" + prefix_ns_uri + ") of target namespace URL for XPath expression (" + tree.getSourceInterval().toString() + ": " + tree.getClass().getSimpleName() + " '" + tree.getText() + "') in XML Schema: " + schema_location);

	}

	/**
	 * Instantiates a new pg schema exception.
	 *
	 * @param tree the parse tree
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param schema_location default schema location
	 * @param prefix_ns_uri prefix of target namespace URI
	 */
	public PgSchemaException(ParseTree tree, boolean wild_card, String composite_text, String schema_location, String prefix_ns_uri) {

		super("Not found prefix (" + prefix_ns_uri + ") of target namespace URL for XPath expression (" + tree.getSourceInterval().toString() + ": " + tree.getClass().getSimpleName() + " '" + (wild_card ? composite_text.replace(".*", "*") : tree.getText()) + "') in XML Schema: " + schema_location);

	}

}
