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

/**
 * Abstract pending attribute.
 *
 * @author yokochi
 */
public abstract class AbstractPendingAttr {

	/** The field (attribute). */
	protected PgField field = null;

	/** The foreign table (simple attribute). */
	protected PgTable foreign_table = null;

	/** The local name (any attribute). */
	protected String local_name = null;

	/** The content. */
	protected String content = null;

	/**
	 * Instance of pending attribute.
	 *
	 * @param field current field
	 * @param content content
	 */
	public AbstractPendingAttr(PgField field, String content) {

		this.field = field;
		this.content = content != null ? content : "";

	}

	/**
	 * Instance of pending simple attribute.
	 *
	 * @param field current field
	 * @param foreign_table foregin table
	 * @param content content
	 */
	public AbstractPendingAttr(PgField field, PgTable foreign_table, String content) {

		this.field = field;
		this.foreign_table = foreign_table;
		this.content = content != null ? content : "";

	}

	/**
	 * Instance of pending any attribute.
	 *
	 * @param local_name local name
	 * @param content content
	 */
	public AbstractPendingAttr(String local_name, String content) {

		field = null;

		this.local_name = local_name;
		this.content = content != null ? content : "";

	}

}
