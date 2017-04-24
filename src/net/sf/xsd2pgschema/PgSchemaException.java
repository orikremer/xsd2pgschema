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

}
