/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017 Masashi Yokochi

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

package com.github.antlr.grammars_v4.xpath;

/**
 * XPath listener exception.
 *
 * @author yokochi
 */
public class xpathListenerException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/**
	 * Instantiates a new xpath listener exception.
	 */
	public xpathListenerException() {

		super();

	}

	/**
	 * Instantiates a new xpath listener exception.
	 *
	 * @param message the message
	 */
	public xpathListenerException(String message) {

		super(message);

	}

	/**
	 * Instantiates a new xpath listener exception.
	 *
	 * @param cause the cause
	 */
	public xpathListenerException(Throwable cause) {

		super(cause);

	}

}
