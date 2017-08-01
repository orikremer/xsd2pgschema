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

package net.sf.xsd2pgschema;

/**
 * Enumerator of XPath component.
 *
 * @author yokochi
 */
public enum XPathCompType {

	/** The table node. */
	table,
	/** The element node. */
	element,
	/** The simple content node. */
	simple_content,
	/** The attribute node. */
	attribute,

	/** The text node. */
	text,
	/** The comment node. */
	comment,
	/** The processing instruction node. */
	processing_instruction

}
