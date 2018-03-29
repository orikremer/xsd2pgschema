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

import java.util.HashSet;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Sphinx xmlpipe2 document id extractor.
 *
 * @author yokochi
 */
public class SphDsDocIdExtractor extends DefaultHandler {

	/** The document key name in PostgreSQL DDL. */
	String document_key_name = null;

	/** The current state for sphinx:document. */
	boolean sph_document = false;

	/** The current state for document_id. */
	boolean document_id = false;

	/** The string builder for document id. */
	StringBuilder sb = null;

	/** The document id stored in data source. */
	HashSet<String> doc_set = null;

	/**
	 * Instance of Sphinx xmlpipe2 document id extractor.
	 *
	 * @param schema PostgreSQL data model
	 * @param doc_set set of document id in data source
	 */
	public SphDsDocIdExtractor(PgSchema schema, HashSet<String> doc_set) {

		document_key_name = schema.option.document_key_name;

		this.doc_set = doc_set;

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) {

		if (qName.equals("sphinx:document")) {

			sph_document = true;

			sb = new StringBuilder();

		}

		else if (!sph_document)
			return;

		else if (qName.equals(document_key_name))
			document_id = true;

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void endElement(String namespaceURI, String localName, String qName) {

		if (!sph_document)
			return;

		if (qName.equals("sphinx:document"))
			sph_document = false;

		else if (qName.equals(document_key_name)) {

			document_id = false;

			doc_set.add(sb.toString());

			sb.setLength(0);

		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	public void characters(char[] chars, int offset, int length) {

		if (!sph_document)
			return;

		else if (document_id)
			sb.append(new String(chars, offset, length));

	}

}
