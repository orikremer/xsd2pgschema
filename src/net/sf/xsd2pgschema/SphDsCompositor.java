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

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.commons.text.StringEscapeUtils;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Sphinx xmlpipe2 compositor. 
 *
 * @author yokochi
 */
public class SphDsCompositor extends DefaultHandler {

	/** The buffered writer for Sphinx data source. */
	BufferedWriter buffw;

	/** The Sphinx maximum field length. (related to max_xmlpipe2_field in sphinx.conf) */
	int sph_max_field_len;

	/** The cutoff (80% of hard limit, max_field_len) of Sphinx field length. */
	int sph_co_field_len;

	/** The current state for sphinx:document. */
	boolean sph_document = false;

	/** The current state for document key. */
	boolean document_key = false;

	/** The current state for default content. */
	boolean content = false;

	/** Whether Sphinx attribute. */
	boolean sph_attr = false;

	/** Whether Sphinx multi-valued attribute. */
	boolean sph_mvattr = false;

	/** The document key name in PostgreSQL DDL. */
	String document_key_name = null;

	/** The current Sphinx attribute name. */
	String sph_attr_name = null;

	/** The string builder for default field. */
	StringBuilder sb = null;

	/** The holder of string builder for each Sphinx attribute. */
	HashMap<String, StringBuilder> buffer = null;

	/** The set of Sphinx attribute. */
	HashSet<String> sph_attrs = null;

	/** The set of Sphinx multi-valued attribute. */
	HashSet<String> sph_mvas = null;

	/**
	 * Instance of Sphinx xmlpipe2 compositor.
	 *
	 * @param document_key_name document key name
	 * @param sph_attrs set of Sphinx attribute
	 * @param sph_mvas set of Sphinx multi-valued attribute
	 * @param buffw buffered writer for Sphinx data source
	 * @param index_filter index filter
	 */
	public SphDsCompositor(String document_key_name, HashSet<String> sph_attrs, HashSet<String> sph_mvas, BufferedWriter buffw, IndexFilter index_filter) {

		this.document_key_name = document_key_name;

		this.sph_attrs = sph_attrs;
		this.sph_mvas = sph_mvas;

		this.buffw = buffw;

		sph_max_field_len = index_filter.sph_max_field_len;
		sph_co_field_len = (int) (index_filter.sph_max_field_len * 0.8); // 80% of hard limit

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) {

		if (qName.equals("sphinx:document")) {

			sph_document = true;

			try {
				buffw.write("<sphinx:document id=\"" + atts.getValue("id") + "\">\n");
			} catch (IOException e) {
				e.printStackTrace();
			}

			sb = new StringBuilder();

			buffer = new HashMap<String, StringBuilder>();

		}

		else if (!sph_document)
			return;

		else {

			if (qName.equals(document_key_name)) {
				document_key = true;
				return;
			}

			if (qName.equals(PgSchemaUtil.simple_content_name)) {
				content = true;
				return;
			}

			if (!sph_attrs.contains(qName))
				return;

			sph_attr = true;
			sph_mvattr = sph_mvas.contains(qName);

			sph_attr_name = qName;

			buffer.putIfAbsent(qName, new StringBuilder());

		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void endElement(String namespaceURI, String localName, String qName) {

		if (!sph_document)
			return;

		if (qName.equals("sphinx:document")) {

			sph_document = false;

			try {

				int len = sb.length();

				if (len > 0) {

					buffw.write("<" + PgSchemaUtil.simple_content_name + ">" + StringEscapeUtils.escapeXml10(sb.substring(0, len - 1)) + "</" + PgSchemaUtil.simple_content_name + ">\n");
					sb.setLength(0);

				}

				write();

				buffw.write("</sphinx:document>\n");

			} catch (IOException e) {
				e.printStackTrace();
			}

			buffer.clear();

		}

		else {

			if (qName.equals(document_key_name)) {
				document_key = false;
				return;
			}

			if (qName.equals(PgSchemaUtil.simple_content_name)) {
				content = false;
				return;
			}

			sph_attr = sph_mvattr = false;
			sph_attr_name = "";

		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	public void characters(char[] chars, int offset, int length) {

		String value = new String(chars, offset, length);

		if (!sph_document)
			return;

		else if (document_key) {

			try {
				buffw.write("<" + document_key_name + ">" + StringEscapeUtils.escapeXml10(value) + "</" + document_key_name + ">\n");
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		else if (content) {

			int field_len = sb.length() + length;

			if (field_len < sph_max_field_len) {

				if (field_len < sph_co_field_len || sb.indexOf(value) == -1)
					sb.append(value + " ");

			}

		}

		else if (sph_attr) {

			value = StringEscapeUtils.escapeXml10(value);

			StringBuilder _sb = buffer.get(sph_attr_name);

			if (sph_mvattr)
				_sb.append(StringEscapeUtils.escapeCsv(value) + ",");
			else
				_sb.append(value);

		}

	}

	/**
	 * Write Sphinx attributes.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void write() throws IOException {

		for (Entry<String, StringBuilder> e : buffer.entrySet()) {

			StringBuilder _sb = e.getValue();

			int len = _sb.length();

			if (len == 0)
				continue;

			String attr_name = e.getKey();

			sph_mvattr = sph_mvas.contains(attr_name);

			buffw.write("<" + attr_name + ">" + StringEscapeUtils.escapeXml10(sph_mvattr ? _sb.substring(0, len - 1) : _sb.toString()) + "</" + attr_name + ">\n");

			_sb.setLength(0);

		}

	}

}
