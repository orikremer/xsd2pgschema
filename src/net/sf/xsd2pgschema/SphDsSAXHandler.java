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

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.text.StringEscapeUtils;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Sphinx xmlpipe2 SAX handler.
 *
 * @author yokochi
 */
public class SphDsSAXHandler extends DefaultHandler {

	/** The PostgreSQL data model. */
	PgSchema schema;

	/** The Sphinx data source writer. */
	FileWriter writer;

	/** The Sphinx maximum field length. (related to max_xmlpipe2_field in sphinx.conf) */
	int sph_max_field_len;

	/** The cutoff (80% of hard limit, max_field_len) of Sphinx field length. */
	int sph_cutoff_field_len;

	/** The current state for sphinx:document. */
	boolean sph_document = false;

	/** The current state for document_id. */
	boolean document_id = false;

	/** The current state for default content. */
	boolean content = false;

	/** Whether Sphinx attribute. */
	boolean sph_attr = false;

	/** Whether Sphinx multi-valued attribute. */
	boolean sph_mvattr = false;

	/** The current Sphinx attribute name. */
	String sph_attr_name = null;

	/** The string builder for default field. */
	StringBuilder sb = null;

	/** The holder of string builder for each Sphinx attribute. */
	HashMap<String, StringBuilder> buffer = null;

	/**
	 * Instance of Sphinx xmlpipe2 SAX handler.
	 *
	 * @param schema PostgreSQL data model
	 * @param writer Sphinx data source writer
	 * @param index_filter index filter
	 */
	public SphDsSAXHandler(PgSchema schema, FileWriter writer, IndexFilter index_filter) {

		this.schema = schema;
		this.writer = writer;

		sph_max_field_len = index_filter.sph_max_field_len;
		sph_cutoff_field_len = (int) (index_filter.sph_max_field_len * 0.8); // 80% of hard limit

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) {

		if (qName.equals("sphinx:document")) {

			sph_document = true;

			try {
				writer.write("<sphinx:document id=\"" + atts.getValue("id") + "\">\n");
			} catch (IOException e) {
				e.printStackTrace();
			}

			sb = new StringBuilder();

			buffer = new HashMap<String, StringBuilder>();

		}

		else if (!sph_document)
			return;

		else {

			if (qName.equals(schema.option.document_key_name)) {
				document_id = true;
				return;
			}

			if (qName.equals(PgSchemaUtil.simple_content_name)) {
				content = true;
				return;
			}

			String[] concat_name = qName.replaceAll("__", "\\.").split("\\.");

			if (concat_name.length != 2)
				return;

			String table_name = concat_name[0];
			String field_name = concat_name[1];

			if (!schema.isSphAttr(table_name, field_name))
				return;

			sph_attr = true;
			sph_mvattr = schema.isSphMVAttr(table_name, field_name);

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

					writer.write("<" + PgSchemaUtil.simple_content_name + ">" + StringEscapeUtils.escapeXml10(sb.substring(0, len - 1)) + "</" + PgSchemaUtil.simple_content_name + ">\n");
					sb.setLength(0);

				}

				write();

				writer.write("</sphinx:document>\n");

			} catch (IOException e) {
				e.printStackTrace();
			}

			buffer.clear();
			buffer = null;

		}

		else {

			if (qName.equals(schema.option.document_key_name)) {
				document_id = false;
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

		else if (document_id) {

			try {
				writer.write("<" + schema.option.document_key_name + ">" + StringEscapeUtils.escapeXml10(value) + "</" + schema.option.document_key_name + ">\n");
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		else if (content) {

			int field_len = sb.length() + length;

			if (field_len < sph_max_field_len) {

				if (field_len < sph_cutoff_field_len || sb.indexOf(value) == -1)
					sb.append(value + " ");

			}

		}

		else if (sph_attr) {

			if (sph_mvattr)
				buffer.get(sph_attr_name).append(StringEscapeUtils.escapeCsv(value) + ",");
			else
				buffer.get(sph_attr_name).append(value.replaceAll("\t", " ") + "\t");

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
			writer.write("<" + attr_name + ">" + StringEscapeUtils.escapeXml10(_sb.substring(0, len - 1)) + "</" + attr_name + ">\n");

			_sb.setLength(0);

		}

	}

}
