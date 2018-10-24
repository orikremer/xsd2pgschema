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

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.LinkedList;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import net.sf.xsd2pgschema.PgSchemaUtil;

/**
 * XML builder.
 *
 * @author yokochi
 */
public class XmlBuilder extends CommonBuilder {

	/** Instance of XMLOutputFactory. */
	public XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

	/** The @xsi:schemaLocation value. */
	public String xsi_schema_location = "";

	/** Whether to append XML declaration. */
	public boolean append_declare = true;

	/** Whether to append namespace declaration. */
	public boolean append_xmlns = true;

	/** Whether to append @xsi:nil="true" for nillable element. */
	public boolean append_nil_elem = true;

	/** Whether data model has nillable element. */
	public boolean has_nillable_element = false;

	/** The indent offset. */
	protected int indent_offset = PgSchemaUtil.indent_offset;

	/** The XML stream writer. */
	public XMLStreamWriter writer = null;

	/** The appended namespace declarations. */
	protected HashSet<String> appended_xmlns = new HashSet<String>();

	/** The pending element. */
	public LinkedList<XmlBuilderPendingElem> pending_elem = new LinkedList<XmlBuilderPendingElem>();

	/**
	 * Set indent offset.
	 *
	 * @param indent_offset indent offset
	 */
	public void setIndentOffset(String indent_offset) {

		this.indent_offset = Integer.valueOf(indent_offset);

		if (this.indent_offset < 0)
			this.indent_offset = 0;
		else if (this.indent_offset > 4)
			this.indent_offset = 4;

	}

	/**
	 * Set XML compact format.
	 */
	public void setCompact() {

		indent_offset = 0;
		setLineFeed(false);

	}

	/**
	 * Set line feed code.
	 *
	 * @param line_feed whether to use line feed code
	 */
	public void setLineFeed(boolean line_feed) {

		this.line_feed = line_feed;
		line_feed_code = line_feed ? "\n" : "";

	}

	/**
	 * Set XML stream writer.
	 *
	 * @param writer XML stream writer
	 * @param out output stream of XML stream writer
	 */
	public void setXmlWriter(XMLStreamWriter writer, OutputStream out) {

		this.writer = writer;
		this.out = out;

	}

	/**
	 * Return current indent offset.
	 *
	 * @return int indent offset
	 */
	public int getIndentOffset() {
		return indent_offset;
	}

	/**
	 * Write pending elements.
	 *
	 * @param attr_only whether element has attribute only
	 * @throws XMLStreamException the XML stream exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void writePendingElems(boolean attr_only) throws XMLStreamException, IOException {

		XmlBuilderPendingElem elem;

		int size;

		while ((elem = pending_elem.pollLast()) != null) {

			size = pending_elem.size();

			elem.attr_only = size > 0 ? false : attr_only;

			elem.write(this);

			if (size > 0)
				writeLineFeedCode();

		}

	}

	/**
	 * Append simple content.
	 *
	 * @param content simple content
	 */
	public void appendSimpleCont(String content) {

		pending_simple_cont.append(content);

	}

	/**
	 * Write pending simple content.
	 *
	 * @throws XMLStreamException the XML stream exception
	 */
	public void writePendingSimpleCont() throws XMLStreamException {

		if (pending_simple_cont.length() == 0)
			return;

		writer.writeCharacters(pending_simple_cont.toString());

		super.clear();

	}

	/** The OutputStream of XML stream writer. */
	private OutputStream out;

	/**
	 * Insert document key.
	 *
	 * @param tag XML start/end element tag template
	 * @param content content
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws XMLStreamException the XML stream exception
	 */
	public void insertDocKey(byte[] tag, String content) throws IOException, XMLStreamException {

		out.write(tag, 1, tag.length - 1);

		writer.writeCharacters(content);

		tag[1] = '/';

		out.write(tag);

		tag[1] = '<';

	}

	/**
	 * Write simple element without consideration of charset.
	 *
	 * @param tag XML start/end element tag template
	 * @param latin_1_encoded whether content is encoded using Latin-1 charset
	 * @param content content
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws XMLStreamException the XML stream exception
	 */
	public void writeSimpleElement(byte[] tag, boolean latin_1_encoded, String content) throws IOException, XMLStreamException {

		out.write(tag, 1, tag.length - (line_feed ? 2 : 1));

		if (latin_1_encoded)
			out.write(PgSchemaUtil.getBytes(content));
		else
			writer.writeCharacters(content);

		tag[1] = '/';

		out.write(tag);

		tag[1] = '<';

	}

	/**
	 * Write simple empty element without consideration of charset.
	 *
	 * @param empty_tag XML empty element tag
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void writeSimpleEmptyElement(byte[] empty_tag) throws IOException {

		out.write(empty_tag);

	}

	/**
	 * Write simple characters without consideration of charset.
	 *
	 * @param string string.
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void writeSimpleCharacters(String string) throws IOException {

		out.write(PgSchemaUtil.getBytes(string));

	}

	/**
	 * Write line feed code.
	 *
	 * @throws XMLStreamException the XML stream exception
	 */
	public void writeLineFeedCode() throws XMLStreamException {

		if (line_feed)
			writer.writeCharacters(line_feed_code);

	}

	/**
	 * Clear XML builder.
	 */
	public void clear() {

		super.clear();

		appended_xmlns.clear();

		XmlBuilderPendingElem elem;

		while ((elem = pending_elem.pollLast()) != null)
			elem.clear();

	}

}
