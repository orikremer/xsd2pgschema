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

	/** Whether to append XML declaration. */
	public boolean append_declare = true;

	/** Whether to append namespace declaration. */
	public boolean append_xmlns = true;

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
	 * Write simple element without consideration of charset.
	 *
	 * @param prefix prefix
	 * @param local_name local name
	 * @param content content
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws XMLStreamException the XML stream exception
	 */
	public void writeSimpleElement(String prefix, String local_name, String content) throws IOException, XMLStreamException {

		String simple_elem_tab = "<<" + (prefix.isEmpty() ? "" : prefix + ":") + local_name + ">" + line_feed_code;

		byte[] bytes = getBytes(simple_elem_tab);

		out.write(bytes, 1, bytes.length - (line_feed ? 2 : 1));

		writer.writeCharacters(content);

		bytes[1] = '/';

		out.write(bytes);

	}

	/**
	 * Write simple empty element without consideration of charset.
	 *
	 * @param prefix prefix
	 * @param local_name local name
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void writeSimpleEmptyElement(String prefix, String local_name) throws IOException {

		String empty_elem_tab = "<" + (prefix.isEmpty() ? "" : prefix + ":") + local_name + " " + PgSchemaUtil.xsi_prefix + ":nil=\"true\"/>" + line_feed_code;

		out.write(getBytes(empty_elem_tab));

	}

	/**
	 * Write simple characters without consideration of charset.
	 *
	 * @param string string.
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws XMLStreamException the XML stream exception
	 */
	public void writeSimpleCharacters(String string) throws IOException, XMLStreamException {

		out.write(getBytes(string));

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
	 * Return byte array of string without consideration of charset.
	 *
	 * @param string string
	 * @return byte[] byte array of the string
	 */
	private byte[] getBytes(String string) {

		int len = string.length();
		char chars[] = new char[len];

		string.getChars(0, len, chars, 0);

		byte ret[] = new byte[len];

		for (int j = 0; j < len; j++)
			ret[j] = (byte) chars[j];

		return ret;
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
