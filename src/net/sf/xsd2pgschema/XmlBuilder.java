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
import java.util.LinkedList;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * XML builder.
 *
 * @author yokochi
 */
public class XmlBuilder extends CommonBuilder {

	/** Whether to append XML declaration. */
	public boolean append_declare = true;

	/** Whether to append namespace declaration. */
	public boolean append_xmlns = true;

	/** The indent offset. */
	protected int indent_offset = PgSchemaUtil.indent_offset;

	/** The XML stream writer. */
	protected XMLStreamWriter writer = null;

	/** The appended namespace declarations. */
	protected HashSet<String> appended_xmlns = new HashSet<String>();

	/** The pending element. */
	protected LinkedList<XmlBuilderPendingElem> pending_elem = new LinkedList<XmlBuilderPendingElem>();

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
	 */
	public void setXmlWriter(XMLStreamWriter writer) {

		this.writer = writer;

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
	 * Return current line feed code.
	 *
	 * @return String line feed code
	 */
	public String getLineFeedCode() {
		return line_feed_code;
	}

	/**
	 * Write pending elements.
	 *
	 * @param attr_only whether element has attribute only
	 * @throws XMLStreamException the XML stream exception
	 */
	public void writePendingElems(boolean attr_only) throws XMLStreamException {

		XmlBuilderPendingElem elem;

		int size;

		while ((elem = pending_elem.pollLast()) != null) {

			size = pending_elem.size();

			elem.attr_only = size > 0 ? false : attr_only;

			elem.write(this);

			if (size > 0)
				writer.writeCharacters(line_feed_code);

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
