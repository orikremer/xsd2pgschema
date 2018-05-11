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

import javax.xml.stream.XMLStreamWriter;

/**
 * XML builder.
 *
 * @author yokochi
 */
public class XmlBuilder {

	/** Whether append namespace declaration. */
	public boolean append_xmlns = true;

	/** Whether insert document key. */
	public boolean insert_doc_key = false;

	/** The indent offset. */
	protected int indent_offset = PgSchemaUtil.indent_offset;

	/** The current line feed code. */
	protected String line_feed_code = "\n";

	/** The XML stream writer. */
	protected XMLStreamWriter writer = null;

	/** The initial indent space. */
	protected String init_indent_space = "";

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
	 * @param line_feed whether use line feed code or not.
	 */
	public void setLineFeed(boolean line_feed) {

		line_feed_code = line_feed ? "\n" : "";

	}

	/**
	 * Set initial indent offset.
	 *
	 * @param init_indent_offset initial indent offset
	 */
	public void setInitIndentOffset(int init_indent_offset) {

		if (init_indent_offset < 0)
			init_indent_offset = 0;
		else if (init_indent_offset > 4)
			init_indent_offset = 4;

		StringBuilder sb = new StringBuilder();

		for (int l = 0; l < init_indent_offset; l++)
			sb.append(" ");

		init_indent_space = sb.toString();

		sb.setLength(0);

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

}
