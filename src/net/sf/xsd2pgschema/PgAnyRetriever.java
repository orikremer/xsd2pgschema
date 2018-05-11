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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.xml.sax.helpers.DefaultHandler;

/**
 * Retrieve any stored in PostgreSQL.
 *
 * @author yokochi
 */
public class PgAnyRetriever extends DefaultHandler {

	/** The root node name. */
	private String root_node_name = null;

	/** The target namespace. */
	private String target_namespace = null;

	/** The prefix of target namespace. */
	private String prefix = null;

	/** The nest tester. */
	private PgSchemaNestTester test = null;

	/** The XML builder. */
	private XmlBuilder xmlb = null;

	/** The XML input factory. */
	private XMLInputFactory in_factory = null;

	/** The current state for root node. */
	private boolean root_node = false;

	/** The current path. */
	private StringBuilder cur_path = null;

	/** The StAX read event handlers. */
	private HashMap<Integer, EventHandler> read_handlers = null;

	/**
	 * Instance of any retriever.
	 */
	public PgAnyRetriever() {

		in_factory = XMLInputFactory.newInstance();

		cur_path = new StringBuilder();

		// StAX read event handlers

		read_handlers = new HashMap<Integer, EventHandler>();

		read_handlers.put(XMLEvent.START_DOCUMENT, new CommonReadHandler());
		read_handlers.put(XMLEvent.END_DOCUMENT, new CommonReadHandler());

		read_handlers.put(XMLEvent.START_ELEMENT, new StartElementReadHandler());
		read_handlers.put(XMLEvent.END_ELEMENT, new EndElementReadHandler());

		read_handlers.put(XMLEvent.ATTRIBUTE, new CommonReadHandler());

		read_handlers.put(XMLEvent.NAMESPACE, new CommonReadHandler());

		read_handlers.put(XMLEvent.CHARACTERS, new CharactersReadHandler());
		read_handlers.put(XMLEvent.SPACE, new CommonReadHandler());
		read_handlers.put(XMLEvent.CDATA, new CommonReadHandler());

		read_handlers.put(XMLEvent.COMMENT, new CommonReadHandler());

		read_handlers.put(XMLEvent.PROCESSING_INSTRUCTION, new CommonReadHandler());

		read_handlers.put(XMLEvent.ENTITY_REFERENCE, new CommonReadHandler());
		read_handlers.put(XMLEvent.ENTITY_DECLARATION, new CommonReadHandler());

		read_handlers.put(XMLEvent.DTD, new CommonReadHandler());

		read_handlers.put(XMLEvent.NOTATION_DECLARATION, new CommonReadHandler());

	}

	/**
	 * Retrieve any content.
	 *
	 * @param in XML input stream
	 * @param table current table
	 * @param test nest test result of this node
	 * @param xmlb XML builder
	 * @return PgSchemaNestTetster result of nest test
	 * @throws XMLStreamException the XML stream exception
	 */
	public PgSchemaNestTester exec(InputStream in, PgTable table, PgSchemaNestTester test, XmlBuilder xmlb) throws XMLStreamException {

		this.root_node_name = table.name;
		this.target_namespace = table.target_namespace;
		this.prefix = table.prefix;

		this.test = test;
		this.xmlb = xmlb;

		root_node = false;

		XMLEventReader reader = in_factory.createXMLEventReader(in);

		while (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();

			EventHandler handler = read_handlers.get(event.getEventType());

			handler.handleEvent(event);

		}

		reader.close();

		cur_path.setLength(0);

		return test;
	}

	/**
	 * The Interface EventHandler.
	 */
	interface EventHandler {

		/**
		 * Handle event.
		 *
		 * @param element the element
		 */
		public void handleEvent(XMLEvent element);
	}

	/**
	 * The Class StartElementReadHandler.
	 */
	class StartElementReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see PgAnyRetriever.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			StartElement start_elem = element.asStartElement();

			String start_elem_name = start_elem.getName().getLocalPart();

			if (start_elem_name.equals(root_node_name))
				root_node = true;

			else if (!root_node)
				return;

			else
				cur_path.append("/" + start_elem_name);

			try {

				if (cur_path.length() > 0) {

					xmlb.writer.writeCharacters((test.has_child_elem ? "" : xmlb.line_feed_code) + test.child_indent_space);

					xmlb.writer.writeStartElement(prefix, start_elem_name, target_namespace);

					test.child_indent_space += test.indent_space;
					test.current_indent_space = test.child_indent_space;

					test.has_child_elem = true;

				}

				Iterator<?> iter_attr = start_elem.getAttributes();

				if (iter_attr != null) {

					while (iter_attr.hasNext()) {

						javax.xml.stream.events.Attribute attr = (Attribute) iter_attr.next();

						String content = attr.getValue();

						if (content != null && !content.isEmpty()) {

							xmlb.writer.writeAttribute(attr.getName().getLocalPart(), content);

							test.has_content = true;

						}

					}

				}

			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/**
	 * The Class EndElementReadHandler.
	 */
	class EndElementReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see PgAnyRetriever.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (!root_node)
				return;

			int len = cur_path.length() - element.asEndElement().getName().getLocalPart().length() - 1;

			cur_path.setLength(len);

			if (len == 0)
				root_node = false;

			else {

				try {

					xmlb.writer.writeEndElement();
					xmlb.writer.writeCharacters(xmlb.line_feed_code);

					test.child_indent_space = test.getParentIndentSpace();
					test.current_indent_space = test.child_indent_space;

				} catch (XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

		}

	}

	/**
	 * The Class CommonReadHandler.
	 */
	class CommonReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see PgAnyRetriever.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {
		}

	}

	/**
	 * The Class CharactersReadHandler.
	 */
	class CharactersReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see PgAnyRetriever.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (!root_node)
				return;

			try {

				String content = element.asCharacters().getData();

				if (content != null && !content.isEmpty()) {

					xmlb.writer.writeCharacters(content);

					test.has_content = true;

				}

			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

}
