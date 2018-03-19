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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

/**
 * Sphinx xmlpipe2 document id remover.
 *
 * @author yokochi
 */
public class SphDsDocIdRemover {

	/** The Sphinx data source input file. */
	private File sph_data_source = null;

	/** The Sphinx data source output file. */
	private File sph_data_extract = null;

	/** The set of deleting document id while synchronization. */
	private HashSet<String> sync_delete_ids = null;

	/** Whether this document unit is omitted or not. */
	private boolean omit_doc_unit = false;

	/** The absolute path of document unit. */
	private final String doc_unit_path = "/docset/document";

	/** The absolute path of document key. */
	private String doc_key_path;

	/** The element path of document key in case that document key is attribute. */
	private String attr_doc_key_holder = null;

	/** Whether document key is attribute or not (element). */
	private boolean attr_doc_key;

	/** The StAX read event handlers. */
	private HashMap<Integer, EventHandler> read_handlers = null;

	/** The current path. */
	private StringBuilder cur_path = null;

	/** Whether no document key has appeared in document unit. */
	private boolean no_document_key;

	/** The list of XML event of interim XML content before document id evaluation. */
	private List<XMLEvent> interim_events = null;

	/** The XML event writer. */
	private XMLEventWriter xml_writer = null;

	/**
	 * Instance of Sphinx xmlpipe2 document id remover.
	 *
	 * @param schema PostgreSQL data model
	 * @param sph_data_source Sphinx data source input file
	 * @param sph_data_extract Sphinx data source output file
	 * @param sync_delete_ids set of deleting document id while synchronization
	 */
	public SphDsDocIdRemover(PgSchema schema, File sph_data_source, File sph_data_extract, HashSet<String> sync_delete_ids) {

		doc_key_path = doc_unit_path + "/" + schema.option.document_key_name;

		this.sph_data_source = sph_data_source;
		this.sph_data_extract = sph_data_extract;
		this.sync_delete_ids = sync_delete_ids;

		attr_doc_key = doc_key_path.substring(doc_key_path.lastIndexOf("/") + 1, doc_key_path.length()).startsWith("@");

		if (attr_doc_key)
			attr_doc_key_holder = doc_key_path.substring(0, doc_key_path.lastIndexOf("/"));

		// StAX read event handlers

		read_handlers = new HashMap<Integer, EventHandler>();

		read_handlers.put(XMLEvent.START_DOCUMENT, new StartDocumentReadHandler());
		read_handlers.put(XMLEvent.END_DOCUMENT, new EndDocumentReadHandler());

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
	 * Remove documents from Sphinx data source.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws XMLStreamException the XML stream exception
	 */
	public void exec() throws IOException, XMLStreamException {

		// XML event reader of source XML file

		XMLInputFactory in_factory = XMLInputFactory.newInstance();

		InputStream in = PgSchemaUtil.getSchemaInputStream(sph_data_source);

		XMLEventReader reader = in_factory.createXMLEventReader(in);

		// XML event writer of extracted XML file

		XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

		// XML event writer of extracted XML file

		xml_writer = out_factory.createXMLEventWriter(new FileOutputStream(sph_data_extract));

		// XML event writer for interim XML content before xml_writer is prepared

		interim_events = new ArrayList<XMLEvent>();

		while (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();

			EventHandler handler = read_handlers.get(event.getEventType());

			handler.handleEvent(event);

		}

		interim_events.clear();

		reader.close();

		in.close();

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
	 * The Class StartDocumentReadHandler.
	 */
	class StartDocumentReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			cur_path = new StringBuilder();

			try {
				xml_writer.add(element);
			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/**
	 * The Class EndDocumentReadHandler.
	 */
	class EndDocumentReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			try {

				xml_writer.add(element);
				xml_writer.close();
				xml_writer = null;

			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

			cur_path.setLength(0);

		}

	}

	/**
	 * The Class StartElementReadHandler.
	 */
	class StartElementReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			StartElement start_elem = element.asStartElement();

			cur_path.append("/" + start_elem.getName().getLocalPart());

			String _cur_path = cur_path.toString();

			boolean doc_unit = _cur_path.equals(doc_unit_path);

			if (doc_unit)
				no_document_key = true;

			if (attr_doc_key && _cur_path.equals(attr_doc_key_holder)) {

				Iterator<?> iter_attr = start_elem.getAttributes();

				if (iter_attr != null) {

					while (iter_attr.hasNext()) {

						javax.xml.stream.events.Attribute attr = (Attribute) iter_attr.next();

						if (doc_key_path.equals(attr_doc_key_holder + "/@" + attr.getName())) {

							no_document_key = false;
							omit_doc_unit = sync_delete_ids.contains(attr.getValue().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", ""));

							break;
						}

					}

				}

			}

			try {
				addXMLEventWriter(element);
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
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			try {
				addXMLEventWriter(element);
			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

			int len = cur_path.length() - element.asEndElement().getName().getLocalPart().length() - 1;

			cur_path.setLength(len);

		}

	}

	/**
	 * The Class CommonReadHandler.
	 */
	class CommonReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			try {
				addXMLEventWriter(element);
			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/**
	 * The Class CharactersReadHandler.
	 */
	class CharactersReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (!attr_doc_key && cur_path.toString().equals(doc_key_path)) {

				no_document_key = false;
				omit_doc_unit = sync_delete_ids.contains(element.asCharacters().getData().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", ""));

			}

			try {
				addXMLEventWriter(element);
			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/**
	 * Add XML event writer.
	 *
	 * @param event current XML event
	 * @throws XMLStreamException the XML stream exception
	 */
	private void addXMLEventWriter(XMLEvent event) throws XMLStreamException {

		boolean in_doc_unit = cur_path.toString().contains(doc_unit_path);

		if (!in_doc_unit)
			xml_writer.add(event);

		else if (no_document_key)
			interim_events.add(event);

		else if (!omit_doc_unit) {

			if (interim_events.size() > 0) {

				interim_events.forEach(_event -> {

					try {
						xml_writer.add(_event);
					} catch (XMLStreamException e) {
						e.printStackTrace();
						System.exit(1);
					}

				});

				interim_events.clear();

			}

			xml_writer.add(event);

		}

	}

}
