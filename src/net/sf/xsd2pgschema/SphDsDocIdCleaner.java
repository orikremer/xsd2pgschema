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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Sphinx xmlpipe2 document id cleaner.
 *
 * @author yokochi
 */
public class SphDsDocIdCleaner {

	/** The Sphinx data source input file path. */
	private Path sph_data_in_path;

	/** The Sphinx data source output file path. */
	private Path sph_data_out_path;

	/** The set of deleting document id while synchronization. */
	private HashSet<String> del_doc_set;

	/** The set of document id stored in data source. */
	private HashSet<String> doc_set;

	/** Whether this document unit is omitted. */
	private boolean omit_doc_unit = false;

	/** The absolute path of document unit. */
	private final String doc_unit_path = "/docset/document";

	/** The absolute path of document key. */
	private String doc_key_path;

	/** The element path of document key in case that document key is attribute. */
	private String attr_doc_key_holder = null;

	/** Whether document key is attribute or element (false). */
	private boolean attr_doc_key;

	/** The StAX read event handlers. */
	private HashMap<Integer, EventHandler> read_handlers = new HashMap<Integer, EventHandler>();

	/** The current path. */
	private StringBuilder cur_path = null;

	/** Whether no document key has appeared in document unit. */
	private boolean no_document_key;

	/** The list of XML event of interim XML content before document id evaluation. */
	private List<XMLEvent> interim_events = null;

	/** The XML event writer. */
	private XMLEventWriter xml_writer = null;

	/**
	 * Instance of Sphinx xmlpipe2 document id cleaner.
	 *
	 * @param document_key_name document key name
	 * @param sph_data_in_path Sphinx data source input file path
	 * @param sph_data_out_path Sphinx data source output file path
	 * @param del_doc_set set of deleting document id while synchronization
	 */
	public SphDsDocIdCleaner(String document_key_name, Path sph_data_in_path, Path sph_data_out_path, HashSet<String> del_doc_set) {

		doc_key_path = doc_unit_path + "/" + document_key_name;

		this.sph_data_in_path = sph_data_in_path;
		this.sph_data_out_path = sph_data_out_path;
		this.del_doc_set = del_doc_set;

		attr_doc_key = doc_key_path.substring(doc_key_path.lastIndexOf('/') + 1, doc_key_path.length()).startsWith("@");

		if (attr_doc_key)
			attr_doc_key_holder = doc_key_path.substring(0, doc_key_path.lastIndexOf('/'));

		// StAX read event handlers

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
	 * Clean Sphinx data source.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws XMLStreamException the XML stream exception
	 */
	public void exec() throws IOException, XMLStreamException {

		doc_set = new HashSet<String>();

		// XML event reader of source XML file

		XMLInputFactory in_factory = XMLInputFactory.newInstance();

		InputStream in = PgSchemaUtil.getSchemaInputStream(sph_data_in_path);

		XMLEventReader reader = in_factory.createXMLEventReader(in);

		// XML event writer of extracted XML file

		XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

		// XML event writer of extracted XML file

		BufferedWriter bout = Files.newBufferedWriter(sph_data_out_path);

		xml_writer = out_factory.createXMLEventWriter(bout);

		// XML event writer for interim XML content before xml_writer is prepared

		interim_events = new ArrayList<XMLEvent>();

		while (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();

			EventHandler handler = read_handlers.get(event.getEventType());

			handler.handleEvent(event);

		}

		interim_events.clear();

		bout.close();

		reader.close();

		in.close();

		doc_set.clear();

		read_handlers.clear();

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
		 * @see SphDsDocIdCleaner.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			cur_path = new StringBuilder();

			try {

				xml_writer.add(element);

			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * The Class EndDocumentReadHandler.
	 */
	class EndDocumentReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see SphDsDocIdCleaner.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			try {

				xml_writer.add(element);
				xml_writer.close();

			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

			cur_path.setLength(0);

		}

	}

	/**
	 * The Class StartElementReadHandler.
	 */
	class StartElementReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see SphDsDocIdCleaner.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
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

							String doc_id = attr.getValue();

							omit_doc_unit = del_doc_set.contains(doc_id) || !doc_set.add(doc_id);

							if (omit_doc_unit) {

								interim_events.clear();

								return;
							}

						}

					}

				}

			}

			try {

				addXMLEventWriter(element);

			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * The Class EndElementReadHandler.
	 */
	class EndElementReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see SphDsDocIdCleaner.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			try {

				addXMLEventWriter(element);

			} catch (XMLStreamException e) {
				e.printStackTrace();
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
		 * @see SphDsDocIdCleaner.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			try {

				addXMLEventWriter(element);

			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * The Class CharactersReadHandler.
	 */
	class CharactersReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see SphDsDocIdCleaner.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (!attr_doc_key && cur_path.toString().equals(doc_key_path)) {

				no_document_key = false;

				String doc_id = element.asCharacters().getData();

				omit_doc_unit = del_doc_set.contains(doc_id) || !doc_set.add(doc_id);

				if (omit_doc_unit) {

					interim_events.clear();

					return;
				}

			}

			try {

				addXMLEventWriter(element);

			} catch (XMLStreamException e) {
				e.printStackTrace();
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
					}

				});

				interim_events.clear();

			}

			xml_writer.add(event);

		}

	}

}
