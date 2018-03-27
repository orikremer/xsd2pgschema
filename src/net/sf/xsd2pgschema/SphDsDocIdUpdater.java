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
import java.util.HashMap;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 * Sphinx xmlpipe2 document id updater.
 *
 * @author yokochi
 */
public class SphDsDocIdUpdater {

	/** The Sphinx data source input file (appending). */
	private File sph_data_source = null;

	/** The Sphinx data source input file (extracted). */
	private File sph_data_extract = null;

	/** The Sphinx data source output file. */
	private File sph_data_update = null;

	/** Whether this document is appending source file or not. */
	private boolean source = true;

	/** The absolute path of document unit. */
	private final String doc_unit_path = "/docset/document";

	/** The StAX read event handlers. */
	private HashMap<Integer, EventHandler> read_handlers = null;

	/** The current path. */
	private StringBuilder cur_path = null;

	/** The XML event writer. */
	private XMLEventWriter xml_writer = null;

	/**
	 * Instance of Sphinx xmlpipe2 document id updater.
	 *
	 * @param sph_data_source Sphinx data source input file (appending)
	 * @param sph_data_extract Sphinx data source input file (extracted)
	 * @param sph_data_update Sphix data source output file
	 */
	public SphDsDocIdUpdater(File sph_data_source, File sph_data_extract, File sph_data_update) {

		this.sph_data_source = sph_data_source;
		this.sph_data_extract = sph_data_extract;
		this.sph_data_update = sph_data_update;

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

		XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

		// XML event writer

		xml_writer = out_factory.createXMLEventWriter(new FileOutputStream(sph_data_update));

		// Processing sph_data_srouce

		XMLInputFactory in_factory = XMLInputFactory.newInstance();

		InputStream in = PgSchemaUtil.getSchemaInputStream(sph_data_source);

		// XML event reader of source XML file

		XMLEventReader reader = in_factory.createXMLEventReader(in);

		while (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();

			EventHandler handler = read_handlers.get(event.getEventType());

			handler.handleEvent(event);

		}

		reader.close();

		in.close();

		// Processing sph_data_extract

		source = false;

		in = PgSchemaUtil.getSchemaInputStream(sph_data_extract);

		// XML event reader of extracted XML file

		reader = in_factory.createXMLEventReader(in);

		while (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();

			EventHandler handler = read_handlers.get(event.getEventType());

			handler.handleEvent(event);

		}

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
		 * @see SphDsDocIdUpdater.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			cur_path = new StringBuilder();

			if (source) {

				try {

					xml_writer.add(element);

				} catch (XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

		}

	}

	/**
	 * The Class EndDocumentReadHandler.
	 */
	class EndDocumentReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see SphDsDocIdUpdater.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (!source) {

				try {

					xml_writer.add(element);
					xml_writer.close();

				} catch (XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			cur_path.setLength(0);

		}

	}

	/**
	 * The Class StartElementReadHandler.
	 */
	class StartElementReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see SphDsDocIdUpdater.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			cur_path.append("/" + element.asStartElement().getName().getLocalPart());

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
		 * @see SphDsDocIdUpdater.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			String local_name = element.asEndElement().getName().getLocalPart();

			try {

				if ((source && !local_name.equals("docset")) || !source)
					addXMLEventWriter(element);

			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

			int len = cur_path.length() - local_name.length() - 1;

			cur_path.setLength(len);

		}

	}

	/**
	 * The Class CommonReadHandler.
	 */
	class CommonReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see SphDsDocIdUpdater.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
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
		 * @see SphDsDocIdUpdater.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
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
	 * Add XML event writer.
	 *
	 * @param event current XML event
	 * @throws XMLStreamException the XML stream exception
	 */
	private void addXMLEventWriter(XMLEvent event) throws XMLStreamException {

		boolean in_doc_unit = cur_path.toString().contains(doc_unit_path);

		if (!in_doc_unit) {

			if (source)
				xml_writer.add(event);

		}

		else
			xml_writer.add(event);

	}

}
