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

	/** The Sphinx data source input file path (base). */
	private Path sph_data_in_base_path;

	/** The Sphinx data source input file path (extracted). */
	private Path sph_data_in_ext_path;

	/** The Sphinx data source output file. */
	private Path sph_data_out_path;

	/** Whether current document is base file or extracted one. */
	private boolean base = true;

	/** The absolute path of document unit. */
	private final String doc_unit_path = "/docset/document";

	/** The StAX read event handlers. */
	private HashMap<Integer, EventHandler> read_handlers = new HashMap<Integer, EventHandler>();

	/** The current path. */
	private StringBuilder cur_path = null;

	/** The XML event writer. */
	private XMLEventWriter xml_writer = null;

	/**
	 * Instance of Sphinx xmlpipe2 document id updater.
	 *
	 * @param sph_data_in_base_path Sphinx data source input file path (base)
	 * @param sph_data_in_ext_path Sphinx data source input file path (extracted)
	 * @param sph_data_out_path Sphix data source output file path
	 */
	public SphDsDocIdUpdater(Path sph_data_in_base_path, Path sph_data_in_ext_path, Path sph_data_out_path) {

		this.sph_data_in_base_path = sph_data_in_base_path;
		this.sph_data_in_ext_path = sph_data_in_ext_path;
		this.sph_data_out_path = sph_data_out_path;

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
	 * Remove documents from Sphinx data source.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws XMLStreamException the XML stream exception
	 */
	public void exec() throws IOException, XMLStreamException {

		XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

		BufferedWriter bout = Files.newBufferedWriter(sph_data_out_path);

		// XML event writer

		xml_writer = out_factory.createXMLEventWriter(bout);

		// Processing sph_data_srouce

		XMLInputFactory in_factory = XMLInputFactory.newInstance();

		InputStream in = PgSchemaUtil.getSchemaInputStream(sph_data_in_base_path);

		// XML event reader of source XML file

		XMLEventReader reader = in_factory.createXMLEventReader(in);

		while (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();

			EventHandler handler = read_handlers.get(event.getEventType());

			handler.handleEvent(event);

		}

		reader.close();

		in.close();

		// Processing sph_data_in_ext_path

		base = false;

		in = PgSchemaUtil.getSchemaInputStream(sph_data_in_ext_path);

		// XML event reader of extracted XML file

		reader = in_factory.createXMLEventReader(in);

		while (reader.hasNext()) {

			XMLEvent event = reader.nextEvent();

			EventHandler handler = read_handlers.get(event.getEventType());

			handler.handleEvent(event);

		}

		reader.close();

		in.close();

		bout.close();

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
		 * @see SphDsDocIdUpdater.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			cur_path = new StringBuilder();

			if (base) {

				try {

					xml_writer.add(element);

				} catch (XMLStreamException e) {
					e.printStackTrace();
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

			if (!base) {

				try {

					xml_writer.add(element);
					xml_writer.close();

				} catch (XMLStreamException e) {
					e.printStackTrace();
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

				if ((base && !local_name.equals("docset")) || !base) {

					addXMLEventWriter(element);

				}

			} catch (XMLStreamException e) {
				e.printStackTrace();
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

			if (base)
				xml_writer.add(event);

		}

		else
			xml_writer.add(event);

	}

}
