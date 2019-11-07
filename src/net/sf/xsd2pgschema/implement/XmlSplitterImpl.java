/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017-2019 Masashi Yokochi

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

package net.sf.xsd2pgschema.implement;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.namespace.QName;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathBaseListener;
import com.github.antlr.grammars_v4.xpath.xpathLexer;
import com.github.antlr.grammars_v4.xpath.xpathListenerException;
import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.MainContext;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.serverutil.PgSchemaClientImpl;
import net.sf.xsd2pgschema.serverutil.PgSchemaClientType;
import net.sf.xsd2pgschema.xpathparser.XPathCompList;

/**
 * Implementation of xmlsplitter.
 *
 * @author yokochi
 */
public class XmlSplitterImpl {

	/** The proc id. */
	private int proc_id = 0;

	/** The shard size. */
	private int shard_size;

	/** The PgSchema client. */
	public PgSchemaClientImpl client;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue;

	/** The StAX read event handlers. */
	private HashMap<Integer, EventHandler> read_handlers = new HashMap<Integer, EventHandler>();

	/** The absolute path of document unit. */
	private String doc_unit_path;

	/** The absolute path of document key. */
	private String doc_key_path;

	/** The element path of document key in case that document key is attribute. */
	private String attr_doc_key_holder = null;

	/** Whether document key is attribute or element (false). */
	private boolean attr_doc_key;

	/** The current path. */
	private StringBuilder cur_path = null;

	/** The XML write event factory. */
	private XMLEventFactory xml_event_factory = null;

	/** Whether XML header is empty. */
	private boolean no_header;

	/** Whether no document key has appeared in document unit. */
	private boolean no_document_key;

	/** The list of XML start event for XML header. */
	private List<XMLEvent> header_start_events = null;

	/** The list of XML end event for XML header. */
	private List<XMLEvent> header_end_events = null;

	/** The list of XML event of interim XML content before xml_writer is prepared. */
	private List<XMLEvent> interim_events = null;

	/** The XML event writer. */
	private XMLEventWriter xml_writer = null;

	/** The XML directory paths. */
	private Path[] xml_dir_paths;

	/** Whether any dictionary has been updated. */
	public boolean updated = false;

	/**
	 * Instance of XmlSplitterImpl.
	 *
	 * @param shard_size shard size
	 * @param is InputStream of XML Schema
	 * @param xml_dir_path XML directory path
	 * @param xml_file_queue XML file queue
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param xpath_doc_key XPath expression pointing document key
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 * @throws xpathListenerException the xpath listener exception
	 */
	public XmlSplitterImpl(final int shard_size, final InputStream is, final Path xml_dir_path, final LinkedBlockingQueue<Path> xml_file_queue, final PgSchemaOption option, final FSTConfiguration fst_conf, final String xpath_doc_key) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException, xpathListenerException {

		this.shard_size = shard_size <= 0 ? 1 : shard_size;

		this.xml_file_queue = xml_file_queue;

		client = new PgSchemaClientImpl(is, option, fst_conf, PgSchemaClientType.xpath_evaluation, Thread.currentThread().getStackTrace()[2].getClassName());

		// prepare shard directories

		xml_dir_paths = new Path[shard_size];

		if (shard_size == 1)
			xml_dir_paths[0] = xml_dir_path;

		else {

			for (int shard_id = 0; shard_id < shard_size; shard_id++) {

				xml_dir_paths[shard_id] = Paths.get(xml_dir_path.toString(), PgSchemaUtil.shard_dir_prefix + shard_id);

				if (!Files.isDirectory(xml_dir_paths[shard_id]))
					Files.createDirectory(xml_dir_paths[shard_id]);

			}

		}

		// validate XPath expression with schema

		xpathLexer lexer = new xpathLexer(CharStreams.fromString(xpath_doc_key));

		CommonTokenStream tokens = new CommonTokenStream(lexer);

		xpathParser parser = new xpathParser(tokens);
		parser.addParseListener(new xpathBaseListener());

		MainContext main = parser.main();

		ParseTree tree = main.children.get(0);
		String main_text = main.getText();

		if (parser.getNumberOfSyntaxErrors() > 0 || tree.getSourceInterval().length() == 0)
			throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

		XPathCompList doc_key = new XPathCompList(client.schema, tree, null);

		if (doc_key.comps.size() == 0)
			throw new xpathListenerException("Insufficient XPath expression. (" + main_text + ")");

		doc_key.validate(true);

		if (doc_key.path_exprs.size() == 0)
			throw new xpathListenerException("Insufficient XPath expression. (" + main_text + ")");

		option.verbose = false;

		XPathCompList doc_unit = new XPathCompList(client.schema, tree, null);

		doc_unit.validate(true);

		if (doc_unit.selectParentPath() == 0)
			throw new xpathListenerException("Cound not specify document unit from XPath expression. (" + main_text + ")");

		// decide path of document unit

		while (!doc_unit.hasPathEndsWithTableNode()) {

			if (doc_unit.selectParentPath() == 0)
				throw new xpathListenerException("Cound not specify document unit from XPath expression. (" + main_text + ")");

		}

		doc_unit.removeDuplicatePath();

		StringBuilder sb = new StringBuilder();

		sb.append("Path for document unit:\n");

		doc_unit.showPathExprs(sb);

		// decide path of document key

		doc_key.removeOrphanPath(doc_unit.path_exprs);

		sb.append("Path for document key:\n");

		doc_key.showPathExprs(sb);

		System.out.print(sb.toString());

		sb.setLength(0);

		switch (doc_key.path_exprs.size()) {
		case 1:
			break;
		default:
			throw new xpathListenerException("Cound not specify document key from XPath expression. (" + main_text + ")");
		}

		doc_unit_path = doc_unit.path_exprs.get(0).getReadablePath();
		doc_key_path = doc_key.path_exprs.get(0).getReadablePath();

		if (doc_key.hasPathEndsWithTextNode())
			doc_key_path = doc_key_path.substring(0, doc_key_path.lastIndexOf('/'));

		attr_doc_key = doc_key_path.substring(doc_key_path.lastIndexOf('/') + 1, doc_key_path.length()).startsWith("@");

		if (attr_doc_key)
			attr_doc_key_holder = doc_key_path.substring(0, doc_key_path.lastIndexOf('/'));

		updated = doc_unit.updated || doc_key.updated;

		doc_unit.clear();
		doc_key.clear();

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
	 * Execute splitting large XML file.
	 */
	public void exec() {

		long start_time = System.currentTimeMillis();

		try {

			xml_event_factory = XMLEventFactory.newInstance();

			XMLInputFactory in_factory = XMLInputFactory.newInstance();

			InputStream in;
			XMLEventReader reader;
			XMLEvent event;
			EventHandler handler;
			Path xml_file_path;

			while ((xml_file_path = xml_file_queue.poll()) != null) {

				System.out.println("Splitting " + xml_file_path.getFileName().toString() + "...");

				// XML event reader of source XML file

				in = PgSchemaUtil.getSchemaInputStream(xml_file_path);

				reader = in_factory.createXMLEventReader(in);

				// XML event writer for XML header

				header_start_events = new ArrayList<XMLEvent>();
				header_end_events = new ArrayList<XMLEvent>();

				// XML event writer for interim XML content before xml_writer is prepared

				interim_events = new ArrayList<XMLEvent>();

				no_header = true;

				while (reader.hasNext()) {

					event = reader.nextEvent();

					handler = read_handlers.get(event.getEventType());

					handler.handleEvent(event);

				}

				header_start_events.clear();
				header_end_events.clear();

				interim_events.clear();

				reader.close();

				in.close();

				System.out.println("\nDone.");

			}

		} catch (IOException | XMLStreamException e) {
			e.printStackTrace();
		}

		long end_time = System.currentTimeMillis();

		System.out.println("Execution time: " + (end_time - start_time) + " ms");

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
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			cur_path = new StringBuilder();

			if (no_header) {

				header_start_events.add(element);
				header_end_events.add(xml_event_factory.createEndDocument());

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

			if (no_header) {

				header_start_events.add(element);

				if (doc_unit) {

					// reverse order of end events

					Collections.reverse(header_end_events);

					no_header = false;

				}

				else {

					// append end event

					QName qname = start_elem.getName();

					header_end_events.add(xml_event_factory.createEndElement(qname.getPrefix(), qname.getNamespaceURI(), qname.getLocalPart()));

				}

			}

			else if (xml_writer == null) {

				if (!doc_unit)
					interim_events.add(element);

				if (attr_doc_key && _cur_path.equals(attr_doc_key_holder)) {

					Iterator<?> iter_attr = start_elem.getAttributes();

					if (iter_attr != null) {

						javax.xml.stream.events.Attribute attr;

						while (iter_attr.hasNext()) {

							attr = (Attribute) iter_attr.next();

							if (doc_key_path.equals(attr_doc_key_holder + "/@" + attr.getName())) {

								try {

									createXMLEventWriter(PgSchemaUtil.collapseWhiteSpace(attr.getValue()));

								} catch (PgSchemaException | IOException | XMLStreamException e) {
									e.printStackTrace();
								}

								break;
							}

						}

					}

				}

			}

			else {

				try {

					xml_writer.add(element);

				} catch (XMLStreamException e) {
					e.printStackTrace();
				}

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

			boolean doc_unit = cur_path.toString().equals(doc_unit_path);

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null) {

				if (!doc_unit)
					interim_events.add(element);

			}

			else {

				try {

					xml_writer.add(element);

				} catch (XMLStreamException e) {
					e.printStackTrace();
				}

			}

			if (doc_unit) {

				try {

					closeXMLEventWriter();

				} catch (XMLStreamException | IOException e) {
					e.printStackTrace();
				}

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

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null) {

				if (!cur_path.toString().equals(doc_unit_path))
					interim_events.add(element);

			}

			else {

				try {

					xml_writer.add(element);

				} catch (XMLStreamException e) {
					e.printStackTrace();
				}

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

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null) {

				if (!cur_path.toString().equals(doc_unit_path))
					interim_events.add(element);

				if (no_document_key && !attr_doc_key && cur_path.toString().equals(doc_key_path)) {

					try {

						createXMLEventWriter(PgSchemaUtil.collapseWhiteSpace(element.asCharacters().getData()));

					} catch (PgSchemaException | IOException | XMLStreamException e) {
						e.printStackTrace();
					}

				}

			}

			else {

				try {

					xml_writer.add(element);

				} catch (XMLStreamException e) {
					e.printStackTrace();
				}

			}

		}

	}

	/** The buffered writer for split XML file. */
	private BufferedWriter bout = null;

	/**
	 * Create XML event writer.
	 *
	 * @param document_id current document key
	 * @throws PgSchemaException the pg schema exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws XMLStreamException the XML stream exception
	 */
	private void createXMLEventWriter(String document_id) throws PgSchemaException, IOException, XMLStreamException {

		if (document_id == null || document_id.isEmpty())
			throw new PgSchemaException("Invalid document id.");

		no_document_key = false;

		Path xml_file_path = Paths.get(xml_dir_paths[proc_id++ % shard_size].toString(), document_id + ".xml");

		XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

		bout = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(xml_file_path), PgSchemaUtil.def_encoding), PgSchemaUtil.def_buffered_output_stream_buffer_size);

		// XML event writer of split XML file

		xml_writer = out_factory.createXMLEventWriter(bout);

		header_start_events.forEach(event -> {

			try {

				xml_writer.add(event);

			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

		});

		if (!interim_events.isEmpty()) {

			interim_events.forEach(event -> {

				try {

					xml_writer.add(event);

				} catch (XMLStreamException e) {
					e.printStackTrace();
				}

			});

			interim_events.clear();

		}

	}

	/**
	 * Close XML event writer.
	 *
	 * @throws XMLStreamException the XML stream exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void closeXMLEventWriter() throws XMLStreamException, IOException {

		header_end_events.forEach(event -> {

			try {

				xml_writer.add(event);

			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

		});

		xml_writer.close();

		bout.close();

		// set null for recursive document generation

		xml_writer = null;

		System.out.print("\rGenerated " + proc_id + " XML documents.");

	}

}
