/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017 Masashi Yokochi

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
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

import org.antlr.v4.runtime.tree.ParseTree;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathListenerException;
import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.MainContext;

import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaOption;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.XPathCompList;

/**
 * Implementation of xmlsplitter.
 *
 * @author yokochi
 */
public class XmlSplitterImpl {

	/** The shard id. */
	private int shard_id = 0;

	/** The shard size. */
	private int shard_size = 1;

	/** The PostgreSQL schema. */
	private PgSchema schema = null;

	/** The StAX read event handlers. */
	private HashMap<Integer, EventHandler> read_handlers = null;

	/** The absolute path of document unit. */
	private String doc_unit_path = null;

	/** The absolute path of document key. */
	private String doc_key_path = null;

	/** The element path of document key in case that document key is attribute. */
	private String attr_doc_key_holder = null;

	/** Whether document key is attribute or not (element). */
	private boolean attr_doc_key;

	/** The current path. */
	private StringBuilder cur_path = null;

	/** The XML write event factory. */
	private XMLEventFactory xml_event_factory = null;

	/** Whether XML header is empty or not. */
	private boolean no_header;

	/** Whether no document key has appeared in document unit. */
	private boolean no_document_key;

	/** The list of XML start event of XML header. */
	private List<XMLEvent> header_start_events = null;

	/** The list of XML end event of XML header. */
	private List<XMLEvent> header_end_events = null;

	/** The list of XML event of interim XML content before xml_writer is prepared. */
	private List<XMLEvent> interim_events = null;

	/** The XML event writer. */
	private XMLEventWriter xml_writer = null;

	/** The xml directory. */
	private File[] xml_dir = null;

	/**
	 * Instance of XmlSplitterImpl.
	 *
	 * @param shard_size shard size
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL schema option
	 * @param parser XPath parser
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 * @throws xpathListenerException the xpath listener exception
	 */
	public XmlSplitterImpl(final int shard_size, final InputStream is, final PgSchemaOption option, final xpathParser parser) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException, xpathListenerException {

		this.shard_size = shard_size <= 0 ? 1 : shard_size;

		// parse XSD document

		DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
		doc_builder_fac.setValidating(false);
		doc_builder_fac.setNamespaceAware(true);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		DocumentBuilder doc_builder = doc_builder_fac.newDocumentBuilder();

		Document xsd_doc = doc_builder.parse(is);

		is.close();

		doc_builder.reset();

		// XSD analysis

		schema = new PgSchema(doc_builder, xsd_doc, null, PgSchemaUtil.getName(xmlsplitter.schema_location), option);

		// prepare shard directories

		xml_dir = new File[shard_size];

		if (shard_size == 1)
			xml_dir[0] = new File(xmlsplitter.xml_dir_name);

		else {

			for (int shard_id = 0; shard_id < shard_size; shard_id++) {

				String xml_dir_name = xmlsplitter.xml_dir_name + "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

				xml_dir[shard_id] = new File(xml_dir_name);

				if (!xml_dir[shard_id].isDirectory()) {

					if (!xml_dir[shard_id].mkdir())
						throw new PgSchemaException("Couldn't create directory '" + xml_dir_name + "'.");

				}

			}

		}

		// validate XPath expression with schema

		MainContext main = parser.main();

		ParseTree tree = main.children.get(0);
		String main_text = main.getText();

		if (parser.getNumberOfSyntaxErrors() > 0 || tree.getSourceInterval().length() == 0)
			throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

		XPathCompList doc_key = new XPathCompList(tree, false);

		if (doc_key.comps.size() == 0)
			throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

		schema.validateXPath(doc_key, true, true);

		if (doc_key.paths.size() == 0)
			throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

		XPathCompList doc_unit = new XPathCompList();

		doc_unit.replacePath(doc_key);

		if (doc_unit.selectParentPath() == 0)
			throw new xpathListenerException("Cound not specify document unit from XPath expression. (" + main_text + ")");

		// decide node of document unit

		while (!doc_unit.hasPathEndsWithTableNode()) {

			if (doc_unit.selectParentPath() == 0)
				throw new xpathListenerException("Cound not specify document unit from XPath expression. (" + main_text + ")");

		}

		doc_unit.removeDuplicatePath();

		System.out.println("document unit:");
		doc_unit.showPath(" ");

		doc_key.removeOrphanPath(doc_unit.paths);

		System.out.println("document key:");
		doc_key.showPath(" ");

		switch (doc_key.paths.size()) {
		case 1:
			break;
		default:
			throw new xpathListenerException("Cound not specify document key from XPath expression. (" + main_text + ")");
		}

		doc_unit_path = doc_unit.paths.get(0);
		doc_key_path = doc_key.paths.get(0);

		if (doc_key.hasPathEndsWithTextNode())
			doc_key_path = doc_key_path.substring(0, doc_key_path.lastIndexOf("/"));

		attr_doc_key = doc_key_path.substring(doc_key_path.lastIndexOf("/") + 1, doc_key_path.length()).startsWith("@");

		if (attr_doc_key)
			attr_doc_key_holder = doc_key_path.substring(0, doc_key_path.lastIndexOf("/"));

		// StAX read handlers

		read_handlers = new HashMap<Integer, EventHandler>();

		read_handlers.put(XMLEvent.START_DOCUMENT, new StartDocumentReadHandler());
		read_handlers.put(XMLEvent.END_DOCUMENT, new EndDocumentReadHandler());

		read_handlers.put(XMLEvent.START_ELEMENT, new StartElementReadHandler());
		read_handlers.put(XMLEvent.END_ELEMENT, new EndElementReadHandler());

		read_handlers.put(XMLEvent.ATTRIBUTE, new AttributeReadHandler());

		read_handlers.put(XMLEvent.NAMESPACE, new NameSpaceReadHandler());

		read_handlers.put(XMLEvent.CHARACTERS, new CharactersReadHandler());
		read_handlers.put(XMLEvent.SPACE, new SpaceReadHandler());
		read_handlers.put(XMLEvent.CDATA, new CDataReadHandler());

		read_handlers.put(XMLEvent.COMMENT, new CommentReadHandler());

		read_handlers.put(XMLEvent.PROCESSING_INSTRUCTION, new ProcessingInstructionReadHandler());

		read_handlers.put(XMLEvent.ENTITY_REFERENCE, new EntityReferenceReadHandler());
		read_handlers.put(XMLEvent.ENTITY_DECLARATION, new EntityDeclarationReadHandler());

		read_handlers.put(XMLEvent.DTD, new DTDReadHandler());

		read_handlers.put(XMLEvent.NOTATION_DECLARATION, new NotationDeclarationReadHandler());

	}

	/**
	 * Execute splitting large XML file.
	 */
	public void exec() {

		try {

			xml_event_factory = XMLEventFactory.newInstance();

			for (File xml_file : xmlsplitter.xml_files) {

				if (!xml_file.isFile())
					continue;

				System.out.println("Splitting " + xml_file.getName() + "...");

				// XML event reader of source XML file

				XMLInputFactory in_factory = XMLInputFactory.newInstance();

				InputStream in = PgSchemaUtil.getInputStream(xml_file);

				XMLEventReader reader = in_factory.createXMLEventReader(in);

				// XML event writer for XML header

				header_start_events = new ArrayList<XMLEvent>();
				header_end_events = new ArrayList<XMLEvent>();

				// XML event writer for interim XML content before xml_writer is prepared

				interim_events = new ArrayList<XMLEvent>();

				no_header = true;

				while (reader.hasNext()) {

					XMLEvent event = reader.nextEvent();

					EventHandler handler = read_handlers.get(event.getEventType());

					handler.handleEvent(event);

				}

				header_start_events.clear();
				header_end_events.clear();

				interim_events.clear();

				reader.close();

				in.close();

				System.out.println("Done.");

			}

		} catch (IOException | XMLStreamException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

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

		File xml_file = new File(xml_dir[shard_id++ % shard_size], document_id + ".xml");

		XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

		// XML event writer of split XML file

		xml_writer = out_factory.createXMLEventWriter(new FileOutputStream(xml_file));

		header_start_events.forEach(event -> {

			try {
				xml_writer.add(event);
			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

		});

		if (!interim_events.isEmpty()) {

			interim_events.forEach(event -> {
				try {
					xml_writer.add(event);
				} catch (XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

			interim_events.clear();

		}

	}

	/**
	 * Close XML event writer.
	 *
	 * @throws XMLStreamException the XML stream exception
	 */
	private void closeXMLEventWriter() throws XMLStreamException {

		header_end_events.forEach(event -> {

			try {
				xml_writer.add(event);
			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

		});

		xml_writer.close();

		xml_writer = null;

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

				QName qname = start_elem.getName();

				header_end_events.add(xml_event_factory.createEndElement(qname.getPrefix(), qname.getNamespaceURI(), qname.getLocalPart()));

				if (doc_unit) {

					// reverse order

					Collections.reverse(header_end_events);

					no_header = false;

				}

			}

			else if (xml_writer == null)
				interim_events.add(element);

			else {

				try {
					xml_writer.add(element);
				} catch (XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			if (attr_doc_key && _cur_path.equals(attr_doc_key_holder)) {

				Iterator<?> iter_attr = start_elem.getAttributes();

				if (iter_attr != null) {

					while (iter_attr.hasNext()) {

						javax.xml.stream.events.Attribute attr = (Attribute) iter_attr.next();

						if (doc_key_path.equals(attr_doc_key_holder + "/@" + attr.getName())) {

							try {

								createXMLEventWriter(attr.getValue().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", ""));

							} catch (PgSchemaException | IOException | XMLStreamException e) {
								e.printStackTrace();
								System.exit(1);
							}

							break;
						}

					}

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

			if (doc_unit) {

				try {
					closeXMLEventWriter();
				} catch (XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			else {

				if (no_header)
					header_start_events.add(element);

				else if (xml_writer == null)
					interim_events.add(element);

				else {

					try {
						xml_writer.add(element);
					} catch (XMLStreamException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

			}

			int len = cur_path.length() - element.asEndElement().getName().getLocalPart().length() - 1;

			cur_path.setLength(len);

		}

	}

	/**
	 * The Class AttributeReadHandler.
	 */
	class AttributeReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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
	 * The Class NameSpaceReadHandler.
	 */
	class NameSpaceReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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

			else if (xml_writer == null)
				interim_events.add(element);

			else {

				try {
					xml_writer.add(element);
				} catch (XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			if (no_document_key && !attr_doc_key && doc_key_path.equals(cur_path.toString())) {

				try {

					createXMLEventWriter(element.asCharacters().getData().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", ""));

				} catch (PgSchemaException | IOException | XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

		}

	}

	/**
	 * The Class SpaceReadHandler.
	 */
	class SpaceReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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
	 * The Class CDataReadHandler.
	 */
	class CDataReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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
	 * The Class CommentReadHandler.
	 */
	class CommentReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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
	 * The Class ProcessingInstructionReadHandler.
	 */
	class ProcessingInstructionReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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
	 * The Class EntityReferenceReadHandler.
	 */
	class EntityReferenceReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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
	 * The Class EntityDeclarationReadHandler.
	 */
	class EntityDeclarationReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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
	 * The Class DTDReadHandler.
	 */
	class DTDReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

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
	 * The Class NotationDeclarationReadHandler.
	 */
	class NotationDeclarationReadHandler implements EventHandler {

		/* (non-Javadoc)
		 * @see XmlSplitterImpl.EventHandler#handleEvent(javax.xml.stream.events.XMLEvent)
		 */
		@Override
		public void handleEvent(XMLEvent element) {

			if (no_header)
				header_start_events.add(element);

			else if (xml_writer == null)
				interim_events.add(element);

			else {

				try {
					xml_writer.add(element);
				} catch (XMLStreamException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

		}

	}

}
