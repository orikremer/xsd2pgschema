/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2018 Masashi Yokochi

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

import net.sf.xsd2pgschema.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Thread function for xml2json.
 *
 * @author yokochi
 */
public class Xml2JsonThrd implements Runnable {

	/** The thread id. */
	private int thrd_id;

	/** The document builder for reusing. */
	private DocumentBuilder doc_builder;

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The JSON directory. */
	private File json_dir = null;

	/**
	 * Instance of Xml2JsonThrd.
	 *
	 * @param thrd_id thread id
	 * @param is InputStream of XML Schema
	 * @param json_dir directory contains JSON files
	 * @param option PostgreSQL data model option
	 * @param jsonb_option JsonBuilder option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2JsonThrd(final int thrd_id, final InputStream is, final File json_dir, final PgSchemaOption option, final JsonBuilderOption jsonb_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

		this.thrd_id = thrd_id;
		this.json_dir = json_dir;

		// parse XSD document

		DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
		doc_builder_fac.setValidating(false);
		doc_builder_fac.setNamespaceAware(true);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		doc_builder = doc_builder_fac.newDocumentBuilder();

		Document xsd_doc = doc_builder.parse(is);

		is.close();

		doc_builder.reset();

		// XSD analysis

		schema = new PgSchema(doc_builder, xsd_doc, null, xml2json.schema_location, option);

		schema.applyXmlPostEditor(xml2json.xml_post_editor);

		schema.initJsonBuilder(jsonb_option);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFile(xml2json.schema_location, null, option.cache_xsd)) : null;

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml2json.xml_file_queue.size();
		boolean show_progress = thrd_id == 0 && total > 1;

		File xml_file;

		while ((xml_file = xml2json.xml_file_queue.poll()) != null) {

			try {

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml2json.xml_file_filter);

				File json_file = new File(json_dir, xml_parser.basename + "json");

				switch (xml2json.json_type) {
				case column:
					schema.xml2ColJson(xml_parser, json_file);
					break;
				case object:
					schema.xml2ObjJson(xml_parser, json_file);
					break;
				case relational:
					schema.xml2Json(xml_parser, json_file);
					break;
				}

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

			if (show_progress)
				System.out.print("\rConverted " + (total - xml2json.xml_file_queue.size()) + " of " + total + " ...");

		}

		schema.closeXml2Json();

		if (thrd_id == 0)
			System.out.println("\nDone.");

	}

}
