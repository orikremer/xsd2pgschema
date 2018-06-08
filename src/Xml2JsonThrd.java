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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

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

	/** The JSON directory path. */
	private Path json_dir_path = null;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter = null;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue = null;

	/**
	 * Instance of Xml2JsonThrd.
	 *
	 * @param thrd_id thread id
	 * @param is InputStream of XML Schema
	 * @param json_dir_path directory path contains JSON files
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param xml_post_editor XML post editor
	 * @param option PostgreSQL data model option
	 * @param jsonb_option JsonBuilder option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2JsonThrd(final int thrd_id, final InputStream is, final Path json_dir_path, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final PgSchemaOption option, final JsonBuilderOption jsonb_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

		this.thrd_id = thrd_id;
		this.json_dir_path = json_dir_path;

		this.xml_file_filter = xml_file_filter;
		this.xml_file_queue = xml_file_queue;

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

		schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, option);

		schema.applyXmlPostEditor(xml_post_editor);

		schema.initJsonBuilder(jsonb_option);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFilePath(option.root_schema_location, null, option.cache_xsd), option.full_check) : null;

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		boolean show_progress = thrd_id == 0 && total > 1;

		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");

		long start_time = System.currentTimeMillis();

		Path xml_file_path;

		while ((xml_file_path = xml_file_queue.poll()) != null) {

			if (show_progress) {

				long current_time = System.currentTimeMillis();

				int remains = xml_file_queue.size();
				int progress = total - remains;

				long etc_time = current_time + (current_time - start_time) * remains / progress;
				Date etc_date = new Date(etc_time);

				System.out.print("\rConverted " + progress + " of " + total + " ... (ETC " + sdf.format(etc_date) + ")");

			}

			try {

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file_path, xml_file_filter);

				Path json_file_path = Paths.get(json_dir_path.toString(), xml_parser.basename + ".json");

				schema.xml2Json(xml_parser, json_file_path);

			} catch (Exception e) {
				System.err.println("Exception occurred while processing XML document: " + xml_file_path.toAbsolutePath().toString());
				e.printStackTrace();
				System.exit(1);
			}

		}

		schema.closeXml2Json();

		if (thrd_id == 0)
			System.out.println("\nDone.");

	}

}
