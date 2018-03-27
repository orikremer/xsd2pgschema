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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Thread function for xml2pgcsv.
 *
 * @author yokochi
 */
public class Xml2PgCsvThrd implements Runnable {

	/** The thread id. */
	private int thrd_id;

	/** The document builder for reusing. */
	private DocumentBuilder doc_builder;

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option = null;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The CSV directory. */
	private File csv_dir = null;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter = null;

	/** The XML file queue. */
	private LinkedBlockingQueue<File> xml_file_queue = null;

	/** The database connection. */
	private Connection db_conn = null;

	/**
	 * Instance of Xml2PgCsvThrd.
	 *
	 * @param thrd_id thread id
	 * @param is InputStream of XML Schema
	 * @param csv_dir directory contains CSV files
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param option PostgreSQL data model option
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws SQLException the SQL exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2PgCsvThrd(final int thrd_id, final InputStream is, final File csv_dir, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<File> xml_file_queue, final PgSchemaOption option, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

		this.thrd_id = thrd_id;

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

		schema = new PgSchema(doc_builder, xsd_doc, null, xml2pgcsv.schema_location, this.option = option);

		schema.applyXmlPostEditor(xml2pgcsv.xml_post_editor);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFile(xml2pgcsv.schema_location, null, option.cache_xsd)) : null;

		if (!pg_option.name.isEmpty()) {

			db_conn = DriverManager.getConnection(pg_option.getDbUrl(), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.pass);

			// test PostgreSQL DDL with schema

			if (pg_option.test)
				schema.testPgSql(db_conn, true);

		}

		this.csv_dir = new File(csv_dir, PgSchemaUtil.thrd_dir_prefix + thrd_id);

		if (!this.csv_dir.isDirectory()) {

			if (!this.csv_dir.mkdir())
				throw new PgSchemaException("Couldn't create directory '" + this.csv_dir + "'.");

		}

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		boolean show_progress = thrd_id == 0 && total > 1;
		boolean synchronizable = option.isSynchronizable();

		int polled = 0;

		File xml_file;

		while ((xml_file = xml_file_queue.poll()) != null) {

			if (synchronizable) {

				try {

					new XmlParser(xml_file, xml_file_filter).identify(option);

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			try {

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml_file_filter);

				schema.xml2PgCsv(xml_parser, csv_dir);

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

			++polled;

			if (show_progress)
				System.out.print("\rConverted " + (total - xml_file_queue.size()) + " of " + total + " ...");

		}

		schema.closeXml2PgCsv();

		if (db_conn == null) {

			if (thrd_id == 0)
				System.out.println("\nDone.");

		}

		else if (polled > 0) {

			try {

				schema.pgCsv2PgSql(db_conn, csv_dir);

			} catch (PgSchemaException e) {
				e.printStackTrace();
				System.exit(1);
			}

			if (csv_dir.isDirectory()) {

				try {

					System.out.println("Done xml (" + polled + " entries) -> db (" + db_conn.getMetaData().getURL().split("/")[3] + ").");

				} catch (SQLException e) {
					e.printStackTrace();
				}

				try {
					FileUtils.deleteDirectory(csv_dir);
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		}

	}

}
