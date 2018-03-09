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
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Thread function for xml2pgsql.
 *
 * @author yokochi
 */
public class Xml2PgSqlThrd implements Runnable {

	/** The thread id. */
	private int thrd_id;

	/** The PostgreSQL option. */
	private PgOption pg_option = null;

	/** The document builder for reusing. */
	private DocumentBuilder doc_builder;

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The database connection. */
	private Connection db_conn = null;

	/**
	 * Instance of Xml2PgSqlThrd.
	 *
	 * @param thrd_id thread id
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL data model option
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws SQLException the SQL exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2PgSqlThrd(final int thrd_id, final InputStream is, final PgSchemaOption option, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

		this.thrd_id = thrd_id;

		this.pg_option = pg_option;

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

		schema = new PgSchema(doc_builder, xsd_doc, null, xml2pgsql.schema_location, option);

		schema.applyXmlPostEditor(xml2pgsql.xml_post_editor);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFile(xml2pgsql.schema_location, null, option.cache_xsd)) : null;

		db_conn = DriverManager.getConnection(pg_option.getDbUrl(), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.pass);

		// test PostgreSQL DDL with schema

		if (pg_option.test)
			schema.testPgSql(db_conn, true);

		db_conn.setAutoCommit(false);

		// delete rows if XML not exists

		if (pg_option.sync) {

			List<String> doc_ids = schema.getDocIdRows(db_conn);

			xml2pgsql.xml_file_queue.forEach(xml_file -> {

				try {

					XmlParser xml_parser = new XmlParser(xml_file, xml2pgsql.xml_file_filter);

					doc_ids.remove(xml_parser.document_id);

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

			schema.deleteRows(db_conn, doc_ids);

		}

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml2pgsql.xml_file_queue.size();
		boolean show_progress = thrd_id == 0 && total > 1;

		int polled = 0;

		File xml_file;

		while ((xml_file = xml2pgsql.xml_file_queue.poll()) != null) {

			try {

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml2pgsql.xml_file_filter, pg_option);

				schema.xml2PgSql(xml_parser, db_conn, pg_option);

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

			++polled;

			if (show_progress)
				System.out.print("\rMigrated " + (total - xml2pgsql.xml_file_queue.size()) + " of " + total + " ...");

		}

		if (polled > 0) {

			try {
				System.out.println("Done xml (" + polled + " entries) -> db (" + db_conn.getMetaData().getURL().split("/")[3] + ").");
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}

}
