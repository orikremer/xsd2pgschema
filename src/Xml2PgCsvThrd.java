/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2017 Masashi Yokochi

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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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

	/** The PostgreSQL schema. */
	private PgSchema schema = null;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The database connection. */
	private Connection db_conn = null;

	/** The CSV directory name. */
	private String csv_dir_name = null;

	/**
	 * Instance of Xml2PgCsvThrd.
	 *
	 * @param thrd_id thread id
	 * @param max_thrds max threads
	 * @param is InputStream of XML Schema
	 * @param csv_dir_name directory name of CSV files
	 * @param option PosgreSQL schema option
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws SQLException the SQL exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2PgCsvThrd(final int thrd_id, final int max_thrds, final InputStream is, final String csv_dir_name, final PgSchemaOption option, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

		this.thrd_id = thrd_id;

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

		schema = new PgSchema(doc_builder, xsd_doc, null, PgSchemaUtil.getSchemaName(xml2pgcsv.schema_location), option);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFile(xml2pgcsv.schema_location, null)) : null;

		if (!pg_option.database.isEmpty())
			db_conn = DriverManager.getConnection(pg_option.getDbUrl(), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.password);

		this.csv_dir_name = csv_dir_name;

		if (max_thrds > 1) {

			this.csv_dir_name += PgSchemaUtil.thrd_dir_prefix + thrd_id + "/";

			File csv_dir = new File(this.csv_dir_name);

			if (!csv_dir.isDirectory()) {

				if (!csv_dir.mkdir())
					throw new PgSchemaException("Couldn't create directory '" + this.csv_dir_name + "'.");

			}

		}

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml2pgcsv.xml_file_queue.size();

		int queue = 0;

		boolean first_csv = true;

		File xml_file;

		while ((xml_file = xml2pgcsv.xml_file_queue.poll()) != null) {

			if (!xml_file.isFile())
				continue;

			try {

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml2pgcsv.xml_file_filter);

				schema.xml2PgCsv(xml_parser, csv_dir_name, db_conn, xml2pgcsv.xml_post_editor, first_csv ? xml2pgcsv.option.append : true);

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

			++queue;

			if (first_csv)
				first_csv = false;

			if (thrd_id == 0)
				System.out.print("\rConverted " + (total - xml2pgcsv.xml_file_queue.size()) + " of " + total + " ...");

		}

		schema.closeXml2PgCsv();

		if (thrd_id == 0 && db_conn == null)
			System.out.println("\nDone.");

		if (queue > 0 && db_conn != null) {

			try {

				schema.pgCsv2PgSql(db_conn, csv_dir_name);

			} catch (PgSchemaException e) {
				e.printStackTrace();
				System.exit(1);
			}

			File csv_dir = new File(csv_dir_name);

			if (csv_dir.isDirectory()) {

				try {

					System.out.println("Done xml (" + queue + " entries) -> db (" + db_conn.getMetaData().getURL().split("/")[3] + ").");

				} catch (SQLException e) {
					e.printStackTrace();
				}

				File[] files = csv_dir.listFiles();

				for (int i = 0; i < files.length; i++)
					files[i].delete();

			}

		}

	}

}
