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
 * Thread function for xml2pgsql
 * @author yokochi
 */
public class Xml2PgSqlThrd implements Runnable {

	private int thrd_id; // thread id
	private int max_thrds; // max threads

	private DocumentBuilder doc_builder; // document builder factory for reuse
	private PgSchema schema = null; // PostgreSQL schema
	private XmlValidator validator = null; // XML validator
	private Connection db_conn = null; // database connection

	/**
	 * Instance of Xml2PgSqlThrd
	 * @param thrd_id thread id
	 * @param max_thrds max threads
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL schema option
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 * @throws SQLException
	 * @throws PgSchemaException
	 */
	public Xml2PgSqlThrd(final int thrd_id, final int max_thrds, final InputStream is, final PgSchemaOption option, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

		this.thrd_id = thrd_id;
		this.max_thrds = max_thrds;

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

		schema = new PgSchema(doc_builder, xsd_doc, null, PgSchemaUtil.getName(xml2pgsql.schema_location), option);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getFile(xml2pgsql.schema_location, null)) : null;

		db_conn = DriverManager.getConnection(pg_option.getDbUrl(), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.password);

	}

	@Override
	public void run() {

		int queue = 0;
		int proc_id = 0;

		for (File xml_file : xml2pgsql.xml_files) {

			if (xml_file.isFile()) {

				if (proc_id++ % max_thrds != thrd_id)
					continue;

				try {

					XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml2pgsql.xml_file_filter);

					schema.xml2PgSql(xml_parser, db_conn, xml2pgsql.xml_post_editor, xml2pgsql.pg_option.update);

					++queue;

				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}

				if (thrd_id == 0)
					System.out.print("\rMigrated " + proc_id + " of " + xml2pgsql.xml_files.length + " ...");

			}

		}

		if (queue > 0) {

			try {
				System.out.println("Done xml (" + queue + " entries) -> db (" + db_conn.getMetaData().getURL().split("/")[3] + ").");
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}

}
