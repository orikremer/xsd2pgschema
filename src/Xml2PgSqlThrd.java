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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

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

	/** The document builder for reusing. */
	private DocumentBuilder doc_builder;

	/** The PostgreSQL data model. */
	private PgSchema schema;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option;

	/** The PostgreSQL option. */
	private PgOption pg_option;

	/** The database name. */
	private String db_name;

	/** The XML validator. */
	private XmlValidator validator;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue;

	/** The instance of message digest for check sum. */
	private MessageDigest md_chk_sum = null;

	/** The database connection. */
	private Connection db_conn;

	/** The document id stored in PostgreSQL. */
	private HashSet<String> doc_rows = null;

	/**
	 * Instance of Xml2PgSqlThrd.
	 *
	 * @param thrd_id thread id
	 * @param is InputStream of XML Schema
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param xml_post_editor XML post editor
	 * @param option PostgreSQL data model option
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws SQLException the SQL exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2PgSqlThrd(final int thrd_id, final InputStream is, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final PgSchemaOption option, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

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

		schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, this.option = option);

		schema.applyXmlPostEditor(xml_post_editor);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFilePath(option.root_schema_location, null, option.cache_xsd), option.full_check) : null;

		this.pg_option = pg_option;

		db_conn = DriverManager.getConnection(pg_option.getDbUrl(PgSchemaUtil.def_encoding), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.pass);

		// test PostgreSQL DDL with schema

		if (pg_option.test)
			schema.testPgSql(db_conn, true);

		db_name = pg_option.name;

		db_conn.setAutoCommit(false);

		// delete rows if XML not exists

		if (synchronizable = option.isSynchronizable(true)) {

			doc_rows = schema.getDocIdRows(db_conn);

			if (option.sync) {

				if (thrd_id == 0) {

					HashSet<String> _doc_rows = new HashSet<String>();

					_doc_rows.addAll(doc_rows);

					xml_file_queue.forEach(xml_file_path -> {

						XmlParser xml_parser = new XmlParser(xml_file_path, xml_file_filter);

						_doc_rows.remove(xml_parser.document_id);

					});

					schema.deleteRows(db_conn, _doc_rows);

					_doc_rows.clear();

				}

			}

		}

		// prepare message digest for check sum

		if (!option.check_sum_algorithm.isEmpty() && synchronizable)
			md_chk_sum = MessageDigest.getInstance(option.check_sum_algorithm);

	}

	/** Whether if synchronizable or not. */
	private boolean synchronizable = false;

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		boolean show_progress = thrd_id == 0 && total > 1;
		boolean update = false;

		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");

		long start_time = System.currentTimeMillis();

		int polled = 0;

		Path xml_file_path;

		while ((xml_file_path = xml_file_queue.poll()) != null) {

			if (show_progress) {

				long current_time = System.currentTimeMillis();

				int remains = xml_file_queue.size();
				int progress = total - remains;

				long etc_time = current_time + (current_time - start_time) * remains / progress;
				Date etc_date = new Date(etc_time);

				System.out.print("\rMigrated " + progress + " of " + total + " ... (ETC " + sdf.format(etc_date) + ")");

			}

			if (synchronizable) {

				try {

					XmlParser xml_parser = new XmlParser(xml_file_path, xml_file_filter);

					update = doc_rows.contains(xml_parser.document_id);

					if (update) {

						if (option.sync_weak)
							continue;

						if (xml_parser.identify(option, md_chk_sum))
							continue;

					}

					else if (option.sync)
						xml_parser.identify(option, md_chk_sum);

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			try {

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file_path, xml_file_filter);

				schema.xml2PgSql(xml_parser, update, db_conn);

			} catch (Exception e) {
				System.err.println("Exception occurred while processing XML document: " + xml_file_path.toAbsolutePath().toString());
				e.printStackTrace();
				System.exit(1);
			}

			++polled;

		}

		schema.closeXml2PgSql();

		if (polled > 0)
			System.out.println("Done xml (" + polled + " entries) -> db (" + db_name + ").");

		else if (show_progress)
			System.out.println("\nDone");

		try {

			if (show_progress && (pg_option.create_doc_key_index || pg_option.drop_doc_key_index)) {

				db_conn.setAutoCommit(true);

				if (pg_option.create_doc_key_index)
					schema.createDocKeyIndex(db_conn, pg_option.min_rows_for_doc_key_index);
				else
					schema.dropDocKeyIndex(db_conn);

			}

			db_conn.close();

		} catch (PgSchemaException | SQLException e) {
			e.printStackTrace();
		}

	}

}
