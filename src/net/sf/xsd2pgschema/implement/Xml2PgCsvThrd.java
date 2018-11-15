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

package net.sf.xsd2pgschema.implement;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.option.PgOption;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.option.XmlFileFilter;
import net.sf.xsd2pgschema.option.XmlPostEditor;
import net.sf.xsd2pgschema.serverutil.PgSchemaClientImpl;
import net.sf.xsd2pgschema.type.PgHashSize;
import net.sf.xsd2pgschema.xmlutil.XmlParser;
import net.sf.xsd2pgschema.xmlutil.XmlValidator;

/**
 * Thread function for either xml2pgcsv or xml2pgtsv.
 *
 * @author yokochi
 */
public class Xml2PgCsvThrd implements Runnable {

	/** The thread id. */
	private int thrd_id;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option;

	/** The PgSchema client. */
	private PgSchemaClientImpl client;

	/** The PostgreSQL option. */
	private PgOption pg_option;

	/** The database name. */
	private String db_name = null;

	/** The XML validator. */
	private XmlValidator validator;

	/** The working directory. */
	private Path work_dir;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue;

	/** the instance of message digest for hash key. */
	private MessageDigest md_hash_key = null;

	/** The instance of message digest for check sum. */
	private MessageDigest md_chk_sum = null;

	/** The database connection. */
	private Connection db_conn = null;

	/**
	 * Instance of Xml2PgCsvThrd (PgShema server client).
	 *
	 * @param thrd_id thread id
	 * @param get_thrd thread to get a PgSchema server client
	 * @param clients array of PgSchema server clients
	 * @param work_dir working directory contains CSV/TSV files
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param xml_post_editor XML post editor
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws SQLException the SQL exception
	 * @throws PgSchemaException the pg schema exception
	 * @throws InterruptedException the interrupted exception
	 */
	public Xml2PgCsvThrd(final int thrd_id, final Thread get_thrd, final PgSchemaClientImpl[] clients, final Path work_dir, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException, InterruptedException {

		if (get_thrd != null)
			get_thrd.join();

		this.client = clients[thrd_id];

		init(thrd_id, work_dir, xml_file_filter, xml_file_queue, xml_post_editor, pg_option);

	}

	/**
	 * Instance of Xml2PgCsvThrd (stand alone).
	 *
	 * @param thrd_id thread id
	 * @param is InputStream of XML Schema
	 * @param work_dir working directory contains CSV/TSV files
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
	public Xml2PgCsvThrd(final int thrd_id, final InputStream is, final Path work_dir, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final PgSchemaOption option, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

		client = new PgSchemaClientImpl(is, option, null, Thread.currentThread().getStackTrace()[2].getClassName());

		init(thrd_id, work_dir, xml_file_filter, xml_file_queue, xml_post_editor, pg_option);

	}

	/**
	 * Setup Xml2PgCsvThrd except for PgSchema server client.
	 *
	 * @param thrd_id thread id
	 * @param work_dir working directory contains CSV/TSV files
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param xml_post_editor XML post editor
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws SQLException the SQL exception
	 * @throws PgSchemaException the pg schema exception
	 */
	private void init(final int thrd_id, final Path work_dir, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

		this.thrd_id = thrd_id;

		this.xml_file_filter = xml_file_filter;
		this.xml_file_queue = xml_file_queue;

		option = client.option;

		client.schema.applyXmlPostEditor(xml_post_editor);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFilePath(option.root_schema_location, null, option.cache_xsd), option.full_check) : null;

		this.pg_option = pg_option;

		if (!pg_option.name.isEmpty()) {

			db_conn = DriverManager.getConnection(pg_option.getDbUrl(PgSchemaUtil.def_encoding), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.pass);

			// test PostgreSQL DDL with schema

			if (pg_option.test)
				client.schema.testPgSql(db_conn, true);

			db_name = pg_option.name;

		}

		this.work_dir = Paths.get(work_dir.toString(), PgSchemaUtil.thrd_dir_prefix + thrd_id);

		if (!Files.isDirectory(this.work_dir))
			Files.createDirectory(this.work_dir);

		synchronizable = option.isSynchronizable(false);

		// prepare message digest for hash key

		if (!option.hash_algorithm.isEmpty() && !option.hash_size.equals(PgHashSize.debug_string))
			md_hash_key = MessageDigest.getInstance(option.hash_algorithm);

		// prepare message digest for check sum

		if (!option.check_sum_algorithm.isEmpty() && synchronizable)
			md_chk_sum = MessageDigest.getInstance(option.check_sum_algorithm);

	}

	/** Whether synchronization is possible. */
	private boolean synchronizable = false;

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		boolean show_progress = thrd_id == 0 && total > 1;

		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
		long start_time = System.currentTimeMillis(), current_time, etc_time;
		int polled = 0, queue_size, progress;
		Date etc_date;

		Path xml_file_path;

		XmlParser xml_parser;

		while ((xml_file_path = xml_file_queue.poll()) != null) {

			if (show_progress) {

				queue_size = xml_file_queue.size();

				if (polled % (queue_size > 100 ? 10 : 1) == 0) {

					current_time = System.currentTimeMillis();

					progress = total - queue_size;

					etc_time = current_time + (current_time - start_time) * queue_size / progress;
					etc_date = new Date(etc_time);

					System.out.print("\rConverted " + progress + " of " + total + " ... (ETC " + sdf.format(etc_date) + ")");

				}

			}

			if (synchronizable) {

				try {

					new XmlParser(xml_file_path, xml_file_filter).identify(option, md_chk_sum);

				} catch (IOException e) {
					e.printStackTrace();
				}

			}

			try {

				xml_parser = new XmlParser(client.doc_builder, validator, xml_file_path, xml_file_filter);

				client.schema.xml2PgCsv(xml_parser, md_hash_key, work_dir);

			} catch (Exception e) {
				System.err.println("Exception occurred while processing XML document: " + xml_file_path.toAbsolutePath().toString());
				e.printStackTrace();
			}

			++polled;

		}

		client.schema.closeXml2PgCsv();

		if (db_conn == null) {

			if (thrd_id == 0)
				System.out.println("\nDone.");

		}

		else if (polled > 0) {

			if (show_progress)
				System.out.println("\nCopying...");

			try {

				client.schema.pgCsv2PgSql(db_conn, work_dir);

			} catch (PgSchemaException e) {
				e.printStackTrace();
			}

			if (Files.isDirectory(work_dir)) {

				System.out.println("Done XML (" + polled + " documents) -> DB (" + db_name + ").");

				try {
					FileUtils.deleteDirectory(work_dir.toFile());
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		}

		else if (show_progress)
			System.out.println("\nDone");

		if (db_conn == null)
			return;

		try {

			if (show_progress) {

				if (pg_option.create_doc_key_index)
					client.schema.createDocKeyIndex(db_conn, pg_option);
				else if (pg_option.drop_doc_key_index)
					client.schema.dropDocKeyIndex(db_conn);

				if (pg_option.create_attr_index)
					client.schema.createAttrIndex(db_conn, pg_option);
				else if (pg_option.drop_attr_index)
					client.schema.dropAttrIndex(db_conn);

				if (pg_option.create_elem_index)
					client.schema.createElemIndex(db_conn, pg_option);
				else if (pg_option.drop_elem_index)
					client.schema.dropElemIndex(db_conn);

				if (pg_option.create_simple_cont_index)
					client.schema.createSimpleContIndex(db_conn, pg_option);
				else if (pg_option.drop_simple_cont_index)
					client.schema.dropSimpleContIndex(db_conn);

			}

			db_conn.close();

		} catch (PgSchemaException | SQLException e) {
			e.printStackTrace();
		}

	}

}
