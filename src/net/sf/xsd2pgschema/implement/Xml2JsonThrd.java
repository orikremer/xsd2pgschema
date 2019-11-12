/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2019 Masashi Yokochi

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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.serverutil.PgSchemaClientImpl;
import net.sf.xsd2pgschema.serverutil.PgSchemaClientType;
import net.sf.xsd2pgschema.type.PgHashSize;
import net.sf.xsd2pgschema.xmlutil.XmlParser;
import net.sf.xsd2pgschema.xmlutil.XmlValidator;
import net.sf.xsd2pgschema.docbuilder.JsonBuilder;
import net.sf.xsd2pgschema.docbuilder.JsonBuilderOption;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.option.XmlFileFilter;
import net.sf.xsd2pgschema.option.XmlPostEditor;

/**
 * Thread function for xml2json.
 *
 * @author yokochi
 */
public class Xml2JsonThrd implements Runnable {

	/** The thread id. */
	private int thrd_id;

	/** The PgSchema client. */
	private PgSchemaClientImpl client;

	/** The JSON builder. */
	private JsonBuilder jsonb;

	/** The XML validator. */
	private XmlValidator validator;

	/** The JSON directory path. */
	private Path json_dir_path;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue;

	/** the instance of message digest for hash key. */
	private MessageDigest md_hash_key = null;

	/**
	 * Instance of Xml2JsonThrd (PgSchema server client).
	 *
	 * @param thrd_id thread id
	 * @param get_thrd thread to get a PgSchema server client
	 * @param clients array of PgSchema server clients
	 * @param json_dir_path directory path contains JSON files
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param jsonb_option JsonBuilder option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 * @throws InterruptedException the interrupted exception
	 */
	public Xml2JsonThrd(final int thrd_id, final Thread get_thrd, final PgSchemaClientImpl[] clients, final Path json_dir_path, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final JsonBuilderOption jsonb_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException, InterruptedException {

		if (get_thrd != null)
			get_thrd.join();

		this.client = clients[thrd_id];

		init(thrd_id, json_dir_path, xml_file_filter, xml_file_queue, jsonb_option);

	}

	/**
	 * Instance of Xml2JsonThrd (stand alone).
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

		client = new PgSchemaClientImpl(is, option, null, PgSchemaClientType.json_conversion, Thread.currentThread().getStackTrace()[2].getClassName(), xml_post_editor, jsonb_option);

		init(thrd_id, json_dir_path, xml_file_filter, xml_file_queue, jsonb_option);

	}

	/**
	 * Setup Xml2JsonThrd except for PgSchema server client.
	 *
	 * @param thrd_id thread id
	 * @param json_dir_path directory path contains JSON files
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param jsonb_option JsonBuilder option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	private void init(final int thrd_id, final Path json_dir_path, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final JsonBuilderOption jsonb_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

		this.thrd_id = thrd_id;
		this.json_dir_path = json_dir_path;

		this.xml_file_filter = xml_file_filter;
		this.xml_file_queue = xml_file_queue;

		PgSchemaOption option = client.option;

		jsonb = new JsonBuilder(client.schema, jsonb_option);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFilePath(option.root_schema_location, null, option.cache_xsd), option.full_check) : null;

		// prepare message digest for hash key

		if (!option.hash_algorithm.isEmpty() && !option.hash_size.equals(PgHashSize.debug_string))
			md_hash_key = MessageDigest.getInstance(option.hash_algorithm);

	}

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

		Path xml_file_path, json_file_path;

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

			try {

				xml_parser = new XmlParser(client.doc_builder, validator, xml_file_path, xml_file_filter);

				json_file_path = Paths.get(json_dir_path.toString(), xml_parser.basename + ".json");

				jsonb.xml2Json(xml_parser, md_hash_key, json_file_path);

			} catch (Exception e) {
				System.err.println("Exception occurred while processing XML document: " + xml_file_path.toAbsolutePath().toString());
				e.printStackTrace();
			}

			++polled;

		}

		if (thrd_id == 0)
			System.out.println("\nDone.");

	}

}
