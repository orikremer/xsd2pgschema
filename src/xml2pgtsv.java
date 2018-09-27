/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

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
import net.sf.xsd2pgschema.implement.Xml2PgCsvThrd;
import net.sf.xsd2pgschema.option.*;
import net.sf.xsd2pgschema.serverutil.*;
import net.sf.xsd2pgschema.type.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.*;

import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

/**
 * TSV conversion and PostgreSQL data migration.
 *
 * @author yokochi
 */
public class xml2pgtsv {

	/** The working directory name. */
	protected static String work_dir_name = "pg_work";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The check sum directory name. */
		String check_sum_dir_name = "";

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(true);

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class,PgSchema.class); // FST optimization

		/** The PostgreSQL option. */
		PgOption pg_option = new PgOption();

		/** The XML file filter. */
		XmlFileFilter xml_file_filter = new XmlFileFilter();

		/** The XML post editor. */
		XmlPostEditor xml_post_editor = new XmlPostEditor();

		/** The XML file queue. */
		LinkedBlockingQueue<Path> xml_file_queue;

		/** The target XML file patterns. */
		HashSet<String> xml_file_names = new HashSet<String>();

		/** The available processors. */
		int cpu_num = Runtime.getRuntime().availableProcessors();

		/** The max threads. */
		int max_thrds = cpu_num;

		boolean touch_xml = false;

		for (int i = 0; i < args.length; i++) {

			if (args[i].startsWith("--"))
				touch_xml = false;

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if (args[i].equals("--xml") && i + 1 < args.length) {
				String xml_file_name = args[++i];

				if (xml_file_name.isEmpty()) {
					System.err.println("XML file name is empty.");
					showUsage();
				}

				xml_file_names.add(xml_file_name);

				touch_xml = true;
			}

			else if (args[i].equals("--xml-file-ext") && i + 1 < args.length) {

				if (!xml_file_filter.setExt(args[++i]))
					showUsage();

			}

			else if (args[i].equals("--xml-file-prefix-digest") && i + 1 < args.length)
				xml_file_filter.setPrefixDigest(args[++i]);

			else if (args[i].equals("--xml-file-ext-digest") && i + 1 < args.length)
				xml_file_filter.setExtDigest(args[++i]);

			else if (args[i].equals("--lower-case-doc-key"))
				xml_file_filter.setLowerCaseDocKey();

			else if (args[i].equals("--upper-case-doc-key"))
				xml_file_filter.setUpperCaseDocKey();

			else if ((args[i].equals("--csv-dir") || args[i].equals("--tsv-dir") || args[i].equals("--work-dir")) && i + 1 < args.length)
				work_dir_name = args[++i];

			else if (args[i].equals("--db-host") && i + 1 < args.length)
				pg_option.pg_host = args[++i];

			else if (args[i].equals("--db-port") && i + 1 < args.length)
				pg_option.pg_port = Integer.valueOf(args[++i]);

			else if (args[i].equals("--db-name") && i + 1 < args.length)
				pg_option.name = args[++i];

			else if (args[i].equals("--db-user") && i + 1 < args.length)
				pg_option.user = args[++i];

			else if (args[i].equals("--db-pass") && i + 1 < args.length)
				pg_option.pass = args[++i];

			else if (args[i].equals("--test-ddl"))
				pg_option.test = true;

			else if (args[i].equals("--create-doc-key-index"))
				pg_option.setCreateDocKeyIndexOption(true);

			else if (args[i].equals("--no-create-doc-key-index"))
				pg_option.setCreateDocKeyIndexOption(false);

			else if (args[i].equals("--drop-doc-key-index"))
				pg_option.setDropDocKeyIndexOption();

			else if (args[i].equals("--min-rows-for-doc-key-index") && i + 1 < args.length)
				pg_option.setMinRowsForDocKeyIndex(args[++i]);

			else if (args[i].equals("--fill-default-value"))
				xml_post_editor.fill_default_value = true;

			else if (args[i].equals("--filt-in") && i + 1 < args.length)
				xml_post_editor.addFiltIn(args[++i]);

			else if (args[i].equals("--filt-out") && i + 1 < args.length)
				xml_post_editor.addFiltOut(args[++i]);

			else if (args[i].equals("--fill-this") && i + 1 < args.length)
				xml_post_editor.addFillThis(args[++i]);

			else if (args[i].equals("--doc-key"))
				option.setDocKeyOption(true);

			else if (args[i].equals("--no-doc-key"))
				option.setDocKeyOption(false);

			else if (args[i].equals("--no-rel"))
				option.cancelRelDataExt();

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--ser-key"))
				option.serial_key = true;

			else if (args[i].equals("--xpath-key"))
				option.xpath_key = true;

			else if (args[i].equals("--case-insensitive"))
				option.setCaseInsensitive();

			else if (args[i].equals("--pg-public-schema"))
				option.pg_named_schema = false;

			else if (args[i].equals("--pg-named-schema"))
				option.pg_named_schema = true;

			else if (args[i].equals("--pg-comma-delimiter"))
				option.usePgCsv();

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--no-key"))  // not effective but argument compatibility
				option.pg_retain_key = false;

			else if (args[i].startsWith("--valid"))
				option.validate = option.full_check = true;

			else if (args[i].startsWith("--no-valid"))
				option.validate = false;

			else if (args[i].equals("--well-formed")) {
				option.validate = true;
				option.full_check = false;
			}

			else if (args[i].equals("--hash-by") && i + 1 < args.length)
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size") && i + 1 < args.length)
				option.hash_size = PgHashSize.getSize(args[++i]);

			else if (args[i].equals("--ser-size") && i + 1 < args.length)
				option.ser_size = PgSerSize.getSize(args[++i]);

			else if (args[i].equals("--doc-key-name") && i + 1 < args.length)
				option.setDocumentKeyName(args[++i]);

			else if (args[i].equals("--ser-key-name") && i + 1 < args.length)
				option.setSerialKeyName(args[++i]);

			else if (args[i].equals("--xpath-key-name") && i + 1 < args.length)
				option.setXPathKeyName(args[++i]);

			else if (args[i].equals("--discarded-doc-key-name") && i + 1 < args.length)
				option.addDiscardedDocKeyName(args[++i]);

			else if (args[i].equals("--inplace-doc-key-name") && i + 1 < args.length) {
				option.addInPlaceDocKeyName(args[++i]);
				option.setDocKeyOption(false);
			}

			else if (args[i].equals("--doc-key-if-no-inplace")) {
				option.document_key_if_no_in_place = true;
				option.setDocKeyOption(false);
			}

			else if (args[i].equals("--no-pgschema-serv"))
				option.pg_schema_server = false;

			else if (args[i].equals("--pgschema-serv-host") && i + 1 < args.length)
				option.pg_schema_server_host = args[++i];

			else if (args[i].equals("--pgschema-serv-port") && i + 1 < args.length)
				option.pg_schema_server_port = Integer.valueOf(args[++i]);

			else if (args[i].equals("--sync") && i + 1 < args.length) {
				pg_option.tryToCreateDocKeyIndexOption(option.sync = true);
				check_sum_dir_name = args[++i];
			}

			else if (args[i].equals("--checksum-by") && i + 1 < args.length) {
				if (!option.setCheckSumAlgorithm(args[++i]))
					showUsage();
			}

			else if (args[i].equals("--max-thrds") && i + 1 < args.length) {
				max_thrds = Integer.valueOf(args[++i]);

				if (max_thrds <= 0 || max_thrds > cpu_num * 2) {
					System.err.println("Out of range (max-thrds).");
					showUsage();
				}
			}

			else if (touch_xml) {
				String xml_file_name = args[i];

				if (xml_file_name.isEmpty()) {
					System.err.println("XML file name is empty.");
					showUsage();
				}

				xml_file_names.add(xml_file_name);
			}

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		option.resolveDocKeyOption();

		if (option.sync && !option.document_key && !option.in_place_document_key) {
			System.out.println("Ignored --sync option because either document key or in-place document key was not defined.");
			pg_option.tryToCreateDocKeyIndexOption(option.sync = false);
		}

		if (option.root_schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = null;

		boolean server_alive = option.pingPgSchemaServer(fst_conf);
		boolean no_data_model = server_alive ? !option.matchPgSchemaServer(fst_conf) : true;

		if (no_data_model) {

			is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

			if (is == null)
				showUsage();

		}

		if (xml_file_names.size() == 0) {
			System.err.println("XML file name is empty.");
			showUsage();
		}

		FilenameFilter filename_filter = new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return name.endsWith(xml_file_filter.getAbsoluteExt());
			}

		};

		xml_file_queue = PgSchemaUtil.getQueueOfTargetFiles(xml_file_names, filename_filter);

		xml_file_names.clear();

		if (xml_file_queue.size() < max_thrds)
			max_thrds = xml_file_queue.size();

		Path work_dir = Paths.get(work_dir_name);

		if (!Files.isDirectory(work_dir)) {

			try {
				Files.createDirectory(work_dir);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		if (option.sync) {

			if (check_sum_dir_name.isEmpty()) {
				System.err.println("Check sum directory is empty.");
				showUsage();
			}

			Path check_sum_dir_path = Paths.get(check_sum_dir_name);

			if (!Files.isDirectory(check_sum_dir_path)) {

				try {
					Files.createDirectory(check_sum_dir_path);
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			option.check_sum_dir_name = check_sum_dir_name;

		}

		final int total = xml_file_queue.size();

		final String class_name = MethodHandles.lookup().lookupClass().getName();

		Thread[] thrd = new Thread[max_thrds];

		long start_time = System.currentTimeMillis();

		// PgSchema server is alive

		if (server_alive) {

			PgSchemaClientImpl[] clients = new PgSchemaClientImpl[max_thrds];
			Thread[] get_thrd = new Thread[max_thrds];

			try {

				// send ADD query to PgSchema server

				if (no_data_model) {

					clients[0] = new PgSchemaClientImpl(is, option, fst_conf, class_name);
					get_thrd[0] = null;

				}

				// send GET query to PgSchema server

				for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

					if (thrd_id == 0 && no_data_model)
						continue;

					Thread _get_thrd = get_thrd[thrd_id] = new Thread(new PgSchemaGetClientThrd(thrd_id, option, fst_conf, class_name, clients));

					_get_thrd.setPriority(Thread.MAX_PRIORITY);
					_get_thrd.start();

				}

				for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

					String thrd_name = class_name + "-" + thrd_id;

					try {

						Thread _thrd = thrd[thrd_id] = new Thread(new Xml2PgCsvThrd(thrd_id, get_thrd[thrd_id], clients, work_dir, xml_file_filter, xml_file_queue, xml_post_editor, pg_option), thrd_name);

						_thrd.start();

					} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | SQLException | PgSchemaException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

			} catch (IOException | ParserConfigurationException | SAXException | PgSchemaException | InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		// stand alone

		else {

			for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

				String thrd_name = class_name + "-" + thrd_id;

				try {

					if (thrd_id > 0)
						is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

					Thread _thrd = thrd[thrd_id] = new Thread(new Xml2PgCsvThrd(thrd_id, is, work_dir, xml_file_filter, xml_file_queue, xml_post_editor, option, pg_option), thrd_name);

					_thrd.start();

				} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | SQLException | PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

		}

		if (!pg_option.name.isEmpty())
			pg_option.clear();

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			try {

				thrd[thrd_id].join();

			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		long end_time = System.currentTimeMillis();

		System.out.println("Execution time: " + (end_time - start_time) + " ms");

		if (!pg_option.name.isEmpty() && option.isSynchronizable(false) && total > 1)
			System.out.println(pg_option.getDbUrl() + " is up-to-date.");

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		PgSchemaOption option = new PgSchemaOption(true);

		System.err.println("xml2pgtsv: XML -> TSV conversion and PostgreSQL data migration");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY --work-dir DIRECTORY (default=\"" + work_dir_name + "\")");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --doc-key (append " + option.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + option.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + option.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + option.xpath_key_name + " column in all relations)");
		System.err.println("        --no-key (turn off constraint of primary key/foreign key/unique)");
		System.err.println("        --validate (turn on XML Schema validation)");
		System.err.println("        --no-validate (turn off XML Schema validation, default)");
		System.err.println("        --well-formed (validate only whether document is well-formed)");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix) | zip (indicates xml.zip suffix)]");
		System.err.println("Option: --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host PG_HOST_NAME (default=\"" + PgSchemaUtil.pg_host + "\")");
		System.err.println("        --db-port PG_PORT_NUMBER (default=\"" + PgSchemaUtil.pg_port + "\")");
		System.err.println("        --test-ddl (perform consistency test on PostgreSQL DDL)");
		System.err.println("        --create-doc-key-index (create PostgreSQL index on document key if not exists, enable if --sync option is selected)");
		System.err.println("        --no-create-doc-key-index (do not create PostgreSQL index on document key, default if no --sync option)");
		System.err.println("        --no-create-doc-key-index (do not create PostgreSQL index on document key, default)");
		System.err.println("        --drop-doc-key-index (drop PostgreSQL index on document key if exists)");
		System.err.println("        --min-rows-for-doc-key-index MIN_ROWS_FOR_INDEX (default=\"" + PgSchemaUtil.pg_min_rows_for_doc_key_index + "\")");
		System.err.println("        --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --pg-public-schema (utilize \"public\" schema, default)");
		System.err.println("        --pg-named-schema (enable explicit named schema)");
		System.err.println("        --pg-comma-delimiter (use comma separated file)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --sync CHECK_SUM_DIRECTORY (generate check sum files for differential udpate, select --create-doc-key-index option by default)");
		System.err.println("        --checksum-by ALGORITHM [MD2 | MD5 (default) | SHA-1 | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16bit); | int (32bit, default)]");
		System.err.println("        --xml-file-prerix-digest DIGESTIBLE_PREFIX (default=\"\")");
		System.err.println("        --xml-file-ext-digest DIGESTIBLE_EXTENSION (default=\".\")");
		System.err.println("        --lower-case-doc-key (lower case document key)");
		System.err.println("        --upper-case-doc-key (upper case document key)");
		System.err.println("        --fill-default value (fill @default value in case of empty)");
		System.err.println("        --filt-in   table_name.column_name");
		System.err.println("        --filt-out  table_name.column_name:regex_pattern(|regex_pattern...)");
		System.err.println("        --fill-this table_name.column_name:filling_text");
		System.err.println("        --doc-key-name DOC_KEY_NAME (default=\"" + option.def_document_key_name + "\")");
		System.err.println("        --ser-key-name SER_KEY_NAME (default=\"" + option.def_serial_key_name + "\")");
		System.err.println("        --xpath-key-name XPATH_KEY_NAME (default=\"" + option.def_xpath_key_name + "\")");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.err.println("        --inplace-doc-key-name INPLACE_DOCUMENT_KEY_NAME");
		System.err.println("        --doc-key-if-no-inplace (append document key if no in-place docuemnt key, select --no-doc-key options by default)");
		System.err.println("        --no-pgschema-serv (not utilize PgSchema server)");
		System.err.println("        --pgschema-serv-host PG_SCHEMA_SERV_HOST_NAME (default=\"" + PgSchemaUtil.pg_schema_server_host + "\")");
		System.err.println("        --pgschema-serv-port PG_SCHEMA_SERV_PORT_NUMBER (default=\"" + PgSchemaUtil.pg_schema_server_port + "\")");
		System.err.println("        --max-thrds MAX_THRDS (default is number of available processors)");
		System.exit(1);

	}

}
