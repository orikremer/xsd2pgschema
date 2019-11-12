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

import net.sf.xsd2pgschema.*;
import net.sf.xsd2pgschema.implement.Xml2SphinxDsThrd;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.*;

import org.apache.commons.io.FilenameUtils;
import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

/**
 * Sphinx data source (xmlpipe2) conversion for full-text indexing
 *
 * @author yokochi
 */
public class xml2sphinxds {

	/** The data source directory name. */
	protected static String ds_dir_name = "sphinx_xmlpipe2";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {

		/** The check sum directory name. */
		String check_sum_dir_name = "";

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(false);

		option.cancelRelDataExt(); // turn off relational data extension

		option.inline_simple_cont = false;

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class,PgSchema.class); // FST optimization

		/** The XML file filter. */
		XmlFileFilter xml_file_filter = new XmlFileFilter();

		/** The XML post editor. */
		XmlPostEditor xml_post_editor = new XmlPostEditor();

		/** The index filter. */
		IndexFilter index_filter = new IndexFilter();

		/** The XML file queue. */
		LinkedBlockingQueue<Path> xml_file_queue;

		/** The target XML file patterns. */
		HashSet<String> xml_file_names = new HashSet<String>();

		/** The data source name. */
		String ds_name = "";

		/** The set of document id stored in data source (key=document id, value=shard id). */
		HashMap<String, Integer> doc_rows = null;

		/** The set of deleting document id while synchronization. */
		HashSet<String>[] sync_del_doc_rows = null;

		/** The shard size. */
		int shard_size = 1;

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

			else if (args[i].equals("--ds-dir") && i + 1 < args.length)
				ds_dir_name = args[++i];

			else if (args[i].equals("--ds-name") && i + 1 < args.length)
				ds_name = args[++i];

			else if (args[i].equals("--attr") && i + 1 < args.length)
				index_filter.addAttr(args[++i]);

			else if (args[i].equals("--mva") && i + 1 < args.length)
				index_filter.addSphMVA(args[++i]);

			else if (args[i].equals("--field") && i + 1 < args.length)
				index_filter.addField(args[++i]);

			else if (args[i].equals("--attr-all"))
				index_filter.setAttrAll();

			else if (args[i].equals("--field-all"))
				index_filter.setFieldAll();

			else if (args[i].equals("--attr-string"))
				index_filter.attr_string = true;

			else if (args[i].equals("--attr-integer"))
				index_filter.attr_integer = true;

			else if (args[i].equals("--attr-float"))
				index_filter.attr_float = true;

			else if (args[i].equals("--attr-date"))
				index_filter.attr_date = true;

			else if (args[i].equals("--attr-time"))
				index_filter.attr_time = true;

			else if (args[i].equals("--min-word-len") && i + 1 < args.length)
				index_filter.setMinWordLen(args[++i]);

			else if (args[i].equals("--max-field-len") && i + 1 < args.length)
				index_filter.setSphMaxFieldLen(args[++i]);

			else if (args[i].equals("--fill-default-value"))
				xml_post_editor.fill_default_value = true;

			else if (args[i].equals("--filt-in") && i + 1 < args.length)
				xml_post_editor.addFiltIn(args[++i]);

			else if (args[i].equals("--filt-out") && i + 1 < args.length)
				xml_post_editor.addFiltOut(args[++i]);

			else if (args[i].equals("--fill-this") && i + 1 < args.length)
				xml_post_editor.addFillThis(args[++i]);

			else if (args[i].equals("--inline-simple-cont"))
				option.inline_simple_cont = true;

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].startsWith("--valid"))
				option.validate = option.full_check = true;

			else if (args[i].startsWith("--no-valid"))
				option.validate = false;

			else if (args[i].equals("--well-formed")) {
				option.validate = true;
				option.full_check = false;
			}

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--hash-by") && i + 1 < args.length)
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size") && i + 1 < args.length) {
				option.hash_size = PgHashSize.getSize(args[++i]);

				if (option.hash_size.equals(PgHashSize.debug_string) || option.hash_size.equals(PgHashSize.native_default))
					option.hash_size = PgHashSize.defaultSize(); // long or int is required
			}

			else if (args[i].equals("--discarded-doc-key-name") && i + 1 < args.length)
				option.addDiscardedDocKeyName(args[++i]);

			else if (args[i].equals("--no-pgschema-serv"))
				option.pg_schema_server = false;

			else if (args[i].equals("--pgschema-serv-host") && i + 1 < args.length)
				option.pg_schema_server_host = args[++i];

			else if (args[i].equals("--pgschema-serv-port") && i + 1 < args.length)
				option.pg_schema_server_port = Integer.valueOf(args[++i]);

			else if (args[i].equals("--update"))
				option.sync = option.sync_weak = false;

			else if (args[i].equals("--sync") && i + 1 < args.length) {
				option.sync = true;
				option.sync_weak = false;
				check_sum_dir_name = args[++i];
			}

			else if (args[i].equals("--sync-weak")) {
				option.sync = false;
				option.sync_weak = true;
			}

			else if (args[i].equals("--checksum-by") && i + 1 < args.length) {
				if (!option.setCheckSumAlgorithm(args[++i]))
					showUsage();
			}

			else if (args[i].equals("--shard-size") && i + 1 < args.length) {
				shard_size = Integer.valueOf(args[++i]);

				if (shard_size <= 0) {
					System.err.println("Out of range (shard-size).");
					showUsage();
				}
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

		if (option.root_schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		PgSchemaClientType client_type = PgSchemaClientType.full_text_indexing;

		InputStream is = null;

		boolean server_alive = option.pingPgSchemaServer(fst_conf);
		boolean no_data_model = server_alive ? !option.matchPgSchemaServer(fst_conf, client_type) : true;

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
				return name.endsWith(xml_file_filter.getAbsoluteExt()) &&
						!name.equals(PgSchemaUtil.sph_schema_name) &&
						!name.startsWith(PgSchemaUtil.sph_document_prefix) &&
						!name.equals(PgSchemaUtil.sph_data_source_name) &&
						!name.equals(PgSchemaUtil.sph_data_extract_name) &&
						!name.equals(PgSchemaUtil.sph_data_update_name);
			}

		};

		xml_file_queue = PgSchemaUtil.getQueueOfTargetFiles(xml_file_names, filename_filter);

		xml_file_names.clear();

		max_thrds = max_thrds / shard_size; // number of thread per a shard

		if (max_thrds == 0)
			max_thrds = 1;

		if (ds_name.isEmpty()) {

			ds_name = PgSchemaUtil.getSchemaFileName(option.root_schema_location);

			String xsd_file_ext = FilenameUtils.getExtension(ds_name);

			if (xsd_file_ext != null && !xsd_file_ext.isEmpty())
				ds_name = ds_name.replaceAll("\\." + xsd_file_ext + "$", "");

		}

		Path ds_dir_path = Paths.get(ds_dir_name);

		if (!Files.isDirectory(ds_dir_path)) {

			try {
				Files.createDirectory(ds_dir_path);
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

			doc_rows = new HashMap<String, Integer>();

			sync_del_doc_rows = new HashSet[shard_size];

			for (int shard_id = 0; shard_id < shard_size; shard_id++)
				sync_del_doc_rows[shard_id] = new HashSet<String>();

		}

		final String class_name = MethodHandles.lookup().lookupClass().getName();

		Xml2SphinxDsThrd[] shard_thrd = new Xml2SphinxDsThrd[shard_size];
		Thread[] thrd = new Thread[shard_size * max_thrds];

		long start_time = System.currentTimeMillis();

		// PgSchema server is alive

		if (server_alive) {

			PgSchemaClientImpl[] clients = new PgSchemaClientImpl[shard_size * max_thrds];
			Thread[] get_thrd = new Thread[shard_size * max_thrds];

			try {

				// send ADD query to PgSchema server

				if (no_data_model) {

					clients[0] = new PgSchemaClientImpl(is, option, fst_conf, client_type, class_name, xml_post_editor, index_filter, true);
					get_thrd[0] = null;

				}

				// send GET query to PgSchema server

				for (int shard_id = 0; shard_id < shard_size; shard_id++) {

					for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

						int _thrd_id = shard_id * max_thrds + thrd_id;

						if (_thrd_id == 0 && no_data_model)
							continue;

						Thread _get_thrd = get_thrd[_thrd_id] = new Thread(new PgSchemaGetClientThrd(_thrd_id, option, fst_conf, client_type, class_name, xml_post_editor, index_filter, clients));

						_get_thrd.setPriority(Thread.MAX_PRIORITY);
						_get_thrd.start();

					}

				}

				for (int shard_id = 0; shard_id < shard_size; shard_id++) {

					for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

						String thrd_name = class_name + "-" + shard_id + "-" + thrd_id;
						int _thrd_id = shard_id * max_thrds + thrd_id;

						try {

							Thread _thrd;

							if (thrd_id == 0)
								_thrd = thrd[_thrd_id] = new Thread(shard_thrd[shard_id] = new Xml2SphinxDsThrd(shard_id, shard_size, thrd_id, get_thrd[_thrd_id], _thrd_id, clients, xml_file_filter, xml_file_queue, index_filter, ds_name, ds_dir_path, doc_rows, sync_del_doc_rows), thrd_name);
							else
								_thrd = thrd[_thrd_id] = new Thread(new Xml2SphinxDsThrd(shard_id, shard_size, thrd_id, get_thrd[_thrd_id], _thrd_id, clients, xml_file_filter, xml_file_queue, index_filter, ds_name, ds_dir_path, doc_rows, sync_del_doc_rows), thrd_name);

							_thrd.start();

						} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | PgSchemaException e) {
							e.printStackTrace();
							System.exit(1);
						}

					}

				}

			} catch (IOException | ParserConfigurationException | SAXException | PgSchemaException | InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		// stand alone

		else {

			for (int shard_id = 0; shard_id < shard_size; shard_id++) {

				for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

					String thrd_name = class_name + "-" + shard_id + "-" + thrd_id;
					int _thrd_id = shard_id * max_thrds + thrd_id;

					try {

						if (shard_id > 0 || thrd_id > 0)
							is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

						Thread _thrd;

						if (thrd_id == 0)
							_thrd = thrd[_thrd_id] = new Thread(shard_thrd[shard_id] = new Xml2SphinxDsThrd(shard_id, shard_size, thrd_id, is, xml_file_filter, xml_file_queue, xml_post_editor, option, index_filter, ds_name, ds_dir_path, doc_rows, sync_del_doc_rows), thrd_name);
						else
							_thrd = thrd[_thrd_id] = new Thread(new Xml2SphinxDsThrd(shard_id, shard_size, thrd_id, is, xml_file_filter, xml_file_queue, xml_post_editor, option, index_filter, ds_name, ds_dir_path, doc_rows, sync_del_doc_rows), thrd_name);

						_thrd.start();

					} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | PgSchemaException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

			}

		}

		for (int shard_id = 0; shard_id < shard_size; shard_id++) {

			for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

				int _thrd_id = shard_id * max_thrds + thrd_id;

				try {

					thrd[_thrd_id].join();

				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			try {

				shard_thrd[shard_id].composite();

			} catch (PgSchemaException | IOException | ParserConfigurationException | SAXException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		long end_time = System.currentTimeMillis();

		System.out.println("Execution time: " + (end_time - start_time) + " ms");

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xml2sphinxds: XML -> Sphinx data source (xmlpipe2) conversion for full-text indexing");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY --ds-dir DIRECTORY (default=\"" + ds_dir_name + "\")");
		System.err.println("        --update (insert if not exists, and update if required, default)");
		System.err.println("        --sync CHECK_SUM_DIRECTORY (insert if not exists, update if required, and delete rows if XML not exists)");
		System.err.println("        --sync-weak (insert if not exists, no update even if exists, no deletion)");
		System.err.println("        --inline-simple-cont (enable inlining simple content)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --validate (turn off XML Schema validation)");
		System.err.println("        --no-validate (turn off XML Schema validation, default)");
		System.err.println("        --well-formed (validate only whether document is well-formed)");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix) | zip (indicates xml.zip suffix)]");
		System.err.println("        --shard-size SHARD_SIZE (default=1)");
		System.err.println("        --min-word-len MIN_WORD_LENGTH (default is " + PgSchemaUtil.min_word_len + ")");
		System.err.println("        --max-field-len MAX_FIELD_LENGTH (default is " + PgSchemaUtil.sph_max_field_len / 1024 / 1024 + "M)");
		System.err.println("Option: --ds-name DS_NAME (default name is determined by quoting XSD file name)");
		System.err.println("        --attr  table_name.column_name");
		System.err.println("        --field table_name.column_name");
		System.err.println("        --mva   table_name.column_name (multi-valued attribute)");
		System.err.println("        --field-all (index all fields, default)");
		System.err.println("        --attr-all (all attributes's values are stored as attribute)");
		System.err.println("        --attr-string (all string values are stored as attribute)");
		System.err.println("        --attr-integer (all integer values are stored as attribute)");
		System.err.println("        --attr-float (all float values are stored as attribute)");
		System.err.println("        --attr-date (all date values are stored as attribute)");
		System.err.println("        --attr-time (all time values are stored as attribute)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int | long (default) | native | debug]");
		System.err.println("        --xml-file-prerix-digest DIGESTIBLE_PREFIX (default=\"\")");
		System.err.println("        --xml-file-ext-digest DIGESTIBLE_EXTENSION (default=\".\")");
		System.err.println("        --lower-case-doc-key (lower case document key)");
		System.err.println("        --upper-case-doc-key (upper case document key)");
		System.err.println("        --fill-default-value (fill @default value in case of empty)");
		System.err.println("        --filt-in   table_name.column_name");
		System.err.println("        --filt-out  table_name.column_name:regex_pattern(|regex_pattern...)");
		System.err.println("        --fill-this table_name.column_name:filling_text");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.err.println("        --no-pgschema-serv (not utilize PgSchema server)");
		System.err.println("        --pgschema-serv-host PG_SCHEMA_SERV_HOST_NAME (default=\"" + PgSchemaUtil.pg_schema_server_host + "\")");
		System.err.println("        --pgschema-serv-port PG_SCHEMA_SERV_PORT_NUMBER (default=" + PgSchemaUtil.pg_schema_server_port + ")");
		System.err.println("        --max-thrds MAX_THRDS (default is number of available processors)");
		System.exit(1);

	}

}
