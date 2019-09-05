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
import net.sf.xsd2pgschema.docbuilder.*;
import net.sf.xsd2pgschema.implement.Xml2JsonThrd;
import net.sf.xsd2pgschema.option.*;
import net.sf.xsd2pgschema.serverutil.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.*;

import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

/**
 * Convert XML documents to JSON documents.
 *
 * @author yokochi
 */
public class xml2json {

	/** The JSON directory name. */
	private static String json_dir_name = "json_work";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(false);

		option.cancelRelDataExt(); // turn off relational data extension

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class,PgSchema.class); // FST optimization

		/** The JSON builder option. */
		JsonBuilderOption jsonb_option = new JsonBuilderOption();

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

			else if (args[i].equals("--obj-json"))
				jsonb_option.type = JsonType.object;

			else if (args[i].equals("--col-json"))
				jsonb_option.type = JsonType.column;

			else if (args[i].equals("--rel-json"))
				jsonb_option.type = JsonType.relational;

			else if (args[i].equals("--json-array-all"))
				jsonb_option.array_all = true;

			else if (args[i].equals("--json-attr-prefix") && i + 1 < args.length)
				jsonb_option.setAttrPrefix(args[++i]);

			else if (args[i].equals("--json-simple-cont-name") && i + 1 < args.length)
				jsonb_option.setSimpleContentName(args[++i]);

			else if (args[i].equals("--json-indent-offset") && i + 1 < args.length)
				jsonb_option.setIndentOffset(args[++i]);

			else if (args[i].equals("--json-key-value-offset") && i + 1 < args.length)
				jsonb_option.setKeyValueOffset(args[++i]);

			else if (args[i].equals("--json-no-linefeed"))
				jsonb_option.setLineFeed(false);

			else if (args[i].equals("--json-compact"))
				jsonb_option.setCompact();

			else if (args[i].equals("--schema-ver") && i + 1 < args.length)
				jsonb_option.setSchemaVer(args[++i]);

			else if (args[i].equals("--json-dir") && i + 1 < args.length)
				json_dir_name = args[++i];

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

			else if (args[i].equals("--case-insensitive")) {
				option.setCaseInsensitive();
				jsonb_option.setCaseInsensitive();
			}

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].startsWith("--valid"))
				option.validate = option.full_check = true;

			else if (args[i].startsWith("--no-valid"))
				option.validate = false;

			else if (args[i].equals("--well-formed")) {
				option.validate = true;
				option.full_check = false;
			}

			else if (args[i].equals("--discarded-doc-key-name") && i + 1 < args.length)
				option.addDiscardedDocKeyName(args[++i]);

			else if (args[i].equals("--no-pgschema-serv"))
				option.pg_schema_server = false;

			else if (args[i].equals("--pgschema-serv-host") && i + 1 < args.length)
				option.pg_schema_server_host = args[++i];

			else if (args[i].equals("--pgschema-serv-port") && i + 1 < args.length)
				option.pg_schema_server_port = Integer.valueOf(args[++i]);

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

		Path json_dir_path = Paths.get(json_dir_name);

		if (!Files.isDirectory(json_dir_path)) {

			try {
				Files.createDirectory(json_dir_path);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

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

						Thread _thrd = thrd[thrd_id] = new Thread(new Xml2JsonThrd(thrd_id, get_thrd[thrd_id], clients, json_dir_path, xml_file_filter, xml_file_queue, xml_post_editor, jsonb_option), thrd_name);

						_thrd.start();

					} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | PgSchemaException e) {
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

					Thread _thrd = thrd[thrd_id] = new Thread(new Xml2JsonThrd(thrd_id, is, json_dir_path, xml_file_filter, xml_file_queue, xml_post_editor, option, jsonb_option), thrd_name);

					_thrd.start();

				} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

		}

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

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		JsonBuilderOption jsonb_option = new JsonBuilderOption();

		System.err.println("xml2json: XML -> JSON document conversion");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY --json-dir DIRECTORY (default=\"" + json_dir_name + "\")");
		System.err.println("        --inline-simple-cont (enable inlining simple content)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --validate (turn on XML Schema validation)");
		System.err.println("        --no-validate (turn off XML Schema validation, default)");
		System.err.println("        --well-formed (validate only whether document is well-formed)");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix) | zip (indicates xml.zip suffix)]");
		System.err.println("        --schema-ver JSON_SCHEMA_VER (choose from \"draft_v7\" (default), \"draft_v6\", \"draft_v4\", or \"latest\" as \"" + JsonSchemaVersion.defaultVersion().toString() + "\")");
		System.err.println("        --obj-json (use object-oriented JSON format)");
		System.err.println("        --col-json (use column-oriented JSON format, default)");
		System.err.println("        --rel-json (use relational-oriented JSON format)");
		System.err.println("Option: --json-attr-prefix ATTR_PREFIX_CODE (default=\"" + jsonb_option.getAttrPrefix() + "\")");
		System.err.println("        --json-simple-cont-name SIMPLE_CONTENT_NAME (default=\"" + jsonb_option.getSimpleContentName() + "\")");
		System.err.println("        --json-indent-offset INTEGER (default=" + jsonb_option.getIndentOffset() + ", min=0, max=4)");
		System.err.println("        --json-key-value-offset INTEGER (default=" + jsonb_option.getKeyValueOffset() + ", min=0, max=4)");
		System.err.println("        --json-no-linefeed (dismiss line feed code)");
		System.err.println("        --json-compact (equals to set --json-indent-offset 0 --json-key-value-offset 0 --json-no-linefeed)");
		System.err.println("        --json-array-all (use JSON array uniformly for descendants, effective only in column- and relational-oriented JSON format)");
		System.err.println("        --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
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
