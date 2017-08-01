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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;

import javax.xml.parsers.*;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.comparator.SizeFileComparator;
import org.xml.sax.SAXException;

/**
 * JSON conversion.
 *
 * @author yokochi
 */
public class xml2json {

	/** The JSON directory name. */
	public static String json_dir_name = "json_work";

	/** The schema location. */
	public static String schema_location = "";

	/** The JSON type. */
	public static JsonType json_type = JsonType.defaultType();

	/** The schema option. */
	public static PgSchemaOption option = new PgSchemaOption(false);

	/** The JSON builder option. */
	public static JsonBuilderOption jsonb_option = new JsonBuilderOption();

	/** The XML file filter. */
	public static XmlFileFilter xml_file_filter = new XmlFileFilter();

	/** The XML post editor. */
	public static XmlPostEditor xml_post_editor = new XmlPostEditor();

	/** The XML files. */
	public static File[] xml_files = null;

	/** The runtime. */
	private static Runtime runtime = Runtime.getRuntime();

	/** The available processors. */
	private static final int cpu_num = runtime.availableProcessors();

	/** The max threads. */
	private static int max_thrds = cpu_num;

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		option.cancelRelDataExt();

		HashSet<String> xml_file_names = new HashSet<String>();

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--case-insensitive"))
				option.case_sense = false;

			else if (args[i].startsWith("--valid"))
				option.validate = true;

			else if (args[i].startsWith("--no-valid"))
				option.validate = false;

			else if (args[i].equals("--xsd"))
				schema_location = args[++i];

			else if (args[i].equals("--xml")) {
				String xml_file_name = args[++i];

				if (xml_file_name.isEmpty()) {
					System.err.println("XML file name is empty.");
					showUsage();
				}

				xml_file_names.add(xml_file_name);
			}

			else if (args[i].equals("--xml-file-ext")) {

				if (!xml_file_filter.setExt(args[++i]))
					showUsage();

			}

			else if (args[i].equals("--xml-file-prefix-digest"))
				xml_file_filter.setPrefixDigest(args[++i]);

			else if (args[i].equals("--xml-file-ext-digest"))
				xml_file_filter.setExtDigest(args[++i]);

			else if (args[i].equals("--discard-json-doc-key"))
				jsonb_option.setDiscardDocKey(args[++i]);

			else if (args[i].equals("--obj-json"))
				json_type = JsonType.object;

			else if (args[i].equals("--col-json"))
				json_type = JsonType.column;

			else if (args[i].equals("--rel-json"))
				json_type = JsonType.relational;

			else if (args[i].equals("--json-array-all"))
				jsonb_option.array_all = true;

			else if (args[i].equals("--attr-json-prefix"))
				jsonb_option.setAttrPrefix(args[++i]);

			else if (args[i].equals("--simple-cont-json-key"))
				jsonb_option.setSimpleContKey(args[++i]);

			else if (args[i].equals("--json-indent-spaces"))
				jsonb_option.setIndentSpaces(args[++i]);

			else if (args[i].equals("--json-key-value-spaces"))
				jsonb_option.setKeyValueSpaces(args[++i]);

			else if (args[i].equals("--json-no-linefeed"))
				jsonb_option.linefeed = false;

			else if (args[i].equals("--json-compact"))
				jsonb_option.setCompact();

			else if (args[i].equals("--json-dir"))
				json_dir_name = args[++i];

			else if (args[i].equals("--filt-in"))
				xml_post_editor.addFiltIn(args[++i]);

			else if (args[i].equals("--filt-out"))
				xml_post_editor.addFiltOut(args[++i]);

			else if (args[i].equals("--fill-this"))
				xml_post_editor.addFillThis(args[++i]);

			else if (args[i].equals("--max-thrds")) {
				max_thrds = Integer.valueOf(args[++i]);

				if (max_thrds <= 0 || max_thrds > cpu_num * 2) {
					System.err.println("Out of range (max-thrds).");
					showUsage();
				}
			}

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = PgSchemaUtil.getInputStream(schema_location, null);

		if (is == null)
			showUsage();

		if (xml_file_names.size() == 0) {
			System.err.println("XML file name is empty.");
			showUsage();
		}

		FilenameFilter filename_filter = new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return xml_file_filter.ext.equals(FilenameUtils.getExtension(name));
			}

		};

		xml_files = PgSchemaUtil.getTargetFiles(xml_file_names, filename_filter);

		if (xml_files.length < max_thrds)
			max_thrds = xml_files.length;

		if (max_thrds > 1 && xml_files.length < PgSchemaUtil.max_sort_xml_files)
			Arrays.sort(xml_files, SizeFileComparator.SIZE_COMPARATOR);

		File json_dir = new File(json_dir_name);

		if (!json_dir.isDirectory()) {

			if (!json_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + json_dir_name + "'.");
				System.exit(1);
			}

		}

		json_dir_name = json_dir_name.replaceFirst("/$", "") + "/";

		Xml2JsonThrd[] proc_thrd = new Xml2JsonThrd[max_thrds];
		Thread[] thrd = new Thread[max_thrds];

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			String thrd_name = "xml2json-" + thrd_id;

			try {

				if (thrd_id > 0)
					is = PgSchemaUtil.getInputStream(schema_location, null);

				proc_thrd[thrd_id] = new Xml2JsonThrd(thrd_id, max_thrds, is, option, jsonb_option);

			} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | PgSchemaException e) {
				e.printStackTrace();
				System.exit(1);
			}

			thrd[thrd_id] = new Thread(proc_thrd[thrd_id], thrd_name);

			thrd[thrd_id].start();

		}

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			try {

				if (thrd[thrd_id] != null)
					thrd[thrd_id].join();

			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		JsonBuilderOption jsonb_option = new JsonBuilderOption();

		System.err.println("xml2json: XML -> JSON conversion");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY --json-dir DIRECTORY (default=\"" + json_dir_name + "\")");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --validate (turn on XML Schema validation)");
		System.err.println("        --no-validate (turn off XML Schema validation, default)");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix)]");
		System.err.println("        --obj-json (use column-oriented JSON format)");
		System.err.println("        --col-json (use column-oriented JSON format, default)");
		System.err.println("        --rel-json (use relational-oriented JSON format)");
		System.err.println("Option: --attr-json-prefix ATTR_PREFIX_CODE (default=\"" + jsonb_option.attr_prefix + "\")");
		System.err.println("        --simple-cont-json-key SIMPLE_CONTENT_NAME (default=\"" + jsonb_option.simple_cont_key + "\")");
		System.err.println("        --discard-json-doc-key DISCARDED_DOC_KEY_NAME (default=\"" + jsonb_option.discard_doc_key + "\").");
		System.err.println("        --json-indent-spaces INTEGER (default=" + jsonb_option.indent_spaces + ", min=0, max=4)");
		System.err.println("        --json-key-value-spaces INTEGER (default=" + jsonb_option.key_value_spaces + ", min=0, max=1)");
		System.err.println("        --json-no-linefeed (avoid to use linefeed code)");
		System.err.println("        --json-compact (equals to set --json-indent-spaces 0 --json-key-value-spaces 0 --json-no-linefeed)");
		System.err.println("        --json-array-all (use JSON array uniformly for descendants, effective only in column- and relational-oriented JSON format)");
		System.err.println("        --xml-file-prerix-digest DIGESTIBLE_PREFIX (default=\"\")");
		System.err.println("        --xml-file-ext-digest DIGESTIBLE_EXTENSION (default=\".\")");
		System.err.println("        --filt-in   table_name.column_name");
		System.err.println("        --filt-out  table_name.column_name:regex_pattern(|regex_pattern...)");
		System.err.println("        --fill-this table_name.column_name:filling_text");
		System.err.println("        --max-thrds MAX_THRDS (default is number of available processors)");
		System.exit(1);

	}

}
