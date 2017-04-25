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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.*;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.comparator.SizeFileComparator;
import org.xml.sax.SAXException;

/**
 * CSV conversion and PostgreSQL data migration.
 * 
 * @author yokochi
 */
public class xml2pgcsv {

	/** The CSV directory name. */
	public static String csv_dir_name = "pg_work";

	/** The schema location. */
	public static String schema_location = "";

	/** The schema option. */
	public static PgSchemaOption option = new PgSchemaOption(true);
	
	/** The PostgreSQL option. */
	public static PgOption pg_option = new PgOption();

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

		List<String> xml_file_names = new ArrayList<String>();

		boolean _document_key = false;
		boolean _no_document_key = false;

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--no-rel"))
				option.cancelRelDataExt();

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--doc-key")) {

				if (_no_document_key) {
					System.err.println("--no-doc-key is set already.");
					showUsage();
				}

				_document_key = true;
			}

			else if (args[i].equals("--no-doc-key")) {

				if (_document_key) {
					System.err.println("--doc-key is set already.");
					showUsage();
				}

				_no_document_key = true;
			}

			else if (args[i].equals("--ser-key"))
				option.serial_key = true;

			else if (args[i].equals("--xpath-key"))
				option.xpath_key = true;

			else if (args[i].equals("--case-insensitive"))
				option.case_sense = false;

			else if (args[i].equals("--append"))
				option.append = true;

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

				if (!xml_file_names.contains(xml_file_name))
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

			else if (args[i].equals("--csv-dir"))
				csv_dir_name = args[++i];

			else if (args[i].matches("^--db-?host.*"))
				pg_option.host = args[++i];

			else if (args[i].matches("^--db-?port.*"))
				pg_option.port = Integer.valueOf(args[++i]);

			else if (args[i].matches("^--db-?name.*"))
				pg_option.database = args[++i];

			else if (args[i].matches("^--db-?user.*"))
				pg_option.user = args[++i];

			else if (args[i].matches("^--db-?pass.*"))
				pg_option.password = args[++i];

			else if (args[i].equals("--filt-in"))
				xml_post_editor.addFiltIn(args[++i]);

			else if (args[i].equals("--filt-out"))
				xml_post_editor.addFiltOut(args[++i]);

			else if (args[i].equals("--fill-this"))
				xml_post_editor.addFillThis(args[++i]);

			else if (args[i].equals("--hash-by"))
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size"))
				option.hash_size = PgHashSize.getPgHashSize(args[++i]);

			else if (args[i].equals("--ser-size"))
				option.ser_size = PgSerSize.getPgSerSize(args[++i]);

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

		if (_document_key)
			option.document_key = true;

		else if (_no_document_key)
			option.document_key = false;

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

		if (max_thrds > 1)
			Arrays.sort(xml_files, SizeFileComparator.SIZE_COMPARATOR);

		File csv_dir = new File(csv_dir_name);

		if (!csv_dir.isDirectory()) {

			if (!csv_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + csv_dir_name + "'.");
				System.exit(1);
			}

		}

		csv_dir_name = csv_dir_name.replaceFirst("/$", "") + "/";

		Xml2PgCsvThrd[] proc_thrd = new Xml2PgCsvThrd[max_thrds];
		Thread[] thrd = new Thread[max_thrds];

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			String thrd_name = "xml2pgcsv-" + thrd_id;

			try {

				if (thrd_id > 0)
					is = PgSchemaUtil.getInputStream(schema_location, null);

				proc_thrd[thrd_id] = new Xml2PgCsvThrd(thrd_id, max_thrds, is, csv_dir_name, option, pg_option);

			} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | SQLException | PgSchemaException e) {
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

		System.err.println("xml2pgcsv: XML -> CSV conversion and PostgreSQL data migration");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY --csv-dir DIRECTORY (default=\"" + csv_dir_name + "\")");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --append (append to existing CSV files)");
		System.err.println("        --doc-key (append " + PgSchemaUtil.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + PgSchemaUtil.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + PgSchemaUtil.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + PgSchemaUtil.xpath_key_name + " column in all relations)");
		System.err.println("        --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --validate (turn on XML Schema validation)");
		System.err.println("        --no-validate (turn off XML Schema validation, default)");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix)]");
		System.err.println("Option: --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host HOST (default=\"" + PgSchemaUtil.host + "\")");
		System.err.println("        --db-port PORT (default=\"" + PgSchemaUtil.port + "\")");
		System.err.println("        --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16bit); | int (32bit, default)]");
		System.err.println("        --xml-file-prerix-digest DIGESTIBLE_PREFIX (default=\"\")");
		System.err.println("        --xml-file-ext-digest DIGESTIBLE_EXTENSION (default=\".\")");
		System.err.println("        --filt-in   table_name.column_name");
		System.err.println("        --filt-out  table_name.column_name:regex_pattern(|regex_pattern...)");
		System.err.println("        --fill-this table_name.column_name:filling_text");
		System.err.println("        --max-thrds MAX_THRDS (default is number of available processors)");
		System.exit(1);

	}

}
