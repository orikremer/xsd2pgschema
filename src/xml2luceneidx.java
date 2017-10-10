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
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.parsers.*;

import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

/**
 * Lucene full-text indexing.
 *
 * @author yokochi
 */
public class xml2luceneidx {

	/** The index directory name. */
	public static String idx_dir_name = "lucene_index";

	/** The schema location. */
	public static String schema_location = "";

	/** The schema option. */
	public static PgSchemaOption option = new PgSchemaOption(false);

	/** The XML file filter. */
	public static XmlFileFilter xml_file_filter = new XmlFileFilter();

	/** The XML post editor. */
	public static XmlPostEditor xml_post_editor = new XmlPostEditor();

	/** The index filter. */
	public static IndexFilter index_filter = new IndexFilter();

	/** The XML file queue. */
	public static LinkedBlockingQueue<File> xml_file_queue = null;

	/** The shard size. */
	public static int shard_size = 1;

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

		HashSet<String> xml_file_names = new HashSet<String>();

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--no-rel"))
				option.cancelRelDataExt();

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

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

			else if (args[i].equals("--idx-dir"))
				idx_dir_name = args[++i];

			else if (args[i].equals("--filt-in"))
				xml_post_editor.addFiltIn(args[++i]);

			else if (args[i].equals("--filt-out"))
				xml_post_editor.addFiltOut(args[++i]);

			else if (args[i].equals("--fill-this"))
				xml_post_editor.addFillThis(args[++i]);

			else if (args[i].equals("--attr"))
				index_filter.addAttr(args[++i]);

			else if (args[i].equals("--field"))
				index_filter.addField(args[++i]);

			else if (args[i].equals("--attr-all"))
				index_filter.setAttrAll();

			else if (args[i].equals("--field-all"))
				index_filter.setFiledAll();

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

			else if (args[i].equals("--min-word-len"))
				index_filter.setMinWordLen(args[++i]);

			else if (args[i].equals("--numeric-idx"))
				index_filter.enableNumericIndex();

			else if (args[i].equals("--hash-by"))
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size"))
				option.hash_size = PgHashSize.getPgHashSize(args[++i]);

			else if (args[i].equals("--shard-size")) {
				shard_size = Integer.valueOf(args[++i]);

				if (shard_size <= 0) {
					System.err.println("Out of range (shard-size).");
					showUsage();
				}
			}

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

		InputStream is = PgSchemaUtil.getSchemaInputStream(schema_location, null);

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

		xml_file_queue = PgSchemaUtil.getTargetFileQueue(xml_file_names, filename_filter);

		if (xml_file_queue.size() < shard_size)
			shard_size = xml_file_queue.size();

		max_thrds = max_thrds / shard_size; // number of thread per a shard

		if (max_thrds == 0)
			max_thrds = 1;

		File idx_dir = new File(idx_dir_name);

		if (!idx_dir.isDirectory()) {

			if (!idx_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + idx_dir_name + "'.");
				System.exit(1);
			}

		}

		Xml2LuceneIdxThrd[] proc_thrd = new Xml2LuceneIdxThrd[shard_size * max_thrds];
		Thread[] thrd = new Thread[shard_size * max_thrds];

		for (int shard_id = 0; shard_id < shard_size; shard_id++) {

			for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

				String thrd_name = "xml2luceneidx-" + shard_id + "-" + thrd_id;
				int _thrd_id = shard_id * max_thrds + thrd_id;

				try {

					if (shard_id > 0 || thrd_id > 0)
						is = PgSchemaUtil.getSchemaInputStream(schema_location, null);

					proc_thrd[_thrd_id] = new Xml2LuceneIdxThrd(shard_id, shard_size, thrd_id, max_thrds, is, option);

				} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

				thrd[_thrd_id] = new Thread(proc_thrd[_thrd_id], thrd_name);

				thrd[_thrd_id].start();

			}

		}

		for (int shard_id = 0; shard_id < shard_size; shard_id++) {

			for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

				int _thrd_id = shard_id * max_thrds + thrd_id;

				try {

					if (thrd[_thrd_id] != null)
						thrd[_thrd_id].join();

				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			try {

				if (max_thrds > 1)
					proc_thrd[shard_id * max_thrds].merge();

			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xm2luceneidx: XML -> Lucene full-text indexing");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY --idx-dir DIRECTORY (default=\"" + idx_dir_name + "\")");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --validate (turn on XML Schema validation)");
		System.err.println("        --no-validate (turn off XML Schema validation, default)");
		System.err.println("        --append (append to existing index files)");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix)]");
		System.err.println("        --shard-size SHARD_SIZE (default=1)");
		System.err.println("        --min-word-len MIN_WORD_LENGTH (default is " + PgSchemaUtil.min_word_len + ")");
		System.err.println("        --numeric-idx (allow to store numeric values in index)");
		System.err.println("Option: --attr  table_name.column_name");
		System.err.println("        --field table_name.column_name");
		System.err.println("        --attr-all");
		System.err.println("        --field-all (default)");
		System.err.println("        --attr-string (all string values are stored as attribute)");
		System.err.println("        --attr-integer (all integer values are stored as attribute)");
		System.err.println("        --attr-float (all float values are stored as attribute)");
		System.err.println("        --attr-date (all date values are stored as attribute)");
		System.err.println("        --attr-time (all time values are stored as attribute)");
		System.err.println("        --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --xml-file-prerix-digest DIGESTIBLE_PREFIX (default=\"\")");
		System.err.println("        --xml-file-ext-digest DIGESTIBLE_EXTENSION (default=\".\")");
		System.err.println("        --filt-in   table_name.column_name");
		System.err.println("        --filt-out  table_name.column_name:regex_pattern(|regex_pattern...)");
		System.err.println("        --fill-this table_name.column_name:filling_text");
		System.err.println("        --max-thrds MAX_THRDS (default is number of available processors)");
		System.exit(1);

	}

}
