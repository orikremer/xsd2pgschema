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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FilenameUtils;

/**
 * Report check sum directory status.
 *
 * @author yokochi
 */
public class chksumstat {

	/** The check sum directory name. */
	private static String check_sum_dir_name = "";

	/** The schema option. */
	private static PgSchemaOption option = new PgSchemaOption(true);

	/** The XML file filter. */
	private static XmlFileFilter xml_file_filter = new XmlFileFilter();

	/** The XML file queue. */
	private static LinkedBlockingQueue<File> xml_file_queue = null;

	/** The set of new document id while synchronization. */
	private static HashSet<String> sync_new_doc_rows = null;

	/** The set of updating document id while synchronization. */
	private static HashSet<String> sync_up_doc_rows = null;

	/** The set of deleting document id while synchronization (key=document id, value=check sum file). */
	private static HashMap<String, String> sync_del_doc_rows = null;

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		option.sync = option.sync_dry_run = true;
		option.sync_weak = false;

		HashSet<String> xml_file_names = new HashSet<String>();

		boolean touch_xml = false;

		for (int i = 0; i < args.length; i++) {

			if (args[i].startsWith("--"))
				touch_xml = false;

			if (args[i].equals("--xml") && i + 1 < args.length) {
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

			else if ((args[i].equals("--sync") || args[i].equals("--sync-dir")) && i + 1 < args.length)
				check_sum_dir_name = args[++i];

			else if (args[i].equals("--update"))
				option.sync_dry_run = false;

			else if (args[i].equals("--checksum-by") && i + 1 < args.length) {
				if (!option.setCheckSumAlgorithm(args[++i]))
					showUsage();
			}

			else if (args[i].equals("--verbose"))
				option.verbose = true;

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

		if (check_sum_dir_name.isEmpty()) {
			System.err.println("Check sum directory is empty.");
			showUsage();
		}

		File check_sum_dir = new File(check_sum_dir_name);

		if (!check_sum_dir.isDirectory()) {

			if (!check_sum_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + check_sum_dir_name + "'.");
				System.exit(1);
			}

		}

		option.check_sum_dir = check_sum_dir;

		String chk_sum_file_ext = option.check_sum_algorithm.toLowerCase();

		FilenameFilter chk_sum_file_filter = new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return chk_sum_file_ext.equals(FilenameUtils.getExtension(name));
			}

		};

		sync_new_doc_rows = new HashSet<String>();
		sync_up_doc_rows = new HashSet<String>();
		sync_del_doc_rows = new HashMap<String, String>();

		for (String check_sum_file_name : check_sum_dir.list(chk_sum_file_filter)) {

			XmlParser xml_parser = new XmlParser(check_sum_file_name.replaceFirst("\\." + chk_sum_file_ext + "$", ""), xml_file_filter);

			sync_del_doc_rows.put(xml_parser.document_id, check_sum_file_name);

		}

		try {

			MessageDigest md_chk_sum = MessageDigest.getInstance(option.check_sum_algorithm);

			File xml_file;

			while ((xml_file = xml_file_queue.poll()) != null) {

				XmlParser xml_parser = new XmlParser(xml_file, xml_file_filter);

				String document_id = xml_parser.document_id;

				if (sync_del_doc_rows.remove(document_id) != null) {

					if (xml_parser.identify(option, md_chk_sum))
						continue;

					sync_up_doc_rows.add(document_id);

				}

				else {

					if (!option.sync_dry_run)
						xml_parser.identify(option, md_chk_sum);

					sync_new_doc_rows.add(document_id);

				}

			}

			if (!option.sync_dry_run && sync_del_doc_rows.size() > 0) {

				sync_del_doc_rows.values().forEach(chk_sum_file_name -> {

					File check_sum_file = new File(check_sum_dir, chk_sum_file_name);

					if (check_sum_file.exists())
						check_sum_file.delete();

				});

			}

		} catch (NoSuchAlgorithmException | IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		System.out.println("# created docs: " + sync_new_doc_rows.size());

		if (option.verbose)
			sync_new_doc_rows.forEach(document_id -> System.out.println(document_id));

		System.out.println("\n# updated docs: " + sync_up_doc_rows.size());

		if (option.verbose)
			sync_up_doc_rows.forEach(document_id -> System.out.println(document_id));

		System.out.println("\n# deleted docs: " + sync_del_doc_rows.size());

		if (option.verbose)
			sync_del_doc_rows.keySet().forEach(document_id -> System.out.println(document_id));

		sync_new_doc_rows.clear();
		sync_up_doc_rows.clear();
		sync_del_doc_rows.clear();

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("chksumstat: Report check sum directory status");
		System.err.println("Usage:  --xml XML_FILE_OR_DIRECTORY --sync-dir CHECK_SUM_DIRECTORY");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix) | zip (indicates xml.zip suffix)]");
		System.err.println("Option: --checksum-by ALGORITHM [MD2 | MD5 (default) | SHA-1 | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --xml-file-prerix-digest DIGESTIBLE_PREFIX (default=\"\")");
		System.err.println("        --xml-file-ext-digest DIGESTIBLE_EXTENSION (default=\".\")");
		System.err.println("        --update (update check sum files anyway)");
		System.err.println("        --verbose");
		System.exit(1);

	}

}
