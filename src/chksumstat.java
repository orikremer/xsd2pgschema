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
import net.sf.xsd2pgschema.implement.ChkSumStatThrd;
import net.sf.xsd2pgschema.option.*;
import net.sf.xsd2pgschema.xmlutil.XmlParser;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

		option.sync = option.sync_dry_run = true; // dry-run synchronization
		option.sync_weak = false;

		/** The XML file filter. */
		XmlFileFilter xml_file_filter = new XmlFileFilter();

		/** The XML file queue. */
		LinkedBlockingQueue<Path> xml_file_queue;

		/** The target XML file patterns. */
		HashSet<String> xml_file_names = new HashSet<String>();

		/** The set of new document id while synchronization. */
		HashSet<String> sync_new_doc_rows = new HashSet<String>();

		/** The set of updating document id while synchronization. */
		HashSet<String> sync_up_doc_rows = new HashSet<String>();

		/** The set of deleting document id while synchronization (key=document id, value=check sum file path). */
		HashMap<String, Path> sync_del_doc_rows = new HashMap<String, Path>();

		/** The available processors. */
		int cpu_num = Runtime.getRuntime().availableProcessors();

		/** The max threads. */
		int max_thrds = cpu_num;

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

		String chk_sum_file_ext = option.check_sum_algorithm.toLowerCase();

		FilenameFilter chk_sum_file_filter = new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return chk_sum_file_ext.equals(FilenameUtils.getExtension(name));
			}

		};

		try {

			Files.list(check_sum_dir_path).filter(check_sum_path -> Files.isRegularFile(check_sum_path) && chk_sum_file_filter.accept(null, check_sum_path.getFileName().toString())).forEach(check_sum_path -> {

				XmlParser xml_parser = new XmlParser(check_sum_path.getFileName().toString().replaceFirst("\\." + chk_sum_file_ext + "$", ""), xml_file_filter);

				sync_del_doc_rows.put(xml_parser.document_id, check_sum_path);

			});

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		final String class_name = MethodHandles.lookup().lookupClass().getName();

		Thread[] thrd = new Thread[max_thrds];

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			String thrd_name = class_name + "-" + thrd_id;

			try {

				Thread _thrd = thrd[thrd_id] = new Thread(new ChkSumStatThrd(xml_file_filter, xml_file_queue, sync_new_doc_rows, sync_up_doc_rows, sync_del_doc_rows, option), thrd_name);

				_thrd.start();

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				System.exit(1);
			}

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

		if (!option.sync_dry_run && sync_del_doc_rows.size() > 0) {

			sync_del_doc_rows.values().forEach(chk_sum_file_path -> {

				try {
					Files.deleteIfExists(chk_sum_file_path);
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

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
		System.err.println("        --max-thrds MAX_THRDS (default is number of available processors)");
		System.err.println("        --verbose");
		System.exit(1);

	}

}
