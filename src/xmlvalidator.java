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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Validate XML documents against XML Schema.
 *
 * @author yokochi
 */
public class xmlvalidator {

	/** The check sum directory name. */
	private static String check_sum_dir_name = "";

	/** The schema option. */
	private static PgSchemaOption option = new PgSchemaOption(true);

	/** The XML file filter. */
	private static XmlFileFilter xml_file_filter = new XmlFileFilter();

	/** The XML file queue. */
	private static LinkedBlockingQueue<Path> xml_file_queue = null;

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

		option.validate = true;

		HashSet<String> xml_file_names = new HashSet<String>();

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

			else if (args[i].equals("--well-formed"))
				option.full_check = false;

			else if (args[i].equals("--verbose"))
				option.verbose = true;

			else if (args[i].equals("--sync") && i + 1 < args.length) {
				option.sync = true;
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

		if (option.root_schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

		if (is == null)
			showUsage();

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

		if (xml_file_queue.size() < max_thrds)
			max_thrds = xml_file_queue.size();

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

			option.check_sum_dir_path = check_sum_dir_path;

		}

		XmlValidatorThrd[] proc_thrd = new XmlValidatorThrd[max_thrds];
		Thread[] thrd = new Thread[max_thrds];

		long start_time = System.currentTimeMillis();

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			String thrd_name = "xmlvalidator-" + thrd_id;

			try {

				if (thrd_id > 0)
					is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

				proc_thrd[thrd_id] = new XmlValidatorThrd(thrd_id, xml_file_filter, xml_file_queue, option);

			} catch (NoSuchAlgorithmException e) {
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

		long end_time = System.currentTimeMillis();

		System.out.println("Execution time: " + (end_time - start_time) + " ms");

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xmlvalidator: Validate XML documents against XML Schema");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY");
		System.err.println("        --well-formed (validate only whether document is well-formed)");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix) | zip (indicates xml.zip suffix)]");
		System.err.println("Option: --sync CHECK_SUM_DIRECTORY (generate check sum files)");
		System.err.println("        --checksum-by ALGORITHM [MD2 | MD5 (default) | SHA-1 | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --max-thrds MAX_THRDS (default is number of available processors)");
		System.err.println("        --verbose");
		System.exit(1);

	}

}
