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
import net.sf.xsd2pgschema.implement.XmlValidatorThrd;
import net.sf.xsd2pgschema.option.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
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

		option.validate = true; // enable XML Schema validation

		/** The XML file filter. */
		XmlFileFilter xml_file_filter = new XmlFileFilter();

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

			else if (args[i].equals("--well-formed"))
				option.full_check = false;

			else if (args[i].equals("--remove-invalid-xml"))
				option.remove_invalid_xml = true;

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

		xml_file_names.clear();

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

		final String class_name = MethodHandles.lookup().lookupClass().getName();

		Thread[] thrd = new Thread[max_thrds];

		long start_time = System.currentTimeMillis();

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			String thrd_name = class_name + "-" + thrd_id;

			try {

				Thread _thrd = thrd[thrd_id] = new Thread(new XmlValidatorThrd(thrd_id, xml_file_filter, xml_file_queue, option), thrd_name);

				_thrd.start();


			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				System.exit(1);
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

		System.err.println("xmlvalidator: Validate XML documents against XML Schema");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY");
		System.err.println("        --well-formed (validate only whether document is well-formed)");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix) | zip (indicates xml.zip suffix)]");
		System.err.println("Option: --sync CHECK_SUM_DIRECTORY (generate check sum files)");
		System.err.println("        --checksum-by ALGORITHM [MD2 | MD5 (default) | SHA-1 | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --max-thrds MAX_THRDS (default is number of available processors)");
		System.err.println("        --remove-invalid-xml (remove invalid XML documents)");
		System.err.println("        --verbose");
		System.exit(1);

	}

}
