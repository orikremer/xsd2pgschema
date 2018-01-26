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
import java.io.InputStream;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FilenameUtils;

/**
 * Validate XML documents against XML Schema.
 *
 * @author yokochi
 */
public class xmlvalidator {

	/** The schema location. */
	public static String schema_location = "";

	/** The XML file filter. */
	public static XmlFileFilter xml_file_filter = new XmlFileFilter();

	/** The XML file queue. */
	public static LinkedBlockingQueue<File> xml_file_queue = null;

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

			if (args[i].equals("--xsd"))
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

		InputStream is = PgSchemaUtil.getSchemaInputStream(schema_location, null, false);

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

		xml_file_queue = PgSchemaUtil.getQueueOfTargetFiles(xml_file_names, filename_filter);

		if (xml_file_queue.size() < max_thrds)
			max_thrds = xml_file_queue.size();

		XmlValidatorThrd[] proc_thrd = new XmlValidatorThrd[max_thrds];
		Thread[] thrd = new Thread[max_thrds];

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			String thrd_name = "xmlvalidator-" + thrd_id;

			if (thrd_id > 0)
				is = PgSchemaUtil.getSchemaInputStream(schema_location, null, false);

			proc_thrd[thrd_id] = new XmlValidatorThrd(thrd_id, max_thrds);

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

		System.err.println("xmlvalidator: Validate XML documents against XML Schema");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix)]");
		System.err.println("Option: --max-thrds MAX_THRDS (default is number of available processors)");
		System.exit(1);

	}

}
