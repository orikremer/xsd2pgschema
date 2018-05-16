/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017-2018 Masashi Yokochi

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

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathListenerException;

/**
 * Split large XML file into small XML files.
 *
 * @author yokochi
 */
public class xmlsplitter {

	/** The XML directory name for split XML files. */
	private static String xml_dir_name = "xml_work";

	/** The schema option. */
	private static PgSchemaOption option = new PgSchemaOption(false);

	/** The XML file filter. */
	private static XmlFileFilter xml_file_filter = new XmlFileFilter();

	/** The source XML file queue. */
	private static LinkedBlockingQueue<File> xml_file_queue = null;

	/** The XPath expression pointing document key. */
	private static String xpath_doc_key = "";

	/** The shard size. */
	private static int shard_size = 1;

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

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

			else if (args[i].equals("--xml-dir") && i + 1 < args.length)
				xml_dir_name = args[++i];

			else if (args[i].equals("--xpath-doc-key") && i + 1 < args.length)
				xpath_doc_key = args[++i];

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--pg-public-schema"))
				option.pg_named_schema = false;

			else if (args[i].equals("--pg-named-schema"))
				option.pg_named_schema = true;

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--verbose"))
				option.verbose = true;

			else if (args[i].equals("--shard-size") && i + 1 < args.length) {
				shard_size = Integer.valueOf(args[++i]);

				if (shard_size <= 0) {
					System.err.println("Out of range (shard-size).");
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

		File xml_dir = new File(xml_dir_name);

		if (!xml_dir.isDirectory()) {

			if (!xml_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + xml_dir_name + "'.");
				System.exit(1);
			}

		}

		try {

			XmlSplitterImpl splitter = new XmlSplitterImpl(shard_size, is, xml_dir, xml_file_queue, option, xpath_doc_key);

			splitter.exec();

		} catch (IOException | NoSuchAlgorithmException | ParserConfigurationException | SAXException | PgSchemaException | xpathListenerException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xmlsplitter: Split large XML into small XML files");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml SRC_XML_FILE_OR_DIRECTORY --xml-dir DST_DIRECTORY (default=\"" + xml_dir_name + "\")");
		System.err.println("        --xml-file-ext SRC_FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix) | zip (indicates xml.zip suffix)]");
		System.err.println("        --xpath-doc-key XPATH_EXPR_FOR_DOC_KEY");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --shard-size SHARD_SIZE (default=1)");
		System.err.println("Option: --pg-public-schema (utilize \"public\" schema, default)");
		System.err.println("        --pg-named-schema (enable explicit named schema)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --verbose");
		System.exit(1);

	}

}
