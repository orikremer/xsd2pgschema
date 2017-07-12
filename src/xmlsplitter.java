/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017 Masashi Yokochi

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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;

import javax.xml.parsers.ParserConfigurationException;

import org.antlr.v4.runtime.*;
import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathBaseListener;
import com.github.antlr.grammars_v4.xpath.xpathLexer;
import com.github.antlr.grammars_v4.xpath.xpathListenerException;
import com.github.antlr.grammars_v4.xpath.xpathParser;

/**
 * Split large XML file into small XML files.
 *
 * @author yokochi
 */
public class xmlsplitter {

	/** The schema location. */
	public static String schema_location = "";

	/** The XML directory name for split XML files. */
	public static String xml_dir_name = "xml_work";

	/** The schema option. */
	public static PgSchemaOption option = new PgSchemaOption(false);

	/** The XML file filter. */
	public static XmlFileFilter xml_file_filter = new XmlFileFilter();

	/** The source XML files. */
	public static File[] xml_files = null;

	/**  The XPath pointing document key. */
	public static String xpath_doc_key = "//entry/accession";

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

			else if (args[i].equals("--xml-dir"))
				xml_dir_name = args[++i];

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--case-insensitive"))
				option.case_sense = false;

			else if (args[i].equals("--xpath-doc-key"))
				xpath_doc_key = args[++i];

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

		File xml_dir = new File(xml_dir_name);

		if (!xml_dir.isDirectory()) {

			if (!xml_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + xml_dir_name + "'.");
				System.exit(1);
			}

		}

		xml_dir_name = xml_dir_name.replaceFirst("/$", "") + "/";

		try {

			InputStream input = new ByteArrayInputStream(xpath_doc_key.getBytes(StandardCharsets.UTF_8));

			@SuppressWarnings("deprecation")
			xpathLexer lexer = new xpathLexer(new ANTLRInputStream(input));

			CommonTokenStream tokens = new CommonTokenStream(lexer);

			xpathParser parser = new xpathParser(tokens);
			parser.addParseListener(new xpathBaseListener());

			XmlSplitter splitter = new XmlSplitter(is, option, parser);

			splitter.exec();

		} catch (NoSuchAlgorithmException | ParserConfigurationException | SAXException | IOException | PgSchemaException | xpathListenerException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xmlsplitter: Split large XML into small XML files");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY --xml-dir DIRECTORY (default=\"" + xml_dir_name + "\")");
		System.err.println("        --xml-file-ext FILE_EXTENSION [xml (default) | gz (indicates xml.gz suffix)]");
		System.err.println("        --xpath-doc-key XPATH_AS_DOC_KEY (default=\"" + xpath_doc_key + "\")");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --case-insensitive (all table and column names are lowercase)");
		System.exit(1);

	}

}
