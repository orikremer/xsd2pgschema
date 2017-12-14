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

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathBaseListener;
import com.github.antlr.grammars_v4.xpath.xpathLexer;
import com.github.antlr.grammars_v4.xpath.xpathListenerException;
import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.MainContext;

/**
 * XPath 1.0 parser with XML Schema validation.
 *
 * @author yokochi
 */
public class xpathparser {

	/** The schema location. */
	public static String schema_location = "";

	/** The schema option. */
	public static PgSchemaOption option = new PgSchemaOption(true);

	/** The XPath query. */
	public static String xpath_query = "";

	/** The XPath variable reference. */
	public static HashMap<String, String> variables = new HashMap<String, String>();

	/** The verbose mode. */
	public static boolean verbose = true;

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd"))
				schema_location = args[++i];

			else if (args[i].equals("--xpath-query"))
				xpath_query = args[++i];

			else if (args[i].equals("--xpath-var")) {
				String[] variable = args[++i].split("=");
				if (variable.length != 2) {
					System.err.println("Invalid variable definition.");
					showUsage();
				}
				variables.put(variable[0], variable[1]);
			}

			else if (args[i].equals("--doc-key")) {
				if (!option.setDocKeyOpt())
					showUsage();
			}

			else if (args[i].equals("--no-doc-key")) {
				if (!option.setNoDocKeyOpt())
					showUsage();
			}

			else if (args[i].equals("--no-rel"))
				option.cancelRelDataExt();

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--ser-key"))
				option.serial_key = true;

			else if (args[i].equals("--xpath-key"))
				option.xpath_key = true;

			else if (args[i].equals("--case-insensitive"))
				option.case_sense = false;

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--hash-by"))
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size"))
				option.hash_size = PgHashSize.getPgHashSize(args[++i]);

			else if (args[i].equals("--ser-size"))
				option.ser_size = PgSerSize.getPgSerSize(args[++i]);

			else if (args[i].equals("--doc-key-name"))
				option.setDocumentKeyName(args[++i]);

			else if (args[i].equals("--ser-key-name"))
				option.setSerialKeyName(args[++i]);

			else if (args[i].equals("--xpath-key-name"))
				option.setXPathKeyName(args[++i]);

			else if (args[i].equals("--discarded-doc-key-name"))
				option.addDiscardedDocKeyName(args[++i]);

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		option.setDocumentKey();

		if (schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = PgSchemaUtil.getSchemaInputStream(schema_location, null);

		if (is == null)
			showUsage();

		try {

			// parse XSD document

			DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
			doc_builder_fac.setValidating(false);
			doc_builder_fac.setNamespaceAware(true);
			doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			DocumentBuilder doc_builder = doc_builder_fac.newDocumentBuilder();

			Document xsd_doc = doc_builder.parse(is);

			is.close();

			doc_builder.reset();

			// XSD analysis

			PgSchema schema = new PgSchema(doc_builder, xsd_doc, null, schema_location, option);

			xpathLexer lexer = new xpathLexer(CharStreams.fromString(xpath_query));

			CommonTokenStream tokens = new CommonTokenStream(lexer);

			xpathParser parser = new xpathParser(tokens);
			parser.addParseListener(new xpathBaseListener());

			// validate XPath expression with schema

			MainContext main = parser.main();

			ParseTree tree = main.children.get(0);
			String main_text = main.getText();

			if (parser.getNumberOfSyntaxErrors() > 0 || tree.getSourceInterval().length() == 0)
				throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

			XPathCompList xpath_comp_list = new XPathCompList(schema, tree, variables, verbose);

			if (xpath_comp_list.comps.size() == 0)
				throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

			xpath_comp_list.validate(false, verbose);

			if (xpath_comp_list.path_exprs.size() == 0)
				throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

			System.out.println("Input XPath query:");
			System.out.println(" " + main_text);

			System.out.println("\nTarget path in XML Schema: " + PgSchemaUtil.getSchemaName(schema_location));
			xpath_comp_list.showPathExprs();

			System.out.println("\nThe XPath query is valid.");

		} catch (IOException | NoSuchAlgorithmException | ParserConfigurationException | SAXException | PgSchemaException | xpathListenerException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xpathparser: XPath 1.0 parser with XML Schema validation");
		System.err.println("Usage:  --xsd SCHEMA_LOCAITON");
		System.err.println("        --xpath-query XPATH_QUERY");
		System.err.println("        --xpath-var KEY=VALUE");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --doc-key (append " + option.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + option.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + option.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + option.xpath_key_name + " column in all relations)");
		System.err.println("Option: --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16bit); | int (32bit, default)]");

		option.setDefaultUserKeys();

		System.err.println("        --doc-key-name DOC_KEY_NAME (default=\"" + option.document_key_name + "\")");
		System.err.println("        --ser-key-name SER_KEY_NAME (default=\"" + option.serial_key_name + "\")");
		System.err.println("        --xpath-key-name XPATH_KEY_NAME (default=\"" + option.xpath_key_name + "\")");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.exit(1);

	}

}
