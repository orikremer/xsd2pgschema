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
import net.sf.xsd2pgschema.option.*;
import net.sf.xsd2pgschema.serverutil.*;
import net.sf.xsd2pgschema.type.*;
import net.sf.xsd2pgschema.xpathparser.XPathCompList;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;

import javax.xml.parsers.ParserConfigurationException;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.nustaq.serialization.FSTConfiguration;
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

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(true);

		option.verbose = true; // turn on verbose mode

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class,PgSchema.class); // FST optimization

		/** The XPath query. */
		String xpath_query = "";

		/** The XPath variable reference. */
		HashMap<String, String> variables = new HashMap<String, String>();

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if (args[i].equals("--xpath-query") && i + 1 < args.length)
				xpath_query = args[++i];

			else if (args[i].equals("--xpath-var") && i + 1 < args.length) {
				String[] variable = args[++i].split("=");
				if (variable.length != 2) {
					System.err.println("Invalid variable definition.");
					showUsage();
				}
				variables.put(variable[0], variable[1]);
			}

			else if (args[i].equals("--doc-key"))
				option.setDocKeyOption(true);

			else if (args[i].equals("--no-doc-key"))
				option.setDocKeyOption(false);

			else if (args[i].equals("--no-rel"))
				option.cancelRelDataExt();

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--ser-key"))
				option.serial_key = true;

			else if (args[i].equals("--xpath-key"))
				option.xpath_key = true;

			else if (args[i].equals("--case-insensitive"))
				option.setCaseInsensitive();

			else if (args[i].equals("--pg-public-schema"))
				option.pg_named_schema = false;

			else if (args[i].equals("--pg-named-schema"))
				option.pg_named_schema = true;

			else if (args[i].equals("--pg-map-big-integer"))
				option.pg_integer = PgIntegerType.big_integer;

			else if (args[i].equals("--pg-map-long-integer"))
				option.pg_integer = PgIntegerType.signed_long_64;

			else if (args[i].equals("--pg-map-integer"))
				option.pg_integer = PgIntegerType.signed_int_32;

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--hash-by") && i + 1 < args.length)
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size") && i + 1 < args.length)
				option.hash_size = PgHashSize.getSize(args[++i]);

			else if (args[i].equals("--ser-size") && i + 1 < args.length)
				option.ser_size = PgSerSize.getSize(args[++i]);

			else if (args[i].equals("--doc-key-name") && i + 1 < args.length)
				option.setDocumentKeyName(args[++i]);

			else if (args[i].equals("--ser-key-name") && i + 1 < args.length)
				option.setSerialKeyName(args[++i]);

			else if (args[i].equals("--xpath-key-name") && i + 1 < args.length)
				option.setXPathKeyName(args[++i]);

			else if (args[i].equals("--discarded-doc-key-name") && i + 1 < args.length)
				option.addDiscardedDocKeyName(args[++i]);

			else if (args[i].equals("--inplace-doc-key-name") && i + 1 < args.length) {
				option.addInPlaceDocKeyName(args[++i]);
				option.setDocKeyOption(false);
			}

			else if (args[i].equals("--doc-key-if-no-inplace")) {
				option.document_key_if_no_in_place = true;
				option.setDocKeyOption(false);
			}

			else if (args[i].equals("--no-pgschema-serv"))
				option.pg_schema_server = false;

			else if (args[i].equals("--pgschema-serv-host") && i + 1 < args.length)
				option.pg_schema_server_host = args[++i];

			else if (args[i].equals("--pgschema-serv-port") && i + 1 < args.length)
				option.pg_schema_server_port = Integer.valueOf(args[++i]);

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		option.resolveDocKeyOption();

		if (option.root_schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = null;

		boolean server_alive = option.pingPgSchemaServer(fst_conf);
		boolean no_data_model = server_alive ? !option.matchPgSchemaServer(fst_conf) : true;

		if (no_data_model) {

			is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

			if (is == null)
				showUsage();

		}

		try {

			PgSchemaClientImpl client = new PgSchemaClientImpl(is, option, fst_conf, MethodHandles.lookup().lookupClass().getName());

			client.schema.prepDicForXPath();

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

			XPathCompList xpath_comp_list = new XPathCompList(client.schema, tree, variables);

			if (xpath_comp_list.comps.size() == 0)
				throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

			xpath_comp_list.validate(false);

			if (xpath_comp_list.path_exprs.size() == 0)
				throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

			StringBuilder sb = new StringBuilder();

			sb.append("Input XPath query:\n " + main_text + "\n\nTarget path in XML Schema: " + option.root_schema_location + "\n");

			xpath_comp_list.showPathExprs(sb);

			sb.append("\nThe XPath query is valid.\n");

			System.out.print(sb.toString());

			sb.setLength(0);

		} catch (IOException | ParserConfigurationException | SAXException | PgSchemaException | xpathListenerException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		PgSchemaOption option = new PgSchemaOption(true);

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
		System.err.println("        --pg-public-schema (utilize \"public\" schema, default)");
		System.err.println("        --pg-named-schema (enable explicit named schema)");
		System.err.println("        --pg-map-big-integer (map xs:integer to BigInteger according to the W3C rules)");
		System.err.println("        --pg-map-long-integer (map xs:integer to signed long 64 bits)");
		System.err.println("        --pg-map-integer (map xs:integer to signed int 32 bits, default)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16bit); | int (32bit, default)]");
		System.err.println("        --doc-key-name DOC_KEY_NAME (default=\"" + option.def_document_key_name + "\")");
		System.err.println("        --ser-key-name SER_KEY_NAME (default=\"" + option.def_serial_key_name + "\")");
		System.err.println("        --xpath-key-name XPATH_KEY_NAME (default=\"" + option.def_xpath_key_name + "\")");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.err.println("        --inplace-doc-key-name INPLACE_DOCUMENT_KEY_NAME");
		System.err.println("        --doc-key-if-no-inplace (append document key if no in-place docuemnt key, select --no-doc-key options by default)");
		System.err.println("        --no-pgschema-serv (not utilize PgSchema server)");
		System.err.println("        --pgschema-serv-host PG_SCHEMA_SERV_HOST_NAME (default=\"" + PgSchemaUtil.pg_schema_server_host + "\")");
		System.err.println("        --pgschema-serv-port PG_SCHEMA_SERV_PORT_NUMBER (default=\"" + PgSchemaUtil.pg_schema_server_port + "\")");
		System.exit(1);

	}

}
