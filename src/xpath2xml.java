/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018-2019 Masashi Yokochi

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
import net.sf.xsd2pgschema.docbuilder.*;
import net.sf.xsd2pgschema.implement.XPathEvaluatorImpl;
import net.sf.xsd2pgschema.option.*;
import net.sf.xsd2pgschema.serverutil.*;
import net.sf.xsd2pgschema.type.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.ParserConfigurationException;

import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathListenerException;

/**
 * XPath 1.0 query evaluation to XML over PostgreSQL.
 *
 * @author yokochi
 */
public class xpath2xml {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** Whether to output processing message to stdout or not (stderr). */
		boolean stdout_msg = false;

		/** The XML directory name. */
		String xml_dir_name = "xml_result";

		/** The output file name or pattern. */
		String out_file_name = "";

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(true);

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class,PgSchema.class); // FST optimization

		/** The PostgreSQL option. */
		PgOption pg_option = new PgOption();

		/** The XML builder. */
		XmlBuilder xmlb = new XmlBuilder();

		/** The XPath queries. */
		ArrayList<String> xpath_queries = new ArrayList<String>();

		/** The XPath variable reference. */
		HashMap<String, String> variables = new HashMap<String, String>();

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if (args[i].equals("--out") && i + 1 < args.length)
				out_file_name = args[++i];

			else if (args[i].equals("--xpath-query") && i + 1 < args.length)
				xpath_queries.add(args[++i]);

			else if (args[i].equals("--xpath-var") && i + 1 < args.length) {
				String[] variable = args[++i].split("=");
				if (variable.length != 2) {
					System.err.println("Invalid variable definition.");
					showUsage();
				}
				variables.put(variable[0], variable[1]);
			}

			else if (args[i].equals("--db-host") && i + 1 < args.length)
				pg_option.pg_host = args[++i];

			else if (args[i].equals("--db-port") && i + 1 < args.length)
				pg_option.pg_port = Integer.valueOf(args[++i]);

			else if (args[i].equals("--db-name") && i + 1 < args.length)
				pg_option.name = args[++i];

			else if (args[i].equals("--db-user") && i + 1 < args.length)
				pg_option.user = args[++i];

			else if (args[i].equals("--db-pass") && i + 1 < args.length)
				pg_option.pass = args[++i];

			else if (args[i].equals("--test-ddl"))
				pg_option.test = true;

			else if (args[i].equals("--fill-default-value"))
				option.fill_default_value = true;

			else if (args[i].equals("--xml-no-declare"))
				xmlb.append_declare = false;

			else if (args[i].equals("--xml-no-xmlns"))
				xmlb.append_xmlns = xmlb.append_nil_elem = false;

			else if (args[i].equals("--xml-no-nil-elem"))
				xmlb.append_nil_elem = false;

			else if (args[i].equals("--xml-allow-frag"))
				xmlb.allow_frag = true;

			else if (args[i].equals("--xml-indent-offset") && i + 1 < args.length)
				xmlb.setIndentOffset(args[++i]);

			else if (args[i].equals("--xml-insert-doc-key"))
				xmlb.setInsertDocKey(true);

			else if (args[i].equals("--xml-no-linefeed"))
				xmlb.setLineFeed(false);

			else if (args[i].equals("--xml-compact"))
				xmlb.setCompact();

			else if (args[i].equals("--out-dir") && i + 1 < args.length)
				xml_dir_name = args[++i];

			else if (args[i].equals("--doc-key"))
				option.setDocKeyOption(true);

			else if (args[i].equals("--no-doc-key"))
				option.setDocKeyOption(false);

			else if (args[i].equals("--no-rel"))
				option.cancelRelDataExt();

			else if (args[i].equals("--inline-simple-cont"))
				option.inline_simple_cont = true;

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

			else if (args[i].equals("--pg-map-big-decimal"))
				option.pg_decimal = PgDecimalType.big_decimal;

			else if (args[i].equals("--pg-map-double-decimal"))
				option.pg_decimal = PgDecimalType.double_precision_64;

			else if (args[i].equals("--pg-map-float-decimal"))
				option.pg_decimal = PgDecimalType.single_precision_32;

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

			else if (args[i].equals("--verbose"))
				option.verbose = true;

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

		if (!out_file_name.isEmpty() && !out_file_name.equals("stdout")) {

			stdout_msg = true;

			Path xml_dir_path = Paths.get(xml_dir_name);

			if (!Files.isDirectory(xml_dir_path)) {

				try {
					Files.createDirectory(xml_dir_path);
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

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

			XPathEvaluatorImpl evaluator = new XPathEvaluatorImpl(is, option, fst_conf, pg_option, stdout_msg); // reuse the instance for repetition

			if (!pg_option.name.isEmpty())
				pg_option.clear();

			xmlb.init(evaluator.client.schema);

			for (int id = 0; id < xpath_queries.size(); id++) {

				String xpath_query = xpath_queries.get(id);

				evaluator.translate(xpath_query, variables, stdout_msg);

				if (!pg_option.name.isEmpty())
					evaluator.composeXml(id, xpath_queries.size(), xml_dir_name, out_file_name, xmlb);

			}

			evaluator.client.schema.closePreparedStatement(true);

		} catch (IOException | NoSuchAlgorithmException | ParserConfigurationException | SAXException | PgSchemaException | xpathListenerException | SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		PgSchemaOption option = new PgSchemaOption(true);

		XmlBuilder xmlb = new XmlBuilder();

		System.err.println("xpath2xml: XPath 1.0 query evaluation to XML over PostgreSQL");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host PG_HOST_NAME (default=\"" + PgSchemaUtil.pg_host + "\")");
		System.err.println("        --db-port PG_PORT_NUMBER (default=" + PgSchemaUtil.pg_port + ")");
		System.err.println("        --test-ddl (perform consistency test on PostgreSQL DDL)");
		System.err.println("        --xpath-query XPATH_QUERY (repeatable)");
		System.err.println("        --xpath-var KEY=VALUE (repeat until you specify all variables)");
		System.err.println("        --out OUTPUT_FILE_OR_PATTERN (default=stdout)");
		System.err.println("        --out-dir OUTPUT_DIRECTORY");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --inline-simple-cont (enable inlining simple content)");
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
		System.err.println("        --pg-map-big-decimal (map xs:decimal to BigDecimal according to the W3C rules, default)");
		System.err.println("        --pg-map-double-decimal (map xs:decimal to double precision 64 bits)");
		System.err.println("        --pg-map-float-decimal (map xs:decimal to single precision 32 bits)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32 bits) | long (64 bits, default) | native (default bits of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16 bits); | int (32 bits, default)]");
		System.err.println("        --doc-key-name DOC_KEY_NAME (default=\"" + option.def_document_key_name + "\")");
		System.err.println("        --ser-key-name SER_KEY_NAME (default=\"" + option.def_serial_key_name + "\")");
		System.err.println("        --xpath-key-name XPATH_KEY_NAME (default=\"" + option.def_xpath_key_name + "\")");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.err.println("        --inplace-doc-key-name INPLACE_DOCUMENT_KEY_NAME");
		System.err.println("        --doc-key-if-no-inplace (append document key if no in-place document key, select --no-doc-key options by default)");
		System.err.println("        --no-pgschema-serv (not utilize PgSchema server)");
		System.err.println("        --pgschema-serv-host PG_SCHEMA_SERV_HOST_NAME (default=\"" + PgSchemaUtil.pg_schema_server_host + "\")");
		System.err.println("        --pgschema-serv-port PG_SCHEMA_SERV_PORT_NUMBER (default=" + PgSchemaUtil.pg_schema_server_port + ")");
		System.err.println("        --xml-no-declare (dismiss XML declaration)");
		System.err.println("        --xml-no-xmlns (dismiss XML namespace declaration)");
		System.err.println("        --xml-no-nil-elem (dismiss nillable element)");
		System.err.println("        --xml-allow-frag (allow fragmented XML document)");
		System.err.println("        --xml-indent-offset INTEGER (default=" + xmlb.getIndentOffset() + ", min=0, max=4)");
		System.err.println("        --xml-insert-doc-key (insert document key in result)");
		System.err.println("        --xml-no-linefeed (dismiss line feed code)");
		System.err.println("        --xml-compact (equals to set --xml-indent-offset 0 --xml-no-linefeed)");
		System.err.println("        --verbose (verbose mode)");
		System.exit(1);

	}

}
