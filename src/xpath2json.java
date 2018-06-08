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

import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathListenerException;

/**
 * XPath 1.0 query evaluator to compose JSON over PostgreSQL.
 *
 * @author yokochi
 */
public class xpath2json {

	/** The JSON directory name. */
	private static String json_dir_name = "json_result";

	/** The output file name or pattern. */
	protected static String out_file_name = "";

	/** The schema option. */
	private static PgSchemaOption option = new PgSchemaOption(true);

	/** The PostgreSQL option. */
	private static PgOption pg_option = new PgOption();

	/** The JSON builder option. */
	private static JsonBuilderOption jsonb_option = new JsonBuilderOption();

	/** The XPath queries. */
	private static ArrayList<String> xpath_queries = new ArrayList<String>();

	/** The XPath variable reference. */
	private static HashMap<String, String> variables = new HashMap<String, String>();

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

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
				pg_option.host = args[++i];

			else if (args[i].equals("--db-port") && i + 1 < args.length)
				pg_option.port = Integer.valueOf(args[++i]);

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

			else if (args[i].equals("--obj-json"))
				jsonb_option.type = JsonType.object;

			else if (args[i].equals("--col-json"))
				jsonb_option.type = JsonType.column;

			else if (args[i].equals("--json-array-all"))
				jsonb_option.array_all = true;

			else if (args[i].equals("--json-attr-prefix") && i + 1 < args.length)
				jsonb_option.setAttrPrefix(args[++i]);

			else if (args[i].equals("--json-simple-cont-name") && i + 1 < args.length)
				jsonb_option.setSimpleContentName(args[++i]);

			else if (args[i].equals("--json-indent-offset") && i + 1 < args.length)
				jsonb_option.setIndentOffset(args[++i]);

			else if (args[i].equals("--json-key-value-offset") && i + 1 < args.length)
				jsonb_option.setKeyValueOffset(args[++i]);

			else if (args[i].equals("--json-insert-doc-key"))
				jsonb_option.insert_doc_key = true;

			else if (args[i].equals("--json-no-linefeed"))
				jsonb_option.setLineFeed(false);

			else if (args[i].equals("--json-compact"))
				jsonb_option.setCompact();

			else if (args[i].equals("--out-dir") && i + 1 < args.length)
				json_dir_name = args[++i];

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

			else if (args[i].equals("--case-insensitive")) {
				option.setCaseInsensitive();
				jsonb_option.setCaseInsensitive();
			}

			else if (args[i].equals("--pg-public-schema"))
				option.pg_named_schema = false;

			else if (args[i].equals("--pg-named-schema"))
				option.pg_named_schema = true;

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--hash-by") && i + 1 < args.length)
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size") && i + 1 < args.length)
				option.hash_size = PgHashSize.getPgHashSize(args[++i]);

			else if (args[i].equals("--ser-size") && i + 1 < args.length)
				option.ser_size = PgSerSize.getPgSerSize(args[++i]);

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
				option.cancelRelDataExt();
				option.setDocKeyOption(false);
			}

			else if (args[i].equals("--doc-key-if-no-inplace")) {
				option.document_key_if_no_in_place = true;
				option.cancelRelDataExt();
				option.setDocKeyOption(false);
			}

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

			Path json_dir_path = Paths.get(json_dir_name);

			if (!Files.isDirectory(json_dir_path)) {

				try {
					Files.createDirectory(json_dir_path);
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

		}

		InputStream is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

		if (is == null)
			showUsage();

		try {

			XPathEvaluatorImpl evaluator = new XPathEvaluatorImpl(is, option, pg_option); // reuse the instance for repetition

			if (!pg_option.name.isEmpty())
				pg_option.clear();

			for (int id = 0; id < xpath_queries.size(); id++) {

				String xpath_query = xpath_queries.get(id);

				evaluator.translate(xpath_query, variables);

				if (!pg_option.name.isEmpty())
					evaluator.composeJson(id, xpath_queries.size(), json_dir_name, out_file_name, jsonb_option);

			}

		} catch (IOException | NoSuchAlgorithmException | ParserConfigurationException | SAXException | PgSchemaException | xpathListenerException | SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		JsonBuilderOption jsonb_option = new JsonBuilderOption();

		System.err.println("xpath2json: XPath 1.0 qeury evaluator to compose JSON over PostgreSQL");
		System.err.println("Usage:  --xsd SCHEMA_LOCAITON --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host HOST (default=\"" + PgSchemaUtil.host + "\")");
		System.err.println("        --db-port PORT (default=\"" + PgSchemaUtil.port + "\")");
		System.err.println("        --test-ddl (perform consistency test on PostgreSQL DDL)");
		System.err.println("        --xpath-query XPATH_QUERY (repeatable)");
		System.err.println("        --xpath-var KEY=VALUE");
		System.err.println("        --out OUTPUT_FILE_OR_PATTERN (default=stdout)");
		System.err.println("        --out-dir OUTPUT_DIRECTORY");
		System.err.println("        --obj-json (use object-oriented JSON format)");
		System.err.println("        --col-json (use column-oriented JSON format, default)");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --doc-key (append " + option.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + option.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + option.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + option.xpath_key_name + " column in all relations)");
		System.err.println("Option: --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --pg-public-schema (utilize \"public\" schema, default)");
		System.err.println("        --pg-named-schema (enable explicit named schema)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16bit); | int (32bit, default)]");
		System.err.println("        --doc-key-name DOC_KEY_NAME (default=\"" + option.def_document_key_name + "\")");
		System.err.println("        --ser-key-name SER_KEY_NAME (default=\"" + option.def_serial_key_name + "\")");
		System.err.println("        --xpath-key-name XPATH_KEY_NAME (default=\"" + option.def_xpath_key_name + "\")");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.err.println("        --inplace-doc-key-name INPLACE_DOCUMENT_KEY_NAME (select --no-rel and --no-doc-key options by default)");
		System.err.println("        --doc-key-if-no-inplace (select --no-rel and --no-doc-key options by default)");
		System.err.println("        --json-attr-prefix ATTR_PREFIX_CODE (default=\"" + jsonb_option.getAttrPrefix() + "\")");
		System.err.println("        --json-simple-cont-name SIMPLE_CONTENT_NAME (default=\"" + jsonb_option.getSimpleContentName() + "\")");
		System.err.println("        --json-indent-offset INTEGER (default=" + jsonb_option.getIndentOffset() + ", min=0, max=4)");
		System.err.println("        --json-key-value-offset INTEGER (default=" + jsonb_option.getKeyValueOffset() + ", min=0, max=4)");
		System.err.println("        --json-insert-doc-key (insert document key in result)");
		System.err.println("        --json-no-linefeed (dismiss line feed code)");
		System.err.println("        --json-compact (equals to set --json-indent-offset 0 --json-key-value-offset 0 --json-no-linefeed)");
		System.err.println("        --json-array-all (use JSON array if possible)");
		System.err.println("        --verbose");
		System.exit(1);

	}

}
