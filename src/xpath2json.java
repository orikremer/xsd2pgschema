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
 * XPath 1.0 query evaluation to JSON over PostgreSQL.
 *
 * @author yokochi
 */
public class xpath2json {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** Whether to output processing message to stdout or not (stderr). */
		boolean stdout_msg = false;

		/** The JSON directory name. */
		String json_dir_name = "json_result";

		/** The output file name or pattern. */
		String out_file_name = "";

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(true);

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class,PgSchema.class); // FST optimization

		/** The PostgreSQL option. */
		PgOption pg_option = new PgOption();

		/** The JSON builder option. */
		JsonBuilderOption jsonb_option = new JsonBuilderOption();

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

			else if (args[i].equals("--obj-json"))
				jsonb_option.type = JsonType.object;

			else if (args[i].equals("--col-json"))
				jsonb_option.type = JsonType.column;

			else if (args[i].equals("--json-attr-prefix") && i + 1 < args.length)
				jsonb_option.setAttrPrefix(args[++i]);

			else if (args[i].equals("--json-simple-cont-name") && i + 1 < args.length)
				jsonb_option.setSimpleContentName(args[++i]);

			else if (args[i].equals("--json-array-all"))
				jsonb_option.array_all = true;

			else if (args[i].equals("--json-allow-frag"))
				jsonb_option.allow_frag = true;

			else if (args[i].equals("--json-deny-frag"))
				jsonb_option.deny_frag = true;

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

			else if (args[i].equals("--schema-ver") && i + 1 < args.length)
				jsonb_option.setSchemaVer(args[++i]);

			else if (args[i].equals("--out-dir") && i + 1 < args.length)
				json_dir_name = args[++i];

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

			else if (args[i].equals("--case-insensitive")) {
				option.setCaseInsensitive();
				jsonb_option.setCaseInsensitive();
			}

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

			else if (args[i].equals("--pg-map-timestamp"))
				option.pg_date = PgDateType.timestamp;

			else if (args[i].equals("--pg-map-date"))
				option.pg_date = PgDateType.date;

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

		PgSchemaClientType client_type = PgSchemaClientType.xpath_evaluation_to_json;

		InputStream is = null;

		boolean server_alive = option.pingPgSchemaServer(fst_conf);
		boolean no_data_model = server_alive ? !option.matchPgSchemaServer(fst_conf, client_type) : true;

		if (no_data_model) {

			is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

			if (is == null)
				showUsage();

		}

		try {

			XPathEvaluatorImpl evaluator = new XPathEvaluatorImpl(is, option, fst_conf, pg_option, jsonb_option, stdout_msg); // reuse the instance for repetition

			if (!pg_option.name.isEmpty())
				pg_option.clear();

			JsonBuilder jsonb = new JsonBuilder(evaluator.client.schema, jsonb_option);

			for (int id = 0; id < xpath_queries.size(); id++) {

				String xpath_query = xpath_queries.get(id);

				evaluator.translate(xpath_query, variables, stdout_msg);

				if (!pg_option.name.isEmpty())
					evaluator.composeJson(id, xpath_queries.size(), json_dir_name, out_file_name, jsonb);

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

		JsonBuilderOption jsonb_option = new JsonBuilderOption();

		System.err.println("xpath2json: XPath 1.0 query evaluation to JSON over PostgreSQL");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host PG_HOST_NAME (default=\"" + PgSchemaUtil.pg_host + "\")");
		System.err.println("        --db-port PG_PORT_NUMBER (default=" + PgSchemaUtil.pg_port + ")");
		System.err.println("        --test-ddl (perform consistency test on PostgreSQL DDL)");
		System.err.println("        --xpath-query XPATH_QUERY (repeatable)");
		System.err.println("        --xpath-var KEY=VALUE (repeat until you specify all variables)");
		System.err.println("        --out OUTPUT_FILE_OR_PATTERN (default=stdout)");
		System.err.println("        --out-dir OUTPUT_DIRECTORY");
		System.err.println("        --schema-ver JSON_SCHEMA_VER (choose from \"2019_09\" (default), \"draft_v8\", \"draft_v7\", \"draft_v6\", \"draft_v4\", or \"latest\" as \"" + JsonSchemaVersion.defaultVersion().toString().replaceAll("draft_", "") + "\")");
		System.err.println("        --obj-json (use object-oriented JSON format)");
		System.err.println("        --col-json (use column-oriented JSON format, default)");
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
		System.err.println("        --pg-map-timestamp (map xs:date to PostgreSQL timestamp type according to the W3C rules)");
		System.err.println("        --pg-map-date (map xs:date to PostgreSQL date type, default)");	
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
		System.err.println("        --json-attr-prefix ATTR_PREFIX_CODE (default=\"" + jsonb_option.getAttrPrefix() + "\")");
		System.err.println("        --json-simple-cont-name SIMPLE_CONTENT_NAME (default=\"" + jsonb_option.getSimpleContentName() + "\")");
		System.err.println("        --json-array-all (use JSON array if possible)");
		System.err.println("        --json-allow-frag (allow fragmented JSON document, do not throw fragmentation warning)");
		System.err.println("        --json-deny-frag (deny fragmented JSON document, output the first root node)");
		System.err.println("        --json-indent-offset INTEGER (default=" + jsonb_option.getIndentOffset() + ", min=0, max=4)");
		System.err.println("        --json-key-value-offset INTEGER (default=" + jsonb_option.getKeyValueOffset() + ", min=0, max=4)");
		System.err.println("        --json-insert-doc-key (insert document key in result)");
		System.err.println("        --json-no-linefeed (dismiss line feed code)");
		System.err.println("        --json-compact (equals to set --json-indent-offset 0 --json-key-value-offset 0 --json-no-linefeed)");
		System.err.println("        --verbose (verbose mode)");
		System.exit(1);

	}

}
