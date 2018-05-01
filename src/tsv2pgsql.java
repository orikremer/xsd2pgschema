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
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.xml.parsers.*;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * PostgreSQL data migration from TSV file.
 *
 * @author yokochi
 */
public class tsv2pgsql {

	/** The working directory name. */
	private static String work_dir_name = xml2pgtsv.work_dir_name;

	/** The schema option. */
	private static PgSchemaOption option = new PgSchemaOption(true);

	/** The PostgreSQL option. */
	private static PgOption pg_option = new PgOption();

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		Connection db_conn = null;

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if ((args[i].equals("--csv-dir") || args[i].equals("--tsv-dir") || args[i].equals("--work-dir")) && i + 1 < args.length)
				work_dir_name = args[++i];

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
				option.case_sense = false;

			else if (args[i].equals("--pg-public-schema"))
				option.pg_named_schema = false;

			else if (args[i].equals("--pg-named-schema"))
				option.pg_named_schema = true;

			else if (args[i].equals("--pg-comma-delimiter"))
				option.pg_tab_delimiter = false;

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

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

		InputStream is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

		if (is == null)
			showUsage();

		File work_dir = new File(work_dir_name);

		if (!work_dir.isDirectory()) {

			if (!work_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + work_dir_name + "'.");
				System.exit(1);
			}

		}

		if (pg_option.name.isEmpty()) {
			System.err.println("Database name is empty.");
			showUsage();
		}

		try {

			// parse XSD document

			DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
			doc_builder_fac.setNamespaceAware(true);
			DocumentBuilder	doc_builder = doc_builder_fac.newDocumentBuilder();

			Document xsd_doc = doc_builder.parse(is);

			is.close();

			doc_builder.reset();

			// XSD analysis

			PgSchema schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, option);

			db_conn = DriverManager.getConnection(pg_option.getDbUrl(), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.pass);

			// test PostgreSQL DDL with schema

			if (pg_option.test)
				schema.testPgSql(db_conn, true);

			schema.pgCsv2PgSql(db_conn, work_dir);

			System.out.println("Done " + (option.pg_tab_delimiter ? "tsv" : "csv") + " -> db (" + pg_option.name + ").");

		} catch (ParserConfigurationException | SAXException | IOException | SQLException | PgSchemaException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("tsv2pgsql: TSV -> PostgreSQL data migration");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --work-dir DIRECTORY (default=\"" + work_dir_name + "\") --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host HOST (default=\"" + PgSchemaUtil.host + "\")");
		System.err.println("        --db-port PORT (default=\"" + PgSchemaUtil.port + "\")");
		System.err.println("        --test-ddl (perform consistency test on PostgreSQL DDL)");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --doc-key (append " + option.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + option.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + option.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + option.xpath_key_name + " column in all relations)");
		System.err.println("Option: --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --pg-public-schema (utilize \"public\" schema, default)");
		System.err.println("        --pg-named-schema (enable explicit named schema)");
		System.err.println("        --pg-comma-delimiter (use comma separated file)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --doc-key-name DOC_KEY_NAME (default=\"" + option.def_document_key_name + "\")");
		System.err.println("        --ser-key-name SER_KEY_NAME (default=\"" + option.def_serial_key_name + "\")");
		System.err.println("        --xpath-key-name XPATH_KEY_NAME (default=\"" + option.def_xpath_key_name + "\")");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.err.println("        --inplace-doc-key-name INPLACE_DOCUMENT_KEY_NAME (select --no-rel and --no-doc-key options by default)");
		System.err.println("        --doc-key-if-no-inplace (select --no-rel and --no-doc-key options by default)");
		System.exit(1);

	}

}
