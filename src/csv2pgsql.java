/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2017 Masashi Yokochi

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
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.xml.parsers.*;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * PostgreSQL data migration from CSV file.
 *
 * @author yokochi
 */
public class csv2pgsql {

	/** The CSV directory name. */
	public static String csv_dir_name = xml2pgcsv.csv_dir_name;

	/** The schema location. */
	public static String schema_location = "";

	/** The schema option. */
	public static PgSchemaOption option = new PgSchemaOption(true);

	/** The PostgreSQL option. */
	public static PgOption pg_option = new PgOption();

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		Connection db_conn = null;

		boolean _document_key = false;
		boolean _no_document_key = false;

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--no-rel"))
				option.cancelRelModelExt();

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--doc-key")) {

				if (_no_document_key) {
					System.err.println("--no-doc-key is set already.");
					showUsage();
				}

				_document_key = true;
			}

			else if (args[i].equals("--no-doc-key")) {

				if (_document_key) {
					System.err.println("--doc-key is set already.");
					showUsage();
				}

				_no_document_key = true;
			}

			else if (args[i].equals("--ser-key"))
				option.serial_key = true;

			else if (args[i].equals("--xpath-key"))
				option.xpath_key = true;

			else if (args[i].equals("--case-insensitive"))
				option.case_sense = false;

			else if (args[i].equals("--no-xsd-cache"))
				option.cache_xsd = false;

			else if (args[i].equals("--xsd"))
				schema_location = args[++i];

			else if (args[i].equals("--csv-dir"))
				csv_dir_name = args[++i];

			else if (args[i].matches("^--db-?host.*"))
				pg_option.host = args[++i];

			else if (args[i].matches("^--db-?port.*"))
				pg_option.port = Integer.valueOf(args[++i]);

			else if (args[i].matches("^--db-?name.*"))
				pg_option.database = args[++i];

			else if (args[i].matches("^--db-?user.*"))
				pg_option.user = args[++i];

			else if (args[i].matches("^--db-?pass.*"))
				pg_option.password = args[++i];

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

		if (_document_key)
			option.document_key = true;

		else if (_no_document_key)
			option.document_key = false;

		if (schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = PgSchemaUtil.getSchemaInputStream(schema_location, null);

		if (is == null)
			showUsage();

		File csv_dir = new File(csv_dir_name);

		if (!csv_dir.isDirectory()) {

			if (!csv_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + csv_dir_name + "'.");
				System.exit(1);
			}

		}

		csv_dir_name = csv_dir_name.replaceFirst("/$", "") + "/";

		if (pg_option.database.isEmpty()) {
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

			PgSchema schema = new PgSchema(doc_builder, xsd_doc, null, schema_location, option);

			db_conn = DriverManager.getConnection(pg_option.getDbUrl(), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.password);

			if (!schema.pgCsv2PgSql(db_conn, csv_dir_name))
				System.exit(1);

			System.out.println("Done csv -> db (" + pg_option.database + ").");

		} catch (ParserConfigurationException | SAXException | IOException | SQLException | NoSuchAlgorithmException | PgSchemaException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("csv2pgsql: CSV -> PostgreSQL data migration");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --csv-dir DIRECTORY (default=\"" + csv_dir_name + "\") --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host HOST (default=\"" + PgSchemaUtil.host + "\")");
		System.err.println("        --db-port PORT (default=\"" + PgSchemaUtil.port + "\")");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --doc-key (append " + option.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + option.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + option.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + option.xpath_key_name + " column in all relations)");
		System.err.println("Option: --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");

		option.setDefaultUserKeys();

		System.err.println("        --doc-key-name DOC_KEY_NAME (default=\"" + option.document_key_name + "\")");
		System.err.println("        --ser-key-name SER_KEY_NAME (default=\"" + option.serial_key_name + "\")");
		System.err.println("        --xpath-key-name XPATH_KEY_NAME (default=\"" + option.xpath_key_name + "\")");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.exit(1);

	}

}
