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
import java.sql.SQLException;
import java.util.HashMap;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathListenerException;

/**
 * Query translator from XPath to SQL.
 *
 * @author yokochi
 */
public class xpath2pgsql {

	/** The schema location. */
	public static String schema_location = "";

	/** The schema option. */
	public static PgSchemaOption option = new PgSchemaOption(true);

	/** The PostgreSQL option. */
	public static PgOption pg_option = new PgOption();

	/** The XPath query. */
	public static String xpath_query = "";

	/** The XPath variable reference. */
	public static HashMap<String, String> variables = new HashMap<String, String>();

	/** The verbose mode. */
	public static boolean verbose = false;

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		boolean _document_key = false;
		boolean _no_document_key = false;

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--no-rel"))
				option.cancelRelDataExt();

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

			else if (args[i].equals("--xsd"))
				schema_location = args[++i];

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

			else if (args[i].equals("--hash-by"))
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size"))
				option.hash_size = PgHashSize.getPgHashSize(args[++i]);

			else if (args[i].equals("--ser-size"))
				option.ser_size = PgSerSize.getPgSerSize(args[++i]);

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

			else if (args[i].equals("--verbose"))
				verbose = true;

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

		InputStream is = PgSchemaUtil.getInputStream(schema_location, null);

		if (is == null)
			showUsage();

		try {

			XPath2PgSqlImpl xpath2pgsql = new XPath2PgSqlImpl(is, option, pg_option);

			xpath2pgsql.translate(xpath_query, variables);

			if (!pg_option.database.isEmpty())
				xpath2pgsql.execute();

		} catch (IOException | NoSuchAlgorithmException | ParserConfigurationException | SAXException | PgSchemaException | xpathListenerException | SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xpath2pgsql: Qeury translator from XPath to SQL");
		System.err.println("Usage:  --xsd SCHEMA_LOCAITON --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host HOST (default=\"" + PgSchemaUtil.host + "\")");
		System.err.println("        --db-port PORT (default=\"" + PgSchemaUtil.port + "\")");
		System.err.println("        --xpath-query XPATH_QUERY");
		System.err.println("        --xpath-var KEY=VALUE");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --doc-key (append " + PgSchemaUtil.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + PgSchemaUtil.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + PgSchemaUtil.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + PgSchemaUtil.xpath_key_name + " column in all relations)");
		System.err.println("        --case-insensitive (all table and column names are lowercase)");
		System.err.println("Option: --hash-by ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16bit); | int (32bit, default)]");
		System.err.println("        --verbose");
		System.exit(1);

	}

}