/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2018 Masashi Yokochi

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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.xml.parsers.*;

import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

/**
 * PostgreSQL data migration from CSV file.
 *
 * @author yokochi
 */
public class csv2pgsql {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The working directory name. */
		String work_dir_name = xml2pgcsv.work_dir_name;

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(true);

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class,PgSchema.class); // FST optimization

		/** The PostgreSQL option. */
		PgOption pg_option = new PgOption();

		option.usePgCsv(); // CSV format

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if ((args[i].equals("--csv-dir") || args[i].equals("--tsv-dir") || args[i].equals("--work-dir")) && i + 1 < args.length)
				work_dir_name = args[++i];

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

			else if (args[i].equals("--create-doc-key-index"))
				pg_option.setCreateDocKeyIndex(true);

			else if (args[i].equals("--no-create-doc-key-index"))
				pg_option.setCreateDocKeyIndex(false);

			else if (args[i].equals("--drop-doc-key-index"))
				pg_option.setDropDocKeyIndex();

			else if (args[i].equals("--min-rows-for-doc-key-index") && i + 1 < args.length)
				pg_option.setMinRowsForDocKeyIndex(args[++i]);

			else if (args[i].equals("--create-attr-index"))
				pg_option.setCreateAttrIndex(true);

			else if (args[i].equals("--no-create-attr-index"))
				pg_option.setCreateAttrIndex(false);

			else if (args[i].equals("--drop-attr-index"))
				pg_option.setDropAttrIndex();

			else if (args[i].equals("--max-attr-cols-for-index") && i + 1 < args.length)
				pg_option.setMaxAttrColsForIndex(args[++i]);

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

			else if (args[i].equals("--pg-tab-delimiter"))
				option.usePgTsv();

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

		Path work_dir = Paths.get(work_dir_name);

		if (!Files.isDirectory(work_dir)) {

			try {
				Files.createDirectory(work_dir);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		if (pg_option.name.isEmpty()) {
			System.err.println("Database name is empty.");
			showUsage();
		}

		try {

			PgSchemaClientImpl client = new PgSchemaClientImpl(is, option, fst_conf, MethodHandles.lookup().lookupClass().getName());

			Connection db_conn = DriverManager.getConnection(pg_option.getDbUrl(PgSchemaUtil.def_encoding), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.pass);

			pg_option.clear();

			// test PostgreSQL DDL with schema

			if (pg_option.test)
				client.schema.testPgSql(db_conn, true);

			client.schema.pgCsv2PgSql(db_conn, work_dir);

			System.out.println("Done " + (option.pg_delimiter == '\t' ? "tsv" : "csv") + " -> db (" + pg_option.name + ").");

			if (pg_option.create_doc_key_index)
				client.schema.createDocKeyIndex(db_conn, pg_option.min_rows_for_doc_key_index);
			else if (pg_option.drop_doc_key_index)
				client.schema.dropDocKeyIndex(db_conn);

			if (pg_option.create_attr_index)
				client.schema.createAttrIndex(db_conn, pg_option.max_attr_cols_for_index, pg_option.min_rows_for_doc_key_index);
			else if (pg_option.drop_attr_index)
				client.schema.dropAttrIndex(db_conn);

			db_conn.close();

		} catch (ParserConfigurationException | SAXException | IOException | SQLException | PgSchemaException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		PgSchemaOption option = new PgSchemaOption(true);

		option.usePgCsv();

		System.err.println("csv2pgsql: CSV -> PostgreSQL data migration");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --work-dir DIRECTORY (default=\"" + xml2pgcsv.work_dir_name + "\") --db-name DATABASE --db-user USER --db-pass PASSWORD (default=\"\")");
		System.err.println("        --db-host PG_HOST_NAME (default=\"" + PgSchemaUtil.pg_host + "\")");
		System.err.println("        --db-port PG_PORT_NUMBER (default=\"" + PgSchemaUtil.pg_port + "\")");
		System.err.println("        --test-ddl (perform consistency test on PostgreSQL DDL)");
		System.err.println("        --create-doc-key-index (create PostgreSQL index on document key if not exists, enable if --sync option is selected)");
		System.err.println("        --no-create-doc-key-index (do not create PostgreSQL index on document key, default if no --sync option)");
		System.err.println("        --drop-doc-key-index (drop PostgreSQL index on document key if exists)");
		System.err.println("        --min-rows-for-doc-key-index MIN_ROWS_FOR_INDEX (default=\"" + PgSchemaUtil.pg_min_rows_for_doc_key_index + "\")");
		System.err.println("        --create-attr-index (create PostgreSQL index on attribute if not exists)");
		System.err.println("        --no-create-attr-index (do not create PostgreSQL index on attribute, default)");
		System.err.println("        --drop-attr-index (drop PostgreSQL index on attribute if exists)");
		System.err.println("        --max-attr-cols-for-index MAX_ATTR_COLS_FOR_INDEX (default=\"" + PgSchemaUtil.pg_max_attr_cols_for_index + "\")");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --doc-key (append " + option.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + option.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + option.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + option.xpath_key_name + " column in all relations)");
		System.err.println("Option: --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --pg-public-schema (utilize \"public\" schema, default)");
		System.err.println("        --pg-named-schema (enable explicit named schema)");
		System.err.println("        --pg-tab-delimiter (use tab separated file)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
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
