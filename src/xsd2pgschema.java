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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.xml.parsers.*;

import org.w3c.dom.Document;

/**
 * PostgreSQL DDL translation.
 *
 * @author yokochi
 */
public class xsd2pgschema {

	/** The schema option. */
	private static PgSchemaOption option = new PgSchemaOption(true);

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		option.ddl_output = true; // output PostgreSQL DDL

		String ddl_file_name = "";

		option.hash_algorithm = ""; // assumed hash algorithm should be empty

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if (args[i].equals("--ddl") && i + 1 < args.length)
				ddl_file_name = args[++i];

			else if (args[i].equals("--doc-key"))
				option.setDocKeyOption(true);

			else if (args[i].equals("--no-doc-key"))
				option.setDocKeyOption(false);

			else if (args[i].equals("--no-rel"))
				option.cancelRelModelExt();

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

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--no-key"))
				option.retain_key = false;

			else if (args[i].equals("--field-annotation"))
				option.no_field_anno = false;

			else if (args[i].equals("--no-field-annotation"))
				option.no_field_anno = true;

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

		if (!ddl_file_name.isEmpty() && !ddl_file_name.equals("stdout")) {

			Path ddl_file_path = Paths.get(ddl_file_name);

			try {
				System.setOut(new PrintStream(new BufferedOutputStream(Files.newOutputStream(ddl_file_path)), true));
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

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

			new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, option);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xsd2pgschema: XML Schema -> PostgreSQL DDL conversion");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --ddl DDL_FILE (default=stdout)");
		System.err.println("        --no-rel (turn off relational model extension)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --doc-key (append " + option.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + option.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + option.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + option.xpath_key_name + " column in all relations)");
		System.err.println("        --no-key (turn off constraint of primary key/foreign key)");
		System.err.println("Option: --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --pg-public-schema (utilize \"public\" schema, default)");
		System.err.println("        --pg-named-schema (enable explicit named schema)");
		System.err.println("        --field-annotation (retrieve field annotation)");
		System.err.println("        --no-field-annotation (do not retrieve field annotation, default)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --hash-by ASSUMED_ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16bit); | int (32bit, default)]");
		System.err.println("        --doc-key-name DOC_KEY_NAME (default=\"" + option.def_document_key_name + "\")");
		System.err.println("        --ser-key-name SER_KEY_NAME (default=\"" + option.def_serial_key_name + "\")");
		System.err.println("        --xpath-key-name XPATH_KEY_NAME (default=\"" + option.def_xpath_key_name + "\")");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.err.println("        --inplace-doc-key-name INPLACE_DOCUMENT_KEY_NAME");
		System.err.println("        --doc-key-if-no-inplace (append document key if no in-place docuemnt key, select --no-doc-key options by default)");
		System.exit(1);

	}

}
