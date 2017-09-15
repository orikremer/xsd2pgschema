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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import javax.xml.parsers.*;

import org.w3c.dom.Document;

/**
 * PostgreSQL DDL translation.
 *
 * @author yokochi
 */
public class xsd2pgschema {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		PgSchemaOption option = new PgSchemaOption(true);
		option.ddl_output = true; // output PostgreSQL DDL

		boolean _document_key = false;
		boolean _no_document_key = false;

		String schema_location = "";
		String ddl_file_name = "";

		option.hash_algorithm = ""; // assumed hash algorithm should be empty

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

			else if (args[i].equals("--field-annotation"))
				option.no_field_anno = false;

			else if (args[i].equals("--no-field-annotation"))
				option.no_field_anno = true;

			else if (args[i].equals("--no-key"))
				option.retain_key = false;

			else if (args[i].equals("--xsd"))
				schema_location = args[++i];

			else if (args[i].equals("--ddl"))
				ddl_file_name = args[++i];

			else if (args[i].equals("--hash-by"))
				option.hash_algorithm = args[++i];

			else if (args[i].equals("--hash-size"))
				option.hash_size = PgHashSize.getPgHashSize(args[++i]);

			else if (args[i].equals("--ser-size"))
				option.ser_size = PgSerSize.getPgSerSize(args[++i]);

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

		if (!ddl_file_name.isEmpty() && !ddl_file_name.equals("stdout")) {

			File ddl_file = new File(ddl_file_name);

			try {
				System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream(ddl_file)), true));
			} catch (FileNotFoundException e) {
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

			new PgSchema(doc_builder, xsd_doc, null, PgSchemaUtil.getSchemaName(schema_location), option);

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
		System.err.println("        --doc-key (append " + PgSchemaUtil.document_key_name + " column in all relations, default with relational model extension)");
		System.err.println("        --no-doc-key (remove " + PgSchemaUtil.document_key_name + " column from all relations, effective only with relational model extension)");
		System.err.println("        --ser-key (append " + PgSchemaUtil.serial_key_name + " column in child relation of list holder)");
		System.err.println("        --xpath-key (append " + PgSchemaUtil.xpath_key_name + " column in all relations)");
		System.err.println("        --no-key (turn off constraint of primary key/foreign key)");
		System.err.println("Option: --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --field-annotation (retrieve field annotation)");
		System.err.println("        --no-field-annotation (do not retrieve field annotation, default)");
		System.err.println("        --hash-by ASSUMED_ALGORITHM [MD2 | MD5 | SHA-1 (default) | SHA-224 | SHA-256 | SHA-384 | SHA-512]");
		System.err.println("        --hash-size BIT_SIZE [int (32bit) | long (64bit, default) | native (default bit of algorithm) | debug (string)]");
		System.err.println("        --ser-size BIT_SIZE [short (16bit); | int (32bit, default)]");
		System.exit(1);

	}

}
