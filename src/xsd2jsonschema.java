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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.security.NoSuchAlgorithmException;

import javax.xml.parsers.*;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * JSON Schema translation.
 *
 * @author yokochi
 */
public class xsd2jsonschema {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		JsonType json_type = JsonType.defaultType();

		PgSchemaOption option = new PgSchemaOption(json_type);
		JsonBuilderOption jsonb_option = new JsonBuilderOption();

		String schema_location = "";
		String json_file_name = "";

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				schema_location = args[++i];

			else if (args[i].equals("--json") && i + 1 < args.length)
				json_file_name = args[++i];

			else if (args[i].equals("--field-annotation"))
				jsonb_option.no_field_anno = false;

			else if (args[i].equals("--no-field-annotation"))
				jsonb_option.no_field_anno = true;

			else if (args[i].equals("--json-array-all"))
				jsonb_option.array_all = true;

			else if (args[i].equals("--attr-json-prefix") && i + 1 < args.length)
				jsonb_option.setAttrPrefix(args[++i]);

			else if (args[i].equals("--simple-cont-json-key") && i + 1 < args.length)
				jsonb_option.setSimpleContentKey(args[++i]);

			else if (args[i].equals("--json-indent-spaces") && i + 1 < args.length)
				jsonb_option.setIndentSpaces(args[++i]);

			else if (args[i].equals("--json-key-value-spaces") && i + 1 < args.length)
				jsonb_option.setKeyValueSpaces(args[++i]);

			else if (args[i].equals("--json-no-linefeed"))
				jsonb_option.linefeed = false;

			else if (args[i].equals("--json-compact"))
				jsonb_option.setCompact();

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--case-insensitive"))
				option.case_sense = false;

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--obj-json"))
				option.setDefaultForJsonSchema(json_type = JsonType.object);

			else if (args[i].equals("--col-json"))
				option.setDefaultForJsonSchema(json_type = JsonType.column);

			else if (args[i].equals("--rel-json"))
				option.setDefaultForJsonSchema(json_type = JsonType.relational);

			else if (args[i].equals("--discarded-doc-key-name") && i + 1 < args.length)
				option.addDiscardedDocKeyName(args[++i]);

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = PgSchemaUtil.getSchemaInputStream(schema_location, null, false);

		if (is == null)
			showUsage();

		if (!json_file_name.isEmpty() && !json_file_name.equals("stdout")) {

			File json_file = new File(json_file_name);

			try {
				System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream(json_file)), true));
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
			DocumentBuilder	doc_builder = doc_builder_fac.newDocumentBuilder();

			Document xsd_doc = doc_builder.parse(is);

			is.close();

			doc_builder.reset();

			// XSD analysis

			PgSchema schema = new PgSchema(doc_builder, xsd_doc, null, schema_location, option);

			schema.initJsonBuilder(jsonb_option);

			switch (json_type) {
			case column:
				schema.realizeColJsonSchema();
				break;
			case object:
				schema.realizeObjJsonSchema();
				break;
			case relational:
				schema.realizeJsonSchema();
				break;
			}

		} catch (ParserConfigurationException | SAXException | IOException | NoSuchAlgorithmException | PgSchemaException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		JsonBuilderOption jsonb_option = new JsonBuilderOption();

		System.err.println("xsd2jsonschema: XML Schema -> JSON Schema conversion");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --json JSON_SCHEMA_FILE (default=stdout)");
		System.err.println("        --obj-json (use object-oriented JSON format)");
		System.err.println("        --col-json (use column-oriented JSON format, default)");
		System.err.println("        --rel-json (use relational-oriented JSON format)");
		System.err.println("Option: --no-wild-card (turn off wild card extension)");
		System.err.println("        --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --field-annotation (retrieve field annotation, default)");
		System.err.println("        --no-field-annotation (do not retrieve field annotation)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --attr-json-prefix ATTR_PREFIX_CODE (default=\"" + jsonb_option.attr_prefix + "\")");
		System.err.println("        --simple-cont-json-key SIMPLE_CONTENT_NAME (default=\"" + jsonb_option.simple_content_key + "\")");
		System.err.println("        --json-indent-spaces INTEGER (default=" + jsonb_option.indent_spaces + ", min=0, max=4)");
		System.err.println("        --json-key-value-spaces INTEGER (default=" + jsonb_option.key_value_spaces + ", min=0, max=1)");
		System.err.println("        --json-no-linefeed (avoid to use linefeed code)");
		System.err.println("        --json-compact (equals to set --json-indent-spaces 0 --json-key-value-spaces 0 --json-no-linefeed)");
		System.err.println("        --json-array-all (use JSON array uniformly for descendants, effective only in column- and relational-oriented JSON format)");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.exit(1);

	}

}
