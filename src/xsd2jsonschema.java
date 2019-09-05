/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2019 Masashi Yokochi

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
import net.sf.xsd2pgschema.option.*;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

		/** The JSON Schema output name. */
		String json_output = "";

		/** The JSON builder option. */
		JsonBuilderOption jsonb_option = new JsonBuilderOption();

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(jsonb_option.type);

		option.pg_schema_server = false; // stand alone

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if (args[i].equals("--json") && i + 1 < args.length)
				json_output = args[++i];

			else if (args[i].equals("--field-annotation"))
				jsonb_option.no_field_anno = false;

			else if (args[i].equals("--no-field-annotation"))
				jsonb_option.no_field_anno = true;

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

			else if (args[i].equals("--json-no-linefeed"))
				jsonb_option.setLineFeed(false);

			else if (args[i].equals("--json-compact"))
				jsonb_option.setCompact();

			else if (args[i].equals("--schema-ver") && i + 1 < args.length)
				jsonb_option.setSchemaVer(args[++i]);

			else if (args[i].equals("--inline-simple-cont"))
				option.inline_simple_cont = true;

			else if (args[i].equals("--no-wild-card"))
				option.wild_card = false;

			else if (args[i].equals("--case-insensitive")) {
				option.setCaseInsensitive();
				jsonb_option.setCaseInsensitive();
			}

			else if (args[i].equals("--no-cache-xsd"))
				option.cache_xsd = false;

			else if (args[i].equals("--obj-json"))
				option.setDefaultForJsonSchema(jsonb_option.type = JsonType.object);

			else if (args[i].equals("--col-json"))
				option.setDefaultForJsonSchema(jsonb_option.type = JsonType.column);

			else if (args[i].equals("--rel-json"))
				option.setDefaultForJsonSchema(jsonb_option.type = JsonType.relational);

			else if (args[i].equals("--discarded-doc-key-name") && i + 1 < args.length)
				option.addDiscardedDocKeyName(args[++i]);

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (option.root_schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

		if (is == null)
			showUsage();

		if (!json_output.isEmpty() && !json_output.equals("stdout")) {

			Path json_file_path = Paths.get(json_output);

			try {
				System.setOut(new PrintStream(new BufferedOutputStream(Files.newOutputStream(json_file_path)), true));
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
			DocumentBuilder	doc_builder = doc_builder_fac.newDocumentBuilder();

			Document xsd_doc = doc_builder.parse(is);

			is.close();

			doc_builder.reset();

			// XSD analysis

			PgSchema schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, option);

			JsonBuilder jsonb = new JsonBuilder(schema, jsonb_option);

			jsonb.realizeJsonSchema();

		} catch (ParserConfigurationException | SAXException | IOException | PgSchemaException e) {
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
		System.err.println("        --schema-ver JSON_SCHEMA_VER (choose from \"draft_v7\" (default), \"draft_v6\", \"draft_v4\", or \"latest\" as \"" + JsonSchemaVersion.defaultVersion().toString() + "\")");
		System.err.println("        --obj-json (use object-oriented JSON format)");
		System.err.println("        --col-json (use column-oriented JSON format, default)");
		System.err.println("        --rel-json (use relational-oriented JSON format)");
		System.err.println("Option: --inline-simple-cont (enable inlining simple content)");
		System.err.println("        --no-wild-card (turn off wild card extension)");
		System.err.println("        --case-insensitive (all table and column names are lowercase)");
		System.err.println("        --field-annotation (retrieve field annotation, default)");
		System.err.println("        --no-field-annotation (do not retrieve field annotation)");
		System.err.println("        --no-cache-xsd (retrieve XML Schemata without caching)");
		System.err.println("        --json-attr-prefix ATTR_PREFIX_CODE (default=\"" + jsonb_option.getAttrPrefix() + "\")");
		System.err.println("        --json-simple-cont-name SIMPLE_CONTENT_NAME (default=\"" + jsonb_option.getSimpleContentName() + "\")");
		System.err.println("        --json-indent-offset INTEGER (default=" + jsonb_option.getIndentOffset() + ", min=0, max=4)");
		System.err.println("        --json-key-value-offset INTEGER (default=" + jsonb_option.getKeyValueOffset() + ", min=0, max=4)");
		System.err.println("        --json-no-linefeed (dismiss line feed code)");
		System.err.println("        --json-compact (equals to set --json-indent-offset 0 --json-key-value-offset 0 --json-no-linefeed)");
		System.err.println("        --json-array-all (use JSON array uniformly for descendants, effective only in column- and relational-oriented JSON format)");
		System.err.println("        --discarded-doc-key-name DISCARDED_DOCUMENT_KEY_NAME");
		System.exit(1);

	}

}
