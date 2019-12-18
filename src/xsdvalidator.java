/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2019 Masashi Yokochi

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
import net.sf.xsd2pgschema.xmlutil.XmlParser;
import net.sf.xsd2pgschema.xmlutil.XmlValidator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Validate XML Schema.
 *
 * @author yokochi
 */
public class xsdvalidator {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(true);

		option.validate = option.full_check = true; // enable XML Schema validation
		option.verbose = true;

		/** The XML file filter. */
		XmlFileFilter xml_file_filter = new XmlFileFilter();

		String xsd_for_xsd = PgSchemaUtil.xsd_for_xsd_11_mutable;

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if (args[i].equals("--schema-ver") && i + 1 < args.length) {
				switch (args[++i]) {
				case "1.1":
				case "11":
				case "1.1-mutable":
				case "11-mutable":
				case "1.1_mutable":
				case "11_mutable":
				case "1.1_2009":
				case "11_2009":
				case "2009":
					xsd_for_xsd = PgSchemaUtil.xsd_for_xsd_11_mutable;
					break;
				case "1.1-immutable":
				case "11-immutable":
				case "1.1_immutable":
				case "11_immutable":
				case "1.1_2012_04":
				case "11_2012_04":
				case "2012_04":
				case "1.1_2012-04":
				case "11_2012-04":
				case "2012-04":
				case "1.1_2012":
				case "11_2012":
				case "2012":				
					xsd_for_xsd = PgSchemaUtil.xsd_for_xsd_11_immutable;
					break;
				case "1.0":
				case "10":
				case "1":
				case "1.0_2001":
				case "10_2001":
				case "1_2001":
				case "2001":
					xsd_for_xsd = PgSchemaUtil.xsd_for_xsd_10;
					break;
				default:
					showUsage();
				}
			}

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (option.root_schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		if (PgSchemaUtil.getSchemaFileName(option.root_schema_location).equals(PgSchemaUtil.xsd_for_xsd_name)) {
			System.err.println("Avoid XSD file name: " + PgSchemaUtil.xsd_for_xsd_name);
			System.exit(1);
		}

		Path xsd_for_xsd_path = Paths.get(PgSchemaUtil.xsd_for_xsd_name);

		if (Files.exists(xsd_for_xsd_path)) {

			try {
				Files.delete(xsd_for_xsd_path);
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		XmlValidator validator = new XmlValidator(PgSchemaUtil.getSchemaFilePath(xsd_for_xsd, null, false), option.full_check);

		try {
			new XmlParser(validator, Paths.get(option.root_schema_location), xml_file_filter, option);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("xsdvalidator: Validate XML Schema");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --xml XML_FILE_OR_DIRECTORY");
		System.err.println("Option: --schema-ver XML_SCHEMA_VER (choose from \"1.1-mutable\" (default), \"1.1-immutable\", or \"1.0\")");
		System.exit(1);

	}

}
