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

		option.validate = true; // enable XML Schema validation
		option.verbose = true;

		/** The XML file filter. */
		XmlFileFilter xml_file_filter = new XmlFileFilter();

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (option.root_schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		if (PgSchemaUtil.getSchemaFileName(option.root_schema_location).equals(PgSchemaUtil.getSchemaFileName(PgSchemaUtil.xsd_for_xsd))) {
			System.err.println("Avoid XSD file name: " + PgSchemaUtil.getSchemaFileName(PgSchemaUtil.xsd_for_xsd));
			System.exit(1);
		}

		XmlValidator validator = new XmlValidator(PgSchemaUtil.getSchemaFilePath(PgSchemaUtil.xsd_for_xsd, null, false));

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
		System.exit(1);

	}

}
