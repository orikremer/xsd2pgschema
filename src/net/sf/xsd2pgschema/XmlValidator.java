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

package net.sf.xsd2pgschema;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.xerces.parsers.DOMParser;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.SAXParseException;

/**
 * XML validator.
 *
 * @author yokochi
 */
public class XmlValidator {

	/** The DOM parser. */
	DOMParser dom_parser = null;

	/** The error handler. */
	ErrHandler err_handler = null;

	/**
	 * Instance of XML validator.
	 *
	 * @param xsd_file XML Schema file
	 * @param full_check whether enable canonical XML Schema validation or not (well-formed only)
	 */
	public XmlValidator(File xsd_file, boolean full_check) {

		dom_parser = new DOMParser();

		if (full_check) {

			try {

				dom_parser.setFeature("http://xml.org/sax/features/validation", true);
				dom_parser.setFeature("http://apache.org/xml/features/validation/schema", true);
				dom_parser.setFeature("http://apache.org/xml/features/validation/schema-full-checking", true);
				dom_parser.setProperty("http://java.sun.com/xml/jaxp/properties/schemaLanguage", PgSchemaUtil.xs_namespace_uri);
				dom_parser.setProperty("http://java.sun.com/xml/jaxp/properties/schemaSource", xsd_file);

			} catch (SAXNotRecognizedException e) {
				e.printStackTrace();
			} catch (SAXNotSupportedException e) {
				e.printStackTrace();
			}

		}

		err_handler = new ErrHandler();

		dom_parser.setErrorHandler(err_handler);

	}

	/**
	 * Execute XML Schema validation.
	 *
	 * @param xml_file_name XML file name
	 * @param in InputStream of XML file
	 * @param chk_sum check sum file to be deleted in case of invalid XML
	 * @param verbose verbose mode
	 */
	public void exec(String xml_file_name, InputStream in, File chk_sum, boolean verbose) {

		err_handler.init();

		try {

			dom_parser.parse(new InputSource(new InputStreamReader(in)));

		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (!err_handler.success) {

			System.err.println(xml_file_name + " is invalid.");

			if (chk_sum != null && chk_sum.exists())
				chk_sum.delete();

			System.exit(1);

		}

		else if (verbose)
			System.out.println(xml_file_name + " is valid.");

		dom_parser.reset();

	}

	/**
	 * Error hander implementation.
	 */
	private static class ErrHandler implements ErrorHandler {

		/** The result of XML Schema validation. */
		public boolean success = true;

		/**
		 * Initialize status flag (success).
		 */
		public void init() {

			success = true;

		}

		/* (non-Javadoc)
		 * @see org.xml.sax.ErrorHandler#error(org.xml.sax.SAXParseException)
		 */
		@Override
		public void error(SAXParseException e) throws SAXException {

			success = false;

			System.err.println("Error: at " + e.getLineNumber());
			System.err.println(e.getMessage());

		}

		/* (non-Javadoc)
		 * @see org.xml.sax.ErrorHandler#fatalError(org.xml.sax.SAXParseException)
		 */
		@Override
		public void fatalError(SAXParseException e) throws SAXException {

			success = false;

			System.err.println("Fatal Error: at " + e.getLineNumber());
			System.err.println(e.getMessage());

		}

		/* (non-Javadoc)
		 * @see org.xml.sax.ErrorHandler#warning(org.xml.sax.SAXParseException)
		 */
		@Override
		public void warning(SAXParseException e) throws SAXException {

			success = false;

			System.out.println("Warning: at " + e.getLineNumber());
			System.out.println(e.getMessage());

		}

	}

}
