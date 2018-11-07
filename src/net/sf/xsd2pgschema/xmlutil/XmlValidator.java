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

package net.sf.xsd2pgschema.xmlutil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.xerces.parsers.DOMParser;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.SAXParseException;

import net.sf.xsd2pgschema.PgSchemaUtil;

/**
 * XML validator.
 *
 * @author yokochi
 */
public class XmlValidator {

	/** The DOM parser. */
	private DOMParser dom_parser = new DOMParser();

	/** The error handler. */
	private ErrHandler err_handler = new ErrHandler();

	/**
	 * Instance of XML validator.
	 *
	 * @param xsd_file_path XML Schema file path
	 * @param full_check whether to enable canonical XML Schema validation or validate well-formed only (false)
	 */
	public XmlValidator(Path xsd_file_path, boolean full_check) {

		if (full_check) {

			try {

				dom_parser.setFeature("http://xml.org/sax/features/validation", true);
				dom_parser.setFeature("http://apache.org/xml/features/validation/schema", true);
				dom_parser.setFeature("http://apache.org/xml/features/validation/schema-full-checking", true);
				dom_parser.setProperty("http://java.sun.com/xml/jaxp/properties/schemaLanguage", PgSchemaUtil.xs_namespace_uri);
				dom_parser.setProperty("http://java.sun.com/xml/jaxp/properties/schemaSource", xsd_file_path.toAbsolutePath().toString());

			} catch (SAXNotRecognizedException e) {
				e.printStackTrace();
			} catch (SAXNotSupportedException e) {
				e.printStackTrace();
			}

		}

		dom_parser.setErrorHandler(err_handler);

	}

	/**
	 * Execute XML Schema validation.
	 *
	 * @param in InputStream of XML file
	 * @param xml_file_path XML file path
	 * @param chk_sum_file_path check sum file path to be deleted in case of invalid XML
	 * @param del_invalid_xml whether to delete invalid XML
	 * @param verbose verbose mode
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void exec(InputStream in, Path xml_file_path, Path chk_sum_file_path, boolean del_invalid_xml, boolean verbose) throws IOException {

		err_handler.init();

		try {

			dom_parser.parse(new InputSource(new InputStreamReader(in)));

		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String xml_file_name = xml_file_path.getFileName().toString();

		if (!err_handler.success) {

			System.err.println(xml_file_name + " is invalid" + (del_invalid_xml ? " and has been deleted." : "."));

			if (del_invalid_xml)
				Files.delete(xml_file_path);

			if (chk_sum_file_path != null)
				Files.deleteIfExists(chk_sum_file_path);

		}

		else if (verbose)
			System.out.println(xml_file_name + " is valid.");

		dom_parser.reset();

	}

	/**
	 * Error hander implementation.
	 */
	static class ErrHandler implements ErrorHandler {

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
