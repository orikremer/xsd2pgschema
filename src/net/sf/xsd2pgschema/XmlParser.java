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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;

import org.apache.commons.io.FilenameUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * XML parser.
 *
 * @author yokochi
 */
public class XmlParser {

	/** The XML document. */
	Document document;

	/** The document id. */
	public String document_id;

	/** The base name of XML file. */
	public String basename;

	/**
	 * Instance of XML parser.
	 *
	 * @param doc_builder instance of DocumentBuilder
	 * @param validator instance of XmlValidator
	 * @param xml_file XML file
	 * @param xml_file_filter XML file filter
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SAXException the SAX exception
	 */
	public XmlParser(DocumentBuilder doc_builder, XmlValidator validator, File xml_file, XmlFileFilter xml_file_filter) throws IOException, SAXException {

		String xml_file_name = xml_file.getName();

		String _xml_file_ext = xml_file_filter.ext;
		String _xml_file_ext_digest = xml_file_filter.ext_digest;

		if (!_xml_file_ext_digest.endsWith("."))
			_xml_file_ext_digest += ".";

		if (_xml_file_ext_digest.endsWith("xml."))
			_xml_file_ext_digest = _xml_file_ext_digest.replaceFirst("xml\\.$", "");

		// xml.gz file

		if (FilenameUtils.getExtension(xml_file_name).equals("gz")) {

			FileInputStream in = new FileInputStream(xml_file);
			GZIPInputStream gzin = new GZIPInputStream(in);

			document = doc_builder.parse(gzin);

			_xml_file_ext_digest += "xml.";
			_xml_file_ext = "gz";

			if (validator != null) {

				in = new FileInputStream(xml_file);
				gzin = new GZIPInputStream(in);

				validator.exec(xml_file.getPath(), gzin);

			}

			gzin.close();
			in.close();

		}

		// xml file

		else {

			document = doc_builder.parse(xml_file);

			if (validator != null) {

				FileInputStream in = new FileInputStream(xml_file);

				validator.exec(xml_file.getPath(), in);

				in.close();

			}

		}

		doc_builder.reset();

		// decide document id quoting XML file name

		document_id = xml_file_name.replaceFirst("^" + xml_file_filter.prefix_digest, "").replaceFirst(_xml_file_ext_digest + _xml_file_ext + "$", "");

		// decide base name of XML file name

		basename = xml_file_name.replaceFirst(_xml_file_ext + "$", "");

	}

	/**
	 * Instance of XML parser only for XML Schema validation.
	 *
	 * @param validator instance of XmlValidator
	 * @param xml_file XML file
	 * @param xml_file_filter XML file filter
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public XmlParser(XmlValidator validator, File xml_file, XmlFileFilter xml_file_filter) throws IOException {

		String xml_file_name = xml_file.getName();

		String _xml_file_ext = xml_file_filter.ext;
		String _xml_file_ext_digest = xml_file_filter.ext_digest;

		if (!_xml_file_ext_digest.endsWith("."))
			_xml_file_ext_digest += ".";

		if (_xml_file_ext_digest.endsWith("xml."))
			_xml_file_ext_digest = _xml_file_ext_digest.replaceFirst("xml\\.$", "");

		// xml.gz file

		if (FilenameUtils.getExtension(xml_file_name).equals("gz")) {

			FileInputStream in = new FileInputStream(xml_file);
			GZIPInputStream gzin = new GZIPInputStream(in);

			_xml_file_ext_digest += "xml.";
			_xml_file_ext = "gz";

			in = new FileInputStream(xml_file);
			gzin = new GZIPInputStream(in);

			validator.exec(xml_file.getPath(), gzin);

			gzin.close();
			in.close();

		}

		// xml file

		else {

			FileInputStream in = new FileInputStream(xml_file);

			validator.exec(xml_file.getPath(), in);

			in.close();

		}

		// decide document id quoting XML file name

		document_id = xml_file_name.replaceFirst("^" + xml_file_filter.prefix_digest, "").replaceFirst(_xml_file_ext_digest + _xml_file_ext + "$", "");

		// decide base name of XML file name

		basename = xml_file_name.replaceFirst(_xml_file_ext + "$", "");

	}

}
