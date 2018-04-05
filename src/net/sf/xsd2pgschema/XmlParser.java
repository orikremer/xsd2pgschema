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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * XML parser.
 *
 * @author yokochi
 */
public class XmlParser {

	/** The XML file.*/
	File xml_file;

	/** The XML document. */
	Document document;

	/** The document id. */
	public String document_id = null;

	/** The base name of XML file. */
	public String basename = null;

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

		init(xml_file, xml_file_filter);

		parse(doc_builder, validator, xml_file_filter);

	}

	/**
	 * Instance of XML parser for XML Schema validation only.
	 *
	 * @param validator instance of XmlValidator
	 * @param xml_file XML file
	 * @param xml_file_filter XML file filter
	 * @param option PostgreSQL data model option
	 * @param md_chk_sum instance of message digest for check sum
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public XmlParser(XmlValidator validator, File xml_file, XmlFileFilter xml_file_filter, PgSchemaOption option, MessageDigest md_chk_sum) throws IOException {

		init(xml_file, xml_file_filter);

		File check_sum = null;

		if (option.sync && option.check_sum_dir != null && md_chk_sum != null)
			check_sum = new File(option.check_sum_dir, xml_file.getName() + "." + option.check_sum_algorithm.toLowerCase());

		validate(validator, check_sum, option.verbose);

	}

	/**
	 * Instance of XML parser (dummy).
	 *
	 * @param xml_file XML file
	 * @param xml_file_filter XML file filter
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public XmlParser(File xml_file, XmlFileFilter xml_file_filter) throws IOException {

		init(xml_file, xml_file_filter);

	}

	/**
	 * Set document id and basename.
	 *
	 * @param xml_file XML file
	 * @param xml_file_filter XML file filter
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void init(File xml_file, XmlFileFilter xml_file_filter) {

		this.xml_file = xml_file;

		String xml_file_name = xml_file.getName();

		String _xml_file_ext = xml_file_filter.ext;
		String _xml_file_ext_digest = xml_file_filter.ext_digest;

		if (!_xml_file_ext_digest.endsWith("."))
			_xml_file_ext_digest += ".";

		if (_xml_file_ext_digest.endsWith("xml."))
			_xml_file_ext_digest = _xml_file_ext_digest.replaceFirst("xml\\.$", "");

		// xml.gz file

		if (FilenameUtils.getExtension(xml_file_name).equals("gz")) {

			_xml_file_ext_digest += "xml.";
			_xml_file_ext = "gz";

		}

		// decide document id quoting XML file name

		document_id = xml_file_name.replaceFirst("^" + xml_file_filter.prefix_digest, "").replaceFirst(_xml_file_ext_digest + _xml_file_ext + "$", "");

		if (!xml_file_filter.case_sense_doc_key)
			document_id = (xml_file_filter.lower_case_doc_key ? document_id.toLowerCase() : document_id.toUpperCase());

		// decide base name of XML file name

		basename = xml_file_name.replaceFirst(_xml_file_ext + "$", "");

	}

	/**
	 * Parse XML document with XML Schema validation.
	 *
	 * @param doc_builder instance of DocumentBuilder
	 * @param validator instance of XmlValidator
	 * @param xml_file_filter XML file filter
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SAXException the SAX exception
	 */
	private void parse(DocumentBuilder doc_builder, XmlValidator validator, XmlFileFilter xml_file_filter) throws IOException, SAXException {

		// xml.gz file

		if (FilenameUtils.getExtension(xml_file.getName()).equals("gz")) {

			FileInputStream in = new FileInputStream(xml_file);
			GZIPInputStream gzin = new GZIPInputStream(in);

			document = doc_builder.parse(gzin);

			if (validator != null) {

				in = new FileInputStream(xml_file);
				gzin = new GZIPInputStream(in);

				validator.exec(xml_file.getPath(), gzin, null, false);

			}

			gzin.close();
			in.close();

		}

		// xml file

		else {

			document = doc_builder.parse(xml_file);

			if (validator != null) {

				FileInputStream in = new FileInputStream(xml_file);

				validator.exec(xml_file.getPath(), in, null, false);

				in.close();

			}

		}

		doc_builder.reset();

	}

	/**
	 * Validate XML document.
	 *
	 * @param validator instance of XmlValidator
	 * @param check_sum check sum file to be deleted in case of invalid XML
	 * @param verbose verbose mode
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void validate(XmlValidator validator, File check_sum, boolean verbose) throws IOException {

		if (FilenameUtils.getExtension(xml_file.getName()).equals("gz")) {

			FileInputStream in = new FileInputStream(xml_file);
			GZIPInputStream gzin = new GZIPInputStream(in);

			in = new FileInputStream(xml_file);
			gzin = new GZIPInputStream(in);

			validator.exec(xml_file.getPath(), gzin, check_sum, verbose);

			gzin.close();
			in.close();

		}

		// xml file

		else {

			FileInputStream in = new FileInputStream(xml_file);

			validator.exec(xml_file.getPath(), in, check_sum, verbose);

			in.close();

		}

	}

	/**
	 * Identify XML document by agreement of check sum.
	 *
	 * @param option PostgreSQL data model option
	 * @param md_chk_sum instance of message digest for check sum
	 * @return boolean identity of XML document
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized boolean identify(PgSchemaOption option, MessageDigest md_chk_sum) throws IOException {

		boolean identity = false;

		if (option.sync && option.check_sum_dir != null && md_chk_sum != null) {

			FileInputStream in = new FileInputStream(xml_file);

			FileChannel ch = in.getChannel();

			md_chk_sum.reset();

			String new_check_sum = String.valueOf(Hex.encodeHex(md_chk_sum.digest(IOUtils.readFully(in, (int) ch.size()))));

			ch.close();

			in.close();

			File check_sum = new File(option.check_sum_dir, xml_file.getName() + "." + option.check_sum_algorithm.toLowerCase());

			if (check_sum.exists()) {

				FileReader fr = new FileReader(check_sum);
				BufferedReader br = new BufferedReader(fr);

				String old_check_sum = br.readLine();

				if (old_check_sum.equals(new_check_sum))
					identity = true;

				br.close();
				fr.close();

			}

			if (!identity) {

				FileWriter fw = new FileWriter(check_sum);
				fw.write(new_check_sum);
				fw.close();

			}

		}

		return identity;
	}

	/**
	 * Remove document.
	 */
	public void clear() {

		xml_file = null;
		document = null;

		document_id = basename = null;

	}

}
