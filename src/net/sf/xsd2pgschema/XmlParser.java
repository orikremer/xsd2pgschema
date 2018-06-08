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
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

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

	/** The XML file path.*/
	private Path xml_file_path;

	/** The XML file name.*/
	private String xml_file_name;

	/** The XML document. */
	protected Document document;

	/** The document id. */
	public String document_id = null;

	/** The base name of XML file. */
	public String basename = null;

	/**
	 * Instance of XML parser.
	 *
	 * @param doc_builder instance of DocumentBuilder
	 * @param validator instance of XmlValidator
	 * @param xml_file_path XML file path
	 * @param xml_file_filter XML file filter
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SAXException the SAX exception
	 */
	public XmlParser(DocumentBuilder doc_builder, XmlValidator validator, Path xml_file_path, XmlFileFilter xml_file_filter) throws IOException, SAXException {

		init(xml_file_path, xml_file_filter);

		parse(doc_builder, validator, xml_file_filter);

	}

	/**
	 * Instance of XML parser for XML Schema validation only.
	 *
	 * @param validator instance of XmlValidator
	 * @param xml_file_path XML file path
	 * @param xml_file_filter XML file filter
	 * @param option PostgreSQL data model option
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public XmlParser(XmlValidator validator, Path xml_file_path, XmlFileFilter xml_file_filter, PgSchemaOption option) throws IOException {

		init(xml_file_path, xml_file_filter);

		validate(validator, option.isSynchronizable(false) ? getCheckSumFilePath(option) : null, option.verbose);

	}

	/**
	 * Instance of XML parser (dummy).
	 *
	 * @param xml_file_path XML file path
	 * @param xml_file_filter XML file filter
	 */
	public XmlParser(Path xml_file_path, XmlFileFilter xml_file_filter) {

		init(xml_file_path, xml_file_filter);

	}

	/**
	 * Instance of XML parser (dummy).
	 *
	 * @param xml_file_name XML file name
	 * @param xml_file_filter XML file filter
	 */
	public XmlParser(String xml_file_name, XmlFileFilter xml_file_filter) {

		this.xml_file_name = xml_file_name;

		init(xml_file_filter);

	}

	/**
	 * Set document id and basename.
	 *
	 * @param xml_file_path XML file path
	 * @param xml_file_filter XML file filter
	 */
	private void init(Path xml_file_path, XmlFileFilter xml_file_filter) {

		this.xml_file_path = xml_file_path;
		this.xml_file_name = xml_file_path.getFileName().toString();

		init(xml_file_filter);

	}

	/**
	 * Set document id and basename.
	 *
	 * @param xml_file_filter XML file filter
	 */
	private void init(XmlFileFilter xml_file_filter) {

		basename = FilenameUtils.getBaseName(xml_file_name);

		switch (xml_file_filter.ext) {
		case "gz":
		case "zip":
			basename = FilenameUtils.getBaseName(basename);
			break;
		}

		// decide document id quoting XML file name

		document_id = xml_file_name.replaceFirst(xml_file_filter.prefix_digest, "").replaceFirst(xml_file_filter.ext_digest, "");

		if (!xml_file_filter.case_sense_doc_key)
			document_id = (xml_file_filter.lower_case_doc_key ? document_id.toLowerCase() : document_id.toUpperCase());

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

		InputStream in = Files.newInputStream(xml_file_path);

		// xml.gz file

		if (FilenameUtils.getExtension(xml_file_name).equals("gz")) {

			GZIPInputStream gzin = new GZIPInputStream(in);

			document = doc_builder.parse(gzin);

			if (validator != null) {

				gzin.close();
				in.close();

				in = Files.newInputStream(xml_file_path);
				gzin = new GZIPInputStream(in);

				validator.exec(xml_file_name, gzin, null, false);

			}

			gzin.close();

		}

		// xml.zip file

		else if (FilenameUtils.getExtension(xml_file_name).equals("zip")) {

			ZipInputStream zin = new ZipInputStream(in);

			document = doc_builder.parse(zin);

			if (validator != null) {

				zin.close();
				in.close();

				in = Files.newInputStream(xml_file_path);
				zin = new ZipInputStream(in);

				validator.exec(xml_file_name, zin, null, false);

			}

			zin.close();

		}

		// xml file

		else {

			document = doc_builder.parse(in);

			if (validator != null) {

				in.close();

				in = Files.newInputStream(xml_file_path);

				validator.exec(xml_file_name, in, null, false);

			}

		}

		in.close();

		doc_builder.reset();

	}

	/**
	 * Validate XML document.
	 *
	 * @param validator instance of XmlValidator
	 * @param check_sum_file_path check sum file path to be deleted in case of validation error
	 * @param verbose verbose mode
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void validate(XmlValidator validator, Path check_sum_file_path, boolean verbose) throws IOException {

		InputStream in = Files.newInputStream(xml_file_path);

		// xml.gz file

		if (FilenameUtils.getExtension(xml_file_name).equals("gz")) {

			GZIPInputStream gzin = new GZIPInputStream(in);

			validator.exec(xml_file_name, gzin, check_sum_file_path, verbose);

			gzin.close();

		}

		else if (FilenameUtils.getExtension(xml_file_name).equals("zip")) {

			ZipInputStream zin = new ZipInputStream(in);

			validator.exec(xml_file_name, zin, check_sum_file_path, verbose);

			zin.close();

		}

		// xml file

		else
			validator.exec(xml_file_name, in, check_sum_file_path, verbose);

		in.close();

	}

	/**
	 * Return check sum file path.
	 *
	 * @param option PostgreSQL data model option
	 * @return Path check sum file path
	 */
	private Path getCheckSumFilePath(PgSchemaOption option) {
		return Paths.get(option.check_sum_dir_path.toString(), xml_file_name + "." + option.check_sum_algorithm.toLowerCase());
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

		if (option.isSynchronizable(false)) {

			InputStream in = Files.newInputStream(xml_file_path);

			String new_check_sum = String.valueOf(Hex.encodeHex(md_chk_sum.digest(IOUtils.readFully(in, (int) Files.size(xml_file_path)))));

			in.close();

			Path check_sum_path = getCheckSumFilePath(option);

			if (Files.exists(check_sum_path)) {

				BufferedReader br = Files.newBufferedReader(check_sum_path);

				String old_check_sum = br.readLine();

				if (old_check_sum.equals(new_check_sum))
					identity = true;

				br.close();

			}

			if (!identity && !option.sync_dry_run) {

				BufferedWriter bw = Files.newBufferedWriter(check_sum_path);

				bw.write(new_check_sum);

				bw.close();

			}

			md_chk_sum.reset();

		}

		return identity;
	}

}
