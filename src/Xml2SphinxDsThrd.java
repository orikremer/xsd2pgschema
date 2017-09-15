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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.TransformerException;

import org.apache.commons.text.StringEscapeUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Thread function for xml2sphinxds.
 *
 * @author yokochi
 */
public class Xml2SphinxDsThrd implements Runnable {

	/** The shard id. */
	private int shard_id;

	/** The shard size. */
	private int shard_size;

	/** The thread id. */
	private int thrd_id;

	/** The max threads. */
	private int max_thrds;

	/** The document builder for reusing. */
	private DocumentBuilder doc_builder;

	/** The PostgreSQL schema. */
	private PgSchema schema = null;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The data source directory name. */
	private String ds_dir_name = null;

	/** The Sphinx schema file. */
	private File sphinx_schema = null;

	/**
	 * Instance of Xml2SphinxDsThrd.
	 *
	 * @param shard_id shard id
	 * @param shard_size shard size
	 * @param thrd_id thread id
	 * @param max_thrds max threads
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL schema option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2SphinxDsThrd(final int shard_id, final int shard_size, final int thrd_id, final int max_thrds, final InputStream is, final PgSchemaOption option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

		this.shard_id = shard_id;
		this.shard_size = shard_size;

		this.thrd_id = thrd_id;
		this.max_thrds = max_thrds;

		// parse XSD document

		DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
		doc_builder_fac.setValidating(false);
		doc_builder_fac.setNamespaceAware(true);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		doc_builder = doc_builder_fac.newDocumentBuilder();

		Document xsd_doc = doc_builder.parse(is);

		is.close();

		doc_builder.reset();

		// XSD analysis

		schema = new PgSchema(doc_builder, xsd_doc, null, PgSchemaUtil.getSchemaName(xml2sphinxds.schema_location), option);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFile(xml2sphinxds.schema_location, null)) : null;

		ds_dir_name = xml2sphinxds.ds_dir_name;

		if (shard_size > 1) {

			ds_dir_name += "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

			File ds_dir = new File(ds_dir_name);

			if (!ds_dir.isDirectory()) {

				if (!ds_dir.mkdir())
					throw new PgSchemaException("Couldn't create directory '" + ds_dir_name + "'.");

			}

		}

		sphinx_schema = new File(ds_dir_name, PgSchemaUtil.sphinx_schema_name);

		if (sphinx_schema.exists()) {

			if (option.append) {

				doc_builder_fac.setNamespaceAware(false);
				doc_builder = doc_builder_fac.newDocumentBuilder();

				Document sphinx_doc = doc_builder.parse(sphinx_schema);

				doc_builder.reset();

				schema.syncSphSchema(sphinx_doc);

				sphinx_doc = null;

			}

			else
				sphinx_schema.delete();

		}

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int proc_sid = 0;
		int proc_tid = 0;

		for (File xml_file : xml2sphinxds.xml_files) {

			if (proc_sid++ % shard_size != shard_id)
				continue;

			if (proc_tid++ % max_thrds != thrd_id)
				continue;

			if (xml_file.isFile()) {

				String sph_doc_name = PgSchemaUtil.sphinx_document_prefix + xml_file.getName().split("\\.")[0] + ".xml";
				File sph_doc = new File(ds_dir_name, sph_doc_name);

				try {

					FileWriter writer = new FileWriter(sph_doc);

					XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml2sphinxds.xml_file_filter);

					writer.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
					writer.write("<sphinx:document id=\"" + schema.getHashKeyString(xml_parser.document_id) + "\">\n");
					writer.write("<" + PgSchemaUtil.document_key_name + ">" + StringEscapeUtils.escapeXml10(xml_parser.document_id) + "</" + PgSchemaUtil.document_key_name + ">\n");

					schema.xml2SphDs(xml_parser, writer, xml2sphinxds.xml_post_editor, xml2sphinxds.index_filter);

					writer.write("</sphinx:document>\n");

					writer.close();

				} catch (IOException | SAXException | ParserConfigurationException | TransformerException | PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

				if (shard_id == 0 && thrd_id == 0)
					System.out.print("\rExtracted " + proc_sid + " of " + xml2sphinxds.xml_files.length + " ...");

			}

		}

		schema.closeXml2SphDs();

	}

	/**
	 * Merge Sphinx data source files.
	 *
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws PgSchemaException the pg schema exception
	 */
	public void merge() throws ParserConfigurationException, SAXException, IOException, PgSchemaException {

		if (thrd_id != 0)
			return;

		// Sphinx xmlpipe2 writer

		File sphinx_data_source = new File(ds_dir_name, PgSchemaUtil.sphinx_data_source_name);
		schema.writeSphSchema(sphinx_data_source, true);

		FileWriter filew = new FileWriter(sphinx_data_source, true);

		SAXParserFactory spf = SAXParserFactory.newInstance();
		SAXParser sax_parser = spf.newSAXParser();

		System.out.println("Merging" + (shard_size == 1 ? "" : (" " + (shard_id + 1) + " of " + shard_size + " ")) + "...");

		int proc_sid = 0;

		for (File xml_file : xml2sphinxds.xml_files) {

			if (proc_sid++ % shard_size != shard_id)
				continue;

			if (xml_file.isFile()) {

				String sph_doc_name = PgSchemaUtil.sphinx_document_prefix + xml_file.getName().split("\\.")[0] + ".xml";
				File sph_doc = new File(ds_dir_name, sph_doc_name);

				if (sph_doc.exists()) {

					SphDsSAXHandler handler = new SphDsSAXHandler(schema, filew);

					try {

						sax_parser.parse(sph_doc, handler);

					} catch (SAXException | IOException e) {
						e.printStackTrace();
					}

					sph_doc.delete();

				}

			}

		}

		filew.write("</sphinx:docset>\n");
		filew.close();

		System.out.println("Done" + (shard_size == 1 ? "" : (" " + (shard_id + 1) + " of " + shard_size + " ")) + ".");

		// Sphinx schema writer for next update or merge

		schema.writeSphSchema(sphinx_schema, false);

		// Sphinx configuration writer

		File sphinx_conf = new File(ds_dir_name, PgSchemaUtil.sphinx_conf_name);
		schema.writeSphConf(sphinx_conf, xml2sphinxds.ds_name, sphinx_data_source);

	}

}
