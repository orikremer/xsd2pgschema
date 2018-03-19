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

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
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

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option = null;

	/** The index filter. */
	private IndexFilter index_filter = null;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The data source directory. */
	private File ds_dir = null;

	/** The Sphinx schema file. */
	private File sphinx_schema = null;

	/** The document id stored in data source. */
	private HashMap<String, Integer> doc_rows = null;

	/**
	 * Instance of Xml2SphinxDsThrd.
	 *
	 * @param shard_id shard id
	 * @param shard_size shard size
	 * @param thrd_id thread id
	 * @param max_thrds max threads
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL data model option
	 * @param index_filter index filter
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2SphinxDsThrd(final int shard_id, final int shard_size, final int thrd_id, final int max_thrds, final InputStream is, final PgSchemaOption option, IndexFilter index_filter) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

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

		schema = new PgSchema(doc_builder, xsd_doc, null, xml2sphinxds.schema_location, this.option = option);

		schema.applyXmlPostEditor(xml2sphinxds.xml_post_editor);

		schema.applyIndexFilter(this.index_filter = index_filter);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFile(xml2sphinxds.schema_location, null, option.cache_xsd)) : null;

		String ds_dir_name = xml2sphinxds.ds_dir_name;

		if (shard_size > 1)
			ds_dir_name += "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

		ds_dir = new File(ds_dir_name);

		if (!ds_dir.isDirectory()) {

			if (!ds_dir.mkdir())
				throw new PgSchemaException("Couldn't create directory '" + ds_dir_name + "'.");

		}

		sphinx_schema = new File(ds_dir, PgSchemaUtil.sph_schema_name);

		if (sphinx_schema.exists()) {

			doc_builder_fac.setNamespaceAware(false);
			doc_builder = doc_builder_fac.newDocumentBuilder();

			Document sphinx_doc = doc_builder.parse(sphinx_schema);

			doc_builder.reset();

			schema.syncSphSchema(sphinx_doc);

			sphinx_doc = null;

		}

		File sph_data_source = new File(ds_dir, PgSchemaUtil.sph_data_source_name);

		if ((option.sync_weak || option.sync) && sph_data_source.exists()) {

			HashSet<String> doc_set = new HashSet<String>();

			SAXParserFactory spf = SAXParserFactory.newInstance();
			SAXParser sax_parser = spf.newSAXParser();

			SphDsDocIdExtractor handler = new SphDsDocIdExtractor(schema, doc_set);

			try {

				sax_parser.parse(sph_data_source, handler);

			} catch (SAXException | IOException e) {
				e.printStackTrace();
			}

			if (option.sync) {

				if (thrd_id == 0) {

					xml2sphinxds.sync_delete_ids[shard_id].addAll(doc_set);

					xml2sphinxds.xml_file_queue.forEach(xml_file -> {

						try {

							XmlParser xml_parser = new XmlParser(xml_file, xml2luceneidx.xml_file_filter);

							xml2sphinxds.sync_delete_ids[shard_id].remove(xml_parser.document_id);

						} catch (IOException e) {
							e.printStackTrace();
							System.exit(1);
						}

					});

					synchronized (xml2sphinxds.sync_lock[shard_id]) {
						xml2sphinxds.sync_lock[shard_id].notifyAll();
					}

				}

				else {

					try {

						synchronized (xml2sphinxds.sync_lock[shard_id]) {
							xml2sphinxds.sync_lock[shard_id].wait();
						}

					} catch (InterruptedException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

				doc_rows = new HashMap<String, Integer>();

				doc_set.forEach(doc_id -> doc_rows.put(doc_id, shard_id));

				doc_set.clear();

				if (shard_size > 1) {

					for (int _shard_id = 0; _shard_id < shard_size; _shard_id++) {

						if (_shard_id == shard_id)
							continue;

						File _sph_data_source = new File(xml2sphinxds.ds_dir_name + "/" + PgSchemaUtil.shard_dir_prefix + _shard_id, PgSchemaUtil.sph_data_source_name);

						if (!_sph_data_source.exists())
							continue;

						SphDsDocIdExtractor _handler = new SphDsDocIdExtractor(schema, doc_rows, _shard_id);

						try {

							sax_parser.reset();
							sax_parser.parse(_sph_data_source, _handler);

						} catch (SAXException | IOException e) {
							e.printStackTrace();
						}

					}

				}

			}

		}

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml2sphinxds.xml_file_queue.size();
		boolean show_progress = shard_id == 0 && thrd_id == 0 && total > 1;
		boolean sync_check = (option.sync_weak || (option.sync && option.check_sum_dir != null && option.check_sum_message_digest != null));

		File xml_file;

		while ((xml_file = xml2sphinxds.xml_file_queue.poll()) != null) {

			if (sync_check) {

				try {

					XmlParser xml_parser = new XmlParser(xml_file, xml2pgsql.xml_file_filter);

					Integer _shard_id = doc_rows.get(xml_parser.document_id);

					if (_shard_id != null) {

						if (option.sync_weak)
							continue;

						xml_parser = new XmlParser(xml_file, xml2pgsql.xml_file_filter, option);

						if (xml_parser.identity)
							continue;

						synchronized (xml2sphinxds.sync_lock[_shard_id]) {
							xml2sphinxds.sync_delete_ids[_shard_id].add(xml_parser.document_id);
						}

					}

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			String sph_doc_name = PgSchemaUtil.sph_document_prefix + xml_file.getName().split("\\.")[0] + ".xml";
			File sph_doc_fiole = new File(ds_dir, sph_doc_name);

			try {

				FileWriter writer = new FileWriter(sph_doc_fiole);

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml2sphinxds.xml_file_filter);

				writer.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
				writer.write("<sphinx:document id=\"" + schema.getHashKeyString(xml_parser.document_id) + "\">\n");
				writer.write("<" + option.document_key_name + ">" + StringEscapeUtils.escapeXml10(xml_parser.document_id) + "</" + option.document_key_name + ">\n");

				schema.xml2SphDs(xml_parser, writer);

				writer.write("</sphinx:document>\n");

				writer.close();

			} catch (IOException | SAXException | ParserConfigurationException | TransformerException | PgSchemaException e) {
				e.printStackTrace();
				System.exit(1);
			}

			if (show_progress)
				System.out.print("\rExtracted " + (total - xml2sphinxds.xml_file_queue.size()) + " of " + total + " ...");

		}

		schema.closeXml2SphDs();

		if (max_thrds == 1) {

			try {

				composite();

			} catch (PgSchemaException | IOException | ParserConfigurationException | SAXException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/**
	 * Composite Sphinx data source files
	 *
	 * @throws PgSchemaException the pg schema exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 */
	public void composite() throws PgSchemaException, IOException, ParserConfigurationException, SAXException {

		if (thrd_id != 0)
			return;

		File sph_data_source = new File(ds_dir, PgSchemaUtil.sph_data_source_name);
		File sph_data_extract = new File(sph_data_source.getParent(), PgSchemaUtil.sph_data_extract_name);

		boolean sync_check = ((option.sync_weak || (option.sync && option.check_sum_dir != null && option.check_sum_message_digest != null)) && sph_data_source.exists());

		if (sync_check) {

			if (option.sync_weak)
				FileUtils.copyFile(sph_data_source, sph_data_extract);

			else if (option.sync) {

				SphDsDocIdRemover stax_parser = new SphDsDocIdRemover(schema, sph_data_source, sph_data_extract, xml2sphinxds.sync_delete_ids[thrd_id]);

				try {

					stax_parser.exec();

				} catch (XMLStreamException | IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

		}

		FilenameFilter filter = new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return FilenameUtils.getExtension(name).equals("xml") &&
						name.startsWith(PgSchemaUtil.sph_document_prefix) &&
						!name.equals(PgSchemaUtil.sph_schema_name) &&
						!name.equals(PgSchemaUtil.sph_data_source_name);
			}

		};

		// Sphinx xmlpipe2 writer

		schema.writeSphSchema(sph_data_source, true);

		FileWriter filew = new FileWriter(sph_data_source, true);

		SAXParserFactory spf = SAXParserFactory.newInstance();
		SAXParser sax_parser = spf.newSAXParser();

		System.out.println("Merging" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + "...");

		for (File sph_doc_file : ds_dir.listFiles(filter)) {

			SphDsCompositor handler = new SphDsCompositor(schema, filew, index_filter);

			try {

				sax_parser.parse(sph_doc_file, handler);

			} catch (SAXException | IOException e) {
				e.printStackTrace();
			}

			sph_doc_file.deleteOnExit();

		}

		filew.write("</sphinx:docset>\n");
		filew.close();

		System.out.println("Done" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + ".");

		// Sphinx schema writer for next update or merge

		schema.writeSphSchema(sphinx_schema, false);

		// Sphinx configuration writer

		File sphinx_conf = new File(ds_dir, PgSchemaUtil.sph_conf_name);
		schema.writeSphConf(sphinx_conf, xml2sphinxds.ds_name, sph_data_source);

		if (!sync_check)
			return;

		// Full merge

		System.out.println("Full merge" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + "...");

		File sph_data_update = new File(ds_dir, PgSchemaUtil.sph_data_update_name);

		SphDsDocIdUpdater stax_parser = new SphDsDocIdUpdater(sph_data_source, sph_data_extract, sph_data_update);

		try {

			stax_parser.exec();

		} catch (XMLStreamException | IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		FileUtils.moveFile(sph_data_update, sph_data_source);
		sph_data_extract.delete();

		System.out.println("Done" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + ".");

	}

}
