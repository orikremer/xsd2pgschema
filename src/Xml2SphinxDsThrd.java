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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FilenameUtils;
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

	/** The data source directory path. */
	private Path ds_dir_path = null;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter = null;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue = null;

	/** The SAX parser. */
	private SAXParser sax_parser = null;

	/** The instance of message digest for check sum. */
	private MessageDigest md_chk_sum = null;

	/** The Sphinx schema file path. */
	private Path sphinx_schema_path = null;

	/**
	 * Instance of Xml2SphinxDsThrd.
	 *
	 * @param shard_id shard id
	 * @param shard_size shard size
	 * @param thrd_id thread id
	 * @param is InputStream of XML Schema
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param xml_post_editor XML post editor
	 * @param option PostgreSQL data model option
	 * @param index_filter index filter
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2SphinxDsThrd(final int shard_id, final int shard_size, final int thrd_id, final InputStream is, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final PgSchemaOption option, IndexFilter index_filter) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

		this.shard_id = shard_id;
		this.shard_size = shard_size;

		this.thrd_id = thrd_id;

		this.xml_file_filter = xml_file_filter;
		this.xml_file_queue = xml_file_queue;

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

		schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, this.option = option);

		schema.applyXmlPostEditor(xml_post_editor);

		schema.applyIndexFilter(this.index_filter = index_filter);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFilePath(option.root_schema_location, null, option.cache_xsd), option.full_check) : null;

		String ds_dir_name = xml2sphinxds.ds_dir_name;

		if (shard_size > 1)
			ds_dir_name += "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

		ds_dir_path = Paths.get(ds_dir_name);

		if (!Files.isDirectory(ds_dir_path))
			Files.createDirectory(ds_dir_path);

		// parse the previous Sphinx schema file if exists

		sphinx_schema_path = Paths.get(ds_dir_name, PgSchemaUtil.sph_schema_name);

		if (Files.isRegularFile(sphinx_schema_path)) {

			doc_builder_fac.setNamespaceAware(false);
			doc_builder = doc_builder_fac.newDocumentBuilder();

			Document sphinx_doc = doc_builder.parse(Files.newInputStream(sphinx_schema_path));

			doc_builder.reset();

			schema.syncSphSchema(sphinx_doc);

			sphinx_doc = null;

		}

		synchronizable = option.isSynchronizable(true);

		// delete indexes if XML not exists

		Path sph_data_source_path = Paths.get(ds_dir_name, PgSchemaUtil.sph_data_source_name);

		if (synchronizable && Files.isRegularFile(sph_data_source_path) && thrd_id == 0) {

			HashSet<String> doc_set = new HashSet<String>();

			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setValidating(false);
			spf.setNamespaceAware(true);
			sax_parser = spf.newSAXParser();

			SphDsDocIdExtractor handler = new SphDsDocIdExtractor(option.document_key_name, doc_set);

			try {

				sax_parser.parse(Files.newInputStream(sph_data_source_path), handler);

			} catch (SAXException | IOException e) {
				e.printStackTrace();
			}

			sax_parser.reset();

			if (option.sync) {

				xml2sphinxds.sync_del_doc_rows[shard_id].addAll(doc_set);

				xml_file_queue.forEach(xml_file_path -> {

					XmlParser xml_parser = new XmlParser(xml_file_path, xml_file_filter);

					xml2sphinxds.sync_del_doc_rows[shard_id].remove(xml_parser.document_id);

				});

				synchronized (xml2sphinxds.doc_rows) {
					doc_set.forEach(doc_id -> xml2sphinxds.doc_rows.put(doc_id, shard_id));
				}

				doc_set.clear();

			}

		}

		// prepare message digest for check sum

		if (!option.check_sum_algorithm.isEmpty() && synchronizable)
			md_chk_sum = MessageDigest.getInstance(option.check_sum_algorithm);

	}

	/** Whether if synchronizable or not. */
	private boolean synchronizable = false;

	/** Whether show progress or not. */
	private boolean show_progress = false;

	/** Whether need to commit or not. */
	private boolean changed = false;

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		show_progress = shard_id == 0 && thrd_id == 0 && total > 1;

		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");

		long start_time = System.currentTimeMillis();

		Path xml_file_path;

		while ((xml_file_path = xml_file_queue.poll()) != null) {

			if (show_progress) {

				long current_time = System.currentTimeMillis();

				int remains = xml_file_queue.size();
				int progress = total - remains;

				long etc_time = current_time + (current_time - start_time) * remains / progress;
				Date etc_date = new Date(etc_time);

				System.out.print("\rExtracted " + progress + " of " + total + " ... (ETC " + sdf.format(etc_date) + ")");

			}

			if (synchronizable) {

				try {

					XmlParser xml_parser = new XmlParser(xml_file_path, xml_file_filter);

					Integer _shard_id = xml2sphinxds.doc_rows != null ? xml2sphinxds.doc_rows.get(xml_parser.document_id) : null;

					if (_shard_id != null) {

						if (option.sync_weak)
							continue;

						if (xml_parser.identify(option, md_chk_sum))
							continue;

						synchronized (xml2sphinxds.sync_del_doc_rows[_shard_id]) {
							xml2sphinxds.sync_del_doc_rows[_shard_id].add(xml_parser.document_id);
							changed = true;
						}

					}

					else if (option.sync)
						xml_parser.identify(option, md_chk_sum);

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			String sph_doc_name = PgSchemaUtil.sph_document_prefix + xml_file_path.getFileName().toString().split("\\.")[0] + ".xml";

			Path sph_doc_file_path = Paths.get(ds_dir_path.toString(), sph_doc_name);

			try {

				BufferedWriter buffw = Files.newBufferedWriter(sph_doc_file_path);

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file_path, xml_file_filter);

				schema.xml2SphDs(xml_parser, buffw);

				buffw.close();

			} catch (IOException | SAXException | PgSchemaException e) {
				System.err.println("Exception occurred while processing XML document: " + xml_file_path.toAbsolutePath().toString());
				e.printStackTrace();
				System.exit(1);
			}

			if (changed)
				continue;

			changed = true;

		}

		schema.closeXml2SphDs();

	}

	/**
	 * Composite Sphinx data source file (xmlpipe2)
	 *
	 * @throws PgSchemaException the pg schema exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 */
	public void composite() throws PgSchemaException, IOException, ParserConfigurationException, SAXException {

		if (thrd_id != 0)
			return;

		String ds_dir_name = ds_dir_path.toString();

		if (show_progress && changed)
			System.out.println("");

		try {

			Path sph_data_source_path = Paths.get(ds_dir_name, PgSchemaUtil.sph_data_source_name);
			Path sph_data_extract_path = Paths.get(ds_dir_name, PgSchemaUtil.sph_data_extract_name);

			// sync-delete documents from the previous xmlpipe2

			boolean has_idx = Files.isRegularFile(sph_data_source_path);

			if (has_idx) {

				if (option.sync && xml2sphinxds.sync_del_doc_rows[shard_id].size() > 0) {

					System.out.println("Cleaning" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + "...");

					SphDsDocIdCleaner stax_parser = new SphDsDocIdCleaner(option.document_key_name, sph_data_source_path, sph_data_extract_path, xml2sphinxds.sync_del_doc_rows[shard_id]);

					try {

						stax_parser.exec();

					} catch (XMLStreamException | IOException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

				else
					Files.copy(sph_data_source_path, sph_data_extract_path, StandardCopyOption.REPLACE_EXISTING);

			}

			FilenameFilter filter = new FilenameFilter() {

				public boolean accept(File dir, String name) {
					return FilenameUtils.getExtension(name).equals("xml") &&
							name.startsWith(PgSchemaUtil.sph_document_prefix) &&
							!name.equals(PgSchemaUtil.sph_schema_name) &&
							!name.equals(PgSchemaUtil.sph_data_source_name) &&
							!name.equals(PgSchemaUtil.sph_data_extract_name) &&
							!name.equals(PgSchemaUtil.sph_data_update_name);
				}

			};

			// composite a xmlpipe2 from partial documents

			schema.writeSphSchema(sph_data_source_path, true);

			BufferedWriter buffw = Files.newBufferedWriter(sph_data_source_path, StandardOpenOption.APPEND);

			if (sax_parser == null) {

				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setValidating(false);
				spf.setNamespaceAware(true);
				sax_parser = spf.newSAXParser();

			}

			List<Path> sph_doc_file_paths = Files.list(ds_dir_path).filter(sph_doc_file_path -> Files.isRegularFile(sph_doc_file_path) && filter.accept(null, sph_doc_file_path.getFileName().toString())).collect(Collectors.toList());

			int total = sph_doc_file_paths.size();

			if (total > 0)
				System.out.println("Merging" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + "...");

			int proc = 0;

			for (Path sph_doc_file_path : sph_doc_file_paths) {

				SphDsCompositor handler = new SphDsCompositor(option.document_key_name, schema.getSphAttrs(), schema.getSphMVAs(), buffw, index_filter);

				try {

					sax_parser.parse(Files.newInputStream(sph_doc_file_path), handler);

				} catch (SAXException | IOException e) {
					e.printStackTrace();
				}

				sax_parser.reset();

				Files.delete(sph_doc_file_path);

				if (show_progress)
					System.out.print("\rMerged " + (++proc) + " of " + total + " ...");

			}

			buffw.write("</sphinx:docset>\n");

			buffw.close();

			if (changed)
				System.out.println("\nDone" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + ".");

			// write Sphinx schema file for next update or merge

			schema.writeSphSchema(sphinx_schema_path, false);

			// write Sphinx configuration file

			Path sphinx_conf_path = Paths.get(ds_dir_name, PgSchemaUtil.sph_conf_name);

			schema.writeSphConf(sphinx_conf_path, xml2sphinxds.ds_name, sph_data_source_path);

			if (!has_idx)
				return;

			else if (total == 0) {

				Files.move(sph_data_extract_path, sph_data_source_path, StandardCopyOption.REPLACE_EXISTING);

				return;
			}

			// merge xmlpipe2 with the previous one

			System.out.println("Full merge" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + "...");

			Path sph_data_update_path = Paths.get(ds_dir_name, PgSchemaUtil.sph_data_update_name);

			SphDsDocIdUpdater stax_parser = new SphDsDocIdUpdater(sph_data_source_path, sph_data_extract_path, sph_data_update_path);

			try {

				stax_parser.exec();

			} catch (XMLStreamException | IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

			Files.move(sph_data_update_path, sph_data_source_path, StandardCopyOption.REPLACE_EXISTING);

			Files.delete(sph_data_extract_path);

			System.out.println("Done" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + ".");

		} finally {

			if (synchronizable && show_progress)
				System.out.println((changed ? "" : "\n") + ds_dir_path.toAbsolutePath().toString() + " (" + xml2sphinxds.ds_name + ") is up-to-date.");

		}

	}

}
