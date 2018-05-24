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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Thread function for xml2luceneidx.
 *
 * @author yokochi
 */
public class Xml2LuceneIdxThrd implements Runnable {

	/** The shard id. */
	private int shard_id;

	/** The shard size. */
	private int shard_size;

	/** The thread id. */
	private int thrd_id;

	/** The doc builder for reusing. */
	private DocumentBuilder doc_builder;

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option = null;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The index directory. */
	private File idx_dir = null;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter = null;

	/** The XML file queue. */
	private LinkedBlockingQueue<File> xml_file_queue = null;

	/** The instance of message digest for check sum. */
	private MessageDigest md_chk_sum = null;

	/**
	 * Instance of Xml2LuceneIdxThrd.
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
	public Xml2LuceneIdxThrd(final int shard_id, final int shard_size, final int thrd_id, final InputStream is, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<File> xml_file_queue, final XmlPostEditor xml_post_editor, final PgSchemaOption option, final IndexFilter index_filter) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

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

		schema.applyIndexFilter(index_filter);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFile(option.root_schema_location, null, option.cache_xsd), option.full_check) : null;

		synchronizable = option.isSynchronizable(true);

		// prepare index writer

		if (thrd_id == 0) {

			String idx_dir_name = xml2luceneidx.idx_dir_name;

			if (shard_size > 1)
				idx_dir_name += "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

			idx_dir = new File(idx_dir_name);

			if (!idx_dir.isDirectory()) {

				if (!idx_dir.mkdir())
					throw new PgSchemaException("Couldn't create directory '" + idx_dir_name + "'.");

			}

			Path idx_dir_path = idx_dir.toPath();

			boolean has_idx = synchronizable && Files.list(idx_dir_path).anyMatch(path -> path.getFileName().toString().matches("^segments_.*"));

			IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

			config.setOpenMode(has_idx ? OpenMode.APPEND : OpenMode.CREATE);

			// delete indexes if XML not exists

			xml2luceneidx.writers[shard_id] = new IndexWriter(FSDirectory.open(idx_dir_path), config);

			if (has_idx) {

				HashMap<String, Integer> doc_map = new HashMap<String, Integer>();

				IndexReader reader = DirectoryReader.open(MMapDirectory.open(idx_dir_path));

				for (int i = 0; i < reader.numDocs(); i++)
					doc_map.put(reader.document(i).get(option.document_key_name), i);

				if (option.sync) {

					HashMap<String, Integer> _doc_map = new HashMap<String, Integer>();

					_doc_map.putAll(doc_map);

					xml_file_queue.forEach(xml_file -> {

						XmlParser xml_parser = new XmlParser(xml_file, xml_file_filter);

						_doc_map.remove(xml_parser.document_id);

					});

					List<Integer> del_ids = _doc_map.entrySet().stream().map(entry -> entry.getValue()).collect(Collectors.toList());

					del_ids.stream().sorted(Comparator.reverseOrder()).forEach(i -> {

						try {

							xml2luceneidx.writers[shard_id].tryDeleteDocument(reader, i);

						} catch (IOException e) {
							e.printStackTrace();
							System.exit(1);
						}

					});;

					del_ids.clear();

					_doc_map.clear();

					xml2luceneidx.writers[shard_id].commit();

					reader.close();

				}

				synchronized (xml2luceneidx.doc_rows) {
					doc_map.entrySet().stream().map(entry -> entry.getKey()).forEach(doc_id -> xml2luceneidx.doc_rows.put(doc_id, shard_id));
				}

				doc_map.clear();

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

		Integer _shard_id = null;
		IndexWriter writer = xml2luceneidx.writers[shard_id];

		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");

		long start_time = System.currentTimeMillis();

		File xml_file;

		while ((xml_file = xml_file_queue.poll()) != null) {

			if (show_progress) {

				long current_time = System.currentTimeMillis();

				int remains = xml_file_queue.size();
				int progress = total - remains;

				long etc_time = current_time + (current_time - start_time) * remains / progress;
				Date etc_date = new Date(etc_time);

				System.out.print("\rIndexed " + progress + " of " + total + " ... (ETC " + sdf.format(etc_date) + ")");

			}

			if (synchronizable) {

				try {

					XmlParser xml_parser = new XmlParser(xml_file, xml_file_filter);

					_shard_id = xml2luceneidx.doc_rows != null ? xml2luceneidx.doc_rows.get(xml_parser.document_id) : null;

					if (_shard_id != null) {

						if (option.sync_weak)
							continue;

						if (xml_parser.identify(option, md_chk_sum))
							continue;

					}

					else if (option.sync)
						xml_parser.identify(option, md_chk_sum);

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			try {

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml_file_filter);

				org.apache.lucene.document.Document lucene_doc = new org.apache.lucene.document.Document();

				schema.xml2LucIdx(xml_parser, lucene_doc);

				if (_shard_id == null)
					writer.addDocument(lucene_doc);

				else {

					Term term = new Term(option.document_key_name, xml_parser.document_id);

					if (shard_id == _shard_id)
						writer.updateDocument(term, lucene_doc);
					else
						xml2luceneidx.writers[_shard_id].updateDocument(term, lucene_doc);

				}

				lucene_doc.clear();

			} catch (Exception e) {
				System.err.println("Exception while processing XML document: " + xml_file.getName());
				e.printStackTrace();
				System.exit(1);
			}

			if (changed)
				continue;

			changed = true;

		}

		schema.closeXml2LucIdx();

	}

	/**
	 * Close index writer.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void close() throws IOException {

		if (thrd_id != 0)
			return;

		if (show_progress && changed)
			System.out.println("\nCommiting" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + "...");

		IndexWriter writer = xml2luceneidx.writers[shard_id];

		try {

			writer.commit();
			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		if (changed)
			System.out.println("Done" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + ".");

		if (synchronizable && show_progress)
			System.out.println((changed ? "" : "\n") + idx_dir.getAbsolutePath() + " is up-to-date.");

	}

}
