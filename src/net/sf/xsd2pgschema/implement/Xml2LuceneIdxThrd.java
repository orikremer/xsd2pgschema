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

package net.sf.xsd2pgschema.implement;

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
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.option.IndexFilter;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.option.XmlFileFilter;
import net.sf.xsd2pgschema.option.XmlPostEditor;
import net.sf.xsd2pgschema.serverutil.PgSchemaClientImpl;
import net.sf.xsd2pgschema.type.PgHashSize;
import net.sf.xsd2pgschema.xmlutil.XmlParser;
import net.sf.xsd2pgschema.xmlutil.XmlValidator;

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

	/** The PostgreSQL data model option. */
	private PgSchemaOption option;

	/** The PgSchema client. */
	private PgSchemaClientImpl client;

	/** The index filter. */
	private IndexFilter index_filter;

	/** The XML validator. */
	private XmlValidator validator;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue;

	/** The index directory path. */
	private Path idx_dir_path;

	/** The Lucene index writers. */
	private IndexWriter[] writers;

	/** The set of document id stored in index (key=document id, value=shard id). */
	private HashMap<String, Integer> doc_rows;

	/** the instance of message digest for hash key. */
	private MessageDigest md_hash_key = null;

	/** The instance of message digest for check sum. */
	private MessageDigest md_chk_sum = null;

	/**
	 * Instance of Xml2LuceneIdxThrd (PgSchema server client).
	 *
	 * @param shard_id shard id
	 * @param shard_size shard size
	 * @param thrd_id thread id
	 * @param get_thrd thread to get a PgSchema server client
	 * @param client_id client id
	 * @param clients array of PgSchema server clients
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param xml_post_editor XML post editor
	 * @param index_filter index filter
	 * @param idx_dir_path index directory path
	 * @param writers array of Lucene index writers
	 * @param doc_rows set of document id stored in index
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 * @throws InterruptedException the interrupted exception
	 */
	public Xml2LuceneIdxThrd(final int shard_id, final int shard_size, final int thrd_id, final Thread get_thrd, final int client_id, final PgSchemaClientImpl[] clients, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final IndexFilter index_filter, final Path idx_dir_path, IndexWriter[] writers, HashMap<String, Integer> doc_rows) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException, InterruptedException {

		if (get_thrd != null)
			get_thrd.join();

		this.client = clients[client_id];

		init(shard_id, shard_size, thrd_id, xml_file_filter, xml_file_queue, xml_post_editor, index_filter, idx_dir_path, writers, doc_rows);

	}

	/**
	 * Instance of Xml2LuceneIdxThrd (stand alone).
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
	 * @param idx_dir_path index directory path
	 * @param writers array of Lucene index writers
	 * @param doc_rows set of document id stored in index
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2LuceneIdxThrd(final int shard_id, final int shard_size, final int thrd_id, final InputStream is, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final PgSchemaOption option, final IndexFilter index_filter, final Path idx_dir_path, IndexWriter[] writers, HashMap<String, Integer> doc_rows) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

		client = new PgSchemaClientImpl(is, option, null, Thread.currentThread().getStackTrace()[2].getClassName());

		init(shard_id, shard_size, thrd_id, xml_file_filter, xml_file_queue, xml_post_editor, index_filter, idx_dir_path, writers, doc_rows);

	}

	/**
	 * Setup Xml2LuceneIdxThrd except for PgSchema server client.
	 *
	 * @param shard_id shard id
	 * @param shard_size shard size
	 * @param thrd_id thread id
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param xml_post_editor XML post editor
	 * @param index_filter index filter
	 * @param idx_dir_path index directory path
	 * @param writers array of Lucene index writers
	 * @param doc_rows set of document id stored in index
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	private void init(final int shard_id, final int shard_size, final int thrd_id, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, final XmlPostEditor xml_post_editor, final IndexFilter index_filter, final Path idx_dir_path, IndexWriter[] writers, HashMap<String, Integer> doc_rows) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

		this.shard_id = shard_id;
		this.shard_size = shard_size;

		this.thrd_id = thrd_id;

		this.xml_file_filter = xml_file_filter;
		this.xml_file_queue = xml_file_queue;

		this.idx_dir_path = idx_dir_path;
		this.writers = writers;
		this.doc_rows = doc_rows;

		option = client.option;

		client.schema.applyXmlPostEditor(xml_post_editor);
		client.schema.applyIndexFilter(this.index_filter = index_filter, option.rel_data_ext);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFilePath(option.root_schema_location, null, option.cache_xsd), option.full_check) : null;

		synchronizable = option.isSynchronizable(true);

		// prepare index writer

		if (thrd_id == 0) {

			Path shard_idx_dir_path = shard_size == 1 ? idx_dir_path : idx_dir_path.resolve(PgSchemaUtil.shard_dir_prefix + shard_id);

			if (!Files.isDirectory(shard_idx_dir_path)) {

				try {
					Files.createDirectory(shard_idx_dir_path);
				} catch (IOException e) {
					throw new PgSchemaException("Couldn't create directory '" + shard_idx_dir_path.toString() + "'.");
				}

			}

			boolean has_idx = synchronizable && Files.list(shard_idx_dir_path).anyMatch(path -> path.getFileName().toString().matches("^segments_.*"));

			IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

			config.setOpenMode(has_idx ? OpenMode.APPEND : OpenMode.CREATE);

			// delete indexes if XML not exists

			writers[shard_id] = new IndexWriter(FSDirectory.open(shard_idx_dir_path), config);

			if (has_idx) {

				HashMap<String, Integer> doc_map = new HashMap<String, Integer>();

				IndexReader reader = DirectoryReader.open(MMapDirectory.open(shard_idx_dir_path));

				for (int i = 0; i < reader.numDocs(); i++)
					doc_map.put(reader.document(i).get(option.document_key_name), i);

				if (option.sync) {

					HashMap<String, Integer> _doc_map = new HashMap<String, Integer>();

					_doc_map.putAll(doc_map);

					xml_file_queue.forEach(xml_file_path -> {

						XmlParser xml_parser = new XmlParser(xml_file_path, xml_file_filter);

						_doc_map.remove(xml_parser.document_id);

					});

					List<Integer> del_ids = _doc_map.entrySet().stream().map(entry -> entry.getValue()).collect(Collectors.toList());

					del_ids.stream().sorted(Comparator.reverseOrder()).forEach(i -> {

						try {

							writers[shard_id].tryDeleteDocument(reader, i);

						} catch (IOException e) {
							e.printStackTrace();
						}

					});;

					del_ids.clear();

					_doc_map.clear();

					writers[shard_id].commit();

					reader.close();

				}

				synchronized (doc_rows) {
					doc_map.entrySet().stream().map(entry -> entry.getKey()).forEach(doc_id -> doc_rows.put(doc_id, shard_id));
				}

				doc_map.clear();

			}

		}

		// prepare message digest for hash key

		if (!option.hash_algorithm.isEmpty() && !option.hash_size.equals(PgHashSize.debug_string))
			md_hash_key = MessageDigest.getInstance(option.hash_algorithm);

		// prepare message digest for check sum

		if (!option.check_sum_algorithm.isEmpty() && synchronizable)
			md_chk_sum = MessageDigest.getInstance(option.check_sum_algorithm);

	}

	/** Whether synchronization is possible. */
	private boolean synchronizable = false;

	/** Whether to show progress. */
	private boolean show_progress = false;

	/** Whether to need to commit. */
	private boolean changed = false;

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		show_progress = shard_id == 0 && thrd_id == 0 && total > 1;

		Integer _shard_id = null;
		IndexWriter writer = writers[shard_id];

		org.apache.lucene.document.Document lucene_doc = new org.apache.lucene.document.Document();
		Term term;

		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
		long start_time = System.currentTimeMillis(), current_time, etc_time;
		int polled = 0, queue_size, progress;
		Date etc_date;

		Path xml_file_path;

		XmlParser xml_parser;

		while ((xml_file_path = xml_file_queue.poll()) != null) {

			if (show_progress) {

				queue_size = xml_file_queue.size();

				if (polled % (queue_size > 100 ? 10 : 1) == 0) {

					current_time = System.currentTimeMillis();

					progress = total - queue_size;

					etc_time = current_time + (current_time - start_time) * queue_size / progress;
					etc_date = new Date(etc_time);

					System.out.print("\rIndexed " + progress + " of " + total + " ... (ETC " + sdf.format(etc_date) + ")");

				}

			}

			if (synchronizable) {

				try {

					xml_parser = new XmlParser(xml_file_path, xml_file_filter);

					_shard_id = doc_rows != null ? doc_rows.get(xml_parser.document_id) : null;

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
				}

			}

			try {

				xml_parser = new XmlParser(client.doc_builder, validator, xml_file_path, xml_file_filter);

				client.schema.xml2LucIdx(xml_parser, md_hash_key, index_filter, lucene_doc);

				if (_shard_id == null)
					writer.addDocument(lucene_doc);

				else {

					term = new Term(option.document_key_name, xml_parser.document_id);

					if (shard_id == _shard_id)
						writer.updateDocument(term, lucene_doc);
					else
						writers[_shard_id].updateDocument(term, lucene_doc);

				}

				lucene_doc.clear();

			} catch (Exception e) {
				System.err.println("Exception occurred while processing XML document: " + xml_file_path.toAbsolutePath().toString());
				e.printStackTrace();
			}

			++polled;

			if (changed)
				continue;

			changed = true;

		}

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

		IndexWriter writer = writers[shard_id];

		try {

			writer.commit();
			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		if (changed)
			System.out.println("Done" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + ".");

		if (synchronizable && show_progress)
			System.out.println((changed ? "" : "\n") + idx_dir_path.toString() + " is up-to-date.");

	}

}
