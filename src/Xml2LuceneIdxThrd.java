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
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.HardlinkCopyDirectoryWrapper;
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

	/** The max threads. */
	private int max_thrds;

	/** The doc builder for reusing. */
	private DocumentBuilder doc_builder;

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option = null;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter = null;

	/** The XML file queue. */
	private LinkedBlockingQueue<File> xml_file_queue = null;

	/** The Lucene index writer. */
	private IndexWriter writer = null;

	/** The document id stored in index. */
	private HashMap<String, Integer> doc_rows = null;

	/**
	 * Instance of Xml2LuceneIdxThrd.
	 *
	 * @param shard_id shard id
	 * @param shard_size shard size
	 * @param thrd_id thread id
	 * @param max_thrds max threads
	 * @param is InputStream of XML Schema
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param option PostgreSQL data model option
	 * @param index_filter index filter
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public Xml2LuceneIdxThrd(final int shard_id, final int shard_size, final int thrd_id, final int max_thrds, final InputStream is, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<File> xml_file_queue, final PgSchemaOption option, final IndexFilter index_filter) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

		this.shard_id = shard_id;
		this.shard_size = shard_size;

		this.thrd_id = thrd_id;
		this.max_thrds = max_thrds;

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

		schema = new PgSchema(doc_builder, xsd_doc, null, xml2luceneidx.schema_location, this.option = option);

		schema.applyXmlPostEditor(xml2luceneidx.xml_post_editor);

		schema.applyIndexFilter(index_filter);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getSchemaFile(xml2luceneidx.schema_location, null, option.cache_xsd)) : null;

		// Lucene index writer

		String idx_dir_name = xml2luceneidx.idx_dir_name;

		if (shard_size > 1)
			idx_dir_name += "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

		if (max_thrds > 1)
			idx_dir_name += "/" + PgSchemaUtil.thrd_dir_prefix + thrd_id;

		File idx_dir = new File(idx_dir_name);

		if (!idx_dir.isDirectory()) {

			if (!idx_dir.mkdir())
				throw new PgSchemaException("Couldn't create directory '" + idx_dir_name + "'.");

		}

		Directory dir = FSDirectory.open(idx_dir.toPath());

		IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

		idx_dir_name = xml2luceneidx.idx_dir_name;

		if (shard_size > 1)
			idx_dir_name += "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

		Path idx_dir_path = Paths.get(idx_dir_name);

		boolean has_idx = option.isSynchronizable() && Files.list(idx_dir_path).anyMatch(path -> path.getFileName().toString().matches("^segments_[0-9]+"));

		config.setOpenMode(has_idx ? OpenMode.CREATE_OR_APPEND : OpenMode.CREATE);

		writer = new IndexWriter(dir, config);

		// delete indexes if XML not exists

		if (has_idx) {

			if (thrd_id == 0)
				xml2luceneidx.sync_writer[shard_id] = writer;

			HashMap<String, Integer> doc_map = new HashMap<String, Integer>();

			IndexReader reader = DirectoryReader.open(MMapDirectory.open(idx_dir_path));

			for (int i = 0; i < reader.numDocs(); i++)
				doc_map.put(reader.document(i).get(option.document_key_name), i);

			if (option.sync) {

				if (thrd_id == 0) {

					HashMap<String, Integer> _doc_map = new HashMap<String, Integer>();

					_doc_map.putAll(doc_map);

					xml_file_queue.forEach(xml_file -> {

						try {

							XmlParser xml_parser = new XmlParser(xml_file, xml_file_filter);

							_doc_map.remove(xml_parser.document_id);

						} catch (IOException e) {
							e.printStackTrace();
							System.exit(1);
						}

					});

					List<Integer> del_ids = _doc_map.entrySet().stream().map(entry -> entry.getValue()).collect(Collectors.toList());

					del_ids.stream().sorted(Comparator.reverseOrder()).forEach(i -> {

						try {

							writer.tryDeleteDocument(reader, i);

						} catch (IOException e) {
							e.printStackTrace();
							System.exit(1);
						}

					});;

					del_ids.clear();

					_doc_map.clear();

					writer.commit();

					synchronized (xml2luceneidx.sync_lock[shard_id]) {
						xml2luceneidx.sync_lock[shard_id].notifyAll();
					}

				}

				else {

					try {

						synchronized (xml2luceneidx.sync_lock[shard_id]) {
							xml2luceneidx.sync_lock[shard_id].wait();
						}

					} catch (InterruptedException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

				reader.close();

			}

			doc_rows = new HashMap<String, Integer>();

			doc_map.entrySet().stream().map(entry -> entry.getKey()).forEach(doc_id -> doc_rows.put(doc_id, shard_id));

			doc_map.clear();

			if (shard_size > 1) {

				for (int _shard_id = 0; _shard_id < shard_size; _shard_id++) {

					if (_shard_id == shard_id)
						continue;

					Path _idx_dir_path = Paths.get(xml2luceneidx.idx_dir_name + "/" + PgSchemaUtil.shard_dir_prefix + _shard_id);

					if (!Files.list(_idx_dir_path).anyMatch(path -> path.getFileName().toString().matches("^segments_[0-9]+")))
						continue;

					IndexReader _reader = DirectoryReader.open(MMapDirectory.open(_idx_dir_path));

					for (int i = 0; i < _reader.numDocs(); i++)
						doc_rows.put(_reader.document(i).get(option.document_key_name), _shard_id);

					_reader.close();

				}

			}

		}

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		boolean show_progress = shard_id == 0 && thrd_id == 0 && total > 1;
		boolean synchronizable = option.isSynchronizable();
		Integer _shard_id = null;

		File xml_file;

		while ((xml_file = xml_file_queue.poll()) != null) {

			if (synchronizable) {

				try {

					XmlParser xml_parser = new XmlParser(xml_file, xml_file_filter);

					_shard_id = doc_rows != null ? doc_rows.get(xml_parser.document_id) : null;

					if (_shard_id != null) {

						if (option.sync_weak)
							continue;

						xml_parser = new XmlParser(xml_file, xml_file_filter, option);

						if (xml_parser.identity)
							continue;

					}

					else if (option.sync)
						new XmlParser(xml_file, xml_file_filter, option);

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			try {

				XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml_file_filter);

				org.apache.lucene.document.Document lucene_doc = new org.apache.lucene.document.Document();

				lucene_doc.add(new StringField(option.document_key_name, xml_parser.document_id, Field.Store.YES));

				schema.xml2LucIdx(xml_parser, lucene_doc);

				if (_shard_id == null)
					writer.addDocument(lucene_doc);

				else {

					Term term = new Term(option.document_key_name, schema.getDocumentId());

					if (shard_id == _shard_id)
						writer.updateDocument(term, lucene_doc);
					else
						xml2luceneidx.sync_writer[_shard_id].updateDocument(term, lucene_doc);

				}


			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

			if (show_progress)
				System.out.print("\rIndexed " + (total - xml_file_queue.size()) + " of " + total + " ...");

		}

		schema.closeXml2LucIdx();

		try {

			writer.commit();
			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Merge Lucene indices.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void merge() throws IOException {

		if (thrd_id != 0 || max_thrds < 2)
			return;

		String dst_idx_dir_name = xml2luceneidx.idx_dir_name;

		if (shard_size > 1)
			dst_idx_dir_name += "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

		FSDirectory dst_idx = FSDirectory.open(Paths.get(dst_idx_dir_name));

		IndexWriter writer = new IndexWriter(dst_idx, new IndexWriterConfig(null).setOpenMode(OpenMode.CREATE));

		String[] src_idx_dir_name = new String[max_thrds];
		Directory[] src_idx = new Directory[max_thrds];

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++) {

			src_idx_dir_name[thrd_id] = dst_idx_dir_name + "/" + PgSchemaUtil.thrd_dir_prefix + thrd_id;

			// try to use hardlinks if possible
			src_idx[thrd_id] = new HardlinkCopyDirectoryWrapper(FSDirectory.open(Paths.get(src_idx_dir_name[thrd_id])));

		}

		System.out.println("Merging" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + "...");

		writer.addIndexes(src_idx);

		System.out.println("Full merge" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + "...");

		writer.forceMerge(1);
		writer.close();

		System.out.println("Done" + (shard_size == 1 ? "" : (" #" + (shard_id + 1) + " of " + shard_size + " ")) + ".");

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++)
			FileUtils.deleteDirectory(new File(src_idx_dir_name[thrd_id]));

	}

}
