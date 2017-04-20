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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.HardlinkCopyDirectoryWrapper;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Thread function for xml2luceneidx
 * @author yokochi
 */
public class Xml2LuceneIdxThrd implements Runnable {

	private int shard_id; // shard id
	private int shard_size; // shard size

	private int thrd_id; // thread id
	private int max_thrds; // max threads

	private DocumentBuilder doc_builder; // document builder factory for reuse
	private PgSchema schema = null; // PostgreSQL schema
	private XmlValidator validator = null; // XML validator
	private IndexWriter writer = null; // Lucene index writer

	/**
	 * Instance of Xml2LuceneIdxThrd
	 * @param shard_id shard id
	 * @param shard_size shard size
	 * @param thrd_id thread id
	 * @param max_thrds max threads
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL schema option
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 * @throws PgSchemaException 
	 */
	public Xml2LuceneIdxThrd(final int shard_id, final int shard_size, final int thrd_id, final int max_thrds, final InputStream is, final PgSchemaOption option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, PgSchemaException {

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

		schema = new PgSchema(doc_builder, xsd_doc, null, PgSchemaUtil.getName(xml2luceneidx.schema_location), option);

		// prepare XML validator

		validator = option.validate ? new XmlValidator(PgSchemaUtil.getFile(xml2luceneidx.schema_location, null)) : null;

		// Lucene index writer

		String idx_dir_name = xml2luceneidx.idx_dir_name;

		File idx_dir = new File(idx_dir_name);

		if (shard_size > 1) {

			idx_dir_name += "/" + PgSchemaUtil.shard_dir_prefix + shard_id;

			idx_dir = new File(idx_dir_name);

			if (!idx_dir.isDirectory()) {

				if (!idx_dir.mkdir())
					throw new PgSchemaException("Couldn't create directory '" + idx_dir_name + "'.");

			}

		}

		if (max_thrds > 1) {

			idx_dir_name += "/" + PgSchemaUtil.thrd_dir_prefix + thrd_id;

			idx_dir = new File(idx_dir_name);

			if (!idx_dir.isDirectory()) {

				if (!idx_dir.mkdir())
					throw new PgSchemaException("Couldn't create directory '" + idx_dir_name + "'.");

			}

		}

		Directory dir = FSDirectory.open(idx_dir.toPath());

		IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

		config.setOpenMode(option.append ? OpenMode.CREATE_OR_APPEND : OpenMode.CREATE);

		writer = new IndexWriter(dir, config);

	}

	@Override
	public void run() {

		int proc_sid = 0;
		int proc_tid = 0;

		for (File xml_file : xml2luceneidx.xml_files) {

			if (xml_file.isFile()) {

				if (proc_sid++ % shard_size != shard_id)
					continue;

				if (proc_tid++ % max_thrds != thrd_id)
					continue;

				try {

					XmlParser xml_parser = new XmlParser(doc_builder, validator, xml_file, xml2luceneidx.xml_file_filter);

					org.apache.lucene.document.Document lucene_doc = new org.apache.lucene.document.Document();

					lucene_doc.add(new StringField(PgSchemaUtil.document_key_name, xml_parser.document_id, Field.Store.YES));

					schema.xml2LucIdx(xml_parser, lucene_doc, xml2luceneidx.xml_post_editor, xml2luceneidx.index_filter);

					writer.addDocument(lucene_doc);

				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}

				if (shard_id == 0 && thrd_id == 0)
					System.out.print("\rIndexed " + proc_sid + " of " + xml2luceneidx.xml_files.length + " ...");

			}

		}

		schema.closeXml2LucIdx();

		try {

			writer.forceMerge(1);
			writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Merge Lecene indexes
	 * @throws IOException
	 */
	public void merge() throws IOException {

		if (thrd_id != 0 || max_thrds == 1)
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

		System.out.println("Merging" + (shard_size == 1 ? "" : (" " + (shard_id + 1) + " of " + shard_size + " ")) + "...");

		writer.addIndexes(src_idx);

		System.out.println("Full merge" + (shard_size == 1 ? "" : (" " + (shard_id + 1) + " of " + shard_size + " ")) + "...");

		writer.forceMerge(1);
		writer.close();

		System.out.println("Done" + (shard_size == 1 ? "" : (" " + (shard_id + 1) + " of " + shard_size + " ")) + ".");

		for (int thrd_id = 0; thrd_id < max_thrds; thrd_id++)
			FileUtils.deleteDirectory(new File(src_idx_dir_name[thrd_id]));

	}

}
