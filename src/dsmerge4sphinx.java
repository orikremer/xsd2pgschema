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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.*;

import org.apache.commons.io.FilenameUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Merge Sphinx data sources.
 * 
 * @author yokochi
 */
public class dsmerge4sphinx {

	/** The schema location. */
	public static String schema_location = "";

	/** The data source name. */
	public static String ds_name = "";

	/** The destination directory name of data source. */
	public static String dst_ds_dir_name = xml2sphinxds.ds_dir_name;

	/** The source directory name of data source. */
	public static List<String> src_ds_dir_list = new ArrayList<String>();

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd"))
				schema_location = args[++i];

			else if (args[i].equals("--src-ds-dir"))
				src_ds_dir_list.add(args[++i]);

			else if (args[i].equals("--dst-ds-dir"))
				dst_ds_dir_name = args[++i];

			else if (args[i].equals("--ds-name"))
				ds_name = args[++i];

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		InputStream is = PgSchemaUtil.getInputStream(schema_location, null);

		if (is == null)
			showUsage();

		if (src_ds_dir_list.size() == 0) {
			System.err.println("There is no source direcotry to merge.");
			showUsage();
		}

		for (String src_ds_dir_name : src_ds_dir_list) {

			File src_ds_dir = new File(src_ds_dir_name);

			if (!src_ds_dir.isDirectory()) {
				System.err.println("Not a directory '" + src_ds_dir_name + "'.");
				System.exit(1);
			}

			File src_sphinx_schema = new File(src_ds_dir_name, PgSchemaUtil.sphinx_schema_name);

			if (!src_sphinx_schema.exists()) {
				System.err.println("Not found '" + PgSchemaUtil.sphinx_schema_name + "' in '" + src_ds_dir_name + "' directory.");
				System.exit(1);
			}

			File src_sphinx_data_source = new File(src_ds_dir_name, PgSchemaUtil.sphinx_data_source_name);

			if (!src_sphinx_data_source.exists()) {
				System.err.println("Not found '" + PgSchemaUtil.sphinx_data_source_name + "' in '" + src_ds_dir_name + "' directory.");
				System.exit(1);
			}

			if (ds_name.isEmpty()) {

				File src_sphinx_conf = new File(src_ds_dir_name, PgSchemaUtil.sphinx_conf_name);

				if (src_sphinx_conf.exists()) {

					try {

						FileReader filer = new FileReader(src_sphinx_conf);
						BufferedReader bufferr = new BufferedReader(filer);

						String line = null;

						while ((line = bufferr.readLine()) != null) {

							if (line.startsWith("source ")) {

								String[] src_name = line.split(" ");

								if (src_name.length > 1) {

									ds_name = src_name[1];

									break;
								}

							}

						}

						bufferr.close();
						filer.close();

					} catch (IOException e) {
						e.printStackTrace();
					}

				}

			}

		}

		if (ds_name.isEmpty()) {

			ds_name = PgSchemaUtil.getFileName(schema_location);

			String xsd_file_ext = FilenameUtils.getExtension(ds_name);

			if (xsd_file_ext != null && !xsd_file_ext.isEmpty())
				ds_name = ds_name.replaceAll("\\." + xsd_file_ext + "$", "");

		}

		File dst_ds_dir = new File(dst_ds_dir_name);

		if (!dst_ds_dir.isDirectory()) {

			if (!dst_ds_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + dst_ds_dir_name + "'.");
				System.exit(1);
			}

		}

		try {

			// parse XSD document

			DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
			doc_builder_fac.setValidating(false);
			doc_builder_fac.setNamespaceAware(true);
			doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
			doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			DocumentBuilder	doc_builder = doc_builder_fac.newDocumentBuilder();

			Document xsd_doc = doc_builder.parse(is);

			is.close();

			doc_builder.reset();

			// XSD analysis

			PgSchema schema = new PgSchema(doc_builder, xsd_doc, null, PgSchemaUtil.getName(schema_location), new PgSchemaOption(false));

			for (String src_ds_dir_name : src_ds_dir_list) {

				File src_sphinx_schema = new File(src_ds_dir_name, PgSchemaUtil.sphinx_schema_name);

				doc_builder_fac.setNamespaceAware(false);
				doc_builder = doc_builder_fac.newDocumentBuilder();

				Document src_sphinx_doc = doc_builder.parse(src_sphinx_schema);

				doc_builder.reset();

				schema.syncSphSchema(src_sphinx_doc);

				src_sphinx_doc = null;

			}

			// Sphinx xmlpipe2 writer

			File dst_sphinx_data_source = new File(dst_ds_dir_name, PgSchemaUtil.sphinx_data_source_name);
			schema.writeSphSchema(dst_sphinx_data_source, true);

			FileWriter filew = new FileWriter(dst_sphinx_data_source, true);

			for (String src_ds_dir_name : src_ds_dir_list) {

				File src_sphinx_data_source = new File(src_ds_dir_name, PgSchemaUtil.sphinx_data_source_name);

				mergeDataSource(schema, filew, src_sphinx_data_source);

			}

			filew.write("</sphinx:docset>\n");
			filew.close();

			// Sphinx schema writer for next update or merge

			File dst_sphinx_schema = new File(dst_ds_dir_name, PgSchemaUtil.sphinx_schema_name);

			schema.writeSphSchema(dst_sphinx_schema, false);

			// Sphinx configuration writer

			File sphinx_conf = new File(dst_ds_dir_name, PgSchemaUtil.sphinx_conf_name);
			schema.writeSphConf(sphinx_conf, ds_name, dst_sphinx_data_source);

		} catch (ParserConfigurationException | SAXException | IOException | NoSuchAlgorithmException | PgSchemaException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Merge data source.
	 *
	 * @param schema the schema
	 * @param filew the filew
	 * @param sph_doc the sph doc
	 */
	private static void mergeDataSource(PgSchema schema, FileWriter filew, File sph_doc) {

		try {

			FileReader filer = new FileReader(sph_doc);
			BufferedReader bufferr = new BufferedReader(filer);

			boolean doc_start = false;

			String line = null;

			while ((line = bufferr.readLine()) != null) {

				if (line.contains("</sphinx:schema>"))
					doc_start = true;

				else if (doc_start) {

					if (line.contains("</sphinx:docset>"))
						break;

					filew.write(line + "\n");

				}

			}

			bufferr.close();
			filer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("dsmerge4sphinx: Merge Sphinx data source files into one");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --dst-ds-dir DIRECTORY (default=\"" + dst_ds_dir_name + "\") --src-ds-dir DIRECTORY (repeat until you specify all directories)");
		System.err.println("Option: --ds-name DS_NAME (default name is determined by data_source.conf file)");
		System.exit(1);

	}

}
