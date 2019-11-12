/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2019 Masashi Yokochi

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
import net.sf.xsd2pgschema.option.*;
import net.sf.xsd2pgschema.serverutil.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.*;

import org.apache.commons.io.FilenameUtils;
import org.nustaq.serialization.FSTConfiguration;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Merge Sphinx data sources.
 *
 * @author yokochi
 */
public class dsmerge4sphinx {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(false);

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class,PgSchema.class); // FST optimization

		/** The data source name. */
		String ds_name = "";

		/** The destination directory name of data source. */
		String dst_ds_dir_name = xml2sphinxds.ds_dir_name;

		/** The source directory name of data source. */
		List<String> src_ds_dir_list = new ArrayList<String>();

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--xsd") && i + 1 < args.length)
				option.root_schema_location = args[++i];

			else if (args[i].equals("--src-ds-dir") && i + 1 < args.length)
				src_ds_dir_list.add(args[++i]);

			else if (args[i].equals("--dst-ds-dir") && i + 1 < args.length)
				dst_ds_dir_name = args[++i];

			else if (args[i].equals("--ds-name") && i + 1 < args.length)
				ds_name = args[++i];

			else if (args[i].equals("--no-pgschema-serv"))
				option.pg_schema_server = false;

			else if (args[i].equals("--pgschema-serv-host") && i + 1 < args.length)
				option.pg_schema_server_host = args[++i];

			else if (args[i].equals("--pgschema-serv-port") && i + 1 < args.length)
				option.pg_schema_server_port = Integer.valueOf(args[++i]);

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (option.root_schema_location.isEmpty()) {
			System.err.println("XSD schema location is empty.");
			showUsage();
		}

		PgSchemaClientType client_type = PgSchemaClientType.full_text_indexing;

		InputStream is = null;

		boolean server_alive = option.pingPgSchemaServer(fst_conf);
		boolean no_data_model = server_alive ? !option.matchPgSchemaServer(fst_conf, client_type) : true;

		if (no_data_model) {

			is = PgSchemaUtil.getSchemaInputStream(option.root_schema_location, null, false);

			if (is == null)
				showUsage();

		}

		if (src_ds_dir_list.size() == 0) {
			System.err.println("There is no source direcotry to merge.");
			showUsage();
		}

		for (String src_ds_dir_name : src_ds_dir_list) {

			Path src_ds_dir_path = Paths.get(src_ds_dir_name);

			if (!Files.isDirectory(src_ds_dir_path)) {
				System.err.println("Not a directory '" + src_ds_dir_name + "'.");
				System.exit(1);
			}

			Path src_sphinx_schema_path = Paths.get(src_ds_dir_name, PgSchemaUtil.sph_schema_name);

			if (!Files.isRegularFile(src_sphinx_schema_path)) {
				System.err.println("Not found '" + PgSchemaUtil.sph_schema_name + "' in '" + src_ds_dir_name + "' directory.");
				System.exit(1);
			}

			Path src_sphinx_data_source_path = Paths.get(src_ds_dir_name, PgSchemaUtil.sph_data_source_name);

			if (!Files.isRegularFile(src_sphinx_data_source_path)) {
				System.err.println("Not found '" + PgSchemaUtil.sph_data_source_name + "' in '" + src_ds_dir_name + "' directory.");
				System.exit(1);
			}

			if (ds_name.isEmpty()) {

				Path src_sphinx_conf_path = Paths.get(src_ds_dir_name, PgSchemaUtil.sph_conf_name);

				if (Files.isRegularFile(src_sphinx_conf_path)) {

					try {

						BufferedReader buffr = Files.newBufferedReader(src_sphinx_conf_path);

						String line = null;
						String[] src_name;

						while ((line = buffr.readLine()) != null) {

							if (line.startsWith("source ")) {

								src_name = line.split(" ");

								if (src_name.length > 1) {

									ds_name = src_name[1];

									break;
								}

							}

						}

						buffr.close();

					} catch (IOException e) {
						e.printStackTrace();
					}

				}

			}

		}

		if (ds_name.isEmpty()) {

			ds_name = PgSchemaUtil.getSchemaFileName(option.root_schema_location);

			String xsd_file_ext = FilenameUtils.getExtension(ds_name);

			if (xsd_file_ext != null && !xsd_file_ext.isEmpty())
				ds_name = ds_name.replaceAll("\\." + xsd_file_ext + "$", "");

		}

		Path dst_ds_dir_path = Paths.get(dst_ds_dir_name);

		try {

			if (!Files.isDirectory(dst_ds_dir_path))
				Files.createDirectory(dst_ds_dir_path);

			IndexFilter index_filter = null; // dummy

			PgSchemaClientImpl client = new PgSchemaClientImpl(is, option, fst_conf, client_type, MethodHandles.lookup().lookupClass().getName(), null, index_filter, true);

			for (String src_ds_dir_name : src_ds_dir_list) {

				Path src_sphinx_schema_path = Paths.get(src_ds_dir_name, PgSchemaUtil.sph_schema_name);

				try {

					client.doc_builder_fac.setNamespaceAware(false);
					DocumentBuilder doc_builder = client.doc_builder_fac.newDocumentBuilder();

					Document src_sphinx_doc = doc_builder.parse(Files.newInputStream(src_sphinx_schema_path));

					doc_builder.reset();

					client.schema.syncSphSchema(src_sphinx_doc);

					src_sphinx_doc = null;

				} catch (SAXException e) {
				}

			}

			// Sphinx xmlpipe2 writer

			Path dst_sphinx_data_source_path = Paths.get(dst_ds_dir_name, PgSchemaUtil.sph_data_source_name);

			client.schema.writeSphSchema(dst_sphinx_data_source_path, true);

			BufferedWriter buffw = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(dst_sphinx_data_source_path), PgSchemaUtil.def_encoding), PgSchemaUtil.def_buffered_output_stream_buffer_size);

			for (String src_ds_dir_name : src_ds_dir_list) {

				Path src_sphinx_data_source_path = Paths.get(src_ds_dir_name, PgSchemaUtil.sph_data_source_name);

				mergeDataSource(client.schema, buffw, src_sphinx_data_source_path);

			}

			buffw.write("</sphinx:docset>\n");

			buffw.close();

			// Sphinx schema writer for next update or merge

			Path dst_sphinx_schema_path = Paths.get(dst_ds_dir_name, PgSchemaUtil.sph_schema_name);

			client.schema.writeSphSchema(dst_sphinx_schema_path, false);

			// Sphinx configuration writer

			Path sphinx_conf_path = Paths.get(dst_ds_dir_name, PgSchemaUtil.sph_conf_name);

			client.schema.writeSphConf(sphinx_conf_path, ds_name, dst_sphinx_data_source_path);

		} catch (ParserConfigurationException | SAXException | IOException | PgSchemaException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Merge data source.
	 *
	 * @param schema PostgreSQL data model
	 * @param buffw buffered writer for Sphinx data source
	 * @param sph_doc_path Sphinx data source file path
	 */
	private static void mergeDataSource(PgSchema schema, BufferedWriter buffw, Path sph_doc_path) {

		try {

			BufferedReader buffr = Files.newBufferedReader(sph_doc_path);

			boolean doc_start = false;

			String line = null;

			while ((line = buffr.readLine()) != null) {

				if (line.contains("</sphinx:schema>"))
					doc_start = true;

				else if (doc_start) {

					if (line.contains("</sphinx:docset>"))
						break;

					buffw.write(line + "\n");

				}

			}

			buffr.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("dsmerge4sphinx: Merge Sphinx data source files into one");
		System.err.println("Usage:  --xsd SCHEMA_LOCATION --dst-ds-dir DIRECTORY (default=\"" + xml2sphinxds.ds_dir_name + "\") --src-ds-dir DIRECTORY (repeat until you specify all directories)");
		System.err.println("Option: --ds-name DS_NAME (default name is determined by data_source.conf file)");
		System.err.println("        --no-pgschema-serv (not utilize PgSchema server)");
		System.err.println("        --pgschema-serv-host PG_SCHEMA_SERV_HOST_NAME (default=\"" + PgSchemaUtil.pg_schema_server_host + "\")");
		System.err.println("        --pgschema-serv-port PG_SCHEMA_SERV_PORT_NUMBER (default=" + PgSchemaUtil.pg_schema_server_port + ")");
		System.exit(1);

	}

}
