/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018-2019 Masashi Yokochi

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

package net.sf.xsd2pgschema.serverutil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.nustaq.serialization.FSTConfiguration;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.docbuilder.JsonBuilderOption;
import net.sf.xsd2pgschema.option.IndexFilter;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.option.XmlPostEditor;
import net.sf.xsd2pgschema.type.PgHashSize;

/**
 * Implementation of PostgreSQL server client.
 *
 * @author yokochi
 */
public class PgSchemaClientImpl {

	/** the document builder factory. */
	public DocumentBuilderFactory doc_builder_fac;

	/** The document builder. */
	public DocumentBuilder doc_builder;

	/** The PostgreSQL data model option. */
	public PgSchemaOption option;

	/** The PostgreSQL data model. */
	public PgSchema schema = null;

	/**
	 * Instance of PgSchemaClientImpl.
	 *
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param client_type PgSchema client type
	 * @param original_caller original caller class name (optional)
	 * @param xml_post_editor XML post editor (optional)
	 * @param stdout_msg whether to output processing message to stdin or not (stderr)
	 * @throws UnknownHostException the unknown host exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaClientImpl(final InputStream is, final PgSchemaOption option, final FSTConfiguration fst_conf, final PgSchemaClientType client_type, final String original_caller, final XmlPostEditor xml_post_editor, final boolean stdout_msg) throws UnknownHostException, IOException, ParserConfigurationException, SAXException, PgSchemaException {

		boolean server_alive = false;

		this.option = option;

		doc_builder_fac = DocumentBuilderFactory.newInstance();
		doc_builder_fac.setValidating(false);
		doc_builder_fac.setNamespaceAware(true);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		doc_builder = doc_builder_fac.newDocumentBuilder();

		if (option.pg_schema_server) {

			try {

				try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					DataInputStream in = new DataInputStream(socket.getInputStream());

					PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.GET, option, client_type));

					PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);

					if (reply.schema_bytes != null) {

						schema = (PgSchema) fst_conf.asObject(reply.schema_bytes);

						if (stdout_msg)
							System.out.print(reply.message);
						else
							System.err.print(reply.message);

					}
					/*
					in.close();
					out.close();
					 */
					server_alive = true;

				}

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (ConnectException e) {
			}

		}

		if (schema == null && is != null) {

			// parse XSD document

			Document xsd_doc = doc_builder.parse(is);

			doc_builder.reset();

			// XSD analysis

			schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, option);

			switch (client_type) {
			case pg_data_migration:
				schema.prepForDataMigration();
				if (xml_post_editor != null)
					schema.applyXmlPostEditor(xml_post_editor);
				break;
			case full_text_indexing:
				throw new PgSchemaException("Use another instance for full-text indexing: PgSchemaClientImpl(*, IndexFilter index_filter, *)");
			case json_conversion:
				throw new PgSchemaException("Use another instance for JSON conversion: PgSchemaClientImpl(*, JsonBuilderOption jsonb_option, *)");
			case xpath_evaluation_to_json:
				throw new PgSchemaException("Use another instance for XPath evaluation to JSON: PgSchemaClientImpl(*, JsonBuilderOption jsonb_option, *)");
			case xpath_evaluation:
				schema.prepForXPathParser();
				break;
			}

			if (server_alive && fst_conf != null && !option.hash_size.equals(PgHashSize.debug_string)) {

				try {

					try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						DataInputStream in = new DataInputStream(socket.getInputStream());

						PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.ADD, fst_conf, schema, client_type, original_caller));

						PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);

						if (stdout_msg)
							System.out.print(reply.message);
						else
							System.err.print(reply.message);
						/*
						in.close();
						out.close();
						 */
					}

				} catch (ClassNotFoundException | ConnectException e) {
					e.printStackTrace();
				}

			}

		}

		if (is != null)
			is.close();

	}

	/**
	 * Instance of PgSchemaClientImpl with index filter.
	 *
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param client_type PgSchema client type
	 * @param original_caller original caller class name (optional)
	 * @param xml_post_editor XML post editor (optional)
	 * @param index_filter index filter
	 * @param stdout_msg whether to output processing message to stdin or not (stderr)
	 * @throws UnknownHostException the unknown host exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaClientImpl(final InputStream is, final PgSchemaOption option, final FSTConfiguration fst_conf, final PgSchemaClientType client_type, final String original_caller, final XmlPostEditor xml_post_editor, final IndexFilter index_filter, final boolean stdout_msg) throws UnknownHostException, IOException, ParserConfigurationException, SAXException, PgSchemaException {

		boolean server_alive = false;

		this.option = option;

		doc_builder_fac = DocumentBuilderFactory.newInstance();
		doc_builder_fac.setValidating(false);
		doc_builder_fac.setNamespaceAware(true);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		doc_builder = doc_builder_fac.newDocumentBuilder();

		if (option.pg_schema_server) {

			try {

				try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					DataInputStream in = new DataInputStream(socket.getInputStream());

					PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.GET, option, client_type));

					PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);

					if (reply.schema_bytes != null) {

						schema = (PgSchema) fst_conf.asObject(reply.schema_bytes);

						if (stdout_msg)
							System.out.print(reply.message);
						else
							System.err.print(reply.message);

					}
					/*
					in.close();
					out.close();
					 */
					server_alive = true;

				}

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (ConnectException e) {
			}

		}

		if (schema == null && is != null) {

			// parse XSD document

			Document xsd_doc = doc_builder.parse(is);

			doc_builder.reset();

			// XSD analysis

			schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, option);

			switch (client_type) {
			case pg_data_migration:
				schema.prepForDataMigration();
				if (xml_post_editor != null)
					schema.applyXmlPostEditor(xml_post_editor);
				break;
			case full_text_indexing:
				if (index_filter != null)
					schema.applyIndexFilter(index_filter);
				if (xml_post_editor != null)
					schema.applyXmlPostEditor(xml_post_editor);
				break;
			case json_conversion:
				throw new PgSchemaException("Use another instance for JSON conversion: PgSchemaClientImpl(*, JsonBuilderOption jsonb_option, *)");
			case xpath_evaluation_to_json:
				throw new PgSchemaException("Use another instance for XPath evaluation to JSON: PgSchemaClientImpl(*, JsonBuilderOption jsonb_option, *)");
			case xpath_evaluation:
				schema.prepForXPathParser();
				break;
			}

			if (server_alive && fst_conf != null) {

				try {

					try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						DataInputStream in = new DataInputStream(socket.getInputStream());

						PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.ADD, fst_conf, schema, client_type, original_caller));

						PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);

						if (stdout_msg)
							System.out.print(reply.message);
						else
							System.err.print(reply.message);
						/*
						in.close();
						out.close();
						 */
					}

				} catch (ClassNotFoundException | ConnectException e) {
					e.printStackTrace();
				}

			}

		}

		if (is != null)
			is.close();

	}

	/**
	 * Instance of PgSchemaClientImpl with JSON builder option.
	 *
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param client_type PgSchema client type
	 * @param original_caller original caller class name (optional)
	 * @param xml_post_editor XML post editor (optional)
	 * @param jsonb_option JSON builder option
	 * @param stdout_msg whether to output processing message to stdin or not (stderr)
	 * @throws UnknownHostException the unknown host exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaClientImpl(final InputStream is, final PgSchemaOption option, final FSTConfiguration fst_conf, final PgSchemaClientType client_type, final String original_caller, final XmlPostEditor xml_post_editor, final JsonBuilderOption jsonb_option, final boolean stdout_msg) throws UnknownHostException, IOException, ParserConfigurationException, SAXException, PgSchemaException {

		boolean server_alive = false;

		this.option = option;

		doc_builder_fac = DocumentBuilderFactory.newInstance();
		doc_builder_fac.setValidating(false);
		doc_builder_fac.setNamespaceAware(true);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		doc_builder = doc_builder_fac.newDocumentBuilder();

		if (option.pg_schema_server) {

			try {

				try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					DataInputStream in = new DataInputStream(socket.getInputStream());

					PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.GET, option, client_type));

					PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);

					if (reply.schema_bytes != null) {

						schema = (PgSchema) fst_conf.asObject(reply.schema_bytes);

						if (stdout_msg)
							System.out.print(reply.message);
						else
							System.err.print(reply.message);

					}
					/*
					in.close();
					out.close();
					 */
					server_alive = true;

				}

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (ConnectException e) {
			}

		}

		if (schema == null && is != null) {

			// parse XSD document

			Document xsd_doc = doc_builder.parse(is);

			doc_builder.reset();

			// XSD analysis

			schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, option);

			switch (client_type) {
			case pg_data_migration:
				schema.prepForDataMigration();
				if (xml_post_editor != null)
					schema.applyXmlPostEditor(xml_post_editor);
				break;
			case full_text_indexing:
				throw new PgSchemaException("Use another instance for full-text indexing: PgSchemaClientImpl(*, IndexFilter index_filter, *)");
			case json_conversion:
				if (xml_post_editor != null)
					schema.applyXmlPostEditor(xml_post_editor);
				schema.prepForJsonBuilder(jsonb_option);
				schema.prepForJsonConversion();
				break;
			case xpath_evaluation_to_json:
				schema.prepForJsonBuilder(jsonb_option);
			case xpath_evaluation:
				schema.prepForXPathParser();
				break;
			}

			if (server_alive && fst_conf != null) {

				try {

					try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						DataInputStream in = new DataInputStream(socket.getInputStream());

						PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.ADD, fst_conf, schema, client_type, original_caller));

						PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);

						if (stdout_msg)
							System.out.print(reply.message);
						else
							System.err.print(reply.message);
						/*
						in.close();
						out.close();
						 */
					}

				} catch (ClassNotFoundException | ConnectException e) {
					e.printStackTrace();
				}

			}

		}

		if (is != null)
			is.close();

	}

}
