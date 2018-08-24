/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

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

package net.sf.xsd2pgschema;

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
	 * @param original_caller original caller class name (optional)
	 * @throws UnknownHostException the unknown host exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaClientImpl(final InputStream is, final PgSchemaOption option, final FSTConfiguration fst_conf, final String original_caller) throws UnknownHostException, IOException, ParserConfigurationException, SAXException, PgSchemaException {

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

					option.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.GET, option));

					PgSchemaServerReply reply = (PgSchemaServerReply) option.readObjectFromStream(fst_conf, in);

					if (reply.schema_bytes != null) {

						schema = (PgSchema) fst_conf.asObject(reply.schema_bytes);
						System.out.print(reply.message);

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

			if (server_alive && fst_conf != null) {

				try {

					try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						DataInputStream in = new DataInputStream(socket.getInputStream());

						option.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(fst_conf, schema, original_caller));

						PgSchemaServerReply reply = (PgSchemaServerReply) option.readObjectFromStream(fst_conf, in);

						System.out.print(reply.message);
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
