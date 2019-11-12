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

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.docbuilder.JsonBuilderOption;
import net.sf.xsd2pgschema.option.IndexFilter;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.option.XmlPostEditor;

/**
 * Thread function for PgSchema server client.
 *
 * @author yokochi
 */
public class PgSchemaGetClientThrd implements Runnable {

	/** The thread id. */
	private int thrd_id;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option;

	/** The FST configuration. */
	private FSTConfiguration fst_conf;

	/** The PgSchema client type. */
	private PgSchemaClientType client_type;

	/** The original caller class name (optional). */
	private String original_caller;

	/** The XML post editor (optional). */
	private XmlPostEditor xml_post_editor = null;

	/** The index filter (optional). */
	private IndexFilter index_filter = null;

	/** The JSON Builder option (optional). */
	private JsonBuilderOption jsonb_option = null;

	/** The array of PgSchema server clients. */
	private PgSchemaClientImpl[] clients;

	/**
	 * Instance of PgSchemaGetClientThrd.
	 *
	 * @param thrd_id thread id
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param client_type PgSchema client type
	 * @param original_caller original caller class name (optional)
	 * @param xml_post_editor XML post editor (optional)
	 * @param clients array of PgSchemaClientImpl
	 */
	public PgSchemaGetClientThrd(final int thrd_id, final PgSchemaOption option, final FSTConfiguration fst_conf, final PgSchemaClientType client_type, final String original_caller, final XmlPostEditor xml_post_editor, final PgSchemaClientImpl[] clients) {

		this.thrd_id = thrd_id;
		this.option = option;
		this.fst_conf = fst_conf;
		this.client_type = client_type;
		this.original_caller = original_caller;
		this.xml_post_editor = xml_post_editor;
		this.clients = clients;

	}

	/**
	 * Instance of PgSchemaGetClientThrd with index filter.
	 *
	 * @param thrd_id thread id
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param client_type PgSchema client type
	 * @param original_caller original caller class name (optional)
	 * @param xml_post_editor XML post editor (optional)
	 * @param index_filter index filter
	 * @param clients array of PgSchemaClientImpl
	 */
	public PgSchemaGetClientThrd(final int thrd_id, final PgSchemaOption option, final FSTConfiguration fst_conf, final PgSchemaClientType client_type, final String original_caller, final XmlPostEditor xml_post_editor, final IndexFilter index_filter, final PgSchemaClientImpl[] clients) {

		this.thrd_id = thrd_id;
		this.option = option;
		this.fst_conf = fst_conf;
		this.client_type = client_type;
		this.original_caller = original_caller;
		this.xml_post_editor = xml_post_editor;
		this.index_filter = index_filter;
		this.clients = clients;

	}

	/**
	 * Instance of PgSchemaGetClientThrd with JSON builder option.
	 *
	 * @param thrd_id thread id
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param client_type PgSchema client type
	 * @param original_caller original caller class name (optional)
	 * @param xml_post_editor XML post editor (optional)
	 * @param jsonb_option JSON builder option
	 * @param clients array of PgSchemaClientImpl
	 */
	public PgSchemaGetClientThrd(final int thrd_id, final PgSchemaOption option, final FSTConfiguration fst_conf, final PgSchemaClientType client_type, final String original_caller, final XmlPostEditor xml_post_editor, final JsonBuilderOption jsonb_option, final PgSchemaClientImpl[] clients) {

		this.thrd_id = thrd_id;
		this.option = option;
		this.fst_conf = fst_conf;
		this.client_type = client_type;
		this.original_caller = original_caller;
		this.xml_post_editor = xml_post_editor;
		this.jsonb_option = jsonb_option;
		this.clients = clients;

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		try {

			if (index_filter == null && jsonb_option == null)
				clients[thrd_id] = new PgSchemaClientImpl(null, option, fst_conf, client_type, original_caller, xml_post_editor);
			else if (index_filter != null)
				clients[thrd_id] = new PgSchemaClientImpl(null, option, fst_conf, client_type, original_caller, xml_post_editor, index_filter);
			else
				clients[thrd_id] = new PgSchemaClientImpl(null, option, fst_conf, client_type, original_caller, xml_post_editor, jsonb_option);

		} catch (IOException | ParserConfigurationException | SAXException | PgSchemaException e) {
			e.printStackTrace();
		}

	}

}
