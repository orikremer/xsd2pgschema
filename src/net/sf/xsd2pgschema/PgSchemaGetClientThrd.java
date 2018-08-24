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

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

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

	/** The original caller class name (optional). */
	private String original_caller;

	/** The array of PgSchema server clients. */
	private PgSchemaClientImpl[] clients;

	/**
	 * Instance of PgSchemaGetClientThrd.
	 *
	 * @param thrd_id thread id
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param original_caller original caller class name
	 * @param clients array of PgSchemaClientImpl
	 */
	public PgSchemaGetClientThrd(final int thrd_id, final PgSchemaOption option, final FSTConfiguration fst_conf, final String original_caller, final PgSchemaClientImpl[] clients) {

		this.thrd_id = thrd_id;
		this.option = option;
		this.fst_conf = fst_conf;
		this.original_caller = original_caller;
		this.clients = clients;

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		try {

			clients[thrd_id] = new PgSchemaClientImpl(null, option, fst_conf, original_caller);

		} catch (IOException | ParserConfigurationException | SAXException | PgSchemaException e) {
			e.printStackTrace();
		}

	}

}
