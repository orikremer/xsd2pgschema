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

import net.sf.xsd2pgschema.*;

import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Thread function for chksumstat.
 *
 * @author yokochi
 */
public class ChkSumStatThrd implements Runnable {

	/** The PostgreSQL data model option. */
	private PgSchemaOption option;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue;

	/** The instance of message digest for check sum. */
	private MessageDigest md_chk_sum;

	/**
	 * Instance of XmlValidatorThrd.
	 *
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param option PostgreSQL data model option
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 */
	public ChkSumStatThrd(final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, PgSchemaOption option) throws NoSuchAlgorithmException {

		this.xml_file_filter = xml_file_filter;
		this.xml_file_queue = xml_file_queue;

		this.option = option;

		md_chk_sum = MessageDigest.getInstance(option.check_sum_algorithm);

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		Path xml_file_path;

		while ((xml_file_path = xml_file_queue.poll()) != null) {

			XmlParser xml_parser = new XmlParser(xml_file_path, xml_file_filter);

			String document_id = xml_parser.document_id;

			Path has_path;

			synchronized (chksumstat.sync_del_doc_rows) {
				has_path = chksumstat.sync_del_doc_rows.remove(document_id);
			}

			if (has_path != null) {

				try {

					if (xml_parser.identify(option, md_chk_sum))
						continue;

				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}

				synchronized (chksumstat.sync_up_doc_rows) {
					chksumstat.sync_up_doc_rows.add(document_id);
				}

			}

			else {

				if (!option.sync_dry_run) {

					try {

						xml_parser.identify(option, md_chk_sum);

					} catch (IOException e) {
						e.printStackTrace();
						System.exit(1);
					}

				}

				synchronized (chksumstat.sync_new_doc_rows) {
					chksumstat.sync_new_doc_rows.add(document_id);
				}

			}

		}

	}

}
