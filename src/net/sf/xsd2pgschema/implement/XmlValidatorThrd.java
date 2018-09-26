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

package net.sf.xsd2pgschema.implement;

import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.option.XmlFileFilter;
import net.sf.xsd2pgschema.xmlutil.XmlParser;
import net.sf.xsd2pgschema.xmlutil.XmlValidator;

/**
 * Thread function for xmlvalidator.
 *
 * @author yokochi
 */
public class XmlValidatorThrd implements Runnable {

	/** The thread id. */
	private int thrd_id;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option;

	/** The XML validator. */
	private XmlValidator validator;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter;

	/** The XML file queue. */
	private LinkedBlockingQueue<Path> xml_file_queue;

	/** The instance of message digest for check sum. */
	private MessageDigest md_chk_sum = null;

	/**
	 * Instance of XmlValidatorThrd.
	 *
	 * @param thrd_id thread id
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 * @param option PostgreSQL data model option
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 */
	public XmlValidatorThrd(final int thrd_id, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<Path> xml_file_queue, PgSchemaOption option) throws NoSuchAlgorithmException {

		this.thrd_id = thrd_id;

		this.xml_file_filter = xml_file_filter;
		this.xml_file_queue = xml_file_queue;

		this.option = option;

		validator = new XmlValidator(PgSchemaUtil.getSchemaFilePath(option.root_schema_location, null, true), option.full_check);

		synchronizable = option.isSynchronizable(false);

		// prepare message digest for check sum

		if (!option.check_sum_algorithm.isEmpty() && synchronizable)
			md_chk_sum = MessageDigest.getInstance(option.check_sum_algorithm);

	}

	/** Whether synchronization is possible. */
	private boolean synchronizable = false;

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		boolean show_progress = thrd_id == 0 && total > 1;

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

					System.out.print("\rDone " + progress + " of " + total + " ... (ETC " + sdf.format(etc_date) + ")");

				}

			}

			if (synchronizable) {

				try {

					xml_parser = new XmlParser(xml_file_path, xml_file_filter);

					if (xml_parser.identify(option, md_chk_sum))
						continue;

				} catch (IOException e) {
					e.printStackTrace();
				}

			}

			try {

				new XmlParser(validator, xml_file_path, xml_file_filter, option);

			} catch (Exception e) {
				e.printStackTrace();
			}

			++polled;

		}

		if (thrd_id == 0)
			System.out.println("\nDone.");

	}

}
