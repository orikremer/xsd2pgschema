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

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Thread function for xmlvalidator.
 *
 * @author yokochi
 */
public class XmlValidatorThrd implements Runnable {

	/** The thread id. */
	private int thrd_id;

	/** The XML validator. */
	private XmlValidator validator = null;

	/** The XML file filter. */
	private XmlFileFilter xml_file_filter = null;

	/** The XML file queue. */
	private LinkedBlockingQueue<File> xml_file_queue = null;

	/**
	 * Instance of XmlValidatorThrd.
	 *
	 * @param thrd_id thread id
	 * @param xml_file_filter XML file filter
	 * @param xml_file_queue XML file queue
	 */
	public XmlValidatorThrd(final int thrd_id, final XmlFileFilter xml_file_filter, final LinkedBlockingQueue<File> xml_file_queue) {

		this.thrd_id = thrd_id;

		this.xml_file_filter = xml_file_filter;
		this.xml_file_queue = xml_file_queue;

		validator = new XmlValidator(PgSchemaUtil.getSchemaFile(xmlvalidator.schema_location, null, true));

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xml_file_queue.size();
		boolean show_progress = thrd_id == 0 && total > 1;

		File xml_file;

		while ((xml_file = xml_file_queue.poll()) != null) {

			try {

				new XmlParser(validator, xml_file, xml_file_filter);

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

			if (show_progress)
				System.out.print("\rDone " + (total - xml_file_queue.size()) + " of " + total + " ...");

		}

		if (thrd_id == 0)
			System.out.println("\nDone.");

	}

}
