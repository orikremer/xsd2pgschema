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

	/**
	 * Instance of XmlValidatorThrd.
	 *
	 * @param thrd_id thread id
	 * @param max_thrds max threads
	 */
	public XmlValidatorThrd(final int thrd_id, final int max_thrds) {

		this.thrd_id = thrd_id;

		validator = new XmlValidator(PgSchemaUtil.getSchemaFile(xmlvalidator.schema_location, null, true));

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		int total = xmlvalidator.xml_file_queue.size();

		File xml_file;

		while ((xml_file = xmlvalidator.xml_file_queue.poll()) != null) {

			if (!xml_file.isFile())
				continue;

			try {

				new XmlParser(validator, xml_file, xmlvalidator.xml_file_filter);

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}

			if (thrd_id == 0)
				System.out.print("\nDone " + (total - xmlvalidator.xml_file_queue.size()) + " of " + total + " ...\r");

		}

		if (thrd_id == 0)
			System.out.println("\nDone.");

	}

}
