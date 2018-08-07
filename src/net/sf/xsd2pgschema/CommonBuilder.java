/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

    https://sourceforge.net/projects/xsd2pgschema/

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in wrinnting, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package net.sf.xsd2pgschema;

/**
 * Common builder.
 *
 * @author yokochi
 */
public class CommonBuilder {

	/** Whether to insert document key. */
	protected boolean insert_doc_key = false;

	/** Whether to use line feed code. */
	protected boolean line_feed = true;

	/** The current line feed code. */
	protected String line_feed_code = "\n";

	/** The pending simple content. */
	protected StringBuilder pending_simple_cont = new StringBuilder();

	/** The count of root nodes. */
	public int root_count = 0;

	/** The count of fragments. */
	public int fragment = 0;

	/**
	 * Set whether to insert document key.
	 *
	 * @param insert_doc_key whether to insert document key
	 */
	public void setInsertDocKey(boolean insert_doc_key) {

		this.insert_doc_key = insert_doc_key;

	}

	/**
	 * Return parent path.
	 *
	 * @param path current path
	 * @return String parent path
	 */
	public String getParentPath(String path) {

		StringBuilder sb = new StringBuilder();

		String[] _path = path.split("/");

		try {

			for (int i = 1; i < _path.length - 1; i++)
				sb.append("/" + _path[i]);

			return sb.toString();

		} finally {
			sb.setLength(0);
		}

	}

	/**
	 * Return the last name of current path.
	 *
	 * @param path current path
	 * @return String the last path name
	 */
	public String getLastNameOfPath(String path) {

		String[] _path = path.split("/");

		int position = _path.length - 1;

		if (position < 0)
			return null;

		return _path[position];
	}

	/**
	 * Reset status.
	 */
	public void resetStatus() {

		root_count = fragment = 0;

	}

	/**
	 * Increment count of root nodes.
	 */
	protected void incRootCount() {

		root_count++;

	}

	/**
	 * Increment count of fragments.
	 */
	protected void incFragment() {

		fragment++;

	}

	/**
	 * Clear pending simple content.
	 */
	public void clear() {

		pending_simple_cont.setLength(0);

	}

}
