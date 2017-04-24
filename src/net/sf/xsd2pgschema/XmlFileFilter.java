/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2017 Masashi Yokochi

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

/**
 * XML file filter.
 *
 * @author yokochi
 */
public class XmlFileFilter {

	/** The ext. */
	public String ext = "xml";

	/** The prefix digest. */
	String prefix_digest = "";
	
	/** The ext digest. */
	String ext_digest = ".";

	/**
	 * Set extension of target file.
	 *
	 * @param ext argument value
	 * @return boolean whether it is valid or not
	 */
	public boolean setExt(String ext) {

		this.ext = ext;

		if (ext == null || !ext.equals("xml") && !ext.equals("gz")) {

			System.err.println("Illegal xml-file-ext option: " + ext + ".");

			return false;
		}

		return true;
	}

	/**
	 * Set digested prefix of file name.
	 *
	 * @param prefix_digest argument value
	 */
	public void setPrefixDigest(String prefix_digest) {

		this.prefix_digest = prefix_digest;

		if (prefix_digest == null)
			this.prefix_digest = "";

	}

	/**
	 * Set digested extension of file name.
	 *
	 * @param ext_digest argument value
	 */
	public void setExtDigest(String ext_digest) {

		this.ext_digest = ext_digest;

		if (ext_digest == null)
			this.ext_digest = ".";

	}

}
