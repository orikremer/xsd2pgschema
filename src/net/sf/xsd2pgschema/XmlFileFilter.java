/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2018 Masashi Yokochi

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

import java.util.regex.Pattern;

/**
 * XML file filter.
 *
 * @author yokochi
 */
public class XmlFileFilter {

	/** The extension of XML file. */
	public String ext = "xml";

	/** The digest-able prefix name. */
	protected String prefix_digest = "";

	/** The digest-able extension name. */
	protected String ext_digest = ".";

	/** Whether case-sensitive document key. */
	protected boolean case_sense_doc_key = true;

	/** Whether lower-case document key or not. */
	protected boolean lower_case_doc_key = true;

	/** Whether ext_digest is resolved. */
	private boolean resolved = false;

	/**
	 * Set extension of target file.
	 *
	 * @param ext argument value
	 * @return boolean whether it is valid or not
	 */
	public boolean setExt(String ext) {

		this.ext = ext;

		if (ext == null || (!ext.equals("xml") && !ext.equals("gz") && !ext.equals("zip"))) {

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

		if (prefix_digest == null)
			prefix_digest = "";

		this.prefix_digest = "^" + Pattern.quote(prefix_digest);

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

		else if (!this.ext_digest.endsWith("."))
			this.ext_digest += ".";

		resolved = false;

	}

	/**
	 * Set lower case document key.
	 */
	public void setLowerCaseDocKey() {

		case_sense_doc_key = false;
		lower_case_doc_key = true;

	}

	/**
	 * Set upper case document key.
	 */
	public void setUpperCaseDocKey() {

		case_sense_doc_key = false;
		lower_case_doc_key = false;

	}

	/**
	 * Return absolute file extension.
	 *
	 * @return String absolute file extension
	 */
	public String getAbsoluteExt() {

		if (!resolved) {

			switch (ext) {
			case "gz":
			case "zip":
				if (ext_digest.endsWith(ext + "."))
					ext_digest = ext_digest.replaceFirst(ext + "\\.$", "");

				if (!ext_digest.endsWith("xml."))
					ext_digest += "xml.";

				ext_digest += ext;
				break;
			default:
				if (ext_digest.endsWith("xml."))
					ext_digest = ext_digest.substring(0, ext_digest.length() - 1);
				else
					ext_digest += "xml";
			}

			ext_digest = Pattern.quote(ext_digest) + "$";

			resolved = true;

		}

		switch (ext) {
		case "gz":
		case "zip":
			return ".xml." + ext;
		default:
			return ext;
		}

	}

}
