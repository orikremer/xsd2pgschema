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

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * PostgreSQL schema construction option.
 *
 * @author yokochi
 */
public class PgSchemaOption {

	/** The relational model extension. */
	protected boolean rel_model_ext = true;

	/** The relational data extension. */
	protected boolean rel_data_ext = true;

	/** The wild card extension. */
	public boolean wild_card = true;

	/** Whether add document key in PostgreSQL DDL. */
	public boolean document_key = true;

	/** Whether add serial key in PostgreSQL DDL. */
	public boolean serial_key = false;

	/** Whether add XPath key in PostgreSQL DDL. */
	public boolean xpath_key = false;

	/** Whether retain case sensitive name in PostgreSQL DDL. */
	public boolean case_sense = true;

	/** Whether prefer local XML Schema file. */
	public boolean cache_xsd = true;

	/** Whether output PostgreSQL DDL. */
	public boolean ddl_output = false;

	/** Whether retain primary key/foreign key constraint in PostgreSQL DDL. */
	public boolean retain_key = true;

	/** Whether not retrieve field annotation in PostgreSQL DDL. */
	public boolean no_field_anno = true;

	/** Whether execute XML Schema validation. */
	public boolean validate = false;

	/** The default document key name in PostgreSQL DDL. */
	public final String def_document_key_name = "document_id";

	/** The default serial key name in PostgreSQL DDL. */
	public final String def_serial_key_name = "serial_id";

	/** The default XPath key name in PostgreSQL DDL. */
	public final String def_xpath_key_name = "xpath_id";

	/** The document key name in PostgreSQL DDL. */
	public String document_key_name = def_document_key_name;

	/** The serial key name in PostgreSQL DDL. */
	public String serial_key_name = def_serial_key_name;

	/** The XPath key name in PostgreSQL DDL. */
	public String xpath_key_name = def_xpath_key_name;

	/** The list of discarded document key names. */
	protected HashSet<String> discarded_document_key_names = null;

	/** The name of hash algorithm. */
	public String hash_algorithm = PgSchemaUtil.def_hash_algorithm;

	/** The size of hash key. */
	public PgHashSize hash_size = PgHashSize.defaultSize();

	/** The size of serial key. */
	public PgSerSize ser_size = PgSerSize.defaultSize();

	/** Whether adopt strict synchronization (insert if not exists, update if required, and delete if XML not exists). */
	public boolean sync = false;

	/** Whether adopt weak synchronization (insert if not exists, no update even if exists, no deletion). */
	public boolean sync_weak = false;

	/** The directory contains check sum files. */
	public File check_sum_dir = null;

	/** The default algorithm for check sum. */
	public String check_sum_algorithm = PgSchemaUtil.def_check_sum_algorithm;

	/** The instance of message digest for check sum. */
	public MessageDigest check_sum_message_digest = null;

	/** The prefix of xs_namespace_uri. */
	protected String xs_prefix = null;

	/** The xs_prefix.isEmpty() ? "" : xs_prefix + ":". */
	protected String xs_prefix_ = null;

	/** Whether attribute selection has been resolved. */
	protected boolean attr_resolved = false;

	/** Whether field selection has been resolved. */
	protected boolean field_resolved = false;

	/** The internal status corresponding to --doc-key option. */
	private boolean _doc_key = false;

	/** The internal status corresponding to --no-doc-key option. */
	private boolean _no_doc_key = false;

	/**
	 * Instance of PostgreSQL data modeling option.
	 *
	 * @param document_key the document key
	 */
	public PgSchemaOption(boolean document_key) {

		this.document_key = document_key;

		discarded_document_key_names = new HashSet<String>();

		setCheckSumAlgorithm(check_sum_algorithm);

	}

	/**
	 * Instance of PgSchemaOption for JSON Schema conversion.
	 *
	 * @param json_type JSON type
	 */
	public PgSchemaOption(JsonType json_type) {

		setDefaultForJsonSchema(json_type);

		discarded_document_key_names = new HashSet<String>();

		setCheckSumAlgorithm(check_sum_algorithm);

	}

	/**
	 * Default settings for JSON Schema conversion.
	 *
	 * @param json_type JSON type
	 */
	public void setDefaultForJsonSchema(JsonType json_type) {

		rel_model_ext = !json_type.equals(JsonType.relational);

		cancelRelDataExt();

	}

	/**
	 * Cancel relational model extension in PostgreSQL.
	 */
	public void cancelRelModelExt() {

		rel_model_ext = false;

		cancelRelDataExt();

	}

	/**
	 * Cancel relational data extension.
	 */
	public void cancelRelDataExt() {

		rel_data_ext = document_key = serial_key = xpath_key = retain_key = false;

	}

	/**
	 * Return minimum size of field.
	 *
	 * @return int minimum size of field
	 */
	public int getMinimumSizeOfField() {
		return (rel_model_ext ? 1 : 0) + (document_key ? 1 : 0);
	}

	/**
	 * Return unqualified name.
	 *
	 * @param qname qualified name
	 * @return String unqualified name
	 */
	public String getUnqualifiedName(String qname) {

		if (qname == null)
			return null;

		if (qname.contains(" "))
			qname = qname.trim();

		if (!case_sense)
			qname = qname.toLowerCase();

		int last_pos = qname.lastIndexOf(':');

		return last_pos == -1 ? qname : qname.substring(last_pos + 1);
	}

	/**
	 * Set internal status corresponding to --doc-key and --no-doc-key options.
	 *
	 * @param doc_key whether add document key or not
	 * @return boolean whether status changed
	 */
	public boolean setDocKeyOption(boolean doc_key) {

		if (doc_key) {

			if (_no_doc_key) {
				System.err.println("--no-doc-key is already set.");
				return false;
			}

			_doc_key = true;

		}

		else {

			if (_doc_key) {
				System.err.println("--doc-key is already set.");
				return false;
			}

			_no_doc_key = true;

		}

		return true;
	}

	/**
	 * Decide whether to add document key or not.
	 */
	public void resolveDocKeyOption() {

		if (_doc_key || _no_doc_key)
			document_key = _doc_key;

	}

	/**
	 * Set prefix of namespace URI representing XML Schema 1.x (http://www.w3.org/2001/XMLSchema)
	 *
	 * @param doc XML Schema document
	 * @param def_schema_location default schema location
	 * @throws PgSchemaException the pg schema exception
	 */
	public void setPrefixOfXmlSchema(Document doc, String def_schema_location) throws PgSchemaException {

		NodeList node_list = doc.getElementsByTagNameNS(PgSchemaUtil.xs_namespace_uri, "*");

		if (node_list == null)
			throw new PgSchemaException("No namespace declaration stands for " + PgSchemaUtil.xs_namespace_uri + " in XML Schema: " + def_schema_location);

		Node xs_namespace_uri_node = node_list.item(0);

		xs_prefix = xs_namespace_uri_node != null ? xs_namespace_uri_node.getPrefix() : null;

		if (xs_prefix == null || xs_prefix.isEmpty())
			xs_prefix_ = xs_prefix = "";
		else
			xs_prefix_ = xs_prefix + ":";

	}

	/**
	 * Set document key name.
	 *
	 * @param document_key_name document key name
	 */
	public void setDocumentKeyName(String document_key_name) {

		if (document_key_name == null || document_key_name.isEmpty())
			return;

		this.document_key_name = document_key_name;

	}

	/**
	 * Set serial key name.
	 *
	 * @param serial_key_name serial key name
	 */
	public void setSerialKeyName(String serial_key_name) {

		if (serial_key_name == null || serial_key_name.isEmpty())
			return;

		this.serial_key_name = serial_key_name;

	}

	/**
	 * Set XPath key name.
	 *
	 * @param xpath_key_name xpath key name
	 */
	public void setXPathKeyName(String xpath_key_name) {

		if (xpath_key_name == null || xpath_key_name.isEmpty())
			return;

		this.xpath_key_name = xpath_key_name;

	}

	/**
	 * Add discarded document key name.
	 *
	 * @param discarded_document_key_name discarded document key name
	 * @return result of addition
	 */
	public boolean addDiscardedDocKeyName(String discarded_document_key_name) {

		if (discarded_document_key_name == null || discarded_document_key_name.isEmpty())
			return false;

		if (discarded_document_key_names == null)
			discarded_document_key_names = new HashSet<String>();

		return discarded_document_key_names.add(discarded_document_key_name);
	}

	/**
	 * Extract one-liner annotation from xs:annotation/xs:appinfo|xs:documentation.
	 *
	 * @param node current node
	 * @param is_table the is table
	 * @return String annotation
	 */
	public String extractAnnotation(Node node, boolean is_table) {

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (!anno.getNodeName().equals(xs_prefix_ + "annotation"))
				continue;

			String annotation = "";

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				String child_name = child.getNodeName();

				if (child_name.equals(xs_prefix_ + "appinfo")) {

					annotation = child.getTextContent().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", "");

					if (!annotation.isEmpty())
						annotation += "\n-- ";

					Element e = (Element) child;

					String src = e.getAttribute("source");

					if (src != null && !src.isEmpty())
						annotation += (is_table ? "\n-- " : ", ") + "URI-reference = " + src + (is_table ? "\n-- " : ", ");

				}

				else if (child_name.equals(xs_prefix_ + "documentation")) {

					annotation += child.getTextContent().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", "");

					Element e = (Element) child;

					String src = e.getAttribute("source");

					if (src != null && !src.isEmpty())
						annotation += (is_table ? "\n-- " : ", ") + "URI-reference = " + src;

				}

			}

			if (annotation != null && !annotation.isEmpty())
				return annotation;
		}

		return null;
	}

	/**
	 * Extract one-liner annotation from xs:annotation/xs:appinfo.
	 *
	 * @param node current node
	 * @return String appinfo of annotation
	 */
	public String extractAppinfo(Node node) {

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (!anno.getNodeName().equals(xs_prefix_ + "annotation"))
				continue;

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				String child_name = child.getNodeName();

				if (child_name.equals(xs_prefix_ + "appinfo")) {

					String annotation = child.getTextContent().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", "");

					Element e = (Element) child;

					String src = e.getAttribute("source");

					if (src != null && !src.isEmpty())
						annotation += ", URI-reference = " + src;

					if (annotation != null && !annotation.isEmpty())
						return annotation;
				}

			}

		}

		return null;
	}

	/**
	 * Extract annotation from xs:annotation/xs:documentation.
	 *
	 * @param node current node
	 * @param one_liner whether return one-liner annotation or exact one
	 * @return String documentation of annotation
	 */
	public String extractDocumentation(Node node, boolean one_liner) {

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (!anno.getNodeName().equals(xs_prefix_ + "annotation"))
				continue;

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				String child_name = child.getNodeName();

				if (child_name.equals(xs_prefix_ + "documentation")) {

					String text = child.getTextContent();

					if (one_liner) {

						String annotation = text.replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", "");

						Element e = (Element) child;

						String src = e.getAttribute("source");

						if (src != null && !src.isEmpty())
							annotation += ", URI-reference = " + src;

						if (annotation != null && !annotation.isEmpty())
							return annotation;
					}

					else if (text != null && !text.isEmpty())
						return text;
				}

			}

		}

		return null;
	}

	/**
	 * Instance message digest for check sum.
	 *
	 * @param check_sum_algorithm algorithm name of message digest
	 * @return whether algorithm name is valid or not
	 */
	public boolean setCheckSumAlgorithm(String check_sum_algorithm) {

		try {

			check_sum_message_digest = MessageDigest.getInstance(check_sum_algorithm);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return false;
		}

		this.check_sum_algorithm = check_sum_algorithm;

		return true;
	}

}
