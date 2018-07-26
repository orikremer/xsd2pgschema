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

import java.nio.file.Files;
import java.nio.file.Path;
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

	/** The root schema location. */
	public String root_schema_location = "";

	/** The relational model extension. */
	protected boolean rel_model_ext = true;

	/** The relational data extension. */
	protected boolean rel_data_ext = true;

	/** The wild card extension. */
	public boolean wild_card = true;

	/** Whether to add document key in PostgreSQL DDL. */
	public boolean document_key = true;

	/** Whether to add serial key in PostgreSQL DDL. */
	public boolean serial_key = false;

	/** Whether to add XPath key in PostgreSQL DDL. */
	public boolean xpath_key = false;

	/** Whether to retain case sensitive name in PostgreSQL DDL. */
	protected boolean case_sense = true;

	/** Whether to enable explicit named schema. */
	public boolean pg_named_schema = false;

	/** Whether to prefer local XML Schema file. */
	public boolean cache_xsd = true;

	/** Whether to output PostgreSQL DDL. */
	public boolean ddl_output = false;

	/** Whether to retain primary key/foreign key constraint in PostgreSQL DDL. */
	public boolean retain_key = true;

	/** Whether not to retrieve field annotation in PostgreSQL DDL. */
	public boolean no_field_anno = true;

	/** Whether to execute XML Schema validation. */
	public boolean validate = false;

	/** Whether to enable canonical XML Schema validation (validate only whether document is well-formed). */
	public boolean full_check = true;

	/** Whether to use TSV format in PostgreSQL data migration. */
	protected boolean pg_tab_delimiter = true;

	/** The current delimiter code. */
	public char pg_delimiter = '\t';

	/** The current null code. */
	public String pg_null = PgSchemaUtil.pg_tsv_null;

	/** The verbose mode. */
	public boolean verbose = false;

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

	/** The list of discarded document key name. */
	protected HashSet<String> discarded_document_key_names = null;

	/** The list of in-place document key name. */
	protected HashSet<String> inplace_document_key_names = null;

	/** The name of hash algorithm. */
	public String hash_algorithm = PgSchemaUtil.def_hash_algorithm;

	/** The size of hash key. */
	public PgHashSize hash_size = PgHashSize.defaultSize();

	/** The size of serial key. */
	public PgSerSize ser_size = PgSerSize.defaultSize();

	/** Whether to adopt strict synchronization (insert if not exists, update if required, and delete if XML not exists). */
	public boolean sync = false;

	/** Whether to adopt weak synchronization (insert if not exists, no update even if exists, no deletion). */
	public boolean sync_weak = false;

	/** Whether to dry-run synchronization (no update on existing check sum files). */
	public boolean sync_dry_run = false;

	/** Whether to run diagnostic synchronization (set all constraints deferred). */
	public boolean sync_rescue = false;

	/** Whether in-place document key exists. */
	public boolean inplace_document_key = false;

	/** Whether to append document key if in-place key not exists. */
	public boolean document_key_if_no_in_place = false;

	/** Whether to fill @default value. */
	public boolean fill_default_value = false;

	/** The directory path contains check sum files. */
	public Path check_sum_dir_path = null;

	/** The default algorithm for check sum. */
	public String check_sum_algorithm = PgSchemaUtil.def_check_sum_algorithm;

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
		inplace_document_key_names = new HashSet<String>();

	}

	/**
	 * Instance of PgSchemaOption for JSON Schema conversion.
	 *
	 * @param json_type JSON type
	 */
	public PgSchemaOption(JsonType json_type) {

		setDefaultForJsonSchema(json_type);

		discarded_document_key_names = new HashSet<String>();
		inplace_document_key_names = new HashSet<String>();

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
	 * @return int the minimum size of field
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

		int last_pos = qname.indexOf(':');

		return last_pos == -1 ? qname : qname.substring(last_pos + 1);
	}

	/**
	 * Set internal status corresponding to --doc-key and --no-doc-key options.
	 *
	 * @param doc_key whether to add document key
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
	 * Decide whether to add document key.
	 */
	public void resolveDocKeyOption() {

		if (_doc_key || _no_doc_key)
			document_key = _doc_key;

		inplace_document_key = inplace_document_key_names.size() > 0;

		if (document_key && inplace_document_key) {
			inplace_document_key = false;
			inplace_document_key_names.clear();
			System.out.println("Ignored --inplace-doc-key-name option because default document key was enabled.");
		}

		if (document_key_if_no_in_place && !inplace_document_key) {
			document_key_if_no_in_place = false;
			document_key = true;
		}

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
	 * Use tab delimiter code.
	 */
	public void usePgTsv() {

		pg_tab_delimiter = true;
		pg_delimiter = '\t';
		pg_null = PgSchemaUtil.pg_tsv_null;

	}

	/**
	 * Use comma delimiter code.
	 */
	public void usePgCsv() {

		pg_tab_delimiter = false;
		pg_delimiter = ',';
		pg_null = "";

	}

	/**
	 * Set case insensitive mode.
	 */
	public void setCaseInsensitive() {

		case_sense = false;

		document_key_name = document_key_name.toLowerCase();
		serial_key_name = serial_key_name.toLowerCase();
		xpath_key_name = xpath_key_name.toLowerCase();

		if (!discarded_document_key_names.isEmpty()) {

			String[] names = discarded_document_key_names.toArray(new String[0]);

			discarded_document_key_names.clear();

			for (String name : names)
				discarded_document_key_names.add(name.toLowerCase());

		}

		if (!inplace_document_key_names.isEmpty()) {

			String[] names = inplace_document_key_names.toArray(new String[0]);

			inplace_document_key_names.clear();

			for (String name : names)
				inplace_document_key_names.add(name.toLowerCase());

		}

	}

	/**
	 * Set document key name.
	 *
	 * @param document_key_name document key name
	 */
	public void setDocumentKeyName(String document_key_name) {

		if (document_key_name == null || document_key_name.isEmpty())
			return;

		this.document_key_name = case_sense ? document_key_name : document_key_name.toLowerCase();

	}

	/**
	 * Set serial key name.
	 *
	 * @param serial_key_name serial key name
	 */
	public void setSerialKeyName(String serial_key_name) {

		if (serial_key_name == null || serial_key_name.isEmpty())
			return;

		this.serial_key_name = case_sense ? serial_key_name : serial_key_name.toLowerCase();

	}

	/**
	 * Set XPath key name.
	 *
	 * @param xpath_key_name xpath key name
	 */
	public void setXPathKeyName(String xpath_key_name) {

		if (xpath_key_name == null || xpath_key_name.isEmpty())
			return;

		this.xpath_key_name = case_sense ? xpath_key_name : xpath_key_name.toLowerCase();

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

		return discarded_document_key_names.add(case_sense ? discarded_document_key_name : discarded_document_key_name.toLowerCase());
	}

	/**
	 * Add in-place document key name.
	 *
	 * @param inplace_document_key_name in-place document key name
	 * @return result of addition
	 */
	public boolean addInPlaceDocKeyName(String inplace_document_key_name) {

		if (inplace_document_key_name == null || inplace_document_key_name.isEmpty())
			return false;

		return inplace_document_key_names.add(case_sense ? inplace_document_key_name : inplace_document_key_name.toLowerCase());
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

					annotation = PgSchemaUtil.collapseWhiteSpace(child.getTextContent());

					if (!annotation.isEmpty())
						annotation += "\n-- ";

					Element e = (Element) child;

					String src = e.getAttribute("source");

					if (src != null && !src.isEmpty())
						annotation += (is_table ? "\n-- " : ", ") + "URI-reference = " + src + (is_table ? "\n-- " : ", ");

				}

				else if (child_name.equals(xs_prefix_ + "documentation")) {

					annotation += PgSchemaUtil.collapseWhiteSpace(child.getTextContent());

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

					String annotation = PgSchemaUtil.collapseWhiteSpace(child.getTextContent());

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
	 * @param one_liner return whether one-liner annotation or exact one
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

						String annotation = PgSchemaUtil.collapseWhiteSpace(text);

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
	 * @return boolean whether algorithm name is valid
	 */
	public boolean setCheckSumAlgorithm(String check_sum_algorithm) {

		try {

			MessageDigest.getInstance(check_sum_algorithm);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return false;
		}

		this.check_sum_algorithm = check_sum_algorithm;

		return true;
	}

	/**
	 * Return whether synchronization is possible.
	 *
	 * @param allow_sync_weak whether to allow weak synchronization
	 * @return boolean whether synchronization is possible
	 */
	public boolean isSynchronizable(boolean allow_sync_weak) {
		return (allow_sync_weak && sync_weak) || (sync && check_sum_dir_path != null && Files.isDirectory(check_sum_dir_path));
	}

}
