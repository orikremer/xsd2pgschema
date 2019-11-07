/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2019 Masashi Yokochi

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

package net.sf.xsd2pgschema.option;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;

import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.annotations.Flat;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.docbuilder.JsonType;
import net.sf.xsd2pgschema.serverutil.PgSchemaClientType;
import net.sf.xsd2pgschema.serverutil.PgSchemaServerQuery;
import net.sf.xsd2pgschema.serverutil.PgSchemaServerQueryType;
import net.sf.xsd2pgschema.serverutil.PgSchemaServerReply;
import net.sf.xsd2pgschema.type.PgDateType;
import net.sf.xsd2pgschema.type.PgDecimalType;
import net.sf.xsd2pgschema.type.PgHashSize;
import net.sf.xsd2pgschema.type.PgIntegerType;
import net.sf.xsd2pgschema.type.PgSerSize;

/**
 * PostgreSQL data model option.
 *
 * @author yokochi
 */
public class PgSchemaOption implements Serializable {

	/** The default version ID. */
	private static final long serialVersionUID = 1L;

	/** The root schema location. */
	public String root_schema_location = "";

	/** The relational model extension. */
	public boolean rel_model_ext = true;

	/** The relational data extension. */
	public boolean rel_data_ext = true;

	/** Whether to inline simple content. */
	public boolean inline_simple_cont = false;

	/** Whether to realize simple bridge tables, otherwise implement them as PostgreSQL views by default. */
	public boolean realize_simple_brdg = false;

	/** The wild card extension. */
	public boolean wild_card = true;

	/** Whether to add document key in PostgreSQL DDL. */
	public boolean document_key = true;

	/** Whether to add serial key in PostgreSQL DDL. */
	public boolean serial_key = false;

	/** Whether to add XPath key in PostgreSQL DDL. */
	public boolean xpath_key = false;

	/** Whether to retain case sensitive name in PostgreSQL DDL. */
	public boolean case_sense = true;

	/** Whether to enable explicit named schema. */
	public boolean pg_named_schema = false;

	/** Whether to retain primary key/foreign key/unique constraint in PostgreSQL DDL. */
	public boolean pg_retain_key = true;

	/** The max tuple size of unique constraint in PostgreSQL DDL derived from xs:key (ignore the limit if non-positive value). */
	public int pg_max_uniq_tuple_size = 1;

	/** Whether to use TSV format in PostgreSQL data migration. */
	public boolean pg_tab_delimiter = true;

	/** The current delimiter code. */
	public char pg_delimiter = '\t';

	/** Whether to set annotation as comment in PostgreSQL DB. */
	public boolean pg_comment_on = false;

	/** The current null code. */
	public String pg_null = PgSchemaUtil.pg_tsv_null;

	/** Whether to delete invalid XML. */
	public boolean del_invalid_xml = false;

	/** The verbose mode. */
	public boolean verbose = false;

	/** Whether to prefer local XML Schema file. */
	@Flat
	public boolean cache_xsd = true;

	/** Whether to output PostgreSQL DDL. */
	@Flat
	public boolean ddl_output = false;

	/** Whether not to retrieve field annotation in PostgreSQL DDL. */
	@Flat
	public boolean no_field_anno = true;

	/** Whether to execute XML Schema validation. */
	@Flat
	public boolean validate = false;

	/** Whether to enable canonical XML Schema validation (validate only whether document is well-formed). */
	@Flat
	public boolean full_check = true;

	/** The default document key name in PostgreSQL DDL. */
	@Flat
	public final String def_document_key_name = "document_id";

	/** The default serial key name in PostgreSQL DDL. */
	@Flat
	public final String def_serial_key_name = "serial_id";

	/** The default XPath key name in PostgreSQL DDL. */
	@Flat
	public final String def_xpath_key_name = "xpath_id";

	/** The document key name in PostgreSQL DDL. */
	public String document_key_name = def_document_key_name;

	/** The serial key name in PostgreSQL DDL. */
	public String serial_key_name = def_serial_key_name;

	/** The XPath key name in PostgreSQL DDL. */
	public String xpath_key_name = def_xpath_key_name;

	/** The list of discarded document key name. */
	public HashSet<String> discarded_document_key_names = null;

	/** The list of in-place document key name. */
	public HashSet<String> in_place_document_key_names = null;

	/** The mapping of integer numbers in PostgreSQL. */
	public PgIntegerType pg_integer = PgIntegerType.defaultType();

	/** The mapping of decimal numbers in PostgreSQL. */
	public PgDecimalType pg_decimal = PgDecimalType.defaultType();

	/** The mapping of xs:date in PostgreSQL. */
	public PgDateType pg_date = PgDateType.defaultType();

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
	public boolean in_place_document_key = false;

	/** Whether to append document key if in-place key not exists. */
	public boolean document_key_if_no_in_place = false;

	/** Whether to fill @default value. */
	public boolean fill_default_value = false;

	/** The directory name contains check sum files. */
	public String check_sum_dir_name = null;

	/** The default algorithm for check sum. */
	public String check_sum_algorithm = PgSchemaUtil.def_check_sum_algorithm;

	/** The default file extension of check sum file. */
	public String check_sum_ext = check_sum_algorithm.toLowerCase();

	/** The JSON item name of xs:simpleContent. */
	public String simple_content_name = PgSchemaUtil.simple_content_name;

	/** Whether to use data model of PgSchema server. */
	@Flat
	public boolean pg_schema_server = true;

	/** The default host name of PgSchema server. */
	@Flat
	public String pg_schema_server_host = PgSchemaUtil.pg_schema_server_host;

	/** The default port number of PgSchema server. */
	@Flat
	public int pg_schema_server_port = PgSchemaUtil.pg_schema_server_port;

	/** The default lifetime of unused PostgreSQL data model on PgSchema server in milliseconds. */
	@Flat
	public long pg_schema_server_lifetime = PgSchemaUtil.pg_schema_server_lifetime;

	/** The prefix of xs_namespace_uri. */
	@Flat
	public String xs_prefix = null;

	/** The xs_prefix.isEmpty() ? "" : xs_prefix + ":". */
	@Flat
	public String xs_prefix_ = null;

	/** Whether XML post editor has been applied. */
	@Flat
	public boolean post_editor_resolved = false;

	/** Whether attribute selection has been resolved. */
	@Flat
	public boolean attr_resolved = false;

	/** Whether field selection has been resolved. */
	@Flat
	public boolean field_resolved = false;

	/** Whether show orphan table. */
	@Flat
	public boolean show_orphan_table = false;

	/** The internal status corresponding to --doc-key option. */
	@Flat
	private boolean _doc_key = false;

	/** The internal status corresponding to --no-doc-key option. */
	@Flat
	private boolean _no_doc_key = false;

	/** Whether check sum directory exists. */
	@Flat
	private boolean _check_sum_dir_exists = false;

	/**
	 * Instance of PostgreSQL data model option.
	 *
	 * @param document_key the document key
	 */
	public PgSchemaOption(boolean document_key) {

		this.document_key = document_key;

		discarded_document_key_names = new HashSet<String>();
		in_place_document_key_names = new HashSet<String>();

	}

	/**
	 * Instance of PostgreSQL data model option for JSON Schema conversion.
	 *
	 * @param json_type JSON type
	 */
	public PgSchemaOption(JsonType json_type) {

		setDefaultForJsonSchema(json_type);

		discarded_document_key_names = new HashSet<String>();
		in_place_document_key_names = new HashSet<String>();

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

		rel_data_ext = document_key = serial_key = xpath_key = pg_retain_key = false;
		inline_simple_cont = true;

	}

	/**
	 * Enable relational data extension.
	 */
	public void enableRelDataExt() {

		rel_model_ext = rel_data_ext = document_key = pg_retain_key = true;
		inline_simple_cont = false;

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

		in_place_document_key = in_place_document_key_names.size() > 0;

		if (document_key && in_place_document_key) {
			in_place_document_key = false;
			in_place_document_key_names.clear();
			System.out.println("Ignored --inplace-doc-key-name option because default document key was enabled.");
		}

		if (document_key_if_no_in_place && !in_place_document_key) {
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

			String[] names = discarded_document_key_names.stream().toArray(String[]::new);

			discarded_document_key_names.clear();

			for (String name : names)
				discarded_document_key_names.add(name.toLowerCase());

		}

		if (!in_place_document_key_names.isEmpty()) {

			String[] names = in_place_document_key_names.stream().toArray(String[]::new);

			in_place_document_key_names.clear();

			for (String name : names)
				in_place_document_key_names.add(name.toLowerCase());

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
	 * @param in_place_document_key_name in-place document key name
	 * @return result of addition
	 */
	public boolean addInPlaceDocKeyName(String in_place_document_key_name) {

		if (in_place_document_key_name == null || in_place_document_key_name.isEmpty())
			return false;

		return in_place_document_key_names.add(case_sense ? in_place_document_key_name : in_place_document_key_name.toLowerCase());
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
		check_sum_ext = check_sum_algorithm.toLowerCase();

		return true;
	}

	/**
	 * Return whether synchronization is possible.
	 *
	 * @param allow_sync_weak whether to allow weak synchronization
	 * @return boolean whether synchronization is possible
	 */
	public boolean isSynchronizable(boolean allow_sync_weak) {
		return (allow_sync_weak && sync_weak) || (sync && check_sum_dir_name != null && (_check_sum_dir_exists || (_check_sum_dir_exists = Files.isDirectory(Paths.get(check_sum_dir_name)))));
	}

	/**
	 * Set item name in JSON document of xs:simpleContent.
	 *
	 * @param simple_content_name item name of xs:simpleContent in JSON document
	 */
	public void setSimpleContentName(String simple_content_name) {

		if (simple_content_name == null)
			simple_content_name= PgSchemaUtil.simple_content_name;

		this.simple_content_name = case_sense ? simple_content_name : simple_content_name.toLowerCase();

	}

	/**
	 * Send PING query to PgSchema server.
	 *
	 * @param fst_conf FST configuration
	 * @return boolean whether PgSchema server is alive
	 */
	public boolean pingPgSchemaServer(FSTConfiguration fst_conf) {

		if (!pg_schema_server)
			return false;

		try (Socket socket = new Socket(InetAddress.getByName(pg_schema_server_host), pg_schema_server_port)) {

			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.PING));

			PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);
			/*
			in.close();
			out.close();
			 */
			return reply.message.contains("OK");

		} catch (IOException | ClassNotFoundException e) {
			return false;
		}

	}

	/**
	 * Send MATCH query to PgSchema server.
	 *
	 * @param fst_conf FST configuration
	 * @param client_type PgSchema client type
	 * @return boolean whether PgSchema server does not have data model (true)
	 */
	public boolean matchPgSchemaServer(FSTConfiguration fst_conf, PgSchemaClientType client_type) {

		if (!pg_schema_server)
			return false;

		try (Socket socket = new Socket(InetAddress.getByName(pg_schema_server_host), pg_schema_server_port)) {

			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.MATCH, this, client_type));

			PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);
			/*
			in.close();
			out.close();
			 */
			return !reply.message.contains("NOT");

		} catch (IOException | ClassNotFoundException e) {
			return false;
		}

	}

	/**
	 * Return equality of PostgreSQL data model option.
	 *
	 * @param option compared PostgreSQL data model option
	 * @return boolean whether the PostgreSQL data model option matches
	 */
	public boolean equals(PgSchemaOption option) {

		if (!root_schema_location.equals(option.root_schema_location))
			return false;

		if (rel_model_ext != option.rel_model_ext)
			return false;

		if (rel_data_ext != option.rel_data_ext)
			return false;

		if (inline_simple_cont != option.inline_simple_cont)
			return false;

		if (realize_simple_brdg != option.realize_simple_brdg)
			return false;

		if (wild_card != option.wild_card)
			return false;

		if (document_key != option.document_key)
			return false;

		if (serial_key != option.serial_key)
			return false;

		if (xpath_key != option.xpath_key)
			return false;

		if (case_sense != option.case_sense)
			return false;

		if (pg_named_schema != option.pg_named_schema)
			return false;

		if (pg_retain_key != option.pg_retain_key)
			return false;

		if (pg_max_uniq_tuple_size != option.pg_max_uniq_tuple_size)
			return false;

		if (pg_tab_delimiter != option.pg_tab_delimiter)
			return false;

		if (pg_delimiter != option.pg_delimiter)
			return false;

		if (!pg_null.equals(option.pg_null))
			return false;

		if (del_invalid_xml != option.del_invalid_xml)
			return false;

		if (verbose != option.verbose)
			return false;

		if (!document_key_name.equals(option.document_key_name))
			return false;

		if (!serial_key_name.equals(option.serial_key_name))
			return false;

		if (!xpath_key_name.equals(option.xpath_key_name))
			return false;

		if (!pg_integer.equals(option.pg_integer))
			return false;

		if (!pg_decimal.equals(option.pg_decimal))
			return false;

		if (!pg_date.equals(option.pg_date))
			return false;

		if (!hash_algorithm.equals(option.hash_algorithm))
			return false;

		if (!hash_size.equals(option.hash_size))
			return false;

		if (!ser_size.equals(option.ser_size))
			return false;

		if (sync != option.sync)
			return false;

		if (sync_weak != option.sync_weak)
			return false;

		if (sync_dry_run != option.sync_dry_run)
			return false;

		if (sync_rescue != option.sync_rescue)
			return false;

		if (in_place_document_key != option.in_place_document_key)
			return false;

		if (document_key_if_no_in_place != option.document_key_if_no_in_place)
			return false;

		if (fill_default_value != option.fill_default_value)
			return false;

		if (!check_sum_algorithm.equals(option.check_sum_algorithm))
			return false;

		if (discarded_document_key_names != null && option.discarded_document_key_names != null) {

			if (discarded_document_key_names.size() > 0 || option.discarded_document_key_names.size() > 0) {

				if (!discarded_document_key_names.containsAll(option.discarded_document_key_names))
					return false;

				if (!option.discarded_document_key_names.containsAll(discarded_document_key_names))
					return false;

			}

		}

		if (in_place_document_key_names != null && option.in_place_document_key_names != null) {

			if (in_place_document_key_names.size() > 0 || option.in_place_document_key_names.size() > 0) {

				if (!in_place_document_key_names.containsAll(option.in_place_document_key_names))
					return false;

				if (!option.in_place_document_key_names.containsAll(in_place_document_key_names))
					return false;

			}

		}

		else if (in_place_document_key_names != null || option.in_place_document_key_names != null)
			return false;

		if (check_sum_dir_name != null && option.check_sum_dir_name != null) {

			if (!check_sum_dir_name.equals(option.check_sum_dir_name))
				return false;

		}

		else if (check_sum_dir_name != null || option.check_sum_dir_name != null)
			return false;

		// JSON builder option

		if (!simple_content_name.equals(option.simple_content_name))
			return false;

		return true;
	}

}
