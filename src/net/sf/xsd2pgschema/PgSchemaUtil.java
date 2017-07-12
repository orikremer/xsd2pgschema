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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Utility functions and default values.
 *
 * @author yokochi
 */
public class PgSchemaUtil {

	/** The namespace URI representing XML Schema 1.x. */
	public final static String xs_namespace_uri = "http://www.w3.org/2001/XMLSchema";

	/** The namespace URI representing JSON Schema. */
	public final static String json_schema_def = "http://json-schema.org/schema#";

	/** The document key name in PostgreSQL. */
	public final static String document_key_name = "document_id";

	/** The serial key name in PostgreSQL. */
	public final static String serial_key_name = "serial_id";

	/** The XPath key name in PostgreSQL. */
	public final static String xpath_key_name = "xpath_id";

	/** The name of xs;simpleContent in PostgreSQL. */
	public final static String simple_cont_name = "content";

	/** The name of xs:any in PostgreSQL. */
	public final static String any_elem_name = "any_element";

	/** The name of xs:anyAttribute in PostgreSQL. */
	public final static String any_attr_name = "any_attribute";

	/** The default hash algorithm. */
	public final static String def_hash_algorithm = "SHA-1";

	/** The default host name. */
	public final static String host = "localhost";

	/** The default port number. */
	public final static int port = 5432;

	/** The minimum word length for index. */
	public final static int min_word_len = 1;

	/** The PostgreSQL maximum length for enumeration/constraint. */
	public final static int max_enum_len = 63;

	/** The default length of indent spaces. */
	public final static int indent_spaces = 2;

	/** The maximum number of XML files to be sorted for balancing data. */
	public final static int max_sort_xml_files = 65536;

	/** The prefix of directory name for sharding. */
	public final static String shard_dir_prefix = "part-";

	/** The prefix of directory name for multi-threading. */
	public final static String thrd_dir_prefix = "thrd-";

	/** The Sphinx schema file name. */
	public final static String sphinx_schema_name = ".schema_part.xml";

	/** The Sphinx configuration file name. */
	public final static String sphinx_conf_name = "data_source.conf";

	/** The prefix of Sphinx data source file. */
	public final static String sphinx_document_prefix = "document_part_";

	/** The Sphinx data source file name. */
	public final static String sphinx_data_source_name = "data_source.xml";

	/** The field name of Sphinx dictionary. */
	public final static String trigram_field_name = "trigrams";

	/** The text node of XPath notation. */
	public final static String text_node_name = "text()";

	/** The comment node of XPath notation. */
	public final static String comment_node_name = "comment()";

	/** The PostgreSQL reserved words. */
	public final static String[] reserved_words = { "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AUTHORIZATION", "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLLATION", "COLUMN", "CONCURRENTLY", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL", "NOT", "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "TABLE", "TABLESAMPLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING", "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH" };

	/** The PostgreSQL reserved operator codes. */
	public final static String[] reserved_ops = { "+", "-", "*", "/", "%", "^", "|/", "||/", "!", "!!", "@", "&", "|", "#", "~", "<<", ">>" };

	/** The PostgreSQL reserved operator codes escaping regular expression match. */
	public final static String[] reserved_ops_rex = { "\\+", "-", "\\*", "/", "%", "\\^", "\\|/", "\\|\\|/", "!", "!!", "@", "\\&", "\\|", "#", "~", "<<", ">>" };

	/**
	 * Return input stream of file with gzip decompression.
	 *
	 * @param  file plane file or gzip compressed file
	 * @return InputStream input stream of file
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static InputStream getInputStream(File file) throws FileNotFoundException, IOException {
		return FilenameUtils.getExtension(file.getName()).equals("gz") ? new GZIPInputStream(new FileInputStream(file)) : new FileInputStream(file);
	}

	/**
	 * Return input stream of schema location.
	 *
	 * @param schema_location schema location
	 * @param schema_parent parent of schema location
	 * @return InputStream input stream of schema location
	 */
	public static InputStream getInputStream(String schema_location, String schema_parent) {

		// local XML Schema file

		if (!schema_location.startsWith("http:") && !schema_location.startsWith("https:")) {

			File schema_file = new File(schema_location);

			if (!schema_file.exists() && schema_parent != null)
				return getInputStream(schema_parent + "/" + getFileName(schema_location), null);

			try {

				return new FileInputStream(schema_file);

			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return null;
			}

		}

		// try to retrieve XML Schema file via http protocol

		else if (schema_location.startsWith("http:")) {

			System.err.println("Retrieving " + schema_location + "...");

			try {

				URL url = new URL(schema_location);
				URLConnection conn = url.openConnection();

				return conn.getInputStream();

			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}

		}

		// try to retrieve XML Schema file via https protocol

		else {

			System.err.println("Retrieving " + schema_location + "...");

			TrustManager[] tm = new TrustManager[] { new X509TrustManager() {

				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}

				@Override
				public void checkClientTrusted(X509Certificate[] certs, String authType) {
				}

				@Override
				public void checkServerTrusted(X509Certificate[] certs, String authType) {
				}

			}

			};

			try {

				URL url = new URL(schema_location);

				SSLContext sc = SSLContext.getInstance("SSL");
				sc.init(null, tm, new java.security.SecureRandom());

				HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
					@Override
					public boolean verify(String hostname, SSLSession session) {
						return true;
					}
				});

				HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
				conn.setSSLSocketFactory(sc.getSocketFactory());

				return conn.getInputStream();

			} catch (NoSuchAlgorithmException | KeyManagementException | IOException e) {
				e.printStackTrace();
				return null;
			}

		}

	}

	/**
	 * Return file of schema location.
	 *
	 * @param schema_location schema location
	 * @param schema_parent parent of schema location
	 * @return File file of schema location
	 */
	public static File getFile(String schema_location, String schema_parent) {

		if (!schema_location.startsWith("http:") && !schema_location.startsWith("https:"))
			return new File(schema_location);

		String schema_file_name = getFileName(schema_location);

		File schema_file = new File(schema_file_name);

		if (schema_file.exists())
			return schema_file;

		InputStream is = getInputStream(schema_location, schema_parent);

		if (is == null)
			return null;

		try {

			IOUtils.copy(is, new FileOutputStream(schema_file));

			return schema_file;

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * Copy file of schema location in current directory if not exist.
	 *
	 * @param schema_location schema location
	 * @param schema_parent parent of schema location
	 * @return File file of schema location
	 */
	public static File copyFileIfNotExist(String schema_location, String schema_parent) {

		String schema_file_name = getFileName(schema_location);

		File schema_file = new File(schema_file_name);

		if (schema_file.exists())
			return schema_file;

		InputStream is = getInputStream(schema_location, schema_parent);

		if (is == null)
			return null;

		try {

			IOUtils.copy(is, new FileOutputStream(schema_file));

			return schema_file;

		} catch (IOException e) {
			return null;
		}

	}

	/**
	 * Return name of schema location.
	 *
	 * @param schema_location schema location
	 * @return String schema location
	 */
	public static String getName(String schema_location) {

		if (schema_location.startsWith("http:") || schema_location.startsWith("https:"))
			return schema_location;

		File schema_file = new File(schema_location);

		return schema_file.getName();
	}

	/**
	 * Return file name of schema location.
	 *
	 * @param schema_location schema location
	 * @return String file name of schema location
	 */
	public static String getFileName(String schema_location) {

		if (schema_location.startsWith("http:") || schema_location.startsWith("https:")) {

			try {

				URL url = new URL(schema_location);

				return Paths.get(url.getPath()).getFileName().toString();

			} catch (MalformedURLException e) {
				e.printStackTrace();
			}

		}

		File schema_file = new File(schema_location);

		return schema_file.getName();
	}

	/**
	 * Return parent of schema location.
	 *
	 * @param schema_location schema location
	 * @return String parent name of schema location
	 */
	public static String getParent(String schema_location) {

		if (schema_location.startsWith("http:") || schema_location.startsWith("https:")) {

			try {

				URL url = new URL(schema_location);

				return new URL(url.getProtocol(), url.getHost(), url.getPort(), Paths.get(url.getPath()).getParent().toString()).toString();

			} catch (MalformedURLException e) {
				e.printStackTrace();
			}

		}

		File schema_file = new File(schema_location);

		return schema_file.getParent();
	}

	/**
	 * Suggest new name in PostgreSQL for a given name.
	 *
	 * @param name name
	 * @return String name without name collision against PostgreSQL reserved words
	 */
	public static String avoidPgReservedWords(String name) {

		if (Arrays.asList(reserved_words).contains(name.toUpperCase()) || name.matches("^.*[A-Z].*$"))
			return "\"" + name + "\"";

		for (String ops : reserved_ops) {

			if (name.contains(ops))
				return "\"" + name + "\"";

		}

		return name;
	}

	/**
	 * Suggest new name in PostgreSQL for a given name.
	 *
	 * @param name name
	 * @return String name without name collision against PostgreSQL operators
	 */
	public static String avoidPgReservedOps(String name) {

		boolean contain_ops = false;

		for (String ops : reserved_ops) {

			if (name.contains(ops)) {
				contain_ops = true;
				break;
			}

		}

		if (!contain_ops)
			return name;

		for (String ops_rex : reserved_ops_rex)
			name = name.replaceAll(ops_rex, "_");

		return name;
	}

	/**
	 * Extract annotation from xs:annotation/xs:appinfo|xs:documentation.
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 * @param is_table the is table
	 * @return String annotation
	 */
	public static String extractAnnotation(PgSchema schema, Node node, boolean is_table) {

		String xs_prefix_ = schema.xs_prefix_;

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (!anno.getNodeName().equals(xs_prefix_ + "annotation"))
				continue;

			String annotation = "";

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				String child_name = child.getNodeName();

				if (child_name.equals(xs_prefix_ + "appinfo")) {

					annotation = child.getTextContent().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", "");

					Element e = (Element) child;

					String src = e.getAttribute("source");

					annotation = (src == null || src.isEmpty() ? annotation : annotation + (is_table ? "\n-- " : ", ") + "URI-reference = " + src) + (is_table ? "\n-- " : ", ");

				}

				else if (child_name.equals(xs_prefix_ + "documentation")) {

					annotation += child.getTextContent().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", "");

					Element e = (Element) child;

					String src = e.getAttribute("source");

					return (src == null || src.isEmpty() ? annotation : annotation + (is_table ? "\n-- " : ", ") + "URI-reference = " + src);
				}

			}

		}

		return null;
	}

	/**
	 * Extract annotation from xs:annotation/xs:appinfo.
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 * @return String appinfo of annotation
	 */
	public static String extractAppinfo(PgSchema schema, Node node) {

		String xs_prefix_ = schema.xs_prefix_;

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (!anno.getNodeName().equals(xs_prefix_ + "annotation"))
				continue;

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				String child_name = child.getNodeName();

				if (child_name.equals(xs_prefix_ + "appinfo")) {

					String annotation = child.getTextContent().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", "");

					Element e = (Element) child;

					String src = e.getAttribute("source");

					return (src == null || src.isEmpty() ? annotation : annotation + ", URI-reference = " + src);
				}

			}

		}

		return null;
	}

	/**
	 * Extract annotation from xs:annotation/xs:documentation.
	 *
	 * @param schema PostgreSQL data model
	 * @param node current node
	 * @return String documentation of annotation
	 */
	public static String extractDocumentation(PgSchema schema, Node node) {

		String xs_prefix_ = schema.xs_prefix_;

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (!anno.getNodeName().equals(xs_prefix_ + "annotation"))
				continue;

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				String child_name = child.getNodeName();

				if (child_name.equals(xs_prefix_ + "documentation")) {

					String annotation = child.getTextContent().replaceAll("\\s+", " ").replaceAll("  ", " ").replaceFirst("^ ", "").replaceFirst(" $", "");

					Element e = (Element) child;

					String src = e.getAttribute("source");

					return (src == null || src.isEmpty() ? annotation : annotation + ", URI-reference = " + src);
				}

			}

		}

		return null;
	}

	/**
	 * Return array of target file.
	 *
	 * @param file_names list of file name
	 * @param filter file name filter
	 * @return File[] array of target file
	 */
	public static File[] getTargetFiles(HashSet<String> file_names, FilenameFilter filter) {

		List<File> files = new ArrayList<File>();

		file_names.forEach(file_name -> {

			File file = new File(file_name);

			if (!file.exists()) {
				System.err.println("Not found + " + file.getPath());
				System.exit(1);
			}

			if (file.isFile()) {

				if (filter.accept(null, file_name))
					files.add(file);

			}

			else if (file.isDirectory())
				files.addAll(Arrays.asList(file.listFiles(filter)));

		});

		return (File[]) files.toArray(new File[0]);
	}

}
