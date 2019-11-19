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

package net.sf.xsd2pgschema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectOutput;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.github.antlr.grammars_v4.xpath.xpathBaseListener;
import com.github.antlr.grammars_v4.xpath.xpathLexer;
import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.MainContext;

import net.sf.xsd2pgschema.xpathparser.XPathComp;
import net.sf.xsd2pgschema.xpathparser.XPathCompList;

/**
 * Utility functions and default values.
 *
 * @author yokochi
 */
public class PgSchemaUtil {

	/** The namespace URI representing XML Schema 1.x. */
	public static final String xs_namespace_uri = "http://www.w3.org/2001/XMLSchema";

	/** The namespace URI representing XML Schema instance. */
	public static final String xsi_namespace_uri = "http://www.w3.org/2001/XMLSchema-instance";

	/** The prefix of xsi_namespace_uri. */
	public static final String xsi_prefix = "xsi";

	/** The default PostgreSQL schema name. */
	public static final String pg_public_schema_name = "public";

	/** The PostgreSQL null value in TSV format. */
	public static final String pg_tsv_null = "\\N";

	/** The name of xs:simpleContent in PostgreSQL. */
	public static final String simple_content_name = "content";

	/** The name of xs:any in PostgreSQL. */
	public static final String any_name = "any_element";

	/** The name of xs:anyAttribute in PostgreSQL. */
	public static final String any_attribute_name = "any_attribute";

	/** The default XML version. */
	public static final String def_xml_version = "1.0";

	/** The default UTF-8 encoding. */
	public static final String def_encoding = "UTF-8";

	/** The default UTF-8 charset. */
	public static final Charset def_charset = StandardCharsets.UTF_8;

	/** The ISO-Latin-1 charset. */
	public static final Charset latin_1_charset = StandardCharsets.ISO_8859_1;

	/** The default hash algorithm. */
	public static final String def_hash_algorithm = "SHA-1";

	/** The default check sum algorithm. */
	public static final String def_check_sum_algorithm = "MD5";

	/** The default host name of PostgreSQL server. */
	public static final String pg_host = "localhost";

	/** The default port number of PostgreSQL server. */
	public static final int pg_port = 5432;

	/** The default host name of PgSchema server. */
	public static final String pg_schema_server_host = "localhost";

	/** The default post number of PgSchema server. */
	public static final int pg_schema_server_port = 5430;

	/** The default lifetime of unused PostgreSQL data model on PgSchema server in milliseconds. */
	public static final long pg_schema_server_lifetime = 86400 * 14 * 1000L;

	/** The limit number of table references, which is a trigger to detect circular dependency. */
	public static final int limit_table_refs = 256;

	/** The minimum word length for index. */
	public static final int min_word_len = 1;

	/** The threshold frequency for index dictionary. */
	public static final int freq_threshold = 10;

	/** The PostgreSQL maximum length for enumeration/constraint. */
	public static final int max_enum_len = 63;

	/** The default indent offset. */
	public static final int indent_offset = 2;

	/** The prefix of directory name for sharding. */
	public static final String shard_dir_prefix = "part-";

	/** The prefix of directory name for multi-threading. */
	public static final String thrd_dir_prefix = "thrd-";

	/** The namespace URI representing Sphinx xmlpipe2. */
	public static final String sph_namespace_uri = "http://sphinxsearch.com/xmlpipe2";

	/** The Sphinx membership operator. */
	public static final String sph_member_op = "__";

	/** The Sphinx schema file name. */
	public static final String sph_schema_name = ".schema_part.xml";

	/** The Sphinx configuration file name. */
	public static final String sph_conf_name = "data_source.conf";

	/** The prefix of Sphinx data source file. */
	public static final String sph_document_prefix = "document_part_";

	/** The Sphinx data source file name. */
	public static final String sph_data_source_name = "data_source.xml";

	/** The extracted Sphinx data source file name. */
	public static final String sph_data_extract_name = "data_extract.xml";

	/** The updated Sphinx data source file name. */
	public static final String sph_data_update_name = "data_update.xml";

	/** The Sphinx maximum field length. (related to max_xmlpipe2_field in sphinx.conf) */
	public static final int sph_max_field_len = 1024 * 1024 * 2;

	/** The start element of simple content in Sphinx data source. */
	public static final String sph_start_simple_content_elem = "<" + simple_content_name + ">";

	/** The end element of simple content in Sphinx data source. */
	public static final String sph_end_simple_content_elem = "</" + simple_content_name + ">\n";

	/** The Lucene dictionary file name. */
	public static final String lucene_dic_file_name = "dictionary";

	/** The text node of XPath notation. */
	public static final String text_node_name = "text()";

	/** The comment node of XPath notation. */
	public static final String comment_node_name = "comment()";

	/** The PostgreSQL reserved words. */
	public static final String[] pg_reserved_words = { "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AUTHORIZATION", "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLLATION", "COLUMN", "CONCURRENTLY", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL", "NOT", "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "TABLE", "TABLESAMPLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING", "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH" };

	/** The PostgreSQL reserved operator codes. */
	public static final String[] pg_reserved_ops = { ".", "-", "->", "->>", "-|-", "!", "!!", "!=", "?-", "?-|", "?#", "?|", "?||", "@", "@-@", "@@", "@@@", "@>", "*", "/", "&", "&&", "&<", "&<|", "&>", "#", "##", "#>", "#>>", "%", "^", "+", "<", "<->", "<@", "<^", "<<", "<<=", "<<|", "<=", "<>", "=", ">", ">^", ">=", ">>", ">>=", "|", "|/", "|&>", "|>>", "||", "||/", "~", "~=" };

	/** The PostgreSQL date format (ISO 8601). */
	public static final String pg_date_format = "yyyy-MM-dd";

	/** The PostgreSQL date/time format (ISO 8601). */
	public static final String pg_date_time_format = "yyyy-MM-dd HH:mm:ss.SSS";

	/** The PostgreSQL date/time format with time zone (ISO 8601). */
	public static final String pg_date_time_tz_format = "yyyy-MM-dd HH:mm:ss.SSSXXX";

	/** The minimum rows for creation of PostgreSQL index. */
	public static final int pg_min_rows_for_index = 1024;

	/** The maximum attribute columns for creation of PostgreSQL index on the attributes (except for in-place document key). */
	public static final int pg_max_attr_cols_for_index = 1;

	/** The limit number of attribute columns for creation of PostgreSQL index on the attributes (except for in-place document key). */
	public static final int pg_limit_attr_cols_for_index = 128;

	/** The maximum element columns for creation of PostgreSQL index on the elements (except for in-place document key). */
	public static final int pg_max_elem_cols_for_index = 1;

	/** The limit number of element columns for creation of PostgreSQL index on the elements (except for in-place document key). */
	public static final int pg_limit_elem_cols_for_index = 128;

	/** The maximum foreign keys in a table for creation of PostgreSQL index on the simple content. */
	public static final int pg_max_fks_for_simple_cont_index = 0;

	/** The limit number of foreign keys in a table for creation of PostgreSQL index on the simple content. */
	public static final int pg_limit_fks_for_simple_cont_index = 8;

	/** The default JDBC fetch size. */
	public static final int def_jdbc_fetch_size = 10;

	/** The default buffer size for BufferedOutputStream(). */
	public static final int def_buffered_output_stream_buffer_size = 1024 * 128;

	/** The compiled pattern matches capital code. */
	public static final Pattern cap_pattern = Pattern.compile(".*[A-Z].*", Pattern.MULTILINE);

	/** The compiled pattern matches URL. */
	public static final Pattern url_pattern = Pattern.compile("^https?:\\/\\/.*", Pattern.MULTILINE);

	/** The compiled pattern for collapsing white spaces. */
	public static final Pattern ws_col_pattern = Pattern.compile("\\s+", Pattern.MULTILINE);

	/** The compiled pattern for replacing white spaces. */
	public static final Pattern ws_rep_pattern = Pattern.compile("[\\t\\n\\r]", Pattern.MULTILINE);

	/** The compiled pattern matches tab code. */
	public static final Pattern tab_pattern = Pattern.compile("\\t", Pattern.MULTILINE);

	/** The compiled pattern matches line feed code. */
	public static final Pattern lf_pattern = Pattern.compile("\\n", Pattern.MULTILINE);

	/** The compiled pattern matches back slash code. */
	public static final Pattern bs_pattern = Pattern.compile("\\\\", Pattern.MULTILINE);

	/** The compiled pattern matches simple content. */
	public static final Pattern null_simple_cont_pattern = Pattern.compile("^\\s+$", Pattern.MULTILINE);

	/** The UTC time zone. */
	public static final TimeZone tz_utc = TimeZone.getTimeZone("UTC");

	/** The local time zone. */
	public static final TimeZone tz_loc = TimeZone.getDefault();

	/** The local zone id. */
	public static final ZoneId zone_loc = ZoneId.systemDefault();

	/**
	 * Return input stream of XSD file path with decompression.
	 *
	 * @param file_path XSD file path
	 * @return InputStream input stream of file
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static InputStream getSchemaInputStream(Path file_path) throws IOException {

		String ext = FilenameUtils.getExtension(file_path.getFileName().toString());

		InputStream in = Files.newInputStream(file_path);

		return ext.equals("gz") ? new GZIPInputStream(in) : ext.equals("zip") ? new ZipInputStream(in) : in;
	}

	/**
	 * Return input stream of schema location.
	 *
	 * @param schema_location schema location
	 * @param schema_parent parent of schema location
	 * @param cache_xsd enable XSD file caching
	 * @return InputStream input stream of schema location
	 */
	public static InputStream getSchemaInputStream(String schema_location, String schema_parent, boolean cache_xsd) {

		if (!cache_xsd && !url_pattern.matcher(schema_location).matches() && schema_parent != null && url_pattern.matcher(schema_parent).matches())
			schema_location = schema_parent + "/" + schema_location;

		// local XML Schema file

		if (!url_pattern.matcher(schema_location).matches()) {

			Path schema_file_path = Paths.get(schema_location);

			if (!Files.isRegularFile(schema_file_path) && schema_parent != null) // schema_parent indicates either URL or file path
				return getSchemaInputStream(schema_parent + "/" + getSchemaFileName(schema_location), null, cache_xsd);

			try {

				return Files.newInputStream(schema_file_path);

			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}

		}

		// try to retrieve XML Schema file via http protocol

		else if (schema_location.startsWith("http:")) {

			System.err.println("Retrieving " + schema_location + "...");

			try {

				URL url = new URL(schema_location);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();

				int status = conn.getResponseCode();

				if (status == HttpURLConnection.HTTP_MOVED_PERM || status == HttpURLConnection.HTTP_MOVED_TEMP || status == HttpURLConnection.HTTP_SEE_OTHER) {

					System.err.print("[URL Redirect] ");

					return getSchemaInputStream(conn.getHeaderField("Location"), schema_parent, cache_xsd);
				}

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
				HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

				int status = conn.getResponseCode();

				if (status == HttpURLConnection.HTTP_MOVED_PERM || status == HttpURLConnection.HTTP_MOVED_TEMP || status == HttpURLConnection.HTTP_SEE_OTHER) {

					System.err.print("[URL Redirect] ");

					return getSchemaInputStream(conn.getHeaderField("Location"), schema_parent, cache_xsd);
				}

				SSLContext sc = SSLContext.getInstance("SSL");
				sc.init(null, tm, new java.security.SecureRandom());

				HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
					@Override
					public boolean verify(String hostname, SSLSession session) {
						return true;
					}
				});

				conn.setSSLSocketFactory(sc.getSocketFactory());

				return conn.getInputStream();

			} catch (NoSuchAlgorithmException | KeyManagementException | IOException e) {
				e.printStackTrace();
				return null;
			}

		}

	}

	/**
	 * Return XSD file path of schema location.
	 *
	 * @param schema_location schema location
	 * @param schema_parent parent of schema location
	 * @param cache_xsd enable XSD file caching
	 * @return Path file path of schema location
	 */
	public static Path getSchemaFilePath(String schema_location, String schema_parent, boolean cache_xsd) {

		boolean is_url = schema_parent != null && url_pattern.matcher(schema_parent).matches();
		boolean use_cache = (is_url && cache_xsd) || !is_url;

		if (use_cache && !url_pattern.matcher(schema_location).matches()) {

			Path schema_location_file_path = Paths.get(schema_location);

			if (Files.isRegularFile(schema_location_file_path))
				return schema_location_file_path;

		}

		String schema_file_name = getSchemaFileName(schema_location);

		Path schema_file_path = Paths.get(schema_file_name);

		if (use_cache && Files.isRegularFile(schema_file_path))
			return schema_file_path;

		if (!is_url && schema_parent != null) { // schema_parent indicates file path

			schema_file_path = Paths.get(schema_parent, schema_file_name);

			if (Files.isRegularFile(schema_file_path))
				return schema_file_path;

		}

		InputStream is = getSchemaInputStream(schema_location, schema_parent, cache_xsd);

		if (is == null)
			return null;

		try {

			if (cache_xsd)
				IOUtils.copy(is, Files.newOutputStream(schema_file_path));

			else {

				Path schema_file_path_part;

				do {

					schema_file_name += "~"; // prevent corruption of schema file
					schema_file_path_part = Paths.get(schema_file_name);

				} while (Files.isRegularFile(schema_file_path_part));

				IOUtils.copy(is, Files.newOutputStream(schema_file_path_part));

				Files.move(schema_file_path_part, schema_file_path, StandardCopyOption.REPLACE_EXISTING);

			}

			return schema_file_path;

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * Return XSD file name of schema location.
	 *
	 * @param schema_location schema location
	 * @return String file name of schema location
	 */
	public static String getSchemaFileName(String schema_location) {

		if (url_pattern.matcher(schema_location).matches()) {

			try {

				URL url = new URL(schema_location);

				return Paths.get(url.getPath()).getFileName().toString();

			} catch (MalformedURLException e) {
				e.printStackTrace();
			}

		}

		Path schema_file_path = Paths.get(schema_location);

		return schema_file_path.getFileName().toString();
	}

	/**
	 * Return parent of schema location.
	 *
	 * @param schema_location schema location
	 * @return String parent name of schema location
	 */
	public static String getSchemaParent(String schema_location) {

		if (url_pattern.matcher(schema_location).matches()) {

			try {

				URL url = new URL(schema_location);

				return new URL(url.getProtocol(), url.getHost(), url.getPort(), Paths.get(url.getPath()).getParent().toString()).toString();

			} catch (MalformedURLException e) {
				e.printStackTrace();
			}

		}

		Path parent_path = Paths.get(schema_location).getParent();

		return parent_path != null ? parent_path.toString() : null;
	}

	/**
	 * Return blocking queue of target file path.
	 *
	 * @param file_names list of file name
	 * @param filter file name filter
	 * @return LinkedBlockingQueue blocking queue of target file path
	 */
	public static LinkedBlockingQueue<Path> getQueueOfTargetFiles(HashSet<String> file_names, FilenameFilter filter) {

		LinkedBlockingQueue<Path> queue = new LinkedBlockingQueue<Path>();

		boolean has_regex_path = file_names.stream().anyMatch(file_name -> !Files.exists(Paths.get(file_name)));

		file_names.forEach(file_name -> {

			Path file_path = Paths.get(file_name);

			if (!Files.exists(file_path)) {

				Pattern pattern = null;

				try {

					pattern = Pattern.compile(file_name);

				} catch (PatternSyntaxException e) {
					System.err.println("Not found + " + file_path.toAbsolutePath().toString());
				}

				Pattern _pattern = pattern;

				Path path = Paths.get(file_name);

				int depth = 1;

				while (path != null) {

					path = path.getParent();

					if (path != null && Files.exists(path)) {

						if (Files.isDirectory(path)) {

							BiPredicate<Path, BasicFileAttributes> matcher = (_path, _attr) -> {

								if (_attr.isDirectory() || _attr.isRegularFile()) {

									Matcher _matcher = _pattern.matcher(_path.toString());

									return _matcher.matches();
								}

								else
									return false;

							};

							try (Stream<Path> stream = Files.find(path, depth, matcher)) {

								stream.forEach(_path -> {

									if (Files.isDirectory(_path)) {

										try (Stream<Path> _stream = Files.list(_path)) {
											queue.addAll(_stream.filter(__path -> Files.isReadable(__path) && filter.accept(null, __path.getFileName().toString())).collect(Collectors.toSet()));
										} catch (IOException e) {
											e.printStackTrace();
										}

									}

									else if (Files.isRegularFile(_path) && filter.accept(null, _path.getFileName().toString()))
										queue.add(_path);

								});

							} catch (IOException e) {
								e.printStackTrace();
							}

						}

						else if (Files.isRegularFile(path)) {

							Matcher _matcher = _pattern.matcher(path.toString());

							if (_matcher.matches() && filter.accept(null, path.getFileName().toString()))
								queue.add(path);

						}

						break;
					}

					depth++;

				}

			}

			if (Files.isRegularFile(file_path)) {

				if (filter.accept(null, file_name))
					queue.add(file_path);

			}

			else if (Files.isDirectory(file_path)) {

				try (Stream<Path> _stream = Files.list(file_path)) {
					queue.addAll(_stream.filter(path -> Files.isReadable(path) && filter.accept(null, path.getFileName().toString())).collect(Collectors.toSet()));
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		});

		if (!has_regex_path)
			return queue;

		HashSet<Path> set = new HashSet<Path>();

		try {

			set.addAll(queue);

			if (queue.size() == set.size())
				return queue;

			queue.clear();

			queue.addAll(set);

			return queue;

		} finally {
			set.clear();
		}

	}

	/**
	 * Return unqualified name.
	 *
	 * @param qname qualified name
	 * @return String unqualified name
	 */
	public static String getUnqualifiedName(String qname) {

		if (qname == null)
			return null;

		if (qname.contains(" "))
			qname = qname.trim();

		int last_pos = qname.indexOf(':');

		return last_pos == -1 ? qname : qname.substring(last_pos + 1);
	}

	/**
	 * Suggest new name in PostgreSQL for a given name.
	 *
	 * @param name name
	 * @return String name without name collision against PostgreSQL reserved words
	 */
	public static String avoidPgReservedWords(String name) {

		if (Arrays.asList(pg_reserved_words).contains(name.toUpperCase()) || cap_pattern.matcher(name).matches())
			return "\"" + name + "\"";

		for (String ops : pg_reserved_ops) {

			if (name.contains(ops))
				return "\"" + name + "\"";

		}

		return name;
	}

	/**
	 * Suggest new name in PostgreSQL for a given name.
	 *
	 * @param delimiter delimiter
	 * @param names array of names
	 * @return String name without name collision against PostgreSQL reserved words
	 */
	public static String avoidPgReservedWords(String delimiter, String[] names) {

		StringBuilder sb = new StringBuilder();

		for (String name : names)
			sb.append(avoidPgReservedWords(name) + delimiter);

		sb.setLength(sb.length() - delimiter.length());

		return sb.toString();
	}

	/**
	 * Suggest new name in PostgreSQL for a given name.
	 *
	 * @param name name
	 * @return String name without name collision against PostgreSQL operators
	 */
	public static String avoidPgReservedOps(String name) {

		for (String ops : pg_reserved_ops)
			name = name.replace(ops, "_");

		return name;
	}

	/**
	 * Return case-insensitive name for a given name.
	 *
	 * @param name name
	 * @return String name case-insensitive name
	 */
	public static String toCaseInsensitive(String name) {
		return avoidPgReservedOps(name.toLowerCase());
	}

	/** The date pattern in ISO 8601 format. */
	private static final String[] date_patterns_iso = new String[] {
			"yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mmXXX", "yyyy-MM-dd'T'HHXXX", "yyyy-MM-ddXXX",
			"yyyy-MM'T'HH:mm:ss.SSSXXX", "yyyy-MM'T'HH:mm:ssXXX", "yyyy-MM'T'HH:mmXXX", "yyyy-MM'T'HHXXX", "yyyy-MMXXX",
			"yyyy'T'HH:mmXXX", "yyyy'T'HHXXX", "yyyyXXX",
			"yyyy-MM'T'HH:mmXXX", "yyyy-MM'T'HHXXX", "yyyy-MMXXX",
			"--MM-dd'T'HH:mmXXX", "--MM-dd'T'HHXXX", "--MM-ddXXX",
			"--dd'T'HH:mmXXX", "--dd'T'HHXXX", "--ddXXX"
	};

	/** The date pattern in UTC. */
	private static final String[] date_patterns_z = new String[] {
			"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm'Z'", "yyyy-MM-dd'T'HH'Z'", "yyyy-MM-dd'Z'",
			"yyyy-MM'T'HH:mm:ss.SSS'Z'", "yyyy-MM'T'HH:mm:ss'Z'", "yyyy-MM'T'HH:mm'Z'", "yyyy-MM'T'HH'Z'", "yyyy-MM'Z'",
			"yyyy'T'HH:mm'Z'", "yyyy'T'HH'Z'", "yyyy'Z'",
			"yyyy-MM'T'HH:mm'Z'", "yyyy-MM'T'HH'Z'", "yyyy-MM'Z'",
			"--MM-dd'T'HH:mm'Z'", "--MM-dd'T'HH'Z'", "--MM-dd'Z'",
			"--dd'T'HH:mm'Z'", "--dd'T'HH'Z'", "--dd'Z'"
	};

	/** The date pattern in local time. */
	private static final String[] date_patterns_loc = new String[] {
			"yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd'T'HH", "yyyy-MM-dd",
			"yyyy-MM'T'HH:mm:ss.SSS", "yyyy-MM'T'HH:mm:ss", "yyyy-MM'T'HH:mm", "yyyy-MM'T'HH", "yyyy-MM",
			"yyyy'T'HH:mm", "yyyy'T'HH", "yyyy",
			"yyyy-MM'T'HH:mm", "yyyy-MM'T'HH", "yyyy-MM",
			"--MM-dd'T'HH:mm", "--MM-dd'T'HH", "--MM-dd",
			"--dd'T'HH:mm", "--dd'T'HH", "--dd"
	};

	/**
	 * Parse string as java.util.Date.
	 *
	 * @param value content
	 * @return Date java.util.Date
	 */
	public static java.util.Date parseDate(String value) {

		java.util.Date date = null;

		try {

			date = DateUtils.parseDate(value, date_patterns_iso);

		} catch (ParseException e1) {

			try {

				date = DateUtils.parseDate(value, date_patterns_z);
				int offset_sec = tz_utc.getRawOffset() / 1000;
				date = DateUtils.addSeconds(date, offset_sec);

			} catch (ParseException e2) {

				try {

					date = DateUtils.parseDate(value, date_patterns_loc);

				} catch (ParseException e3) {
					return null;
				}

			}

		}

		return date;
	}

	/**
	 * Replace all white space characters.
	 *
	 * @param text string
	 * @return String replaced string
	 */
	public static String replaceWhiteSpace(String text) {
		return ws_rep_pattern.matcher(text).replaceAll(" ");
	}

	/**
	 * Collapse all white space characters.
	 *
	 * @param text string
	 * @return String collapsed string
	 */
	public static String collapseWhiteSpace(String text) {
		return ws_col_pattern.matcher(replaceWhiteSpace(text)).replaceAll(" ").trim();
	}

	/**
	 * Escape characters for TSV format.
	 *
	 * @param text string
	 * @return String escaped string
	 */
	public static String escapeTsv(String text) {
		return tab_pattern.matcher(lf_pattern.matcher(bs_pattern.matcher(text).replaceAll("\\\\\\\\")).replaceAll("\\\\n")).replaceAll("\\\\t");
	}

	/**
	 * Extract one-liner annotation from xs:annotation/xs:appinfo|xs:documentation.
	 *
	 * @param node current node
	 * @param is_table the is table
	 * @return String annotation
	 */
	public static String extractAnnotation(Node node, boolean is_table) {

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (anno.getNodeType() != Node.ELEMENT_NODE || !anno.getNamespaceURI().equals(xs_namespace_uri))
				continue;

			if (!((Element) anno).getLocalName().equals("annotation"))
				continue;

			Element child_elem;
			String child_name, src;
			String annotation = "";

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(xs_namespace_uri))
					continue;

				child_elem = (Element) child;
				child_name = child_elem.getLocalName();

				if (child_name.equals("appinfo")) {

					annotation = collapseWhiteSpace(child.getTextContent());

					if (!annotation.isEmpty())
						annotation += "\n-- ";

					src = child_elem.getAttribute("source");

					if (src != null && !src.isEmpty())
						annotation += (is_table ? "\n-- " : ", ") + "URI-reference = " + src + (is_table ? "\n-- " : ", ");

				}

				else if (child_name.equals("documentation")) {

					annotation += collapseWhiteSpace(child.getTextContent());

					src = child_elem.getAttribute("source");

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
	public static String extractAppinfo(Node node) {

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (anno.getNodeType() != Node.ELEMENT_NODE || !anno.getNamespaceURI().equals(xs_namespace_uri))
				continue;

			if (!((Element) anno).getLocalName().equals("annotation"))
				continue;

			Element child_elem;
			String child_name, src;

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(xs_namespace_uri))
					continue;

				child_elem = (Element) child;
				child_name = child_elem.getLocalName();

				if (child_name.equals("appinfo")) {

					String annotation = collapseWhiteSpace(child.getTextContent());

					src = child_elem.getAttribute("source");

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
	public static String extractDocumentation(Node node, boolean one_liner) {

		for (Node anno = node.getFirstChild(); anno != null; anno = anno.getNextSibling()) {

			if (anno.getNodeType() != Node.ELEMENT_NODE || !anno.getNamespaceURI().equals(xs_namespace_uri))
				continue;

			if (!((Element) anno).getLocalName().equals("annotation"))
				continue;

			Element child_elem;
			String child_name, src, annotation;

			for (Node child = anno.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(xs_namespace_uri))
					continue;

				child_elem = (Element) child;
				child_name = child_elem.getLocalName();

				if (child_name.equals("documentation")) {

					String text = child.getTextContent();

					if (one_liner) {

						annotation = collapseWhiteSpace(text);

						src = child_elem.getAttribute("source");

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
	 * Extract table name from xs:selector/@xpath.
	 *
	 * @param node current node
	 * @return String child table name
	 * @throws PgSchemaException the pg schema exception
	 */
	public static String extractSelectorXPath(Node node) throws PgSchemaException {

		StringBuilder sb = new StringBuilder();

		try {

			Element child_elem;

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(xs_namespace_uri))
					continue;

				child_elem = (Element) child;

				if (!child_elem.getLocalName().equals("selector"))
					continue;

				String xpath_expr = child_elem.getAttribute("xpath");

				if (xpath_expr == null || xpath_expr.isEmpty())
					return null;

				xpathLexer lexer = new xpathLexer(CharStreams.fromString(xpath_expr));

				CommonTokenStream tokens = new CommonTokenStream(lexer);

				xpathParser parser = new xpathParser(tokens);
				parser.addParseListener(new xpathBaseListener());

				MainContext main = parser.main();

				ParseTree tree = main.children.get(0);
				String main_text = main.getText();

				if (parser.getNumberOfSyntaxErrors() > 0 || tree.getSourceInterval().length() == 0)
					throw new PgSchemaException("Invalid XPath expression. (" + main_text + ")");

				XPathCompList xpath_comp_list = new XPathCompList(tree);

				if (xpath_comp_list.comps.size() == 0)
					throw new PgSchemaException("Insufficient XPath expression. (" + main_text + ")");

				XPathComp[] last_qname_comp = xpath_comp_list.getLastQNameComp();

				for (XPathComp comp : last_qname_comp) {

					if (comp != null)
						sb.append(getUnqualifiedName(comp.tree.getText()) + ", ");

				}

				xpath_comp_list.clear();

				int len = sb.length();

				if (len > 0)
					sb.setLength(len - 2);

				break;
			}

			return sb.toString();

		} finally {
			sb.setLength(0);
		}

	}

	/**
	 * Extract field names from xs:field/@xpath.
	 *
	 * @param node current node
	 * @return String child field names separated by comma
	 * @throws PgSchemaException the pg schema exception
	 */
	public static String extractFieldXPath(Node node) throws PgSchemaException {

		StringBuilder sb = new StringBuilder();

		try {

			Element child_elem;

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE || !child.getNamespaceURI().equals(xs_namespace_uri))
					continue;

				child_elem = (Element) child;

				if (!child_elem.getLocalName().equals("field"))
					continue;

				String xpath_expr = child_elem.getAttribute("xpath");

				if (xpath_expr == null || xpath_expr.isEmpty())
					return null;

				xpathLexer lexer = new xpathLexer(CharStreams.fromString(xpath_expr));

				CommonTokenStream tokens = new CommonTokenStream(lexer);

				xpathParser parser = new xpathParser(tokens);
				parser.addParseListener(new xpathBaseListener());

				MainContext main = parser.main();

				ParseTree tree = main.children.get(0);
				String main_text = main.getText();

				if (parser.getNumberOfSyntaxErrors() > 0 || tree.getSourceInterval().length() == 0)
					throw new PgSchemaException("Invalid XPath expression. (" + main_text + ")");

				XPathCompList xpath_comp_list = new XPathCompList(tree);

				if (xpath_comp_list.comps.size() == 0)
					throw new PgSchemaException("Insufficient XPath expression. (" + main_text + ")");

				XPathComp[] last_qname_comp = xpath_comp_list.getLastQNameComp();

				for (XPathComp comp : last_qname_comp) {

					if (comp != null)
						sb.append(getUnqualifiedName(comp.tree.getText()) + ", ");

				}

				xpath_comp_list.clear();

			}

			int len = sb.length();

			if (len > 0)
				sb.setLength(len - 2);

			return sb.toString();

		} finally {
			sb.setLength(0);
		}

	}

	/**
	 * Read object from blocking I/O.
	 *
	 * @param fst_conf FST configuration
	 * @param in data input stream
	 * @return Object object
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ClassNotFoundException the class not found exception
	 * @see <a href="https://github.com/RuedigerMoeller/fast-serialization/blob/1.x/src/test_nojunit/java/gitissue10/GitIssue10.java">https://github.com/RuedigerMoeller/fast-serialization/blob/1.x/src/test_nojunit/java/gitissue10/GitIssue10.java</a>
	 */
	public static Object readObjectFromStream(FSTConfiguration fst_conf, DataInputStream in) throws IOException, ClassNotFoundException {

		int len = in.readInt();
		byte buffer[] = new byte[len]; // this could be reused !

		while (len > 0)
			len -= in.read(buffer, buffer.length - len, len);

		return fst_conf.getObjectInput(buffer).readObject();
	}

	/**
	 * Write object to blocking I/O.
	 *
	 * @param fst_conf FST configuration
	 * @param out data output stream
	 * @param object object
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @see <a href="https://github.com/RuedigerMoeller/fast-serialization/blob/1.x/src/test_nojunit/java/gitissue10/GitIssue10.java">https://github.com/RuedigerMoeller/fast-serialization/blob/1.x/src/test_nojunit/java/gitissue10/GitIssue10.java</a>
	 */
	public static void writeObjectToStream(FSTConfiguration fst_conf, DataOutputStream out, Object object) throws IOException {

		// write object
		FSTObjectOutput fst_out = fst_conf.getObjectOutput(); // could also do new with minor perf impact

		// write object to internal buffer
		fst_out.writeObject(object);

		// write length
		out.writeInt(fst_out.getWritten());

		// write bytes
		out.write(fst_out.getBuffer(), 0, fst_out.getWritten());

		fst_out.flush(); // return for reuse to fst_conf

	}

}
