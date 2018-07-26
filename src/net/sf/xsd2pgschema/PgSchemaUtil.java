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

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.text.ParseException;
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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;

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

	/** The default encoding. */
	public static final String def_encoding = "UTF-8";

	/** The default hash algorithm. */
	public static final String def_hash_algorithm = "SHA-1";

	/** The default check sum algorithm. */
	public static final String def_check_sum_algorithm = "MD5";

	/** The default host name. */
	public static final String host = "localhost";

	/** The default port number. */
	public static final int port = 5432;

	/** The minimum word length for index. */
	public static final int min_word_len = 1;

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

	/** The field name of Sphinx dictionary. */
	public static final String trigram_field_name = "trigrams";

	/** The text node of XPath notation. */
	public static final String text_node_name = "text()";

	/** The comment node of XPath notation. */
	public static final String comment_node_name = "comment()";

	/** The PostgreSQL reserved words. */
	public static final String[] pg_reserved_words = { "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "AUTHORIZATION", "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLLATION", "COLUMN", "CONCURRENTLY", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL", "NOT", "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "TABLE", "TABLESAMPLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING", "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH" };

	/** The PostgreSQL reserved operator codes. */
	public static final String[] pg_reserved_ops = { "+", "-", "*", "/", "%", "^", "|/", "||/", "!", "!!", "@", "&", "|", "#", "~", "<<", ">>" };

	/** The minimum rows for creation of PostgreSQL index on document key. */
	public static final int pg_min_rows_for_doc_key_index = 10000;

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

	/**
	 * Return input stream of XSD file path with decompression.
	 *
	 * @param  file_path XSD file path
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
	 * Return XSD name of schema location.
	 *
	 * @param schema_location schema location
	 * @return String schema location
	 */
	public static String getSchemaName(String schema_location) {

		if (url_pattern.matcher(schema_location).matches())
			return schema_location;

		Path schema_file_path = Paths.get(schema_location);

		return schema_file_path.getFileName().toString();
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

		boolean has_regex_path = file_names.parallelStream().anyMatch(file_name -> !Files.exists(Paths.get(file_name)));

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
	 * @param name name
	 * @return String name without name collision against PostgreSQL operators
	 */
	public static String avoidPgReservedOps(String name) {

		for (String ops : pg_reserved_ops)
			name = name.replace(ops, "_");

		return name;
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
				TimeZone tz = TimeZone.getTimeZone("UTC");
				int offset_sec = tz.getRawOffset() / 1000;
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
		return PgSchemaUtil.ws_rep_pattern.matcher(text).replaceAll(" ");
	}

	/**
	 * Collapse all white space characters.
	 *
	 * @param text string
	 * @return String collapsed string
	 */
	public static String collapseWhiteSpace(String text) {
		return PgSchemaUtil.ws_col_pattern.matcher(replaceWhiteSpace(text)).replaceAll(" ").trim();
	}

	/**
	 * Escape characters for TSV format.
	 *
	 * @param text string
	 * @return String escaped string
	 */
	public static String escapeTsv(String text) {
		return PgSchemaUtil.tab_pattern.matcher(PgSchemaUtil.lf_pattern.matcher(PgSchemaUtil.bs_pattern.matcher(text).replaceAll("\\\\\\\\")).replaceAll("\\\\n")).replaceAll("\\\\t");
	}

}
