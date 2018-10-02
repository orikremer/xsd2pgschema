/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017-2018 Masashi Yokochi

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

package net.sf.xsd2pgschema.implement;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.nustaq.serialization.FSTConfiguration;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathBaseListener;
import com.github.antlr.grammars_v4.xpath.xpathLexer;
import com.github.antlr.grammars_v4.xpath.xpathListenerException;
import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.MainContext;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.serverutil.PgSchemaClientImpl;
import net.sf.xsd2pgschema.docbuilder.JsonBuilderOption;
import net.sf.xsd2pgschema.docbuilder.XmlBuilder;
import net.sf.xsd2pgschema.option.PgOption;
import net.sf.xsd2pgschema.option.PgSchemaOption;
import net.sf.xsd2pgschema.xpathparser.XPathCompList;
import net.sf.xsd2pgschema.xpathparser.XPathCompType;

/**
 * Implementation of XPath evaluator.
 *
 * @author yokochi
 */
public class XPathEvaluatorImpl {

	/** The PostgreSQL data model option. */
	private PgSchemaOption option;

	/** The PgSchema client. */
	private PgSchemaClientImpl client;

	/** The database connection. */
	private Connection db_conn = null;

	/** The XPath component list. */
	private XPathCompList xpath_comp_list = null;

	/** The XML stream writer. */
	private XMLStreamWriter xml_writer = null;

	/**
	 * Instance of XPathEvaluatorImpl.
	 *
	 * @param is InputStream of XML Schema
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws SQLException the SQL exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathEvaluatorImpl(final InputStream is, final PgSchemaOption option, final FSTConfiguration fst_conf, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

		client = new PgSchemaClientImpl(is, this.option = option, fst_conf, Thread.currentThread().getStackTrace()[2].getClassName());

		if (!pg_option.name.isEmpty()) {

			db_conn = DriverManager.getConnection(pg_option.getDbUrl(PgSchemaUtil.def_encoding), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.pass);

			// test PostgreSQL DDL with schema

			if (pg_option.test)
				client.schema.testPgSql(db_conn, false);

		}

	}

	/**
	 * Translate XPath to SQL.
	 *
	 * @param xpath_query XPath query
	 * @param variables XPath variable reference
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws xpathListenerException the xpath listener exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public void translate(String xpath_query, HashMap<String, String> variables) throws IOException, xpathListenerException, PgSchemaException {

		long start_time = System.currentTimeMillis();

		xpathLexer lexer = new xpathLexer(CharStreams.fromString(xpath_query));

		CommonTokenStream tokens = new CommonTokenStream(lexer);

		xpathParser parser = new xpathParser(tokens);
		parser.addParseListener(new xpathBaseListener());

		// validate XPath expression with schema

		MainContext main = parser.main();

		ParseTree tree = main.children.get(0);
		String main_text = main.getText();

		if (parser.getNumberOfSyntaxErrors() > 0 || tree.getSourceInterval().length() == 0)
			throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

		xpath_comp_list = new XPathCompList(client.schema, tree, variables);

		if (xpath_comp_list.comps.size() == 0)
			throw new xpathListenerException("Insufficient XPath expression. (" + main_text + ")");

		long end_time = System.currentTimeMillis();

		xpath_comp_list.validate(false);

		long end_time_ = System.currentTimeMillis();

		if (xpath_comp_list.path_exprs.size() == 0)
			throw new xpathListenerException("Insufficient XPath expression. (" + main_text + ")");

		System.out.println("Input XPath query:");
		System.out.println(main_text);

		System.out.println("\nTarget path in XML Schema: " + PgSchemaUtil.getSchemaName(option.root_schema_location));
		xpath_comp_list.showPathExprs();

		// translate XPath to SQL

		long start_time2 = System.currentTimeMillis();

		xpath_comp_list.translateToSqlExpr();

		long end_time2 = System.currentTimeMillis();

		System.out.println("\nSQL expression:");
		xpath_comp_list.showSqlExpr();

		System.out.println("\nXPath parse time: " + (end_time - start_time) + " ms");
		System.out.println("XPath validation time: " + (end_time_ - end_time) + " ms");
		System.out.println("\nSQL translation time: " + (end_time2 - start_time2) + " ms\n");

	}

	/**
	 * Execute translated SQL.
	 *
	 * @param id query id
	 * @param total the total number of query
	 * @param out_dir_name output directory name
	 * @param out_file_name output file name
	 * @throws PgSchemaException the pg schema exception
	 */
	public void execute(int id, int total, String out_dir_name, String out_file_name) throws PgSchemaException {

		if (xpath_comp_list == null)
			throw new PgSchemaException("Not parsed XPath expression ever.");

		try {

			Path out_file_path = null;

			BufferedOutputStream bout = null;

			if (!out_file_name.isEmpty() && !out_file_name.equals("stdout")) {

				if (total > 1) {

					String file_ext = FilenameUtils.getExtension(out_file_name);

					if (file_ext != null && !file_ext.isEmpty())
						out_file_name = FilenameUtils.removeExtension(out_file_name) + (id + 1) + "." + file_ext;
					else
						out_file_name += (id + 1);

				}

				out_file_path = Paths.get(out_dir_name, out_file_name);

				bout = new BufferedOutputStream(Files.newOutputStream(out_file_path));

			}

			else
				bout = new BufferedOutputStream(System.out);

			BufferedOutputStream _bout = bout;

			Statement stat = db_conn.createStatement();

			long start_time = System.currentTimeMillis();

			xpath_comp_list.path_exprs.forEach(path_expr -> {

				XPathCompType terminus = path_expr.terminus;

				try {

					ResultSet rset = stat.executeQuery(path_expr.sql);

					ResultSetMetaData meta = rset.getMetaData();

					// field or text node

					if (terminus.isField() || terminus.isText()) {

						_bout.write(meta.getColumnName(1).getBytes());
						_bout.write('\n');

						while (rset.next()) {

							_bout.write(rset.getString(1).getBytes());
							_bout.write('\n');

						}

					}

					// table node

					else {

						int column_count = meta.getColumnCount();

						for (int i = 1; i <= column_count; i++) {

							_bout.write(meta.getColumnName(i).getBytes());
							_bout.write((i < column_count ? option.pg_delimiter : '\n'));

						}

						String value;

						while (rset.next()) {

							for (int i = 1; i <= column_count; i++) {

								value = rset.getString(i);

								if (value == null || value.isEmpty())
									_bout.write(option.pg_null.getBytes());
								else
									_bout.write(option.pg_delimiter == '\t' ? PgSchemaUtil.escapeTsv(value).getBytes() : StringEscapeUtils.escapeCsv(value).getBytes());

								_bout.write((i < column_count ? option.pg_delimiter : '\n'));

							}

						}

					}

					rset.close();

				} catch (SQLException | IOException e) {
					e.printStackTrace();
				}

			});

			long end_time = System.currentTimeMillis();

			stat.close();

			if (out_file_path != null) {

				bout.close();

				System.out.println("Generated " + (option.pg_delimiter == '\t' ? "TSV" : "CSV") + " document: " + out_file_path.toAbsolutePath().toString());
				System.out.println("\nSQL execution time: " + (end_time - start_time) + " ms");

			}

			else {

				bout.write(String.valueOf("\nSQL execution time: " + (end_time - start_time) + " ms\n").getBytes());
				bout.flush();

			}

		} catch (SQLException | IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Execute translated SQL and compose XML document.
	 *
	 * @param id query id
	 * @param total the total number of query
	 * @param out_dir_name output directory name
	 * @param out_file_name output file name or pattern
	 * @param xmlb XML builder
	 * @throws PgSchemaException the pg schema exception
	 */
	public void composeXml(int id, int total, String out_dir_name, String out_file_name, XmlBuilder xmlb) throws PgSchemaException {

		if (xpath_comp_list == null)
			throw new PgSchemaException("Not parsed XPath expression ever.");

		XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

		try {

			Path out_file_path = null;

			BufferedOutputStream bout = null;

			if (!out_file_name.isEmpty() && !out_file_name.equals("stdout")) {

				if (total > 1) {

					String file_ext = FilenameUtils.getExtension(out_file_name);

					if (file_ext != null && !file_ext.isEmpty())
						out_file_name = FilenameUtils.removeExtension(out_file_name) + (id + 1) + "." + file_ext;
					else
						out_file_name += (id + 1);

				}

				out_file_path = Paths.get(out_dir_name, out_file_name);

				bout = new BufferedOutputStream(Files.newOutputStream(out_file_path));

				xml_writer = out_factory.createXMLStreamWriter(bout);

			}

			else
				xml_writer = out_factory.createXMLStreamWriter(System.out);

			if (xmlb.append_declare) {

				xml_writer.writeStartDocument(PgSchemaUtil.def_encoding, PgSchemaUtil.def_xml_version);
				xml_writer.writeCharacters(xmlb.getLineFeedCode());

			}

			xmlb.setXmlWriter(xml_writer);

			client.schema.initXmlBuilder(xmlb);

			xmlb.resetStatus();

			Statement stat = db_conn.createStatement();

			long start_time = System.currentTimeMillis();

			xpath_comp_list.path_exprs.forEach(path_expr -> {

				XPathCompType terminus = path_expr.terminus;

				try {

					ResultSet rset = stat.executeQuery(path_expr.sql);

					// table node

					if (terminus.equals(XPathCompType.table)) {

						while (rset.next())
							client.schema.pgSql2Xml(db_conn, path_expr, rset);

					}

					// field or text node

					else
						client.schema.pgSql2XmlFrag(xpath_comp_list, path_expr, rset);

					rset.close();

				} catch (SQLException | PgSchemaException e) {
					e.printStackTrace();
				}

			});

			long end_time = System.currentTimeMillis();

			stat.close();

			if (xmlb.append_declare)
				xml_writer.writeEndDocument();

			if (out_file_path != null) {

				bout.close();

				System.out.println("Generated XML document: " + out_file_path.toAbsolutePath().toString());

			}

			else
				xml_writer.close();

			System.out.println("\nSQL execution time: " + (end_time - start_time) + " ms");

			if (xmlb.getRootCount() > 1)
				throw new PgSchemaException("[WARNING] The XML document has multiple root nodes.");
			if (xmlb.getFragment() > 1)
				throw new PgSchemaException("[WARNING] The XML document has multiple fragments.");

		} catch (IOException | XMLStreamException | SQLException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Execute translated SQL and compose JSON document.
	 *
	 * @param id query id
	 * @param total the total number of query
	 * @param out_dir_name output directory name
	 * @param out_file_name output file name or pattern
	 * @param jsonb_option JSON builder option
	 * @throws PgSchemaException the pg schema exception
	 */
	public void composeJson(int id, int total, String out_dir_name, String out_file_name, JsonBuilderOption jsonb_option) throws PgSchemaException {

		if (xpath_comp_list == null)
			throw new PgSchemaException("Not parsed XPath expression ever.");

		client.schema.initJsonBuilder(jsonb_option);

		try {

			Path out_file_path = null;

			BufferedOutputStream bout = null;

			if (!out_file_name.isEmpty() && !out_file_name.equals("stdout")) {

				if (total > 1) {

					String file_ext = FilenameUtils.getExtension(out_file_name);

					if (file_ext != null && !file_ext.isEmpty())
						out_file_name = FilenameUtils.removeExtension(out_file_name) + (id + 1) + "." + file_ext;
					else
						out_file_name += (id + 1);

				}

				out_file_path = Paths.get(out_dir_name, out_file_name);

				bout = new BufferedOutputStream(Files.newOutputStream(out_file_path));

			}

			else
				bout = new BufferedOutputStream(System.out);

			client.schema.jsonb.resetStatus();

			Statement stat = db_conn.createStatement();

			long start_time = System.currentTimeMillis();

			xpath_comp_list.path_exprs.forEach(path_expr -> {

				XPathCompType terminus = path_expr.terminus;

				try {

					ResultSet rset = stat.executeQuery(path_expr.sql);

					// table node

					if (terminus.equals(XPathCompType.table)) {

						while (rset.next())
							client.schema.pgSql2Json(db_conn, path_expr, rset);

					}

					// field or text node

					else
						client.schema.pgSql2JsonFrag(xpath_comp_list, path_expr, rset);

					rset.close();

				} catch (SQLException | PgSchemaException e) {
					e.printStackTrace();
				}

			});

			client.schema.writeJsonBuilder(bout);

			long end_time = System.currentTimeMillis();

			stat.close();

			if (out_file_path != null) {

				bout.close();

				System.out.println("Generated JSON document: " + out_file_path.toAbsolutePath().toString());
				System.out.println("\nSQL execution time: " + (end_time - start_time) + " ms");

			}

			else {

				bout.write(String.valueOf("\nSQL execution time: " + (end_time - start_time) + " ms\n").getBytes());
				bout.flush();

			}

			if (client.schema.jsonb.getRootCount() > 1)
				throw new PgSchemaException("[WARNING] The JSON document has multiple root nodes.");
			if (client.schema.jsonb.getFragment() > 1)
				throw new PgSchemaException("[WARNING] The JSON document has multiple fragments.");

		} catch (IOException | SQLException e) {
			throw new PgSchemaException(e);
		}

	}

}