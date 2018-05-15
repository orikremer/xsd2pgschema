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

import net.sf.xsd2pgschema.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.text.StringEscapeUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathBaseListener;
import com.github.antlr.grammars_v4.xpath.xpathLexer;
import com.github.antlr.grammars_v4.xpath.xpathListenerException;
import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.MainContext;

/**
 * Implementation of xpath evaluator.
 *
 * @author yokochi
 */
public class XPathEvaluatorImpl {

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option = null;

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
	 * @param pg_option PostgreSQL option
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws SAXException the SAX exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NoSuchAlgorithmException the no such algorithm exception
	 * @throws SQLException the SQL exception
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathEvaluatorImpl(final InputStream is, final PgSchemaOption option, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

		// parse XSD document

		DocumentBuilderFactory doc_builder_fac = DocumentBuilderFactory.newInstance();
		doc_builder_fac.setValidating(false);
		doc_builder_fac.setNamespaceAware(true);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		doc_builder_fac.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		DocumentBuilder doc_builder = doc_builder_fac.newDocumentBuilder();

		Document xsd_doc = doc_builder.parse(is);

		is.close();

		doc_builder.reset();

		// XSD analysis

		schema = new PgSchema(doc_builder, xsd_doc, null, option.root_schema_location, this.option = option);

		if (!pg_option.name.isEmpty()) {

			db_conn = DriverManager.getConnection(pg_option.getDbUrl(), pg_option.user.isEmpty() ? System.getProperty("user.name") : pg_option.user, pg_option.pass);

			// test PostgreSQL DDL with schema

			if (pg_option.test)
				schema.testPgSql(db_conn, false);

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

		xpath_comp_list = new XPathCompList(schema, tree, variables);

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
	 * @param out_file_name output file name
	 * @throws PgSchemaException the pg schema exception
	 */
	public void execute(String out_file_name) throws PgSchemaException {

		FileOutputStream fout = null;

		BufferedOutputStream bout = null;

		try {

			if (!out_file_name.isEmpty() && !out_file_name.equals("stdout")) {

				File out_file = new File(out_file_name);

				fout = new FileOutputStream(out_file);

				bout = new BufferedOutputStream(fout);

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

					// field or text node

					if (terminus.isField() || terminus.isText()) {

						_bout.write(path_expr.sql_subject.field.getName().getBytes());
						_bout.write('\n');

						while (rset.next()) {

							_bout.write(rset.getString(1).getBytes());
							_bout.write('\n');

						}


					}

					// table node

					else {

						ResultSetMetaData meta = rset.getMetaData();

						int column_count = meta.getColumnCount();

						for (int i = 1; i <= column_count; i++) {

							_bout.write(meta.getColumnName(i).getBytes());
							_bout.write((i < column_count ? option.pg_delimiter : '\n'));

						}

						while (rset.next()) {

							for (int i = 1; i <= column_count; i++) {

								String value = rset.getString(i);

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
					System.exit(1);
				}

			});

			long end_time = System.currentTimeMillis();

			stat.close();

			if (fout != null) {

				bout.close();
				fout.close();

				System.out.println("Generated result document: " + out_file_name);

				System.out.println("\nSQL execution time: " + (end_time - start_time) + " ms");

			}

			else
				bout.write(String.valueOf("\nSQL execution time: " + (end_time - start_time) + " ms\n").getBytes());

		} catch (SQLException | IOException e) {
			throw new PgSchemaException(e);
		}

	}

	/**
	 * Execute translated SQL and compose XML document.
	 *
	 * @param out_file_name output file name
	 * @param xmlb XML builder
	 * @throws PgSchemaException the pg schema exception
	 */
	public void evaluate(String out_file_name, XmlBuilder xmlb) throws PgSchemaException {

		XMLOutputFactory out_factory = XMLOutputFactory.newInstance();

		FileOutputStream fout = null;

		BufferedOutputStream bout = null;

		try {

			if (!out_file_name.isEmpty() && !out_file_name.equals("stdout")) {

				File out_file = new File(out_file_name);

				fout = new FileOutputStream(out_file);

				bout = new BufferedOutputStream(fout);

				xml_writer = out_factory.createXMLStreamWriter(bout);

			}

			else
				xml_writer = out_factory.createXMLStreamWriter(System.out);

			Statement stat = db_conn.createStatement();

			long start_time = System.currentTimeMillis();

			xpath_comp_list.path_exprs.forEach(path_expr -> {

				XPathCompType terminus = path_expr.terminus;

				PgTable table = path_expr.sql_subject.table;

				String table_name = table.getName();
				String table_ns = table.getTargetNamespace();
				String table_prefix = table.getPrefix();

				try {

					ResultSet rset = stat.executeQuery(path_expr.sql);

					// field or text node

					if (terminus.isField() || terminus.isText()) {

						SAXParser any_attr_parser = null;

						PgAnyRetriever any_retriever = null;

						if (option.wild_card) {

							switch (terminus) {
							case any_attribute:
								SAXParserFactory spf = SAXParserFactory.newInstance();
								any_attr_parser = spf.newSAXParser();
								break;
							case any_element:
								any_retriever = new PgAnyRetriever();
								break;
							default:
							}

						}

						PgField field = path_expr.sql_subject.field;

						String field_name = field.getName();
						String field_ns = field.getTagetNamespace();
						String field_prefix = field.getPrefix();

						while (rset.next()) {

							String content;

							switch (terminus) {
							case element:
								content = field.retrieveValue(rset, 1);

								if ((content != null && !content.isEmpty()) || field.isRequired()) {

									if (content != null && !content.isEmpty()) {

										if (field.isXsNamespace()) {

											xml_writer.writeStartElement(table_prefix, field_name, table_ns);

											if (xmlb.append_xmlns)
												xml_writer.writeNamespace(table_prefix, table_ns);

										}

										else {

											xml_writer.writeStartElement(field_prefix, field_name, field_ns);

											if (xmlb.append_xmlns)
												xml_writer.writeNamespace(field_prefix, field_ns);

										}

										xml_writer.writeCharacters(content);

										xml_writer.writeEndElement();

									}

									else {

										if (field.isXsNamespace()) {

											xml_writer.writeEmptyElement(table_prefix, field_name, table_ns);

											if (xmlb.append_xmlns) {
												xml_writer.writeNamespace(table_prefix, table_ns);
												xml_writer.writeNamespace(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri);
											}

										}

										else {

											xml_writer.writeEmptyElement(field_prefix, field_name, field_ns);

											if (xmlb.append_xmlns) {
												xml_writer.writeNamespace(field_prefix, field_ns);
												xml_writer.writeNamespace(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri);
											}

										}

										xml_writer.writeAttribute(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri, "nil", "true");

									}

									xml_writer.writeCharacters(xmlb.getLineFeedCode());

								}
								break;
							case simple_content:
								content = field.retrieveValue(rset, 1);

								if (content != null && !content.isEmpty()) {

									xml_writer.writeStartElement(table_prefix, table_name, table_ns);

									if (xmlb.append_xmlns)
										xml_writer.writeNamespace(table_prefix, table_ns);

									xml_writer.writeCharacters(content);

									xml_writer.writeEndElement();

									xml_writer.writeCharacters(xmlb.getLineFeedCode());

								}
								break;
							case attribute:
								content = field.retrieveValue(rset, 1);

								if (content != null && !content.isEmpty()) {

									xml_writer.writeStartElement(table_prefix, table_name, table_ns);

									if (xmlb.append_xmlns)
										xml_writer.writeNamespace(table_prefix, table_ns);

									if (field_ns.equals(PgSchemaUtil.xs_namespace_uri))
										xml_writer.writeAttribute(field_name, rset.getString(1));
									else
										xml_writer.writeAttribute(field_prefix, field_ns, field_name, content);

									xml_writer.writeEndElement();

									xml_writer.writeCharacters(xmlb.getLineFeedCode());

								}

								else if (field.isRequired()) {

									xml_writer.writeStartElement(table_prefix, table_name, table_ns);

									if (xmlb.append_xmlns)
										xml_writer.writeNamespace(table_prefix, table_ns);

									if (field_ns.equals(PgSchemaUtil.xs_namespace_uri))
										xml_writer.writeAttribute(field_name, rset.getString(1));
									else
										xml_writer.writeAttribute(field_prefix, field_ns, field_name, "");

									xml_writer.writeEndElement();

									xml_writer.writeCharacters(xmlb.getLineFeedCode());

								}
								break;
							case any_attribute:
							case any_element:
								SQLXML xml_object = rset.getSQLXML(1);

								if (xml_object != null) {

									InputStream in = xml_object.getBinaryStream();

									if (in != null) {

										xml_writer.writeStartElement(table_prefix, table_name, table_ns);

										if (xmlb.append_xmlns)
											xml_writer.writeNamespace(table_prefix, table_ns);

										switch (terminus) {
										case any_attribute:
											PgAnyAttrRetriever any_attr = new PgAnyAttrRetriever(table_name, xmlb);
											any_attr_parser.parse(in, any_attr);
											break;
										default:
											any_retriever.exec(in, table, new PgSchemaNestTester(table, xmlb), xmlb);
										}

										xml_writer.writeEndElement();

										xml_writer.writeCharacters(xmlb.getLineFeedCode());

										in.close();

									}

									xml_object.free();

								}
								break;
							case text:
								content = rset.getString(1);

								if (content != null && !content.isEmpty()) {

									String column_name = rset.getMetaData().getColumnName(1);

									PgField _field = table.getField(column_name);

									if (_field != null)
										content = field.retrieveValue(rset, 1);

									xml_writer.writeCharacters(content);
									xml_writer.writeCharacters(xmlb.getLineFeedCode());

								}

								break;
							case comment:
								content = rset.getString(1);

								if (content != null && !content.isEmpty()) {

									xml_writer.writeComment(content);
									xml_writer.writeCharacters(xmlb.getLineFeedCode());

								}
								break;
							case processing_instruction:
								content = rset.getString(1);

								if (content != null && !content.isEmpty()) {

									xml_writer.writeProcessingInstruction(content);
									xml_writer.writeCharacters(xmlb.getLineFeedCode());

								}
								break;
							default:
								continue;
							}

						}

					}

					// table node (not implemented yet)

					else {

						xmlb.setInitIndentOffset(0);
						xmlb.setXmlWriter(xml_writer);

						while (rset.next())
							schema.pgSql2Xml(db_conn, table, rset, xmlb);

						schema.closePgSql2Xml();

					}

					rset.close();

				} catch (SQLException | XMLStreamException | PgSchemaException | SAXException | IOException | ParserConfigurationException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

			long end_time = System.currentTimeMillis();

			stat.close();

			if (fout != null) {

				bout.close();
				fout.close();

				System.out.println("Generated result document: " + out_file_name);

			}

			System.out.println("");

			xml_writer.close();

			System.out.println("SQL execution time: " + (end_time - start_time) + " ms");

		} catch (IOException | XMLStreamException | SQLException e) {
			throw new PgSchemaException(e);
		}

	}

}
