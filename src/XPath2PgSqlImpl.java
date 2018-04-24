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

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.github.antlr.grammars_v4.xpath.xpathBaseListener;
import com.github.antlr.grammars_v4.xpath.xpathLexer;
import com.github.antlr.grammars_v4.xpath.xpathListenerException;
import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.MainContext;

/**
 * Implementation of xpath2pgsql.
 *
 * @author yokochi
 */
public class XPath2PgSqlImpl {

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The database connection. */
	private Connection db_conn = null;

	/** The XPath component list. */
	private XPathCompList xpath_comp_list = null;

	/**
	 * Instance of XPath2PgSqlImpl.
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
	public XPath2PgSqlImpl(final InputStream is, final PgSchemaOption option, final PgOption pg_option) throws ParserConfigurationException, SAXException, IOException, NoSuchAlgorithmException, SQLException, PgSchemaException {

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

		schema = new PgSchema(doc_builder, xsd_doc, null, xpath2pgsql.schema_location, option);

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
			throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

		xpath_comp_list.validate(false);

		if (xpath_comp_list.path_exprs.size() == 0)
			throw new xpathListenerException("Invalid XPath expression. (" + main_text + ")");

		System.out.println("Input XPath query:");
		System.out.println(" " + main_text);

		System.out.println("\nTarget path in XML Schema: " + PgSchemaUtil.getSchemaName(xpath2pgsql.schema_location));
		xpath_comp_list.showPathExprs();

		// translate XPath to SQL

		xpath_comp_list.translateToSqlExpr();

		System.out.println("\nSQL expression:");
		xpath_comp_list.showSqlExpr();

	}

	/**
	 * Execute translated SQL.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	public void execute() throws PgSchemaException {

		try {

			Statement stat = db_conn.createStatement();

			xpath_comp_list.path_exprs.forEach(path_expr -> {

				try {

					System.out.println("\nSQL result set: " + path_expr.sql + "\n");

					ResultSet rset = stat.executeQuery(path_expr.sql);

					ResultSetMetaData meta = rset.getMetaData();

					int column_count = meta.getColumnCount();

					for (int i = 1; i <= column_count; i++)
						System.out.print(meta.getColumnName(i) + (i < column_count ? ", " : "\n"));

					while (rset.next()) {

						for (int i = 1; i <= column_count; i++)
							System.out.print(rset.getString(i) + (i < column_count ? ", " : "\n"));

					}

					rset.close();

					System.out.println("");

				} catch (SQLException e) {
					e.printStackTrace();
					System.exit(1);
				}

			});

			stat.close();

		} catch (SQLException e) {
			throw new PgSchemaException(e);
		}

	}

}
