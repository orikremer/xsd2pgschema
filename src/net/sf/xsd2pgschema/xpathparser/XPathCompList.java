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

package net.sf.xsd2pgschema.xpathparser;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;

import com.github.antlr.grammars_v4.xpath.xpathParser.AbbreviatedStepContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.AbsoluteLocationPathNorootContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.AdditiveExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.AndExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.AxisSpecifierContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.EqualityExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.FunctionCallContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.MultiplicativeExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.NCNameContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.NameTestContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.NodeTestContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.OrExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.PredicateContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.PrimaryExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.QNameContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.RelationalExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.RelativeLocationPathContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.StepContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.UnaryExprNoRootContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.VariableReferenceContext;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;
import net.sf.xsd2pgschema.option.PgSchemaOption;

/**
 * XPath parse tree serializer.
 *
 * @author yokochi
 */
public class XPathCompList {

	/** Serialized XPath parse tree. */
	public List<XPathComp> comps = null;

	/** Instance of path expression. */
	public List<XPathExpr> path_exprs = new ArrayList<XPathExpr>();

	/** Instance of predicate expression. */
	private List<XPathPredExpr> pred_exprs = null;

	/** XPath variable reference. */
	private HashMap<String, String> variables = null;

	/** Whether UnionExprNoRootContext node exists. */
	private boolean union_expr = false;

	/** Whether either document key or in-place document key exists in PostgreSQL DDL. */
	private boolean document_key = true;

	/** Whether serial key exists in PostgreSQL DDL. */
	private boolean serial_key = false;

	/** The serial key name in PostgreSQL DDL. */
	private String serial_key_name;

	/** The verbose mode. */
	private boolean verbose = false;

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** The PostgreSQL data model option. */
	private PgSchemaOption option = null;

	/** The default schema location. */
	private String def_schema_location = null;

	/** The list of PostgreSQL table. */
	private List<PgTable> tables = null;

	/** The root table. */
	private PgTable root_table = null;

	/** Instance of path expression for UnionExprNoRootContext node. */
	private List<XPathExpr> path_exprs_union = null;

	/** The prefix of namespace URI in PostgreSQL XPath. */
	private final String pg_xpath_prefix = "ns";

	/** The qualifier of namespace URI in PostgreSQL XPath. */
	private final String pg_xpath_prefix_ = pg_xpath_prefix + ":";

	// XPath parser being aware of XML Schema

	/**
	 * Instance of XPath parse tree serializer (XPath parser being aware of XML Schema).
	 *
	 * @param schema PostgreSQL data model
	 * @param tree XPath parse tree
	 * @param variables XPath variable reference
	 */
	public XPathCompList(PgSchema schema, ParseTree tree, HashMap<String, String> variables) {

		this.schema = schema;

		option = schema.option;

		def_schema_location = schema.getDefaultSchemaLocation();

		tables = schema.getTableList();

		root_table = schema.getRootTable();

		document_key = option.document_key || option.in_place_document_key;

		serial_key = option.serial_key;

		verbose = option.verbose;

		serial_key_name = option.serial_key_name;

		comps = new ArrayList<XPathComp>();

		if (verbose)
			System.out.println("Abstract syntax tree of query: '" + tree.getText() + "'");

		if (!testParserTree(tree, " "))
			return;

		union_counter = step_counter = 0;

		if (verbose)
			System.out.println("\nSerialized axis and node-test of query: '"+ tree.getText() + "'");

		serializeTree(tree);

		if (verbose)
			System.out.println();

		this.variables = variables;

	}

	/**
	 * Instance of XPathCompList (internal use only).
	 */
	private XPathCompList() {
	}

	/**
	 * Clear XPath parse tree serializer.
	 */
	public void clear() {

		if (comps != null)
			comps.clear();

		if (path_exprs != null)
			path_exprs.clear();

		if (path_exprs_union != null)
			path_exprs_union.clear();

		if (pred_exprs != null)
			pred_exprs.clear();

	}

	/**
	 * Clear XPathCompList (internal use only).
	 */
	private void clearPathExprs() {

		if (path_exprs != null)
			path_exprs.clear();

	}

	/**
	 * Return whether XPath parse tree is effective.
	 *
	 * @param tree XPath parse tree
	 * @param indent indent code for output
	 * @return boolean whether XPath parse tree is effective
	 */
	private boolean testParserTree(ParseTree tree, String indent) {

		boolean valid = false;

		ParseTree child;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			if (verbose)
				System.out.println(indent + child.getClass().getSimpleName() + " '" + child.getText() + "' " + child.getSourceInterval().toString());

			if (testParserTree(child, indent + " ") || child.getChildCount() > 1)
				valid = true;

		}

		return valid;
	}

	/** The union counter. */
	private int union_counter;

	/** The step counter. */
	private int step_counter;

	/** Whether wild card follows. */
	private boolean wild_card = false;

	/**
	 * Serialize XPath parse tree to XPath component list.
	 *
	 * @param tree XPath parse tree
	 */
	private void serializeTree(ParseTree tree) {

		ParseTree child;

		Class<?> anyClass;
		String text;

		boolean union_expr;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			anyClass = child.getClass();
			text = child.getText();

			if (anyClass.equals(TerminalNodeImpl.class)) {

				if (text.equals("*") && tree.getClass().equals(MultiplicativeExprContext.class))
					continue;

				if (!text.equals("/") && !text.equals("//") && !text.equals("|") && !text.equals("*")) {
					union_counter++;
					continue;
				}

				union_expr = text.equals("|");

				if (union_expr) {

					union_counter++;
					step_counter = 0;

				}

				if (text.equals("*")) {

					step_counter--;
					wild_card = true;

				}

				else if (wild_card) {

					step_counter++;
					wild_card = false;

				}

				XPathComp comp = new XPathComp(union_counter, step_counter, child);

				comps.add(comp);

				if (verbose)
					System.out.println(union_counter + "." + step_counter + " - " + anyClass.getSimpleName() + " '" + text + "'");

				if (!wild_card)
					step_counter++;

				if (union_expr) {

					union_counter++;
					step_counter = 0;

				}

				continue;
			}

			else if (anyClass.equals(StepContext.class)) {

				if (verbose)
					System.out.println(union_counter + "." + step_counter + " - '" + text + "' ->");

				traceChildOfStepContext(child);

				if (verbose)
					System.out.println();

				if (!wild_card)
					step_counter++;

				continue;
			}

			serializeTree(child);

		}

	}

	/**
	 * Trace child node of StepContext node.
	 *
	 * @param tree XPath parse tree
	 */
	private void traceChildOfStepContext(ParseTree tree) {

		ParseTree child;

		Class<?> anyClass;
		String text;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			anyClass = child.getClass();
			text = child.getText();

			if (hasEffectiveChildOfQNameContext(child)) {

				comps.add(new XPathComp(union_counter, step_counter, child));

				if (verbose)
					System.out.print(" " + anyClass.getSimpleName() + " '" + text + "'");

				// no need to trace more

				break;
			}

			else if (hasChildOfTerminalNodeImpl(child)) {

				comps.add(new XPathComp(union_counter, step_counter, child));

				if (verbose)
					System.out.print(" " + anyClass.getSimpleName() + " '" + text + "'");

				// no need to trace prefix

				if (anyClass.equals(NameTestContext.class))
					break;

			}

			traceChildOfStepContext(child);

		}

	}

	/**
	 * Return whether current tree has effective QNameContext child node.
	 *
	 * @param tree XPath parse tree
	 * @return boolean whether current tree has effective QNameContext child node
	 */
	private boolean hasEffectiveChildOfQNameContext(ParseTree tree) {

		ParseTree child;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			if (child.getClass().equals(QNameContext.class))
				return child.getChildCount() > 1;

		}

		return false;
	}

	/**
	 * Return whether current tree has multiple child nodes without null text.
	 *
	 * @param tree XPath parse tree
	 * @return boolean whether current tree has multiple child nodes without null text
	 */
	private boolean hasEffectiveChildren(ParseTree tree) {

		if (tree.getChildCount() < 2)
			return false;

		int children = 0;

		ParseTree child;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			if (!child.getText().isEmpty())
				children++;

		}

		return children > 1;
	}

	/**
	 * Return whether current tree has TerminalNodeImpl child node.
	 *
	 * @param tree XPath parse tree
	 * @return boolean whether current tree has TerminialNodeImpl child node
	 */
	private boolean hasChildOfTerminalNodeImpl(ParseTree tree) {

		ParseTree child;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class))
				return true;

		}

		return false;
	}

	/**
	 * Return text content of child TerminalNodeImpl node.
	 *
	 * @param tree XPath parse tree
	 * @return String text content of child TerminalNodeImpl node
	 */
	private String getTextOfChildTerminalNodeImpl(ParseTree tree) {

		ParseTree child;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class))
				return child.getText();

		}

		return null;
	}

	/**
	 * Return array of text content of child TerminalNodeImpl node.
	 *
	 * @param tree XPath parse tree
	 * @return String[] array of text content of child TerminalNodeImpl node
	 */
	private String[] getTextArrayOfChildTerminalNodeImpl(ParseTree tree) {

		List<String> list = new ArrayList<String>();

		try {

			ParseTree child;

			for (int i = 0; i < tree.getChildCount(); i++) {

				child = tree.getChild(i);

				if (child.getClass().equals(TerminalNodeImpl.class))
					list.add(child.getText());

			}

			return list.toArray(new String[0]);

		} finally {
			list.clear();
		}

	}

	/**
	 * Return whether current tree is path context.
	 *
	 * @param anyClass class object
	 * @return boolean whether current tree is path context
	 */
	private boolean isPathContextClass(Class<?> anyClass) {
		return anyClass.equals(RelativeLocationPathContext.class)
				|| anyClass.equals(AbsoluteLocationPathNorootContext.class)
				|| anyClass.equals(StepContext.class)
				|| anyClass.equals(NodeTestContext.class);
	}

	/**
	 * Return the last union id.
	 *
	 * @return int the last union id
	 */
	private int getLastUnionId() {
		return comps.size() > 0 ? comps.get(comps.size() - 1).union_id : -1;
	}

	/**
	 * Return the last step id.
	 *
	 * @param union_id union id
	 * @return int the last step id
	 */
	private int getLastStepId(int union_id) {

		if (comps.parallelStream().filter(comp -> comp.union_id == union_id).count() == 0)
			return -1;

		return comps.parallelStream().filter(comp -> comp.union_id == union_id).max(Comparator.comparingInt(comp -> comp.step_id)).get().step_id;
	}

	/**
	 * Return array of given XPath component.
	 *
	 * @param union_id current union id
	 * @return XPathComp[] array of XPath component
	 */
	private XPathComp[] arrayOf(int union_id) {
		return comps.stream().filter(comp -> comp.union_id == union_id).toArray(XPathComp[]::new);
	}

	/**
	 * Return array of given XPath component.
	 *
	 * @param union_id current union id
	 * @param step_id current step id
	 * @return XPathComp[] array of XPath component
	 */
	private XPathComp[] arrayOf(int union_id, int step_id) {
		return comps.stream().filter(comp -> comp.union_id == union_id && comp.step_id == step_id).toArray(XPathComp[]::new);
	}

	/**
	 * Return array of PredicateContext of given XPath component.
	 *
	 * @param union_id current union id
	 * @param step_id current step id
	 * @return XPathComp[] array of PredicateContext XPath component
	 */
	private XPathComp[] arrayOfPredicateContext(int union_id, int step_id) {
		return comps.stream().filter(comp -> comp.union_id == union_id && comp.step_id == step_id && comp.tree.getClass().equals(PredicateContext.class)).toArray(XPathComp[]::new);
	}

	/**
	 * Return size of path expression in XPath component list.
	 *
	 * @return int the size of path expression in XPath component list
	 */
	private int sizeOfPathExpr() {
		return getLastUnionId() + 1 - (int) comps.parallelStream().filter(comp -> comp.tree.getClass().equals(TerminalNodeImpl.class) && comp.tree.getText().equals("|")).count();
	}

	/**
	 * Return previous step of XPath component.
	 *
	 * @param comp current XPath component in list
	 * @return XPathComp parent XPath component
	 */
	private XPathComp previousOf(XPathComp comp) {

		int step_id = comp.step_id - (comp.tree.getClass().equals(TerminalNodeImpl.class) ? 1 : 2);

		if (step_id < 0)
			return null;

		XPathComp[] prev_comps = comps.stream().filter(_comp -> _comp.union_id == comp.union_id && _comp.step_id == step_id).toArray(XPathComp[]::new);

		if (prev_comps == null || prev_comps.length == 0)
			return null;

		for (int comp_id = 0; comp_id < prev_comps.length; comp_id++) {

			if (prev_comps[comp_id].tree.getClass().equals(PredicateContext.class))
				return prev_comps[--comp_id];

		}

		return prev_comps[prev_comps.length - 1];
	}

	/**
	 * Add a path expression.
	 *
	 * @param path_expr current path expression
	 */
	private void add(XPathExpr path_expr) {

		path_exprs.add(path_expr);

	}

	/**
	 * Add all path expressions.
	 *
	 * @param list XPath component list
	 */
	private void addAll(XPathCompList list) {

		path_exprs.addAll(list.path_exprs);

	}

	/**
	 * Validate XPath expression against XML Schema.
	 *
	 * @param ends_with_text whether to append text node in the ends, if possible
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validate(boolean ends_with_text) throws PgSchemaException {

		XPathComp[] comps;

		Class<?> anyClass;

		for (int union_id = 0; union_id <= getLastUnionId(); union_id++) {

			for (int step_id = 0; step_id <= getLastStepId(union_id); step_id++) {

				comps = arrayOf(union_id, step_id);

				for (XPathComp comp : comps) {

					anyClass = comp.tree.getClass();

					// TerminalNodeImpl node

					if (anyClass.equals(TerminalNodeImpl.class))
						testTerminalNodeImpl(comp, false);

					// AbbreviatedStepContext node

					else if (anyClass.equals(AbbreviatedStepContext.class))
						testAbbreviateStepContext(comp, false);

					// AxisSpecifierContext node

					else if (anyClass.equals(AxisSpecifierContext.class))
						testAxisSpecifierContext(comp, comps);

					// NCNameContext node

					else if (anyClass.equals(NCNameContext.class))
						testNCNameContext(comp, comps, false);

					// NodeTestContext node

					else if (anyClass.equals(NodeTestContext.class))
						testNodeTestContext(comp, comps, false);

					// NameTestContext node

					else if (anyClass.equals(NameTestContext.class))
						testNameTestContext(comp, comps, false);

					// PredicateContext node

					else if (anyClass.equals(PredicateContext.class)) {

						for (XPathComp pred_comp : arrayOfPredicateContext(union_id, step_id))
							testPredicateContext(pred_comp);

						break;
					}

					else
						throw new PgSchemaException(comp.tree);

				}

			}

		}

		applyUnionExpr();

		if (ends_with_text && hasPathEndsWithoutTextNode()) {

			removePathEndsWithTableNode();
			appendTextNode();

		}

		removeDuplicatePath();

	}

	/**
	 * Test TerminalNodeImpl node.
	 *
	 * @param comp current XPath component
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testTerminalNodeImpl(XPathComp comp, boolean predicate) throws PgSchemaException {

		String text = comp.tree.getText();
		int step_id = comp.step_id;

		switch (text) {
		case "/":
		case "//":
			// first TerminalNodeImpl node

			if (step_id == 0) { }

			// check terminus of paths

			else {

				if (hasPathEndsWithTextNode()) {

					XPathComp[] child_comps = arrayOf(comp.union_id, step_id + 1);

					if (child_comps == null || child_comps.length == 0) {

						if (removePathEndsWithTextNode() == 0) {

							if (!predicate)
								throw new PgSchemaException(comp.tree, previousOf(comp).tree);

						}

					}

					else {

						XPathComp child_last_comp = child_comps[child_comps.length - 1];

						if (!child_last_comp.tree.getClass().equals(NodeTestContext.class) || !child_last_comp.tree.getText().equals("text()")) {

							if (removePathEndsWithTextNode() == 0) {

								if (!predicate)
									throw new PgSchemaException(comp.tree, previousOf(comp).tree);

							}

						}

					}

				}

				if (text.equals("//"))
					appendAbbrevPath();

			}
			break;
		case "*": // delegate to other NCNameContext node
			break;
		case "|":
			if (!predicate) {

				union_expr = true;

				if (path_exprs_union == null)
					path_exprs_union = new ArrayList<XPathExpr>();

				path_exprs_union.addAll(path_exprs);

				path_exprs.clear();

			}
			break;
		default:
			throw new PgSchemaException(comp.tree);
		}

	}

	/**
	 * Test AbbreviateStepContext node.
	 *
	 * @param comp current XPath component
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testAbbreviateStepContext(XPathComp comp, boolean predicate) throws PgSchemaException {

		switch (comp.tree.getText()) {
		case ".":
			break;
		case "..":
			if (selectParentPath() == 0) {

				if (!predicate) {

					XPathComp prev_comp = previousOf(comp);

					if (prev_comp != null)
						throw new PgSchemaException(comp.tree, prev_comp.tree);
					else
						throw new PgSchemaException(comp.tree);

				}

			}
			break;
		default:
			throw new PgSchemaException(comp.tree);
		}

	}

	/**
	 * Test AxisSpecifierContext node.
	 *
	 * @param comp current XPath component
	 * @param comps array of XPath component of the same step
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testAxisSpecifierContext(XPathComp comp, XPathComp[] comps) throws PgSchemaException {

		// delegate to succeeding predicate

		if (comps.length > 1) {

			Class<?> anyClass = comps[1].getClass();

			if (anyClass.equals(NCNameContext.class) || anyClass.equals(NodeTestContext.class) || anyClass.equals(NameTestContext.class))
				return;

		}

		else
			throw new PgSchemaException(comp.tree);

	}

	/**
	 * Test NCNameContext node.
	 *
	 * @param comp current XPath component
	 * @param comps array of XPath component of the same step
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContext(XPathComp comp, XPathComp[] comps, boolean predicate) throws PgSchemaException {

		boolean wild_card = false;

		if (comps.length == 1)
			testNCNameContextWithChildAxis(comp, isAbsolutePath(comp.union_id), true, wild_card, null, predicate);

		else {

			boolean target_comp = false;

			Class<?> _anyClass;

			for (XPathComp _comp : comps) {

				_anyClass = _comp.tree.getClass();

				if (_anyClass.equals(NameTestContext.class) || _anyClass.equals(PredicateContext.class))
					break;

				else if (_anyClass.equals(NCNameContext.class)) {

					if (_comp.equals(comp))
						target_comp = true;

					break;
				}

			}

			if (!target_comp)
				return;

			XPathComp first_comp = comps[0];

			for (XPathComp _comp : comps) {

				_anyClass = _comp.tree.getClass();

				if (_anyClass.equals(PredicateContext.class))
					break;

				else if (_anyClass.equals(AxisSpecifierContext.class)) {

					if (!_comp.equals(first_comp))
						throw new PgSchemaException(_comp.tree);

				}

				else if (_anyClass.equals(TerminalNodeImpl.class))
					wild_card = true;

			}

			String composite_text = null;

			if (wild_card) {

				StringBuilder sb = new StringBuilder();

				String _text;

				for (XPathComp _comp : comps) {

					_anyClass = _comp.tree.getClass();
					_text = _comp.tree.getText();

					if (_anyClass.equals(PredicateContext.class))
						break;

					if (_anyClass.equals(NCNameContext.class))
						sb.append(_text);

					else if (_anyClass.equals(NameTestContext.class)) {

						String local_part = _text;

						if (local_part.contains(":"))
							local_part = local_part.split(":")[1];

						sb.append((local_part.equals("*") ? "." : "") + local_part); // '*' -> regular expression '.*'

					}

					else if (_anyClass.equals(TerminalNodeImpl.class)) // '*' -> regular expression '.*'
						sb.append("." + _text);

					else if (!_anyClass.equals(AxisSpecifierContext.class))
						throw new PgSchemaException(_comp.tree);

				}

				composite_text = sb.toString();

				sb.setLength(0);

			}

			if (first_comp.tree.getClass().equals(AxisSpecifierContext.class)) {

				switch (first_comp.tree.getText()) {
				case "ancestor::":
					testNCNameContextWithAncestorAxis(comp, false, wild_card, composite_text, predicate);
					break;
				case "ancestor-or-self::":
					testNCNameContextWithAncestorAxis(comp, true, wild_card, composite_text, predicate);
					break;
				case "attribute::":
				case "@":
					testNCNameContextWithAttributeAxis(comp, wild_card, composite_text, predicate);
					break;
				case "child::":
					testNCNameContextWithChildAxis(comp, isAbsolutePath(comp.union_id), true, wild_card, composite_text, predicate);
					break;
				case "descendant::":
					testNCNameContextWithChildAxis(comp, false, false, wild_card, composite_text, predicate);
					break;
				case "descendant-or-self::":
					testNCNameContextWithChildAxis(comp, false, true, wild_card, composite_text, predicate);
					break;
				case "preceding-sibling::":	// non-sense in schema analysis
				case "following-sibling::": // non-sense in schema analysis
				case "self::":
					testNCNameContextWithChildAxis(comp, true, true, wild_card, composite_text, predicate);
					break;
				case "following::": // non-sense in schema analysis
				case "preceding::": // non-sense in schema analysis
					testNCNameContextWithChildAxis(comp, false, true, wild_card, composite_text, predicate);
					break;
				case "parent::":
					testNCNameContextWithParentAxis(comp, wild_card, composite_text, predicate);
					break;
				default: // namespace
					throw new PgSchemaException(first_comp.tree);
				}

			}

			else
				testNCNameContextWithChildAxis(comp, isAbsolutePath(comp.union_id), true, wild_card, composite_text, predicate);

		}

	}

	/**
	 * Test NCNameContext node having child axis.
	 *
	 * @param comp current XPath component
	 * @param abs_path whether absolute location path or abbreviate location path
	 * @param inc_self whether to include self node
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContextWithChildAxis(XPathComp comp, boolean abs_path, boolean inc_self, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : comp.tree.getText();

		boolean init_path = path_exprs.isEmpty();

		// first NCNameContext node

		if (init_path) {

			if (abs_path) {

				if (!root_table.matchesNodeName(text, wild_card))
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

				if (inc_self)
					add(new XPathExpr(getAbsoluteXPathOfTable(root_table, null), XPathCompType.table));

			}

			else {

				tables.stream().filter(table -> table.matchesNodeName(text, wild_card) && !table.virtual).forEach(table -> {

					String table_xpath = getAbsoluteXPathOfTable(table, null);

					if (table_xpath != null && inc_self)
						add(new XPathExpr(table_xpath, XPathCompType.table));

					if (table.fields.parallelStream().anyMatch(field -> field.simple_content && !field.simple_attribute)) {

						String simple_content_xpath = getAbsoluteXPathOfTable(table, null);

						if (simple_content_xpath != null && inc_self)
							add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

					}

				});

				int _path_exprs_size;

				for (PgTable table : tables) {

					_path_exprs_size = path_exprs.size();

					table.fields.stream().filter(field -> field.matchesNodeName(option, text, false, wild_card) && field.element).forEach(field -> {

						String element_xpath = getAbsoluteXPathOfElement(table, field.xname, null);

						if (element_xpath != null && inc_self)
							add(new XPathExpr(element_xpath, XPathCompType.element));

					});

					if (table.has_any && (wild_card || _path_exprs_size == path_exprs.size())) {

						table.fields.stream().filter(field -> field.any).forEach(field -> {

							String element_xpath = getAbsoluteXPathOfElement(table, text, null);

							if (element_xpath != null && inc_self)
								add(new XPathExpr(element_xpath, XPathCompType.any_element));

						});

					}

				}

				if (path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

			}

		}

		// succeeding NCNameContext node

		else {

			XPathCompList rep_list = new XPathCompList();

			path_exprs.forEach(path_expr -> {

				String ref_path = path_expr.path;

				XPathCompList _list = new XPathCompList();

				// inside any element

				if (path_expr.terminus.equals(XPathCompType.any_element))
					_list.add(new XPathExpr(path_expr.path + " " + text, XPathCompType.any_element));

				else {

					String cur_table = getLastNameOfPath(path_expr.path);

					// not specified table

					if (cur_table == null) {

						if (abs_path) {

							if (root_table.matchesNodeName(text, wild_card) && inc_self)
								_list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, ref_path), XPathCompType.table));

						}

						else {

							tables.stream().filter(table -> table.matchesNodeName(text, wild_card) && !table.virtual).forEach(table -> {

								String table_xpath = getAbsoluteXPathOfTable(table, ref_path);

								if (table_xpath != null && inc_self)
									_list.add(new XPathExpr(table_xpath, XPathCompType.table));

								if (table.fields.parallelStream().anyMatch(field -> field.simple_content && !field.simple_attribute)) {

									String simple_content_xpath = getAbsoluteXPathOfTable(table, ref_path);

									if (simple_content_xpath != null && inc_self)
										_list.add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

								}

							});

							int _path_exprs_size;

							for (PgTable table : tables) {

								_path_exprs_size = _list.path_exprs.size();

								table.fields.stream().filter(field -> field.matchesNodeName(option, text, false, wild_card) && field.element).forEach(field -> {

									String element_xpath = getAbsoluteXPathOfElement(table, ref_path, field.xname);

									if (element_xpath != null && inc_self)
										_list.add(new XPathExpr(element_xpath, XPathCompType.element));

								});

								if (table.has_any && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

									table.fields.stream().filter(field -> field.element).forEach(field -> {

										String element_xpath = getAbsoluteXPathOfElement(table, ref_path, text);

										if (element_xpath != null && inc_self)
											_list.add(new XPathExpr(element_xpath, XPathCompType.any_element));

									});

								}

							}

						}

					}

					// specified table

					else {

						PgTable table = getTable(path_expr);

						if (table == null) {

							XPathComp prev_comp = previousOf(comp);

							try {

								if (prev_comp != null)
									throw new PgSchemaException(comp.tree, prev_comp.tree);
								else
									throw new PgSchemaException(comp.tree);

							} catch (PgSchemaException e) {
								e.printStackTrace();
							}

						}

						// check current element

						int _path_exprs_size = _list.path_exprs.size();

						table.fields.stream().filter(field -> field.element && field.matchesNodeName(option, text, false, wild_card)).forEach(field -> {

							String element_xpath = field.xname;

							if (element_xpath != null && inc_self)
								_list.add(new XPathExpr(path_expr.path + "/" + element_xpath, XPathCompType.element));

						});

						if (table.has_any && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

							table.fields.stream().filter(field -> field.any).forEach(field -> {

								String element_xpath = text;

								if (element_xpath != null && inc_self)
									_list.add(new XPathExpr(path_expr.path + "/" + element_xpath, XPathCompType.any_element));

							});

						}

						// check current nested_key

						boolean has_any = false;
						int _touched_size;

						HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

						Integer[] ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
						Integer[] _ft_ids = null;

						while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

							boolean first_nest = _ft_ids == null;
							_touched_size = touched_ft_ids.size();

							for (Integer foreign_table_id : ft_ids) {

								if (!touched_ft_ids.add(foreign_table_id))
									continue;

								PgTable foreign_table = schema.getTable(foreign_table_id);

								// check foreign table

								if (foreign_table.matchesNodeName(text, wild_card) && !foreign_table.virtual) {

									String table_xpath = getAbsoluteXPathOfTable(foreign_table, ref_path);

									if (table_xpath != null && (inc_self || first_nest))
										_list.add(new XPathExpr(table_xpath, XPathCompType.table));

									if (foreign_table.fields.parallelStream().anyMatch(field -> field.simple_content && !field.simple_attribute)) {

										String simple_content_xpath = getAbsoluteXPathOfTable(foreign_table, ref_path);

										if (simple_content_xpath != null && (inc_self || first_nest))
											_list.add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

									}

								}

								// check foreign element

								foreign_table.fields.stream().filter(field -> field.element && field.matchesNodeName(option, text, false, wild_card)).forEach(field -> {

									String element_xpath = getAbsoluteXPathOfElement(foreign_table, ref_path, field.xname);

									if (element_xpath != null && (inc_self || first_nest))
										_list.add(new XPathExpr(element_xpath, XPathCompType.element));

								});

								if (foreign_table.has_any)
									has_any = true;

								// check foreign nested_key

								if (foreign_table.virtual || !abs_path) {

									Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

									if (__ft_ids != null && __ft_ids.length > 0)
										_ft_ids = __ft_ids;

								}

							}

							ft_ids = _ft_ids;

							if (touched_ft_ids.size() == _touched_size)
								break;

						}

						touched_ft_ids.clear();

						if (has_any && _path_exprs_size == _list.path_exprs.size()) {

							ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
							_ft_ids = null;

							while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

								boolean first_nest = _ft_ids == null;
								_touched_size = touched_ft_ids.size();

								for (Integer foreign_table_id : ft_ids) {

									if (!touched_ft_ids.add(foreign_table_id))
										continue;

									PgTable foreign_table = schema.getTable(foreign_table_id);

									if (foreign_table.has_any && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

										foreign_table.fields.stream().filter(field -> field.any).forEach(field -> {

											String element_xpath = getAbsoluteXPathOfElement(foreign_table, ref_path, text);

											if (element_xpath != null && (inc_self || first_nest))
												_list.add(new XPathExpr(element_xpath, XPathCompType.any_element));

										});

									}

									// check foreign nested_key

									if (foreign_table.virtual || !abs_path) {

										Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

										if (__ft_ids != null && __ft_ids.length > 0)
											_ft_ids = __ft_ids;

									}

								}

								ft_ids = _ft_ids;

								if (touched_ft_ids.size() == _touched_size)
									break;

							}

							touched_ft_ids.clear();

						}

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

		}

	}

	/**
	 * Test NCNameContext node having attribute axis.
	 *
	 * @param comp current XPath component
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContextWithAttributeAxis(XPathComp comp, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : comp.tree.getText();

		boolean init_path = path_exprs.isEmpty();

		// first NCNameContext node

		if (init_path) {

			if (isAbsolutePath(comp.union_id))
				throw new PgSchemaException(comp.tree);

			else {

				int _path_exprs_size;

				for (PgTable table : tables) {

					_path_exprs_size = path_exprs.size();

					table.fields.stream().filter(field -> field.matchesNodeName(option, text, true, wild_card) && (field.attribute || field.simple_attribute || field.simple_attr_cond)).forEach(field -> {

						String attribute_xpath = getAbsoluteXPathOfAttribute(table, field.attribute ? field.xname : field.foreign_table_xname, null);

						if (attribute_xpath != null)
							add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

					});

					if (table.has_any_attribute && (wild_card || _path_exprs_size == path_exprs.size())) {

						table.fields.stream().filter(field -> field.any_attribute).forEach(field -> {

							String attribute_xpath = getAbsoluteXPathOfAttribute(table, text, null);

							if (attribute_xpath != null)
								add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

						});

					}

				}

				if (path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

			}

		}

		// succeeding NCNameContext node

		else {

			boolean abs_location_path = isAbsolutePath(comp.union_id);

			XPathCompList rep_list = new XPathCompList();

			path_exprs.forEach(path_expr -> {

				String ref_path = path_expr.path;

				XPathCompList _list = new XPathCompList();

				// inside any element

				if (path_expr.terminus.equals(XPathCompType.any_element))
					_list.add(new XPathExpr(path_expr.path + " @" + text, XPathCompType.any_element));

				else {

					String cur_table = getLastNameOfPath(path_expr.path);

					// not specified table

					if (cur_table == null) {

						if (abs_location_path) {

							if (!path_expr.terminus.equals(XPathCompType.any_element)) {

								int _path_exprs_size;

								for (PgTable table : tables) {

									_path_exprs_size = _list.path_exprs.size();

									table.fields.stream().filter(field -> field.matchesNodeName(option, text, true, wild_card) && (field.attribute || field.simple_attribute || field.simple_attr_cond)).forEach(field -> {

										String attribute_xpath = getAbsoluteXPathOfAttribute(table, ref_path, field.attribute ? field.xname : field.foreign_table_xname);

										if (attribute_xpath != null)
											_list.add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

									});

									if (table.has_any_attribute && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

										table.fields.stream().filter(field -> field.any_attribute).forEach(field -> {

											String attribute_xpath = getAbsoluteXPathOfAttribute(table, ref_path, text);

											if (attribute_xpath != null)
												_list.add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

										});

									}

								}

							}

						}

					}

					// specified table

					else {

						PgTable table = getTable(path_expr);

						if (table == null) {

							XPathComp prev_comp = previousOf(comp);

							try {

								if (prev_comp != null)
									throw new PgSchemaException(comp.tree, prev_comp.tree);
								else
									throw new PgSchemaException(comp.tree);

							} catch (PgSchemaException e) {
								e.printStackTrace();
							}

						}

						// check current attribute

						int _path_exprs_size = _list.path_exprs.size();

						table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond) && field.matchesNodeName(option, text, true, wild_card)).forEach(field -> {

							String attribute_xpath = "@" + (field.attribute ? field.xname : field.foreign_table_xname);

							if (attribute_xpath != null)
								_list.add(new XPathExpr(path_expr.path + "/" + attribute_xpath, XPathCompType.attribute));

						});

						if (table.has_any_attribute && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

							table.fields.stream().filter(field -> field.any_attribute).forEach(field -> {

								String attribute_xpath = "@" + text;

								if (attribute_xpath != null)
									_list.add(new XPathExpr(path_expr.path + "/" + attribute_xpath, XPathCompType.any_attribute));

							});

						}

						// check current nested_key

						boolean has_any_attribute = false;
						int _touched_size;

						HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

						Integer[] ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
						Integer[] _ft_ids = null;

						while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

							_touched_size = touched_ft_ids.size();

							for (Integer foreign_table_id : ft_ids) {

								if (!touched_ft_ids.add(foreign_table_id))
									continue;

								PgTable foreign_table = schema.getTable(foreign_table_id);

								// check foreign attribute

								foreign_table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond) && field.matchesNodeName(option, text, true, wild_card)).forEach(field -> {

									String attribute_xpath = getAbsoluteXPathOfAttribute(foreign_table, ref_path, field.attribute ? field.xname : field.foreign_table_xname);

									if (attribute_xpath != null)
										_list.add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

								});

								if (foreign_table.has_any_attribute)
									has_any_attribute = true;

								// check foreign nested_key

								if (foreign_table.virtual || foreign_table.has_nested_key_as_attr || !abs_location_path) {

									Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

									if (__ft_ids != null && __ft_ids.length > 0)
										_ft_ids = __ft_ids;

								}

							}

							ft_ids = _ft_ids;

							if (touched_ft_ids.size() == _touched_size)
								break;

						}

						touched_ft_ids.clear();

						if (has_any_attribute && _path_exprs_size == _list.path_exprs.size()) {

							ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
							_ft_ids = null;

							while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

								_touched_size = touched_ft_ids.size();

								for (Integer foreign_table_id : ft_ids) {

									if (!touched_ft_ids.add(foreign_table_id))
										continue;

									PgTable foreign_table = schema.getTable(foreign_table_id);

									if (!foreign_table.virtual && foreign_table.content_holder && !path_expr.path.contains(foreign_table.xname))
										continue;

									// check foreign attribute

									if (foreign_table.has_any_attribute && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

										String attribute_xpath = getAbsoluteXPathOfAttribute(foreign_table, ref_path, text);

										if (attribute_xpath != null)
											_list.add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

									}

									// check foreign nested_key

									if (foreign_table.virtual || foreign_table.has_nested_key_as_attr || !abs_location_path) {

										Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

										if (__ft_ids != null && __ft_ids.length > 0)
											_ft_ids = __ft_ids;

									}

								}

								ft_ids = _ft_ids;

								if (touched_ft_ids.size() == _touched_size)
									break;

							}

							touched_ft_ids.clear();

						}

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

		}

	}

	/**
	 * Test NCNameContext node having parent axis.
	 *
	 * @param comp current XPath component
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContextWithParentAxis(XPathComp comp, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		if (selectParentPath() == 0) {

			if (predicate)
				return;

			throw new PgSchemaException(comp.tree, previousOf(comp).tree);

		}

		testNCNameContext(comp, wild_card, composite_text, predicate);

	}

	/**
	 * Test NCNameContext node having ancestor axis.
	 *
	 * @param comp current XPath component
	 * @param inc_self whether to include self node
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContextWithAncestorAxis(XPathComp comp, boolean inc_self, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		List<XPathExpr> _path_exprs = new ArrayList<XPathExpr>();

		try {

			if (inc_self)
				_path_exprs.addAll(path_exprs);

			while (selectParentPath() > 0)
				_path_exprs.addAll(path_exprs);

			replacePathExprs(_path_exprs);

			testNCNameContext(comp, wild_card, composite_text, predicate);

		} finally {
			_path_exprs.clear();
		}

	}

	/**
	 * Test NCNameContext node.
	 *
	 * @param comp current XPath component
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContext(XPathComp comp, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = comp.tree.getText();

		Iterator<XPathExpr> iter = path_exprs.iterator();

		XPathExpr path_expr;
		String[] _path;
		String last_path;

		while (iter.hasNext()) {

			path_expr = iter.next();

			_path = path_expr.path.split("/");
			last_path = _path[_path.length - 1];

			if (!(wild_card ? last_path.matches(composite_text) : last_path.equals(text)))
				iter.remove();

		}

		if (path_exprs.size() == 0) {

			if (predicate)
				return;

			throw new PgSchemaException(comp.tree, previousOf(comp).tree);

		}

	}

	/**
	 * Test NodeTestContext node.
	 *
	 * @param comp current XPath component
	 * @param comps array of XPath component of the same step
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNodeTestContext(XPathComp comp, XPathComp[] comps, boolean predicate) throws PgSchemaException {

		XPathComp first_comp = comps[0];

		String text = comp.tree.getText();

		if (comps.length == 1) {

			switch (text) {
			case "node()":
				testNodeTestContextWithChildAxis(comp, isAbsolutePath(comp.union_id), true, predicate);
				break;
			case "text()":
				removePathEndsWithTableNode();

				if (hasPathEndsWithTextNode())
					throw new PgSchemaException(comp.tree);

				appendTextNode();
				break;
			case "comment()":
				if (hasPathEndsWithTextNode())
					throw new PgSchemaException(comp.tree);

				appendCommentNode();
				break;
			default:
				if (text.startsWith("processing-instruction"))
					appendProcessingInstructionNode(text);

				else
					throw new PgSchemaException(comp.tree);
			}

		}

		else if (comps.length == 2 && first_comp.tree.getClass().equals(AxisSpecifierContext.class)) {

			switch (text) {
			case "node()":
				switch (first_comp.tree.getText()) {
				case "ancestor::":
					testNodeTestContextWithAncestorAxis(comp, false, predicate);
					break;
				case "ancestor-or-self::":
					testNodeTestContextWithAncestorAxis(comp, true, predicate);
					break;
				case "attribute::":
				case "@":
					testNodeTestContextWithAttributeAxis(comp, predicate);
					break;
				case "child::":
					testNodeTestContextWithChildAxis(comp, isAbsolutePath(comp.union_id), true, predicate);
					break;
				case "descendant::":
					testNodeTestContextWithChildAxis(comp, false, false, predicate);
					break;
				case "descendant-or-self::":
					testNodeTestContextWithChildAxis(comp, false, true, predicate);
					break;
				case "preceding-sibling::":	// non-sense in schema analysis
				case "following-sibling::": // non-sense in schema analysis
				case "self::":
					testNodeTestContextWithChildAxis(comp, true, true, predicate);
					break;
				case "following::": // non-sense in schema analysis
				case "preceding::": // non-sense in schema analysis
					testNodeTestContextWithChildAxis(comp, false, true, predicate);
					break;
				case "parent::":
					testNodeTestContextWithParentAxis(comp, predicate);
					break;
				default: // namespace
					throw new PgSchemaException(first_comp.tree);
				}
				break;
			default:
				throw new PgSchemaException(comp.tree, first_comp.tree);
			}

		}

		else
			throw new PgSchemaException(comp.tree);

	}

	/**
	 * Test NodeTestContext node having child axis.
	 *
	 * @param comp current XPath component
	 * @param abs_path whether absolute location path or abbreviate location path
	 * @param inc_self whether to include self node
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNodeTestContextWithChildAxis(XPathComp comp, boolean abs_path, boolean inc_self, boolean predicate) throws PgSchemaException {

		boolean init_path = path_exprs.isEmpty();

		// first NodeTestContext node

		if (init_path) {

			if (abs_path) {

				if (inc_self)
					add(new XPathExpr(getAbsoluteXPathOfTable(root_table, null), XPathCompType.table));

			}

			else {

				tables.stream().filter(table -> !table.virtual).forEach(table -> {

					String table_xpath = getAbsoluteXPathOfTable(table, null);

					if (table_xpath != null && inc_self)
						add(new XPathExpr(table_xpath, XPathCompType.table));

					if (table.fields.parallelStream().anyMatch(field -> field.simple_content && !field.simple_attribute)) {

						String simple_content_xpath = getAbsoluteXPathOfTable(table, null);

						if (simple_content_xpath != null && inc_self)
							add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

					}

				});

				int _path_exprs_size;

				for (PgTable table : tables) {

					_path_exprs_size = path_exprs.size();

					table.fields.stream().filter(field -> field.element).forEach(field -> {

						String element_xpath = getAbsoluteXPathOfElement(table, field.xname, null);

						if (element_xpath != null && inc_self)
							add(new XPathExpr(element_xpath, XPathCompType.element));

					});

					if (table.has_any && _path_exprs_size == path_exprs.size()) {

						table.fields.stream().filter(field -> field.any).forEach(field -> {

							String element_xpath = getAbsoluteXPathOfElement(table, "*", null);

							if (element_xpath != null && inc_self)
								add(new XPathExpr(element_xpath, XPathCompType.any_element));

						});

					}

				}

				if (path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, def_schema_location);

			}

		}

		// succeeding NodeTestContext node

		else {

			XPathCompList rep_list = new XPathCompList();

			path_exprs.forEach(path_expr -> {

				String ref_path = path_expr.path;

				XPathCompList _list = new XPathCompList();

				// inside any element

				if (path_expr.terminus.equals(XPathCompType.any_element))
					_list.add(new XPathExpr(path_expr.path + " *", XPathCompType.any_element));

				else {

					String cur_table = getLastNameOfPath(path_expr.path);

					// not specified table

					if (cur_table == null) {

						if (abs_path) {

							if (inc_self)
								_list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, ref_path), XPathCompType.table));

						}

						else {

							tables.stream().filter(table -> !table.virtual).forEach(table -> {

								String table_xpath = getAbsoluteXPathOfTable(table, ref_path);

								if (table_xpath != null && inc_self)
									_list.add(new XPathExpr(table_xpath, XPathCompType.table));

								table.fields.stream().filter(field -> field.simple_content && !field.simple_attribute).forEach(field -> {

									String simple_content_xpath = getAbsoluteXPathOfTable(table, ref_path);

									if (simple_content_xpath != null && inc_self)
										_list.add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

								});

							});

							int _path_exprs_size;

							for (PgTable table : tables) {

								_path_exprs_size = _list.path_exprs.size();

								table.fields.stream().filter(field -> field.element).forEach(field -> {

									String element_xpath = getAbsoluteXPathOfElement(table, ref_path, field.xname);

									if (element_xpath != null && inc_self)
										_list.add(new XPathExpr(element_xpath, XPathCompType.element));

								});

								if (table.has_any && (_path_exprs_size == _list.path_exprs.size())) {

									table.fields.stream().filter(field -> field.any).forEach(field -> {

										String element_xpath = getAbsoluteXPathOfElement(table, ref_path, "*");

										if (element_xpath != null && inc_self)
											_list.add(new XPathExpr(element_xpath, XPathCompType.any_element));

									});

								}

							}

						}

					}

					// specified table

					else {

						PgTable table = getTable(path_expr);

						if (table == null) {

							XPathComp prev_comp = previousOf(comp);

							try {

								if (prev_comp != null)
									throw new PgSchemaException(comp.tree, prev_comp.tree);
								else
									throw new PgSchemaException(comp.tree);

							} catch (PgSchemaException e) {
								e.printStackTrace();
							}

						}

						// check current element

						int _path_exprs_size = _list.path_exprs.size();

						table.fields.stream().filter(field -> field.element).forEach(field -> {

							String element_xpath = field.xname;

							if (element_xpath != null && inc_self)
								_list.add(new XPathExpr(path_expr.path + "/" + element_xpath, XPathCompType.element));

						});

						if (table.has_any && _path_exprs_size == _list.path_exprs.size()) {

							table.fields.stream().filter(field -> field.any).forEach(field -> {

								String element_xpath = "*";

								if (element_xpath != null && inc_self)
									_list.add(new XPathExpr(path_expr.path + "/" + element_xpath, XPathCompType.any_element));

							});

						}

						// check current nested_key

						boolean has_any = false;
						int _touched_size;

						HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

						Integer[] ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
						Integer[] _ft_ids = null;

						while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

							boolean first_nest = _ft_ids == null;
							_touched_size = touched_ft_ids.size();

							for (Integer foreign_table_id : ft_ids) {

								if (!touched_ft_ids.add(foreign_table_id))
									continue;

								PgTable foreign_table = tables.get(foreign_table_id);

								// check foreign table

								if (!foreign_table.virtual) {

									String table_xpath = getAbsoluteXPathOfTable(foreign_table, ref_path);

									if (table_xpath != null && (inc_self || _ft_ids == null))
										_list.add(new XPathExpr(table_xpath, XPathCompType.table));

									if (foreign_table.fields.parallelStream().anyMatch(field -> field.simple_content && !field.simple_attribute)) {

										String simple_content_xpath = getAbsoluteXPathOfTable(foreign_table, ref_path);

										if (simple_content_xpath != null && (inc_self || first_nest))
											_list.add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

									}

								}

								// check foreign element

								foreign_table.fields.stream().filter(field -> field.element).forEach(field -> {

									String element_xpath = getAbsoluteXPathOfElement(foreign_table, ref_path, field.xname);

									if (element_xpath != null && (inc_self || first_nest))
										_list.add(new XPathExpr(element_xpath, XPathCompType.element));

								});

								if (foreign_table.has_any)
									has_any = true;

								// check foreign nested_key

								if (foreign_table.virtual || !abs_path) {

									Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

									if (__ft_ids != null && __ft_ids.length > 0)
										_ft_ids = __ft_ids;

								}

							}

							ft_ids = _ft_ids;

							if (touched_ft_ids.size() == _touched_size)
								break;

						}

						touched_ft_ids.clear();

						if (has_any && (_path_exprs_size == _list.path_exprs.size())) {

							ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
							_ft_ids = null;

							while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

								boolean first_nest = _ft_ids == null;
								_touched_size = touched_ft_ids.size();

								for (Integer foreign_table_id : ft_ids) {

									if (!touched_ft_ids.add(foreign_table_id))
										continue;

									PgTable foreign_table = schema.getTable(foreign_table_id);

									if (foreign_table.has_any && _path_exprs_size == _list.path_exprs.size()) {

										foreign_table.fields.stream().filter(field -> field.any).forEach(field -> {

											String element_xpath = getAbsoluteXPathOfElement(foreign_table, ref_path, "*");

											if (element_xpath != null && (inc_self || first_nest))
												_list.add(new XPathExpr(element_xpath, XPathCompType.any_element));

										});

									}

									// check foreign nested_key

									if (foreign_table.virtual || !abs_path) {

										Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

										if (__ft_ids != null && __ft_ids.length > 0)
											_ft_ids = __ft_ids;

									}

								}

								ft_ids = _ft_ids;

								if (touched_ft_ids.size() == _touched_size)
									break;

							}

							touched_ft_ids.clear();

						}

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, def_schema_location);

		}

	}

	/**
	 * Test NodeTestContext node having attribute axis.
	 *
	 * @param comp current XPath component
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNodeTestContextWithAttributeAxis(XPathComp comp, boolean predicate) throws PgSchemaException {

		boolean init_path = path_exprs.isEmpty();

		// first NodeTestContext node

		if (init_path) {

			if (isAbsolutePath(comp.union_id))
				throw new PgSchemaException(comp.tree);

			else {

				int _path_exprs_size;

				for (PgTable table : tables) {

					_path_exprs_size = path_exprs.size();

					table.fields.stream().filter(field -> field.attribute || field.simple_attribute || field.simple_attr_cond).forEach(field -> {

						String attribute_xpath = getAbsoluteXPathOfAttribute(table, field.attribute ? field.xname : field.foreign_table_xname, null);

						if (attribute_xpath != null)
							add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

					});

					if (table.has_any_attribute && _path_exprs_size == path_exprs.size()) {

						String attribute_xpath = getAbsoluteXPathOfAttribute(table, "*", null);

						if (attribute_xpath != null)
							add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

					}

				}

				if (path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, def_schema_location);

			}

		}

		// succeeding NodeTestContext node

		else {

			boolean abs_location_path = isAbsolutePath(comp.union_id);

			XPathCompList rep_list = new XPathCompList();

			path_exprs.forEach(path_expr -> {

				String ref_path = path_expr.path;

				XPathCompList _list = new XPathCompList();

				if (path_expr.terminus.equals(XPathCompType.any_element))
					_list.add(new XPathExpr(path_expr.path + " @*", XPathCompType.any_attribute));

				else {

					String cur_table = getLastNameOfPath(path_expr.path);

					// not specified table

					if (cur_table == null) {

						if (abs_location_path) {

							if (!path_expr.terminus.equals(XPathCompType.any_element)) {

								int _path_exprs_size;

								for (PgTable table : tables) {

									_path_exprs_size = _list.path_exprs.size();

									table.fields.stream().filter(field -> field.attribute || field.simple_attribute || field.simple_attr_cond).forEach(field -> {

										String attribute_xpath = getAbsoluteXPathOfAttribute(table, ref_path, field.attribute ? field.xname : field.foreign_table_xname);

										if (attribute_xpath != null)
											_list.add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

									});

									if (table.has_any_attribute && (_path_exprs_size == _list.path_exprs.size())) {

										String attribute_xpath = getAbsoluteXPathOfAttribute(table, ref_path, "*");

										if (attribute_xpath != null)
											_list.add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

									}

								}

							}

						}

					}

					// specified table

					else {

						PgTable table = getTable(path_expr);

						if (table == null) {

							XPathComp prev_comp = previousOf(comp);

							try {

								if (prev_comp != null)
									throw new PgSchemaException(comp.tree, prev_comp.tree);
								else
									throw new PgSchemaException(comp.tree);

							} catch (PgSchemaException e) {
								e.printStackTrace();
							}

						}

						// check current attribute

						int _path_exprs_size = _list.path_exprs.size();

						table.fields.stream().filter(field -> field.attribute || field.simple_attribute || field.simple_attr_cond).forEach(field -> {

							String attribute_xpath = "@" + (field.attribute ? field.xname : field.foreign_table_xname);

							if (attribute_xpath != null)
								_list.add(new XPathExpr(path_expr.path + "/" + attribute_xpath, XPathCompType.attribute));

						});

						if (table.has_any_attribute && _path_exprs_size == _list.path_exprs.size()) {

							String attribute_xpath = "@*";

							if (attribute_xpath != null)
								_list.add(new XPathExpr(path_expr.path + "/" + attribute_xpath, XPathCompType.any_attribute));

						}

						// check current nested_key

						boolean has_any_attribute = false;
						int _touched_size;

						HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

						Integer[] ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
						Integer[] _ft_ids = null;

						while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

							_touched_size = touched_ft_ids.size();

							for (Integer foreign_table_id : ft_ids) {

								if (!touched_ft_ids.add(foreign_table_id))
									continue;

								PgTable foreign_table = schema.getTable(foreign_table_id);

								if (!foreign_table.virtual && foreign_table.content_holder && !path_expr.path.contains(foreign_table.xname))
									continue;

								// check foreign attribute

								foreign_table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond)).forEach(field -> {

									String attribute_xpath = getAbsoluteXPathOfAttribute(foreign_table, ref_path, field.attribute ? field.xname : field.foreign_table_xname);

									if (attribute_xpath != null)
										_list.add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

								});

								if (foreign_table.has_any_attribute)
									has_any_attribute = true;

								// check foreign nested_key

								if (foreign_table.virtual || foreign_table.has_nested_key_as_attr || !abs_location_path) {

									Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

									if (__ft_ids != null && __ft_ids.length > 0)
										_ft_ids = __ft_ids;

								}

							}

							ft_ids = _ft_ids;

							if (touched_ft_ids.size() == _touched_size)
								break;

						}

						touched_ft_ids.clear();

						if (has_any_attribute && (_path_exprs_size == _list.path_exprs.size())) {

							ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
							_ft_ids = null;

							while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

								_touched_size = touched_ft_ids.size();

								for (Integer foreign_table_id : ft_ids) {

									if (!touched_ft_ids.add(foreign_table_id))
										continue;

									PgTable foreign_table = schema.getTable(foreign_table_id);

									if (!foreign_table.virtual && foreign_table.content_holder && !path_expr.path.contains(foreign_table.xname))
										continue;

									if (foreign_table.has_any_attribute && _path_exprs_size == _list.path_exprs.size()) {

										String attribute_xpath = getAbsoluteXPathOfAttribute(foreign_table, ref_path, "*");

										if (attribute_xpath != null)
											_list.add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

									}

									// check foreign nested_key

									if (foreign_table.virtual || foreign_table.has_nested_key_as_attr || !abs_location_path) {

										Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

										if (__ft_ids != null && __ft_ids.length > 0)
											_ft_ids = __ft_ids;

									}

								}

								ft_ids = _ft_ids;

								if (touched_ft_ids.size() == _touched_size)
									break;

							}

							touched_ft_ids.clear();

						}

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, def_schema_location);

		}

	}

	/**
	 * Test NodeTestContext node having parent axis.
	 *
	 * @param comp current XPath component
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNodeTestContextWithParentAxis(XPathComp comp, boolean predicate) throws PgSchemaException {

		if (selectParentPath() == 0) {

			if (predicate)
				return;

			throw new PgSchemaException(comp.tree, previousOf(comp).tree);

		}

	}

	/**
	 * Test NameTestContext node.
	 *
	 * @param comp current XPath component
	 * @param comps array of XPath component of the same step
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContext(XPathComp comp, XPathComp[] comps, boolean predicate) throws PgSchemaException {

		boolean wild_card = false;

		String text = comp.tree.getText();

		if (comps.length == 1) {

			String prefix = "";
			String local_part = text;

			if (text.contains(":")) {

				String[] _text = text.split(":");

				prefix = _text[0];
				local_part = _text[1];

			}

			String namespace_uri = schema.getNamespaceUriForPrefix(prefix);

			if (namespace_uri == null || namespace_uri.isEmpty())
				throw new PgSchemaException(comp.tree, def_schema_location, prefix);

			testNameTestContextWithChildAxis(comp, namespace_uri, local_part, isAbsolutePath(comp.union_id), true, wild_card, null, predicate);

		}

		else {

			XPathComp first_comp = comps[0];

			Class<?> _anyClass;

			for (XPathComp _comp : comps) {

				_anyClass = _comp.tree.getClass();

				if (_anyClass.equals(PredicateContext.class))
					break;

				else if (_anyClass.equals(AxisSpecifierContext.class)) {

					if (!_comp.equals(first_comp))
						throw new PgSchemaException(_comp.tree);

				}

				else if (_anyClass.equals(TerminalNodeImpl.class))
					wild_card = true;

			}

			String composite_text = null;

			if (wild_card) {

				StringBuilder sb = new StringBuilder();

				String _text;

				for (XPathComp _comp : comps) {

					_anyClass = _comp.tree.getClass();
					_text = _comp.tree.getText();

					if (_anyClass.equals(PredicateContext.class))
						break;

					if (_anyClass.equals(NCNameContext.class))
						sb.append(_text);

					else if (_anyClass.equals(NameTestContext.class)) {

						String local_part = _text;

						if (local_part.contains(":"))
							local_part = local_part.split(":")[1];

						sb.append((local_part.equals("*") ? "." : "") + local_part); // '*' -> regular expression '.*'

					}

					else if (_anyClass.equals(TerminalNodeImpl.class)) // '*' -> regular expression '.*'
						sb.append("." + _text);

					else if (!_anyClass.equals(AxisSpecifierContext.class))
						throw new PgSchemaException(_comp.tree);

				}

				composite_text = sb.toString();

				sb.setLength(0);

			}

			String prefix = "";
			String local_part = text;

			if (text.contains(":")) {

				String[] _text = text.split(":");

				prefix = _text[0];
				local_part = _text[1];

			}

			String namespace_uri = schema.getNamespaceUriForPrefix(prefix);

			if (namespace_uri == null || namespace_uri.isEmpty())
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location, prefix);

			if (first_comp.tree.getClass().equals(AxisSpecifierContext.class)) {

				switch (first_comp.tree.getText()) {
				case "ancestor::":
					testNameTestContextWithAncestorAxis(comp, namespace_uri, local_part, false, wild_card, composite_text, predicate);
					break;
				case "ancestor-or-self::":
					testNameTestContextWithAncestorAxis(comp, namespace_uri, local_part, true, wild_card, composite_text, predicate);
					break;
				case "attribute::":
				case "@":
					testNameTestContextWithAttributeAxis(comp, prefix.isEmpty() ? PgSchemaUtil.xs_namespace_uri : namespace_uri, local_part, wild_card, composite_text, predicate);
					break;
				case "child::":
					testNameTestContextWithChildAxis(comp, namespace_uri, local_part, isAbsolutePath(comp.union_id), true, wild_card, composite_text, predicate);
					break;
				case "descendant::":
					testNameTestContextWithChildAxis(comp, namespace_uri, local_part, false, false, wild_card, composite_text, predicate);
					break;
				case "descendant-or-self::":
					testNameTestContextWithChildAxis(comp, namespace_uri, local_part, false, true, wild_card, composite_text, predicate);
					break;
				case "preceding-sibling::":	// non-sense in schema analysis
				case "following-sibling::": // non-sense in schema analysis
				case "self::":
					testNameTestContextWithChildAxis(comp, namespace_uri, local_part, true, true, wild_card, composite_text, predicate);
					break;
				case "following::": // non-sense in schema analysis
				case "preceding::": // non-sense in schema analysis
					testNameTestContextWithChildAxis(comp, namespace_uri, local_part, false, true, wild_card, composite_text, predicate);
					break;
				case "parent::":
					testNameTestContextWithParentAxis(comp, namespace_uri, local_part, wild_card, composite_text, predicate);
					break;
				default: // namespace
					throw new PgSchemaException(first_comp.tree);
				}

			}

			else
				testNameTestContextWithChildAxis(comp, namespace_uri, local_part, isAbsolutePath(comp.union_id), true, wild_card, composite_text, predicate);

		}

	}

	/**
	 * Test NameTestContext node having child axis.
	 *
	 * @param comp current XPath component
	 * @param namespace_uri namespace URI of current QName
	 * @param local_part local part name of current QName
	 * @param abs_path whether absolute location path or abbreviate location path
	 * @param inc_self whether to include self node
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContextWithChildAxis(XPathComp comp, String namespace_uri, String local_part, boolean abs_path, boolean inc_self, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : comp.tree.getText();

		boolean init_path = path_exprs.isEmpty();

		// first NameTestContext node

		if (init_path) {

			if (abs_path) {

				if (root_table.target_namespace == null || !root_table.target_namespace.contains(namespace_uri) || !root_table.matchesNodeName(text, wild_card))
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

				if (inc_self)
					add(new XPathExpr(getAbsoluteXPathOfTable(root_table, null), XPathCompType.table));

			}

			else {

				tables.stream().filter(table -> !table.virtual && table.target_namespace != null && table.target_namespace.contains(namespace_uri) && table.matchesNodeName(text, wild_card)).forEach(table -> {

					String table_xpath = getAbsoluteXPathOfTable(table, null);

					if (table_xpath != null && inc_self)
						add(new XPathExpr(table_xpath, XPathCompType.table));

					if (table.fields.parallelStream().anyMatch(field -> field.simple_content && !field.simple_attribute && field.target_namespace.contains(PgSchemaUtil.xs_namespace_uri) && field.matchesNodeName(option, text, false, wild_card))) {

						String simple_content_xpath = getAbsoluteXPathOfTable(table, null);

						if (simple_content_xpath != null && inc_self)
							add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

					}

				});

				int _path_exprs_size;

				for (PgTable table : tables) {

					_path_exprs_size = path_exprs.size();

					table.fields.stream().filter(field -> field.element && field.target_namespace.contains(namespace_uri) && field.matchesNodeName(option, text, false, wild_card)).forEach(field -> {

						String element_xpath = getAbsoluteXPathOfElement(table, field.xname, null);

						if (element_xpath != null && inc_self)
							add(new XPathExpr(element_xpath, XPathCompType.element));

					});

					if (table.has_any && (wild_card || _path_exprs_size == path_exprs.size())) {

						table.fields.stream().filter(field -> field.any && field.target_namespace.contains(namespace_uri)).forEach(field -> {

							String element_xpath = getAbsoluteXPathOfElement(table, text, null);

							if (element_xpath != null && inc_self)
								add(new XPathExpr(element_xpath, XPathCompType.any_element));

						});

					}

				}

				if (path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

			}

		}

		// succeeding NameTestContext node

		else {

			XPathCompList rep_list = new XPathCompList();

			path_exprs.forEach(path_expr -> {

				String ref_path = path_expr.path;

				XPathCompList _list = new XPathCompList();

				if (path_expr.terminus.equals(XPathCompType.any_element))
					_list.add(new XPathExpr(path_expr.path + " " + text, XPathCompType.any_element));

				else {

					String cur_table = getLastNameOfPath(path_expr.path);

					// not specified table

					if (cur_table == null) {

						if (abs_path) {

							if (inc_self && root_table.target_namespace != null && root_table.target_namespace.contains(namespace_uri) && root_table.matchesNodeName(text, wild_card))
								_list.add(new XPathExpr(getAbsoluteXPathOfTable(root_table, ref_path), XPathCompType.table));

						}

						else {

							tables.stream().filter(table -> !table.virtual && table.target_namespace != null && table.target_namespace.contains(namespace_uri) && table.matchesNodeName(text, wild_card)).forEach(table -> {

								String table_xpath = getAbsoluteXPathOfTable(table, ref_path);

								if (table_xpath != null && inc_self)
									_list.add(new XPathExpr(table_xpath, XPathCompType.table));

								table.fields.stream().filter(field -> field.simple_content && !field.simple_attribute && field.target_namespace.contains(PgSchemaUtil.xs_namespace_uri) && field.matchesNodeName(option, text, false, wild_card)).forEach(field -> {

									String simple_content_xpath = getAbsoluteXPathOfTable(table, ref_path);

									if (simple_content_xpath != null && inc_self)
										_list.add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

								});

							});

							int _path_exprs_size;

							for (PgTable table : tables) {

								_path_exprs_size = _list.path_exprs.size();

								table.fields.stream().filter(field -> field.element && field.target_namespace.contains(namespace_uri) && field.matchesNodeName(option, text, false, wild_card)).forEach(field -> {

									String element_xpath = getAbsoluteXPathOfElement(table, ref_path, field.xname);

									if (element_xpath != null && inc_self)
										_list.add(new XPathExpr(element_xpath, XPathCompType.element));

								});

								if (table.has_any && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

									table.fields.stream().filter(field -> field.any && field.target_namespace.contains(namespace_uri)).forEach(field -> {

										String element_xpath = getAbsoluteXPathOfElement(table, ref_path, text);

										if (element_xpath != null && inc_self)
											_list.add(new XPathExpr(element_xpath, XPathCompType.any_element));

									});

								}

							}

						}

					}

					// specified table

					else {

						PgTable table = getTable(path_expr);

						if (table == null) {

							XPathComp prev_comp = previousOf(comp);

							try {

								if (prev_comp != null)
									throw new PgSchemaException(comp.tree, prev_comp.tree);
								else
									throw new PgSchemaException(comp.tree);

							} catch (PgSchemaException e) {
								e.printStackTrace();
							}

						}

						// check current element

						int _path_exprs_size = _list.path_exprs.size();

						table.fields.stream().filter(field -> field.element && field.target_namespace.contains(namespace_uri) && field.matchesNodeName(option, text, false, wild_card)).forEach(field -> {

							String element_xpath = field.xname;

							if (element_xpath != null && inc_self)
								_list.add(new XPathExpr(path_expr.path + "/" + element_xpath, XPathCompType.element));

						});

						if (table.has_any && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

							table.fields.stream().filter(field -> field.any && field.target_namespace.contains(namespace_uri)).forEach(field -> {

								String element_xpath = text;

								if (element_xpath != null && inc_self)
									_list.add(new XPathExpr(path_expr.path + "/" + element_xpath, XPathCompType.any_element));

							});

						}

						// check current nested_key

						boolean has_any = false;
						int _touched_size;

						HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

						Integer[] ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
						Integer[] _ft_ids = null;

						while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

							boolean first_nest = _ft_ids == null;
							_touched_size = touched_ft_ids.size();

							for (Integer foreign_table_id : ft_ids) {

								if (!touched_ft_ids.add(foreign_table_id))
									continue;

								PgTable foreign_table = tables.get(foreign_table_id);

								// check foreign table

								if (!foreign_table.virtual && foreign_table.target_namespace != null && foreign_table.target_namespace.contains(namespace_uri) && foreign_table.matchesNodeName(text, wild_card)) {

									String table_xpath = getAbsoluteXPathOfTable(foreign_table, ref_path);

									if (table_xpath != null && (inc_self || _ft_ids == null))
										_list.add(new XPathExpr(table_xpath, XPathCompType.table));

									if (foreign_table.fields.parallelStream().anyMatch(field -> field.simple_content && !field.simple_attribute && field.target_namespace.contains(PgSchemaUtil.xs_namespace_uri) && field.matchesNodeName(option, text, false, wild_card))) {

										String simple_content_xpath = getAbsoluteXPathOfTable(foreign_table, ref_path);

										if (simple_content_xpath != null && (inc_self || first_nest))
											_list.add(new XPathExpr(simple_content_xpath, XPathCompType.simple_content));

									}

								}

								// check foreign element

								foreign_table.fields.stream().filter(field -> field.element && field.target_namespace.contains(namespace_uri) && field.matchesNodeName(option, text, false, wild_card)).forEach(field -> {

									String element_xpath = getAbsoluteXPathOfElement(foreign_table, ref_path, field.xname);

									if (element_xpath != null && (inc_self || first_nest))
										_list.add(new XPathExpr(element_xpath, XPathCompType.element));

								});

								if (foreign_table.has_any)
									has_any = true;

								// check foreign nested_key

								if (foreign_table.virtual || !abs_path) {

									Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

									if (__ft_ids != null && __ft_ids.length > 0)
										_ft_ids = __ft_ids;

								}

							}

							ft_ids = _ft_ids;

							if (touched_ft_ids.size() == _touched_size)
								break;

						}

						touched_ft_ids.clear();

						if (has_any && _path_exprs_size == _list.path_exprs.size()) {

							ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
							_ft_ids = null;

							while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

								boolean first_nest = _ft_ids == null;
								_touched_size = touched_ft_ids.size();

								for (Integer foreign_table_id : ft_ids) {

									if (!touched_ft_ids.add(foreign_table_id))
										continue;

									PgTable foreign_table = schema.getTable(foreign_table_id);

									if (foreign_table.has_any && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

										foreign_table.fields.stream().filter(field -> field.any && field.target_namespace.contains(namespace_uri)).forEach(field -> {

											String element_xpath = getAbsoluteXPathOfElement(foreign_table, ref_path, text);

											if (element_xpath != null && (inc_self || first_nest))
												_list.add(new XPathExpr(element_xpath, XPathCompType.any_element));

										});

									}

									// check foreign nested_key

									if (foreign_table.virtual || !abs_path) {

										Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

										if (__ft_ids != null && __ft_ids.length > 0)
											_ft_ids = __ft_ids;

									}

								}

								ft_ids = _ft_ids;

								if (touched_ft_ids.size() == _touched_size)
									break;

							}

							touched_ft_ids.clear();

						}

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

		}

	}

	/**
	 * Test NameTestContext node having attribute axis.
	 *
	 * @param comp current XPath component
	 * @param namespace_uri namespace URI of current QName
	 * @param local_part local part of current QName
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContextWithAttributeAxis(XPathComp comp, String namespace_uri, String local_part, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : comp.tree.getText();

		boolean init_path = path_exprs.isEmpty();

		// first NameTestContext node

		if (init_path) {

			if (isAbsolutePath(comp.union_id))
				throw new PgSchemaException(comp.tree);

			else {

				int _path_exprs_size;

				for (PgTable table : tables) {

					_path_exprs_size = path_exprs.size();

					table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond) && field.target_namespace.contains(namespace_uri) && field.matchesNodeName(option, text, true, wild_card)).forEach(field -> {

						String attribute_xpath = getAbsoluteXPathOfAttribute(table, field.attribute ? field.xname : field.foreign_table_xname, null);

						if (attribute_xpath != null)
							add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

					});

					if (table.has_any_attribute && (wild_card || _path_exprs_size == path_exprs.size())) {

						table.fields.stream().filter(field -> field.any_attribute && field.target_namespace.contains(namespace_uri)).forEach(field -> {

							String attribute_xpath = getAbsoluteXPathOfAttribute(table, text, null);

							if (attribute_xpath != null)
								add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

						});

					}

				}

				if (path_exprs.size() == 0 && !predicate)
					throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

			}

		}

		// succeeding NameTestContext node

		else {

			boolean abs_location_path = isAbsolutePath(comp.union_id);

			XPathCompList rep_list = new XPathCompList();

			path_exprs.forEach(path_expr -> {

				String ref_path = path_expr.path;

				XPathCompList _list = new XPathCompList();

				if (path_expr.terminus.equals(XPathCompType.any_element))
					_list.add(new XPathExpr(path_expr.path + " @" + text, XPathCompType.any_attribute));

				else {

					String cur_table = getLastNameOfPath(path_expr.path);

					// not specified table

					if (cur_table == null) {

						if (abs_location_path) {

							int _path_exprs_size;

							for (PgTable table : tables) {

								_path_exprs_size = _list.path_exprs.size();

								table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond) && field.target_namespace.contains(namespace_uri) && field.matchesNodeName(option, text, true, wild_card)).forEach(field -> {

									String attribute_xpath = getAbsoluteXPathOfAttribute(table, ref_path, field.attribute ? field.xname : field.foreign_table_xname);

									if (attribute_xpath != null)
										_list.add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

								});

								if (table.has_any_attribute && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

									table.fields.stream().filter(field -> field.any_attribute && field.target_namespace.contains(namespace_uri)).forEach(field -> {

										String attribute_xpath = getAbsoluteXPathOfAttribute(table, ref_path, text);

										if (attribute_xpath != null)
											_list.add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

									});

								}

							}

						}

					}

					// specified table

					else {

						PgTable table = getTable(path_expr);

						if (table == null) {

							XPathComp prev_comp = previousOf(comp);

							try {

								if (prev_comp != null)
									throw new PgSchemaException(comp.tree, prev_comp.tree);
								else
									throw new PgSchemaException(comp.tree);

							} catch (PgSchemaException e) {
								e.printStackTrace();
							}

						}

						// check current attribute

						int _path_exprs_size = _list.path_exprs.size();

						table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond) && field.target_namespace.contains(namespace_uri) && field.matchesNodeName(option, text, true, wild_card)).forEach(field -> {

							String attribute_xpath = "@" + (field.attribute ? field.xname : field.foreign_table_xname);

							if (attribute_xpath != null)
								_list.add(new XPathExpr(path_expr.path + "/" + attribute_xpath, XPathCompType.attribute));

						});

						if (table.has_any_attribute && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

							table.fields.stream().filter(field -> field.any_attribute && field.target_namespace.contains(namespace_uri)).forEach(field -> {

								String attribute_xpath = "@" + text;

								if (attribute_xpath != null)
									_list.add(new XPathExpr(path_expr.path + "/" + attribute_xpath, XPathCompType.any_attribute));

							});

						}

						// check current nested_key

						boolean has_any_attribute = false;
						int _touched_size;

						HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

						Integer[] ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
						Integer[] _ft_ids = null;

						while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

							_touched_size = touched_ft_ids.size();

							for (Integer foreign_table_id : ft_ids) {

								if (!touched_ft_ids.add(foreign_table_id))
									continue;

								PgTable foreign_table = tables.get(foreign_table_id);

								if (!foreign_table.virtual && foreign_table.content_holder && !path_expr.path.contains(foreign_table.xname))
									continue;

								// check foreign attribute

								foreign_table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond) && field.target_namespace.contains(namespace_uri) && field.matchesNodeName(option, text, true, wild_card)).forEach(field -> {

									String attribute_xpath = getAbsoluteXPathOfAttribute(foreign_table, ref_path, field.attribute ? field.xname : field.foreign_table_xname);

									if (attribute_xpath != null)
										_list.add(new XPathExpr(attribute_xpath, XPathCompType.attribute));

								});

								if (foreign_table.has_any_attribute)
									has_any_attribute = true;

								// check foreign nested_key

								if (foreign_table.virtual || foreign_table.has_nested_key_as_attr || !abs_location_path) {

									Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

									if (__ft_ids != null && __ft_ids.length > 0)
										_ft_ids = __ft_ids;

								}

							}

							ft_ids = _ft_ids;

							if (touched_ft_ids.size() == _touched_size)
								break;

						}

						touched_ft_ids.clear();

						if (has_any_attribute && (_path_exprs_size == _list.path_exprs.size())) {

							ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
							_ft_ids = null;

							while (ft_ids != null && ft_ids.length > 0 && _list.path_exprs.size() == 0) {

								_touched_size = touched_ft_ids.size();

								for (Integer foreign_table_id : ft_ids) {

									if (!touched_ft_ids.add(foreign_table_id))
										continue;

									PgTable foreign_table = tables.get(foreign_table_id);

									if (!foreign_table.virtual && foreign_table.content_holder && !path_expr.path.contains(foreign_table.xname))
										continue;

									if (foreign_table.has_any_attribute && (wild_card || _path_exprs_size == _list.path_exprs.size())) {

										foreign_table.fields.stream().filter(field -> field.any_attribute && field.target_namespace.contains(namespace_uri)).forEach(field -> {

											String attribute_xpath = getAbsoluteXPathOfAttribute(foreign_table, ref_path, text);

											if (attribute_xpath != null)
												_list.add(new XPathExpr(attribute_xpath, XPathCompType.any_attribute));

										});

									}

									// check foreign nested_key

									if (foreign_table.virtual || foreign_table.has_nested_key_as_attr || !abs_location_path) {

										Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

										if (__ft_ids != null && __ft_ids.length > 0)
											_ft_ids = __ft_ids;

									}

								}

								ft_ids = _ft_ids;

								if (touched_ft_ids.size() == _touched_size)
									break;

							}

							touched_ft_ids.clear();

						}

					}

				}

				rep_list.addAll(_list);
				_list.clearPathExprs();

			});

			replacePathExprs(rep_list);

			if (rep_list.path_exprs.size() > 0)
				rep_list.clearPathExprs();

			else if (!predicate)
				throw new PgSchemaException(comp.tree, wild_card, composite_text, def_schema_location);

		}

	}

	/**
	 * Test NodeTestContext node having ancestor axis.
	 *
	 * @param comp current XPath component
	 * @param inc_self whether to include self node
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNodeTestContextWithAncestorAxis(XPathComp comp, boolean inc_self, boolean predicate) throws PgSchemaException {

		List<XPathExpr> _path_exprs = new ArrayList<XPathExpr>();

		try {

			if (inc_self)
				_path_exprs.addAll(path_exprs);

			while (selectParentPath() > 0)
				_path_exprs.addAll(path_exprs);

			replacePathExprs(_path_exprs);

			if (path_exprs.size() == 0) {

				if (predicate)
					return;

				throw new PgSchemaException(comp.tree, previousOf(comp).tree);

			}

		} finally {
			_path_exprs.clear();
		}

	}

	/**
	 * Test NameTestContext node having parent axis.
	 *
	 * @param comp current XPath component
	 * @param namespace_uri namespace URI of current QName
	 * @param local_part local part of current QName
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContextWithParentAxis(XPathComp comp, String namespace_uri, String local_part, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		if (selectParentPath() == 0) {

			if (predicate)
				return;

			throw new PgSchemaException(comp.tree, previousOf(comp).tree);

		}

		testNameTestContext(comp, namespace_uri, local_part, wild_card, composite_text, predicate);

	}

	/**
	 * Test NameTestContext node having ancestor axis.
	 *
	 * @param comp current XPath component
	 * @param namespace_uri namespace URI of current QName
	 * @param local_part local part of current QName
	 * @param inc_self whether to include self node
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContextWithAncestorAxis(XPathComp comp, String namespace_uri, String local_part, boolean inc_self, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		List<XPathExpr> _path_exprs = new ArrayList<XPathExpr>();

		try {

			if (inc_self)
				_path_exprs.addAll(path_exprs);

			while (selectParentPath() > 0)
				_path_exprs.addAll(path_exprs);

			replacePathExprs(_path_exprs);

			testNameTestContext(comp, namespace_uri, local_part, wild_card, composite_text, predicate);

		} finally {
			_path_exprs.clear();
		}

	}

	/**
	 * Test NameTestContext node.
	 *
	 * @param comp current XPath component
	 * @param namespace_uri namespace URI of current QName
	 * @param local_part local part of current QName
	 * @param wild_card whether wild card follows
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContext(XPathComp comp, String namespace_uri, String local_part, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : local_part;

		Iterator<XPathExpr> iter = path_exprs.iterator();

		XPathExpr path_expr;
		String[] name;
		int len;

		while (iter.hasNext()) {

			path_expr = iter.next();

			name = path_expr.path.split("/");

			len = name.length;

			PgTable table;

			switch (path_expr.terminus) {
			case table:
				if (len < 1)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				table = getTable(path_expr);

				if (table == null)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				if (table.target_namespace == null || !table.target_namespace.equals(namespace_uri) || !table.matchesNodeName(text, wild_card))
					iter.remove();
				break;
			case element:
			case simple_content:
			case attribute:
			case any_element:
			case any_attribute:
				if (len < 2)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				table = getParentTable(path_expr);

				if (table == null)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				String field_xname;

				switch (path_expr.terminus) {
				case any_element:
					field_xname = PgSchemaUtil.any_name;
					break;
				case any_attribute:
					field_xname = PgSchemaUtil.any_attribute_name;
					break;
				default:
					field_xname = name[len - 1];
				}

				PgField field = table.getCanField(field_xname);

				if (field != null) {

					if (!field.target_namespace.equals(namespace_uri) || (!path_expr.terminus.equals(XPathCompType.any_element) && !path_expr.terminus.equals(XPathCompType.any_attribute) && !field.matchesNodeName(option, text, path_expr.terminus.equals(XPathCompType.attribute), wild_card)))
						iter.remove();

					continue;
				}

				HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

				Integer[] ft_ids = table.fields.stream().filter(_field -> _field.nested_key).map(_field -> _field.foreign_table_id).toArray(Integer[]::new);
				Integer[] _ft_ids = null;

				boolean found_field = false;

				int _touched_size;

				PgTable foreign_table;
				PgField foreign_field;

				while (ft_ids != null && ft_ids.length > 0 && !found_field) {

					_touched_size = touched_ft_ids.size();

					for (Integer foreign_table_id : ft_ids) {

						if (!touched_ft_ids.add(foreign_table_id))
							continue;

						foreign_table = schema.getTable(foreign_table_id);
						foreign_field = foreign_table.getCanField(field_xname);

						if (foreign_field != null) {

							found_field = true;

							if (!foreign_field.target_namespace.equals(namespace_uri) || (!path_expr.terminus.equals(XPathCompType.any_element) && !path_expr.terminus.equals(XPathCompType.any_attribute) && !foreign_field.matchesNodeName(option, text, path_expr.terminus.equals(XPathCompType.attribute), wild_card)))
								iter.remove();

							break;
						}

						if (foreign_table.virtual && !found_field) {

							Integer[] __ft_ids = foreign_table.fields.stream().filter(_field -> _field.nested_key).map(_field -> _field.foreign_table_id).toArray(Integer[]::new);

							if (__ft_ids != null && __ft_ids.length > 0)
								_ft_ids = __ft_ids;

						}

					}

					ft_ids = _ft_ids;

					if (touched_ft_ids.size() == _touched_size)
						break;

				}

				touched_ft_ids.clear();

				if (!found_field)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				break;
			default:
				throw new PgSchemaException(comp.tree, previousOf(comp).tree);
			}

		}

		if (path_exprs.size() == 0) {

			if (predicate)
				return;

			throw new PgSchemaException(comp.tree, previousOf(comp).tree);

		}

	}

	/**
	 * Test PredicateContext node.
	 *
	 * @param comp current XPath component
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testPredicateContext(XPathComp comp) throws PgSchemaException {

		if (pred_exprs == null)
			pred_exprs = new ArrayList<XPathPredExpr>();

		int pred_size = pred_exprs.size();

		XPathCompList pred_list = new XPathCompList(schema, comp.tree, variables);

		int path_expr_size = pred_list.sizeOfPathExpr();

		XPathComp[] union_comps, pred_comps;
		XPathPredExpr predicate;

		Class<?> anyClass;

		for (XPathExpr path_expr : path_exprs) {

			// no path expression in predicate

			if (path_expr_size == 0)
				pred_exprs.add(new XPathPredExpr(comp, path_expr, -1));

			// validate path expression with schema

			for (int union_id = 0; union_id <= pred_list.getLastUnionId(); union_id++) {

				union_comps = pred_list.arrayOf(union_id);

				if (union_comps.length == 0) // no path expression
					continue;

				predicate = new XPathPredExpr(comp, path_expr, union_id);

				pred_list.replacePathExprs(predicate);

				for (int step_id = 0; step_id <= pred_list.getLastStepId(union_id); step_id++) {

					pred_comps = pred_list.arrayOf(union_id, step_id);

					for (XPathComp pred_comp : pred_comps) {

						anyClass = pred_comp.tree.getClass();

						// TerminalNodeImpl node

						if (anyClass.equals(TerminalNodeImpl.class))
							pred_list.testTerminalNodeImpl(pred_comp, true);

						// AbbreviatedStepContext node

						else if (anyClass.equals(AbbreviatedStepContext.class))
							pred_list.testAbbreviateStepContext(pred_comp, true);

						// AxisSpecifierContext node

						else if (anyClass.equals(AxisSpecifierContext.class))
							pred_list.testAxisSpecifierContext(pred_comp, pred_comps);

						// NCNameContext node

						else if (anyClass.equals(NCNameContext.class))
							pred_list.testNCNameContext(pred_comp, pred_comps, true);

						// NodeTestContext node

						else if (anyClass.equals(NodeTestContext.class))
							pred_list.testNodeTestContext(pred_comp, pred_comps, true);

						// NameTestContext node

						else if (anyClass.equals(NameTestContext.class))
							pred_list.testNameTestContext(pred_comp, pred_comps, true);

						else
							throw new PgSchemaException(pred_comp.tree);

						if (pred_list.path_exprs.isEmpty())
							break;

					}

					if (pred_list.path_exprs.isEmpty())
						break;

				}

				// store valid path expressions in predicate

				if (pred_list.path_exprs.size() > 0) {

					predicate.replaceDstPathExprs(pred_list.path_exprs);
					pred_exprs.add(predicate);

				}

				else
					throw new PgSchemaException(union_comps[0].tree, def_schema_location);

				pred_list.clearPathExprs();

			}

		}

		if (pred_size == pred_exprs.size()) // invalid predicate
			throw new PgSchemaException(comp.tree, def_schema_location);

	}

	/**
	 * Return absolute XPath expression of current table.
	 *
	 * @param table current table
	 * @param ref_path reference node path
	 * @return String absolute XPath expression of current table
	 */
	private String getAbsoluteXPathOfTable(PgTable table, String ref_path) {
		return getAbsoluteXPathOfTable(table, ref_path, false, false, null);
	}

	/**
	 * Return absolute XPath expression of current table.
	 *
	 * @param table current table
	 * @param ref_path reference node path
	 * @param attr whether last node is attribute
	 * @param as_attr whether child nested key as attribute
	 * @param sb StringBuilder to store path
	 * @return String absolute XPath expression of current table
	 */
	private String getAbsoluteXPathOfTable(PgTable table, String ref_path, boolean attr, boolean as_attr, StringBuilder sb) {

		if (sb == null)
			sb = new StringBuilder();

		String table_xname = table.xname;

		if (table.equals(root_table)) {

			sb.append((sb.length() > 0 ? "/" : "") + table_xname);

			String[] path = sb.toString().split("/");

			sb.setLength(0);

			for (int l = path.length - 1; l >= 0; l--)
				sb.append("/" + path[l]);

			try {

				return sb.toString();

			} finally {
				sb.setLength(0);
			}

		}

		if (!table.virtual && !as_attr)
			sb.append((sb.length() > 0 ? "/" : "") + table_xname);

		Optional<PgTable> opt;

		if (attr) {

			opt = tables.parallelStream().filter(foreign_table -> foreign_table.nested_fields > 0 && foreign_table.fields.stream().anyMatch(field -> field.nested_key_as_attr && schema.getTable(field.foreign_table_id).equals(table) && (ref_path == null || (ref_path != null && ((foreign_table.virtual && field.containsParentNodeName(ref_path)) || (!foreign_table.virtual && (foreign_table.has_nested_key_as_attr || ref_path.contains(foreign_table.xname)))))))).findFirst();

			if (opt.isPresent())
				return getAbsoluteXPathOfTable(opt.get(), ref_path, attr, true, sb);

		}

		opt = tables.parallelStream().filter(foreign_table -> foreign_table.nested_fields > 0 && foreign_table.fields.stream().anyMatch(field -> field.nested_key && (as_attr ? !field.nested_key_as_attr : true) && schema.getTable(field.foreign_table_id).equals(table) && (ref_path == null || (ref_path != null && ((foreign_table.virtual && field.containsParentNodeName(ref_path)) || (!foreign_table.virtual && (foreign_table.has_nested_key_as_attr || ref_path.contains(foreign_table.xname)))))))).findFirst();

		return opt.isPresent() ? getAbsoluteXPathOfTable(opt.get(), ref_path, attr, false, sb) : null;
	}

	/**
	 * Return absolute XPath expression of current attribute.
	 *
	 * @param table current table
	 * @param ref_path reference node path
	 * @param text current attribute name
	 * @return String absolute XPath expression of current attribute
	 */
	private String getAbsoluteXPathOfAttribute(PgTable table, String ref_path, String text) {

		StringBuilder sb = new StringBuilder();

		sb.append("@" + text);

		return getAbsoluteXPathOfTable(table, ref_path, true, false, sb);
	}

	/**
	 * Return absolute XPath expression of current element.
	 *
	 * @param table current table
	 * @param ref_path reference node path
	 * @param text current element name
	 * @return String absolute XPath expression of current attribute
	 */
	private String getAbsoluteXPathOfElement(PgTable table, String ref_path, String text) {

		StringBuilder sb = new StringBuilder();

		sb.append(text);

		return getAbsoluteXPathOfTable(table, ref_path, false, false, sb);
	}

	/**
	 * Return table of XPath expression.
	 *
	 * @param path_expr XPath expression
	 * @return PgTable table
	 */
	protected PgTable getTable(XPathExpr path_expr) {

		String table_xname = path_expr.getLastPathName();

		int count = (int) tables.parallelStream().filter(table -> table.xname.equals(table_xname)).count();

		switch (count) {
		case 0:
			return null;
		case 1:
			return tables.parallelStream().filter(table -> table.xname.equals(table_xname)).findFirst().get();
		}

		String path = path_expr.getReadablePath();

		Optional<PgTable> opt = tables.parallelStream().filter(table -> table.xname.equals(table_xname) && getAbsoluteXPathOfTable(table, null).endsWith(path)).findFirst();

		return opt != null ? opt.get() : null;
	}

	/**
	 * Return parent table of XPath expression.
	 *
	 * @param path_expr XPath expression
	 * @return PgTable parent table
	 */
	public PgTable getParentTable(XPathExpr path_expr) {
		return getTable(new XPathExpr(path_expr.getParentPath(), XPathCompType.table));
	}

	/**
	 * Return parent table of XPathSql expression.
	 *
	 * @param sql_expr XPath SQL expression
	 * @return PgTable parent table
	 */
	protected PgTable getParentTable(XPathSqlExpr sql_expr) {
		return getTable(new XPathExpr(sql_expr.getParentPath(), XPathCompType.table));
	}

	/**
	 * Return whether any path ends with table node.
	 *
	 * @return boolean whether any path ends with table node
	 */
	public boolean hasPathEndsWithTableNode() {
		return path_exprs.parallelStream().anyMatch(path_expr -> path_expr.terminus.equals(XPathCompType.table));
	}

	/**
	 * Return whether any path ends with text node.
	 *
	 * @return boolean whether any path ends with text node
	 */
	public boolean hasPathEndsWithTextNode() {
		return path_exprs.parallelStream().anyMatch(path_expr -> path_expr.terminus.isText());
	}

	/**
	 * Return whether any path ends without text node.
	 *
	 * @return boolean whether any path ends without text node
	 */
	private boolean hasPathEndsWithoutTextNode() {
		return path_exprs.parallelStream().anyMatch(path_expr -> !path_expr.terminus.isText());
	}

	/**
	 * Remove any path that ends with table node.
	 *
	 * @return int the number of survived paths
	 */
	private int removePathEndsWithTableNode() {

		path_exprs.removeIf(path_expr -> path_expr.terminus.equals(XPathCompType.table));

		return path_exprs.size();
	}

	/**
	 * Remove any path that ends with field node.
	 *
	 * @return int the number of survived paths
	 *
	private int removePathEndsWithFieldNode() {

		path_exprs.removeIf(path_expr -> path_expr.terminus.isField());

		return path_exprs.size();
	}
	 */
	/**
	 * Remove any path that ends with text node.
	 *
	 * @return int the number of survived paths
	 */
	private int removePathEndsWithTextNode() {

		path_exprs.removeIf(path_expr -> path_expr.terminus.isText());

		return path_exprs.size();
	}

	/**
	 * Remove any path that ends without table node.
	 *
	 * @return int the number of survived paths
	 *
	private int removePathEndsWithoutTableNode() {

		path_exprs.removeIf(path_expr -> !path_expr.terminus.equals(XPathCompType.table));

		return path_exprs.size();
	}
	 */
	/**
	 * Remove any path that ends without field node.
	 *
	 * @return int the number of survived paths
	 *
	private int removePathEndsWithoutFieldNode() {

		path_exprs.removeIf(path_expr -> !path_expr.terminus.isField());

		return path_exprs.size();
	}
	 */
	/**
	 * Remove any path that ends without text node.
	 *
	 * @return int the number of survived paths
	 *
	private int removePathEndsWithoutTextNode() {

		path_exprs.removeIf(path_expr -> !path_expr.terminus.isText());

		return path_exprs.size();
	}
	 */
	/**
	 * Remove any orphan path.
	 *
	 * @param parent_path_exprs list of parental path expressions
	 * @return int the number of survived paths
	 */
	public int removeOrphanPath(List<XPathExpr> parent_path_exprs) {

		Iterator<XPathExpr> iter = path_exprs.iterator();

		while (iter.hasNext()) {

			XPathExpr path_expr = iter.next();

			if (!parent_path_exprs.parallelStream().anyMatch(src_path_expr -> path_expr.path.startsWith(src_path_expr.path)))
				iter.remove();

		}

		return path_exprs.size();
	}

	/**
	 * Remove any duplicate path.
	 *
	 * @return int the number of survived paths
	 */
	public int removeDuplicatePath() {

		if (path_exprs == null || path_exprs.size() == 0)
			return 0;

		Iterator<XPathExpr> iter = path_exprs.iterator();

		boolean unified = false;

		while (iter.hasNext()) {

			XPathExpr _path_expr = iter.next();

			if (path_exprs.parallelStream().filter(path_expr -> path_expr.path.equals(_path_expr.path)).count() > 1) {

				iter.remove();

				unified = true;

				break;
			}

		}

		if (unified)
			return removeDuplicatePath();

		return path_exprs.size();
	}

	/**
	 * Return whether absolute path used.
	 *
	 * @param union_id union id
	 * @return boolean whether absolute path used
	 */
	private boolean isAbsolutePath(int union_id) {

		if (path_exprs.isEmpty()) {

			XPathComp first_comp = comps.parallelStream().filter(comp -> comp.union_id == union_id && comp.step_id == 0).findFirst().get();

			return first_comp.tree.getClass().equals(TerminalNodeImpl.class) && first_comp.tree.getText().equals("/");
		}

		return !path_exprs.get(0).path.endsWith("//");
	}

	/**
	 * Append abbreviation path of all paths.
	 */
	private void appendAbbrevPath() {

		path_exprs.forEach(path_expr -> path_expr.path = path_expr.path + "//");

	}

	/**
	 * Append text node.
	 */
	private void appendTextNode() {

		List<XPathExpr> _path_exprs = new ArrayList<XPathExpr>(path_exprs);

		path_exprs.clear();

		_path_exprs.forEach(path_expr -> {

			switch (path_expr.terminus) {
			case element:
			case simple_content:
			case attribute:
				path_exprs.add(new XPathExpr(path_expr.path + "/" + PgSchemaUtil.text_node_name, XPathCompType.text, path_expr.terminus));
				break;
			case any_element:
			case any_attribute:
				path_exprs.add(new XPathExpr(path_expr.path + " " + PgSchemaUtil.text_node_name, XPathCompType.text, path_expr.terminus));
			default:
				path_exprs.add(path_expr);
			}

		});

		_path_exprs.clear();

	}

	/**
	 * Append comment node.
	 */
	private void appendCommentNode() {

		List<XPathExpr> _path_exprs = new ArrayList<XPathExpr>(path_exprs);

		path_exprs.clear();

		_path_exprs.forEach(path_expr -> {

			switch (path_expr.terminus) {
			case table:
			case element:
			case simple_content:
			case attribute:
				path_exprs.add(new XPathExpr(path_expr.path + "/" + PgSchemaUtil.comment_node_name, XPathCompType.comment, path_expr.terminus));
				break;
			case any_element:
			case any_attribute:
				path_exprs.add(new XPathExpr(path_expr.path + " " + PgSchemaUtil.comment_node_name, XPathCompType.comment, path_expr.terminus));
			default:
				path_exprs.add(path_expr);
			}

		});

		_path_exprs.clear();

	}

	/**
	 * Append processing-instruction node.
	 *
	 * @param expression expression of processing-instruction()
	 */
	private void appendProcessingInstructionNode(String expression) {

		if (path_exprs.isEmpty())
			path_exprs.add(new XPathExpr("/" + expression, XPathCompType.processing_instruction));

		else {

			List<XPathExpr> _path_exprs = new ArrayList<XPathExpr>(path_exprs);

			path_exprs.clear();

			_path_exprs.forEach(path_expr -> {

				switch (path_expr.terminus) {
				case table:
				case element:
				case simple_content:
				case attribute:
					path_exprs.add(new XPathExpr(path_expr.path + "/" + expression, XPathCompType.processing_instruction, path_expr.terminus));
					break;
				case any_element:
				case any_attribute:
					path_exprs.add(new XPathExpr(path_expr.path + " " + expression, XPathCompType.processing_instruction, path_expr.terminus));
					break;
				default:
					path_exprs.add(path_expr);
				}

			});

			_path_exprs.clear();

		}

	}

	/**
	 * Select parent path.
	 *
	 * @return int the number of survived paths
	 */
	public int selectParentPath() {

		path_exprs.forEach(path_expr -> {

			path_expr.path = path_expr.getParentPath();
			path_expr.terminus = path_expr.getParentTerminus();

		});

		path_exprs.removeIf(path_expr -> path_expr.path.isEmpty());

		return path_exprs.size();
	}

	/**
	 * Replace path expressions.
	 *
	 * @param src_list source XPath component list
	 */
	private void replacePathExprs(XPathCompList src_list) {

		replacePathExprs(src_list.path_exprs);

	}

	/**
	 * Replace path expressions.
	 *
	 * @param src_predicate source XPath predicate
	 */
	private void replacePathExprs(XPathPredExpr src_predicate) {

		replacePathExprs(src_predicate.dst_path_exprs);

	}

	/**
	 * Replace path expressions.
	 *
	 * @param src_path_exprs source path expressions
	 */
	private void replacePathExprs(List<XPathExpr> src_path_exprs) {

		path_exprs = new ArrayList<XPathExpr>(src_path_exprs);

	}

	/**
	 * Apply union expression.
	 */
	private void applyUnionExpr() {

		if (!union_expr)
			return;

		path_exprs_union.addAll(path_exprs);

		path_exprs = new ArrayList<XPathExpr>(path_exprs_union);

		path_exprs_union.clear();

		union_expr = false;

	}

	/**
	 * Show path expressions.
	 */
	public void showPathExprs() {

		path_exprs.forEach(path_expr -> System.out.println(path_expr.getReadablePath() + " (terminus type: " + path_expr.terminus.name() + ")"));

	}

	/** Alias name of tables in SQL main query. */
	private HashMap<PgTable, String> main_aliases = null; // key = table, value = alias name

	/** Alias id of tables in SQL sub query. */
	private int sub_alias_id = 0;

	/**
	 * Translate to XPath expression to SQL expression.
	 */
	public void translateToSqlExpr() {

		path_exprs.forEach(path_expr -> {

			String path = path_expr.path;
			XPathCompType terminus = path_expr.terminus;

			switch (terminus) {
			case text:
				path = path_expr.getParentPath();
				terminus = path_expr.getParentTerminus();
			case element:
			case simple_content:
			case attribute:
			case any_element:
			case any_attribute:
				XPathSqlExpr sql_expr = getXPathSqlExprOfPath(path, terminus);
				try {
					path_expr.setSubjectSql(new XPathSqlExpr(schema, path, sql_expr.table, sql_expr.xname, sql_expr.pg_xpath_code, null, terminus));
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
				break;
			case table:
				try {
					path_expr.setSubjectSql(new XPathSqlExpr(schema, path, getTable(path_expr), "*", null, null, terminus));
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
				break;
			default:
				try {
					throw new PgSchemaException("Couldn't retrieve " + path_expr.terminus.name() + " via SQL.");
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
			}

			Set<XPathComp> src_comps = null;

			if (pred_exprs != null && pred_exprs.size() > 0) {

				String _path = path; // finalized

				src_comps = pred_exprs.stream().filter(predicate -> _path.startsWith(predicate.src_path_expr.path)).map(predicate -> predicate.src_comp).collect(Collectors.toSet());

				try {

					for (XPathComp src_comp : src_comps) {

						if (verbose)
							System.out.println("\nReversed abstract syntax tree of predicate: '" + src_comp.tree.getText() + "'");

						testPredicateTree2SqlExpr(src_comp, path_expr);

					}

				} catch (PgSchemaException e) {
					e.printStackTrace();
				}

			}

			StringBuilder sb = new StringBuilder();

			try {

				main_aliases = new HashMap<PgTable, String>();
				sub_alias_id = 0;

				HashMap<PgTable, String> target_tables = new HashMap<PgTable, String>(); // key = table, value = table path
				HashMap<PgTable, String> joined_tables = new HashMap<PgTable, String>();

				target_tables.put(path_expr.sql_subject.table, path_expr.terminus.equals(XPathCompType.table) || path_expr.terminus.equals(XPathCompType.simple_content) ? path_expr.sql_subject.path : path_expr.sql_subject.getParentPath());

				if (path_expr.sql_predicates != null)
					path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.table != null).forEach(sql_expr -> target_tables.put(sql_expr.table, sql_expr.terminus.equals(XPathCompType.table) || sql_expr.terminus.equals(XPathCompType.simple_content) ? sql_expr.path : sql_expr.getParentPath()));

				boolean single = target_tables.size() == 1;

				sb.append("SELECT ");

				appendSqlColumnName(path_expr.sql_subject, sb);

				sb.append(" FROM ");

				if (src_comps != null) {

					// remove subject table from target

					joined_tables.put(path_expr.sql_subject.table, target_tables.get(path_expr.sql_subject.table));

					HashMap<PgTable, String> linking_tables = new HashMap<PgTable, String>();

					LinkedList<LinkedList<PgTable>> linking_orders = new LinkedList<LinkedList<PgTable>>();
					LinkedList<LinkedList<PgTable>> _linking_orders = new LinkedList<LinkedList<PgTable>>();

					// simple type attribute in subject expression

					if (path_expr.terminus.equals(XPathCompType.attribute) && !path_expr.sql_subject.field.attribute) {

						LinkedList<PgTable> linking_order = new LinkedList<PgTable>();

						testJoinClauseForSimpleTypeAttr(path_expr.sql_subject.table, path_expr.sql_subject.path, linking_tables, linking_order);

						if (linking_tables.size() > 0) {

							target_tables.putAll(linking_tables);
							linking_tables.clear();

						}

						linking_orders.add(linking_order);
						_linking_orders.add(new LinkedList<PgTable>(linking_order));

					}

					target_tables.remove(path_expr.sql_subject.table);

					// simple type attribute in predicate expression

					path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.terminus.equals(XPathCompType.attribute) && !sql_expr.field.attribute).forEach(sql_expr -> {

						LinkedList<PgTable> linking_order = new LinkedList<PgTable>();

						testJoinClauseForSimpleTypeAttr(sql_expr.table, sql_expr.path, linking_tables, linking_order);

						if (linking_tables.size() > 0) {

							target_tables.putAll(linking_tables);
							linking_tables.clear();

						}

						linking_orders.add(linking_order);
						_linking_orders.add(new LinkedList<PgTable>(linking_order));

					});

					HashMap<PgTable, String> _target_tables = new HashMap<PgTable, String>(target_tables);
					HashMap<PgTable, String> _joined_tables = new HashMap<PgTable, String>(joined_tables);

					testJoinClause(_target_tables, _joined_tables, linking_tables, _linking_orders);

					if (linking_tables.size() > 0) {

						target_tables.putAll(linking_tables);
						linking_tables.clear();

					}

					_joined_tables.forEach((_table_, _path_) -> appendSqlTable(_table_, sb));

					_joined_tables.clear();

					sb.setLength(sb.length() - 2); // remove last ", "

					sb.append(" WHERE ");

					for (XPathComp src_comp : src_comps) {

						translatePredicateTree2SqlImpl(src_comp, path_expr, sb);
						sb.append(" AND ");

					}

					appendJoinClause(target_tables, joined_tables, linking_orders, sb);

					sb.setLength(sb.length() - 5); // remove last " AND "

					joined_tables.clear();

				}

				else {

					target_tables.forEach((_table_, _path_) -> appendSqlTable(_table_, sb));

					sb.setLength(sb.length() - 2); // remove last ", "

					target_tables.clear();

				}

				main_aliases.clear();

				path_expr.sql = sb.toString();

				if (single) {

					String subject_table_name = schema.getPgNameOf(path_expr.sql_subject.table);

					if (path_expr.sql.contains(subject_table_name + "."))
						path_expr.sql = path_expr.sql.replace(subject_table_name + ".", "");

				}

				if (path_expr.sql.endsWith(" WHERE "))
					path_expr.sql = path_expr.sql.replaceFirst(" WHERE $", "");

				if (sub_alias_id == 0)
					path_expr.sql = path_expr.sql.replaceAll(" as t[0-9]+", "");

			} catch (PgSchemaException e) {
				e.printStackTrace();
			} finally {
				sb.setLength(0);
				if (path_expr.sql_predicates != null)
					path_expr.sql_predicates.clear();
			}

		});

	}

	/**
	 * Append SQL table.
	 *
	 * @param table PostgreSQL table
	 * @param sb StringBuilder to store SQL expression
	 */
	private void appendSqlTable(PgTable table, StringBuilder sb) {

		String alias_name = main_aliases.get(table);

		if (alias_name == null)
			alias_name = "t" + (main_aliases.size() + 1);

		main_aliases.put(table, alias_name);

		sb.append(schema.getPgNameOf(table) + " as " + alias_name + ", ");

	}

	/**
	 * Append SQL column name.
	 *
	 * @param sql_expr SQL expression
	 * @param sb StringBuilder to store SQL expression
	 */
	private void appendSqlColumnName(XPathSqlExpr sql_expr, StringBuilder sb) {

		if (sql_expr.unary_operator != null)
			sb.append(sql_expr.unary_operator);

		switch (sql_expr.terminus) {
		case any_element:
		case any_attribute:
			sb.append(sql_expr.pg_xpath_code);
			break;
		default:
			sb.append(schema.getPgNameOf(sql_expr.table) + "." + (sql_expr.terminus.equals(XPathCompType.table) ? sql_expr.pname : PgSchemaUtil.avoidPgReservedWords(sql_expr.pname)));
		}

	}

	/**
	 * Append SQL column name.
	 *
	 * @param table PostgreSQL table
	 * @param column_name SQL column name
	 * @param sb StringBuilder to store SQL expression
	 */
	private void appendSqlColumnName(PgTable table, String column_name, StringBuilder sb) {

		sb.append(schema.getPgNameOf(table) + "." + PgSchemaUtil.avoidPgReservedWords(column_name));

	}

	/** Path expression counter in predicate. */
	private int path_expr_counter = 0;

	/** Class of child node under PredicateContext node. */
	private Class<?> _predicateContextClass = null;

	/** Whether PredicateContext node has Boolean FunctionCallContext node. */
	private boolean _predicateContextHasBooleanFunc = false;

	/**
	 * Translate predicate expression to SQL expression.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr source path expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testPredicateTree2SqlExpr(XPathComp src_comp, XPathExpr src_path_expr) throws PgSchemaException {

		path_expr_counter = 0;

		if (!testPredicateTree(src_comp, src_path_expr, src_comp.tree, false, " "))
			src_path_expr.sql_predicates.clear();

	}

	/**
	 * Return whether XPath parse tree of predicate is effective.
	 *
	 * @param path_expr current path
	 * @param src_comp XPath component of source predicate
	 * @param tree XPath parse tree
	 * @param has_children whether parent has children
	 * @param indent indent code for output
	 * @return boolean whether valid predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private boolean testPredicateTree(XPathComp src_comp, XPathExpr src_path_expr, ParseTree tree, boolean has_children, String indent) throws PgSchemaException {

		boolean valid = false, _has_children;

		ParseTree child;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class))
				continue;

			_has_children = !child.getText().isEmpty() && (hasEffectiveChildren(child) || hasChildOfTerminalNodeImpl(child));

			if (testPredicateTree(src_comp, src_path_expr, child, _has_children, indent + " ") || _has_children)
				valid = true;

		}

		if (has_children) {

			ParseTree parent = tree.getParent();

			while (!(!parent.getText().isEmpty() && (hasEffectiveChildren(parent) || hasChildOfTerminalNodeImpl(parent))))
				parent = parent.getParent();

			Class<?> parentClass = parent.getClass();

			// PathContext node has already been validated

			if (!isPathContextClass(parentClass)) {

				Class<?> currentClass = tree.getClass();

				// arbitrary PathContext node

				if (isPathContextClass(currentClass))
					testPathContext(src_comp, src_path_expr, parent, tree);

				// PrimaryExprContext node

				else if (currentClass.equals(PrimaryExprContext.class))
					testPrimaryExprContext(src_path_expr, parent, tree);

				// VariableReferenceContext node

				else if (currentClass.equals(VariableReferenceContext.class))
					testVariableReferenceContext(src_path_expr, parent, tree);

				// EqualityExprContext node

				else if (currentClass.equals(EqualityExprContext.class))
					testEqualityExprContext(src_path_expr, parent, tree);

				// RelationalExprContext node

				else if (currentClass.equals(RelationalExprContext.class))
					testRelationalExprContext(src_path_expr, parent, tree);

				// AdditiveExprContext node

				else if (currentClass.equals(AdditiveExprContext.class))
					testAdditiveExprContext(src_path_expr, parent, tree, 0);

				// MultiplicativeExprContext node

				else if (currentClass.equals(MultiplicativeExprContext.class))
					testMultiplicativeExprContext(src_path_expr, parent, tree);

				// UnaryExprNoRootContext node

				else if (currentClass.equals(UnaryExprNoRootContext.class))
					testUnaryExprNoRootContext(src_path_expr, parent, tree);

				// FunctionCallContext node

				else if (currentClass.equals(FunctionCallContext.class))
					testFunctionCallContext(src_path_expr, parent, tree);

				if (parent.getClass().equals(PredicateContext.class)) {

					_predicateContextClass = currentClass;
					_predicateContextHasBooleanFunc = false;

					if (currentClass.equals(FunctionCallContext.class)) {

						String func_name = null;

						for (int i = 0; i < tree.getChildCount(); i++) {

							child = tree.getChild(i);

							if (child.getClass().equals(TerminalNodeImpl.class))
								continue;

							func_name = child.getText();

							break;
						}

						switch (func_name) {
						case "boolean":
						case "not":
						case "true":
						case "false":
						case "lang":
							_predicateContextHasBooleanFunc = true;
						}

					}

				}

				if (verbose)
					System.out.println(indent + tree.getClass().getSimpleName() + " <- " + parent.getClass().getSimpleName() + " '" + tree.getText() + "' " + tree.getSourceInterval().toString());

			}

		}

		return valid;
	}

	/**
	 * Test arbitrary PathContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testPathContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		int pred_path_size = (int) pred_exprs.parallelStream().filter(predicate -> predicate.src_comp.equals(src_comp)).count();

		if (path_expr_counter >= pred_path_size)
			throw new PgSchemaException(tree);

		// designation of predicate

		XPathPredExpr predicate = pred_exprs.stream().filter(_predicate -> _predicate.src_comp.equals(src_comp)).toArray(XPathPredExpr[]::new)[path_expr_counter];

		predicate.dst_path_exprs.forEach(path_expr -> {

			String path = path_expr.path;
			XPathCompType terminus = path_expr.terminus;

			switch (terminus) {
			case text:
				path = path_expr.getParentPath();
				terminus = path_expr.getParentTerminus();
			case element:
			case simple_content:
			case attribute:
			case any_element:
			case any_attribute:
				XPathSqlExpr sql_expr = getXPathSqlExprOfPath(path, terminus);

				try {
					src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, path, sql_expr.table, sql_expr.xname, sql_expr.pg_xpath_code, null, terminus, parent, tree));
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
				break;
			case table:
				try {
					src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, path, getTable(path_expr), "*", null, null, terminus, parent, tree));
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
				break;
			default:
				try {
					throw new PgSchemaException("Couldn't retrieve " + path_expr.terminus.name() + " via SQL.");
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}

			}

		});

		path_expr_counter++;

	}

	/**
	 * Test PrimaryExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testPrimaryExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, tree.getText(), XPathCompType.text, parent, tree));

	}

	/**
	 * Test VariableReferenceContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testVariableReferenceContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		if (variables == null)
			throw new PgSchemaException(tree);

		String var_name = null;

		ParseTree child;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class))
				continue;

			var_name = child.getText();

			break;
		}

		String value = variables.get(var_name);

		if (value == null)
			throw new PgSchemaException(tree);

		src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, value, XPathCompType.text, parent, tree));

	}

	/**
	 * Test EqualityExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testEqualityExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		if (sql_predicates.size() != 2) {

			trimAnySqlPredicate(src_path_expr, sql_predicates);

			if (sql_predicates.size() != 2)
				throw new PgSchemaException(tree);
		}

		String terminal_code = getTextOfChildTerminalNodeImpl(tree);

		boolean equality = terminal_code.equals("=");

		int predicate_count = sizeOfPredicate(sql_predicates);

		int first_sql_id;

		XPathSqlExpr sql_expr_1 = sql_predicates.get(0);
		XPathSqlExpr sql_expr_2 = sql_predicates.get(1);

		switch (predicate_count) {
		case 2:
			boolean contains_space = sql_expr_1.predicate.contains(" ") || sql_expr_2.predicate.contains(" ");

			if (contains_space)
				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, "( " + sql_expr_1.predicate + " " + terminal_code + " " + sql_expr_2.predicate + " )", XPathCompType.text, parent, tree));

			else {

				boolean _equality = sql_expr_1.predicate.equals(sql_expr_2.predicate);

				if (equality != _equality)
					throw new PgSchemaException(tree);

			}

			src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			break;
		case 1:
			first_sql_id = sql_expr_1.predicate == null || sql_expr_2.predicate != null ? 0 : 1;

			XPathSqlExpr sql_relation = sql_predicates.get(first_sql_id);
			XPathSqlExpr sql_predicate = sql_predicates.get((first_sql_id + 1) % 2);

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, sql_relation.path, sql_relation.table, sql_relation.xname, sql_relation.pg_xpath_code, sql_predicate.predicate, sql_relation.terminus, parent, tree, null, terminal_code));

			src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			break;
		case 0:
			if (sql_expr_1.equalsRelationally(sql_expr_2)) {

				if (!equality)
					throw new PgSchemaException(tree);

				src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			}
			break;
		}

	}

	/**
	 * Test RelationalExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testRelationalExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		if (sql_predicates.size() != 2) {

			trimAnySqlPredicate(src_path_expr, sql_predicates);

			if (sql_predicates.size() != 2)
				throw new PgSchemaException(tree);
		}

		String terminal_code = getTextOfChildTerminalNodeImpl(tree);

		int predicate_count = sizeOfPredicate(sql_predicates);

		int first_sql_id;

		XPathSqlExpr sql_expr_1 = sql_predicates.get(0);
		XPathSqlExpr sql_expr_2 = sql_predicates.get(1);

		switch (predicate_count) {
		case 2:
			try {

				BigDecimal value1 = new BigDecimal(sql_expr_1.predicate);
				BigDecimal value2 = new BigDecimal(sql_expr_2.predicate);

				int comp = value1.compareTo(value2);

				switch (terminal_code) {
				case "<":
					if (comp != -1)
						throw new PgSchemaException(tree);
					break;
				case ">":
					if (comp != 1)
						throw new PgSchemaException(tree);
					break;
				case "<=":
					if (comp == 1)
						throw new PgSchemaException(tree);
					break;
				case ">=":
					if (comp != -1)
						throw new PgSchemaException(tree);
					break;
				default:
					throw new PgSchemaException(tree);
				}

			} catch (NumberFormatException e) {
				throw new PgSchemaException(tree);
			}

			src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			break;
		case 1:
			first_sql_id = sql_expr_1.predicate == null || sql_expr_2.predicate != null ? 0 : 1;

			XPathSqlExpr sql_relation = sql_predicates.get(first_sql_id);
			XPathSqlExpr sql_predicate = sql_predicates.get((first_sql_id + 1) % 2);

			if (first_sql_id != 0) {

				switch (terminal_code) {
				case "<":
					terminal_code = ">";
					break;
				case ">":
					terminal_code = "<";
					break;
				case "<=":
					terminal_code = ">=";
					break;
				case ">=":
					terminal_code = "<=";
					break;
				default:
					throw new PgSchemaException(tree);
				}

			}

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, sql_relation.path, sql_relation.table, sql_relation.xname, sql_relation.pg_xpath_code, sql_predicate.predicate, sql_relation.terminus, parent, tree, null, terminal_code));

			src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			break;
		case 0:
			if (sql_expr_1.equalsRelationally(sql_expr_2)) {

				switch (terminal_code) {
				case "<=":
				case ">=":
					break;
				default:
					throw new PgSchemaException(tree);
				}

				src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			}
			break;
		}

	}

	/**
	 * Test AdditiveExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param offset offset id
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testAdditiveExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, int offset) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		trimAnySqlPredicate(src_path_expr, sql_predicates);

		int start_id = startIdOfSuccessivePredicate(sql_predicates, offset);

		if (start_id < 0)
			return;

		int end_id = endIdOfSuccessivePredicate(sql_predicates, start_id);

		if (end_id < 0)
			throw new PgSchemaException(tree);

		String[] terminal_codes = getTextArrayOfChildTerminalNodeImpl(tree);

		BigDecimal result = null, value;
		XPathSqlExpr sql_expr;

		for (int expr_id = start_id; expr_id < end_id; expr_id++) {

			if (result == null) {

				result = new BigDecimal(sql_predicates.get(expr_id).predicate);

				continue;
			}

			sql_expr = sql_predicates.get(expr_id);

			if (sql_expr.predicate.equals("0"))
				continue;

			value = new BigDecimal(sql_expr.predicate);

			switch (terminal_codes[expr_id - 1]) {
			case "+":
				result = result.add(value);
				break;
			case "-":
				result = result.subtract(value);
				break;
			default:
				throw new PgSchemaException(tree);
			}

		}

		// withdraw all successive predicates by calculated one

		if (end_id - start_id == sql_predicates.size()) {

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, result.toString(), XPathCompType.text, parent, tree));

			src_path_expr.sql_predicates.removeIf(_sql_expr -> _sql_expr.parent_tree.equals(tree));

		}

		// otherwise, reduce successive predicates

		else {

			// store calculated value to the starting predicate

			src_path_expr.sql_predicates.get(start_id).predicate = result.toString();

			// set zero to the other predicates

			for (int expr_id = start_id + 1; expr_id < end_id; expr_id++)
				src_path_expr.sql_predicates.get(expr_id).predicate = "0";

			// find next successive predicates

			testAdditiveExprContext(src_path_expr, parent, tree, end_id);

		}

	}

	/**
	 * Test MultiplicativeExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testMultiplicativeExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		trimAnySqlPredicate(src_path_expr, sql_predicates);

		int pred_size = sql_predicates.size();

		if (pred_size < 2)
			return;

		if (pred_size != 2)
			throw new PgSchemaException(tree);

		String terminal_code = getTextOfChildTerminalNodeImpl(tree);

		int predicate_count = sizeOfPredicate(sql_predicates);

		XPathSqlExpr sql_expr_1 = sql_predicates.get(0);
		XPathSqlExpr sql_expr_2 = sql_predicates.get(1);

		switch (predicate_count) {
		case 2:
			try {

				BigDecimal value1 = new BigDecimal(sql_expr_1.predicate);
				BigDecimal value2 = new BigDecimal(sql_expr_2.predicate);

				switch (terminal_code) {
				case "*":
					value1 = value1.multiply(value2);
					break;
				case "div":
					value1 = value1.divide(value2);
					break;
				case "mod":
					value1 = value1.remainder(value2);
					break;
				default:
					throw new PgSchemaException(tree);
				}

				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, value1.toString(), XPathCompType.text, parent, tree));

				src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			} catch (NumberFormatException e) {
				throw new PgSchemaException(tree);
			}
			break;
		default:
		}

	}

	/**
	 * Test UnaryExprNoRootContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testUnaryExprNoRootContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		trimAnySqlPredicate(src_path_expr, sql_predicates);

		if (sql_predicates.size() != 1)
			throw new PgSchemaException(tree);

		String terminal_code = getTextOfChildTerminalNodeImpl(tree);

		int predicate_count = sizeOfPredicate(sql_predicates);

		XPathSqlExpr sql_expr = sql_predicates.get(0);

		switch (predicate_count) {
		case 1:
			try {

				BigDecimal result = null;

				switch (terminal_code) {
				case "-":
					result = new BigDecimal(sql_expr.predicate).negate();
					break;
				default:
					throw new PgSchemaException(tree);
				}

				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, result.toString(), XPathCompType.text, parent, tree));

				src_path_expr.sql_predicates.removeIf(_sql_expr -> _sql_expr.parent_tree.equals(tree));

			} catch (NumberFormatException e) {
				throw new PgSchemaException(tree);
			}
			break;
		default:
			switch (terminal_code) {
			case "-":
				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, sql_expr.path, sql_expr.table, sql_expr.xname, sql_expr.pg_xpath_code, null, sql_expr.terminus, parent, tree, terminal_code, null));

				src_path_expr.sql_predicates.removeIf(_sql_expr -> _sql_expr.parent_tree.equals(tree));
				break;
			default:
				throw new PgSchemaException(tree);
			}

		}

	}

	/**
	 * Test FunctionCallContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testFunctionCallContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = null;

		if (src_path_expr.sql_predicates != null && src_path_expr.sql_predicates.parallelStream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).count() > 0)
			sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		String func_name = null;

		ParseTree child;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class))
				continue;

			func_name = child.getText();

			break;
		}

		switch (func_name) {
		case "last":
		case "position":
		case "count":
		case "id":
		case "local-name":
		case "namespace-uri":
		case "name":
			if (testNodeSetFunctionCallContext(src_path_expr, parent, tree, func_name, sql_predicates))
				return;
			break;
		case "string":
		case "concat":
		case "starts-with":
		case "contains":
		case "substring-before":
		case "substring-after":
		case "substring":
		case "string-length":
		case "normalize-space":
		case "translate":
			if (testStringFunctionCallContext(src_path_expr, parent, tree, func_name, sql_predicates))
				return;
			break;
		case "boolean":
		case "not":
		case "true":
		case "false":
		case "lang":
			if (testBooleanFunctionCallContext(src_path_expr, parent, tree, func_name, sql_predicates))
				return;
			break;
		case "number":
		case "sum":
		case "floor":
		case "ceiling":
		case "round":
			if (testNumberFunctionCallContext(src_path_expr, parent, tree, func_name, sql_predicates))
				return;
			break;
		default:
			throw new PgSchemaException(tree);
		}

		src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

	}

	/**
	 * Test NodeSetFunctionCallContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param func_name function name
	 * @param sql_predicates list of SQL expression of current node
	 * @return boolean whether to delegate to other function
	 * @throws PgSchemaException the pg schema exception
	 */
	private boolean testNodeSetFunctionCallContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, String func_name, List<XPathSqlExpr> sql_predicates) throws PgSchemaException {

		int pred_size = sql_predicates != null ? sql_predicates.size() : 0;

		StringBuilder sb = new StringBuilder();

		try {

			XPathSqlExpr sql_expr;

			PgTable src_table = getTable(src_path_expr);
			String alias_name;

			switch (func_name) {
			case "last":
				if (!serial_key) {
					try {
						throw new PgSchemaException(tree, "serial key", serial_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}

				if (!document_key) {
					try {
						throw new PgSchemaException(tree, "document key", document_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}

				sb.append("( SELECT max( ");

				appendSqlColumnName(src_table, serial_key_name, sb);

				sb.append(" ) FROM ");

				alias_name = "s" + (++sub_alias_id);

				sb.append(schema.getPgNameOf(src_table) + " as " + alias_name);

				sb.append(" WHERE " + alias_name + "." + PgSchemaUtil.avoidPgReservedWords(schema.getDocKeyName(src_table)) + " = t1." + PgSchemaUtil.avoidPgReservedWords(schema.getDocKeyName(main_aliases.entrySet().parallelStream().filter(entry -> entry.getValue().equals("t1")).findFirst().get().getKey())));

				sb.append(" )");

				break;
			case "position":
				if (!serial_key) {
					try {
						throw new PgSchemaException(tree, "serial key", serial_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}

				appendSqlColumnName(src_table, serial_key_name, sb);
				break;
			case "count":
				if (pred_size != 1)
					throw new PgSchemaException(tree);

				sql_expr = sql_predicates.get(0);

				if (sql_expr.predicate != null)
					throw new PgSchemaException(tree);

				if (!document_key) {
					try {
						throw new PgSchemaException(tree, "document key", document_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}

				sb.append("( SELECT count( ");

				appendSqlColumnName(sql_expr, sb);

				sb.append(" ) FROM ");

				alias_name = "s" + (++sub_alias_id);

				sb.append(schema.getPgNameOf(src_table) + " as " + alias_name);

				sb.append(" WHERE " + alias_name + "." + PgSchemaUtil.avoidPgReservedWords(schema.getDocKeyName(sql_expr.table)) + " = t1." + PgSchemaUtil.avoidPgReservedWords(schema.getDocKeyName(main_aliases.entrySet().parallelStream().filter(entry -> entry.getValue().equals("t1")).findFirst().get().getKey())));

				sb.append(" )");
				break;
			case "id":
				if (pred_size != 1)
					throw new PgSchemaException(tree);

				sql_expr = sql_predicates.get(0);

				if (sql_expr.predicate != null)
					sb.append(sql_expr.predicate);

				else
					appendSqlColumnName(sql_expr, sb);
				break;
			case "local-name":
				if (pred_size > 1)
					throw new PgSchemaException(tree);

				if (sql_predicates == null)
					sb.append("''");

				else {

					sql_expr = sql_predicates.get(0);

					if (sql_expr.predicate != null)
						sb.append(sql_expr.predicate);

					else {

						switch (sql_expr.terminus) {
						case table:
							sb.append("'" + sql_expr.table.pname + "'");
							break;
						case element:
						case simple_content:
						case attribute:
							sb.append("'" + sql_expr.pname + "'");
							break;
						case any_element:
							sb.append("'" + getLastNameOfPath(sql_expr.getXPathFragment()) + "'");
							break;
						case any_attribute:
							sb.append("'" + getLastNameOfPath(sql_expr.getXPathFragment()).replaceFirst("^@", "") + "'");
							break;
						default:
							throw new PgSchemaException(tree);
						}

					}

				}
				break;
			case "namespace-uri":
				if (pred_size > 1)
					throw new PgSchemaException(tree);

				if (sql_predicates == null)
					sb.append("''");

				else {

					sql_expr = sql_predicates.get(0);

					if (sql_expr.predicate != null)
						sb.append(sql_expr.predicate);

					else {

						PgTable table;

						switch (sql_expr.terminus) {
						case table:
							table = sql_expr.table;

							sb.append("'" + table.target_namespace != null ? table.target_namespace.split(" ")[0] : "" + "'");
							break;
						case element:
						case simple_content:
						case attribute:
						case any_element:
						case any_attribute:
							table = getParentTable(sql_expr);
							PgField field = table.getCanField(sql_expr.xname);

							sb.append("'" + field.target_namespace != null ? field.target_namespace.split(" ")[0] : "" + "'");
							break;
						default:
							throw new PgSchemaException(tree);
						}

					}

				}
				break;
			case "name":
				if (pred_size > 1)
					throw new PgSchemaException(tree);

				if (sql_predicates == null)
					sb.append("''");

				else {

					sql_expr = sql_predicates.get(0);

					if (sql_expr.predicate != null)
						sb.append(sql_expr.predicate);

					else {

						PgTable table;
						String prefix;

						switch (sql_expr.terminus) {
						case table:
							table = sql_expr.table;
							prefix = table.prefix;

							sb.append("'" + (!prefix.isEmpty() ? prefix + ":" : "") + sql_expr.table.pname + "'");
							break;
						case element:
						case simple_content:
						case attribute:
						case any_element:
						case any_attribute:
							table = getParentTable(sql_expr);
							PgField field = table.getCanField(sql_expr.xname);
							prefix = field.is_xs_namespace ? "" : field.prefix;

							switch (sql_expr.terminus) {
							case any_element:
								sb.append("'" + (!prefix.isEmpty() ? prefix + ":" : "") + getLastNameOfPath(sql_expr.getXPathFragment()) + "'");
								break;
							case any_attribute:
								sb.append("'" + (!prefix.isEmpty() ? prefix + ":" : "") + getLastNameOfPath(sql_expr.getXPathFragment()).replaceFirst("^@", "") + "'");
								break;
							default:
								sb.append("'" + (!prefix.isEmpty() ? prefix + ":" : "") + sql_expr.pname + "'");
							}
							break;
						default:
							throw new PgSchemaException(tree);
						}

					}

				}
				break;
			default:
				throw new PgSchemaException(tree);
			}

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, sb.toString(), XPathCompType.text, parent, tree));

		} finally {
			sb.setLength(0);
		}

		return false;
	}


	/**
	 * Test StringFunctionCallContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param func_name function name
	 * @param sql_predicates list of SQL expression of current node
	 * @return boolean whether to delegate to other function
	 * @throws PgSchemaException the pg schema exception
	 */
	private boolean testStringFunctionCallContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, String func_name, List<XPathSqlExpr> sql_predicates) throws PgSchemaException {

		int pred_size = sql_predicates != null ? sql_predicates.size() : 0;

		StringBuilder sb = new StringBuilder();

		try {

			XPathSqlExpr sql_expr_str;
			XPathSqlExpr sql_expr_sub;

			switch (func_name) {
			case "string":
				if (pred_size > 1)
					throw new PgSchemaException(tree);

				if (sql_predicates == null)
					sb.append("''");

				else {

					sql_expr_str = sql_predicates.get(0);

					if (sql_expr_str.predicate != null)
						sb.append(sql_expr_str.predicate);

					else
						appendSqlColumnName(sql_expr_str, sb);

				}
				break;
			case "concat":
				if (pred_size == sizeOfPredicate(sql_predicates)) {

					sb.append("'");
					sql_predicates.forEach(sql_expr -> sb.append(sql_expr.predicate.substring(1, sql_expr.predicate.length() - 1)));
					sb.append("'");

				} else {

					sb.append("concat( ");

					sql_predicates.forEach(sql_expr -> {

						if (sql_expr.predicate != null) {

							if (sb.substring(sb.length() - 3).equals("', ")) {

								sb.setLength(sb.length() - 3);
								sb.append(sql_expr.predicate.substring(1));

							}

							else
								sb.append(sql_expr.predicate);

						}

						else
							appendSqlColumnName(sql_expr, sb);

						sb.append(", ");

					});

					sb.setLength(sb.length() - 2); // remove last ", "

					sb.append(" )");

				}
				break;
			case "starts-with":
				if (pred_size != 2)
					throw new PgSchemaException(tree);

				sql_expr_str = sql_predicates.get(0);
				sql_expr_sub = sql_predicates.get(1);

				if (pred_size == sizeOfPredicate(sql_predicates)) {

					String first_arg = sql_expr_str.predicate;
					first_arg = first_arg.substring(1, first_arg.length() - 1);

					String second_arg = sql_expr_sub.predicate;
					second_arg = second_arg.substring(1, second_arg.length() - 1);

					sb.append(first_arg.startsWith(second_arg) ? "TRUE" : "FALSE");

				}

				else {

					sb.append("( substr( ");

					if (sql_expr_str.predicate != null)
						sb.append(sql_expr_str.predicate);
					else
						appendSqlColumnName(sql_expr_str, sb);

					sb.append(", 1, ");

					if (sql_expr_sub.predicate != null)
						sb.append(sql_expr_sub.predicate.length() - 2);

					else {

						sb.append("length( ");

						appendSqlColumnName(sql_expr_sub, sb);

						sb.append(" )");

					}

					sb.append(" ) = ");

					if (sql_expr_sub.predicate != null)
						sb.append(sql_expr_sub.predicate);
					else
						appendSqlColumnName(sql_expr_sub, sb);

					sb.append(" )");

				}
				break;
			case "contains":
				if (pred_size != 2)
					throw new PgSchemaException(tree);

				sql_expr_str = sql_predicates.get(0);
				sql_expr_sub = sql_predicates.get(1);

				if (pred_size == sizeOfPredicate(sql_predicates)) {

					String first_arg = sql_expr_str.predicate;
					first_arg = first_arg.substring(1, first_arg.length() - 1);

					String second_arg = sql_expr_sub.predicate;
					second_arg = second_arg.substring(1, second_arg.length() - 1);

					sb.append(first_arg.contains(second_arg) ? "TRUE" : "FALSE");

				}

				else {

					sb.append("( strpos( ");

					if (sql_expr_str.predicate != null)
						sb.append(sql_expr_str.predicate);
					else
						appendSqlColumnName(sql_expr_str, sb);

					sb.append(", ");

					if (sql_expr_sub.predicate != null)
						sb.append(sql_expr_sub.predicate);
					else
						appendSqlColumnName(sql_expr_sub, sb);

					sb.append(" ) > 0 ");

					sb.append(" )");

				}
				break;
			case "substring-before":
			case "substring-after":
				if (pred_size != 2)
					throw new PgSchemaException(tree);

				sql_expr_str = sql_predicates.get(0);
				sql_expr_sub = sql_predicates.get(1);

				if (pred_size == sizeOfPredicate(sql_predicates)) {

					String first_arg = sql_expr_str.predicate;
					first_arg = first_arg.substring(1, first_arg.length() - 1);

					String second_arg = sql_expr_sub.predicate;
					second_arg = second_arg.substring(1, second_arg.length() - 1);

					if (first_arg.contains(second_arg)) {

						switch (func_name) {
						case "substring-before":
							first_arg = first_arg.substring(0, first_arg.indexOf(second_arg));
							break;
						case "substring-after":
							first_arg = first_arg.substring(first_arg.indexOf(second_arg) + second_arg.length(), first_arg.length());
							break;
						}

					}

					else
						first_arg = "";

					sb.append("'" + first_arg + "'");

				}

				else {

					String first_arg = sql_expr_str.predicate;
					if (first_arg != null)
						first_arg = first_arg.substring(1, first_arg.length() - 1);

					String second_arg = sql_expr_sub.predicate;
					if (second_arg != null)
						second_arg = second_arg.substring(1, second_arg.length() - 1);

					switch (func_name) {
					case "substring-before":
						sb.append("left( ");
						break;
					case "substring-after":
						sb.append("right( ");
						break;
					}

					if (sql_expr_str.predicate != null)
						sb.append(sql_expr_str.predicate);
					else
						appendSqlColumnName(sql_expr_str, sb);

					sb.append(", ");

					if (func_name.equals("substring-after")) {

						if (sql_expr_str.predicate != null)
							sb.append(first_arg.length());

						else {

							sb.append("length( ");
							appendSqlColumnName(sql_expr_str, sb);
							sb.append(" )");

						}

						sb.append(" - ");

						if (sql_expr_sub.predicate != null)
							sb.append(second_arg.length());

						else {

							sb.append("length( ");
							appendSqlColumnName(sql_expr_sub, sb);
							sb.append(" )");

						}

						sb.append(" + 1 - ");

					}

					sb.append("strpos( ");

					if (sql_expr_str.predicate != null)
						sb.append(sql_expr_str.predicate);
					else
						appendSqlColumnName(sql_expr_str, sb);

					sb.append(", ");

					if (sql_expr_sub.predicate != null)
						sb.append(sql_expr_sub.predicate);
					else
						appendSqlColumnName(sql_expr_sub, sb);

					sb.append(" )");

					if (func_name.equals("substring-before"))
						sb.append(" - 1");

					sb.append(" )");

				}
				break;
			case "substring":
				if (pred_size < 2 || pred_size > 3)
					throw new PgSchemaException(tree);

				sql_expr_str = sql_predicates.get(0);
				XPathSqlExpr sql_expr_start = sql_predicates.get(1);
				XPathSqlExpr sql_expr_length = pred_size < 3 ? null : sql_predicates.get(2);

				if (pred_size == sizeOfPredicate(sql_predicates)) {

					String first_arg = sql_expr_str.predicate;
					first_arg = first_arg.substring(1, first_arg.length() - 1);

					try {

						BigDecimal start_value = new BigDecimal(sql_expr_start.predicate);
						int start = start_value.setScale(0, RoundingMode.HALF_EVEN).toBigInteger().intValue() - 1;

						if (pred_size < 3)
							first_arg = first_arg.substring(start < 0 ? 0 : start);

						else {

							BigDecimal length_value = new BigDecimal(sql_expr_length.predicate);
							int length = length_value.setScale(0, RoundingMode.HALF_EVEN).toBigInteger().intValue();
							int end = start + length;

							first_arg = first_arg.substring(start < 0 ? 0 : start, end < 0 ? 0 : end > first_arg.length() ? first_arg.length() : end);

						}

						sb.append("'" + first_arg + "'");

					} catch (NumberFormatException e) {
						throw new PgSchemaException(tree);
					}

				}

				else {

					sb.append("substr( ");

					if (sql_expr_str.predicate != null)
						sb.append(sql_expr_str.predicate);
					else
						appendSqlColumnName(sql_expr_str, sb);

					sb.append(", ");

					if (sql_expr_start.predicate != null) {

						try {

							BigDecimal start_value = new BigDecimal(sql_expr_start.predicate);
							int start = start_value.setScale(0, RoundingMode.HALF_EVEN).toBigInteger().intValue();

							sb.append(start);

						} catch (NumberFormatException e) {
							throw new PgSchemaException(tree);
						}

					}

					else {

						sb.append("round( ");

						appendSqlColumnName(sql_expr_start, sb);

						sb.append(" )");

					}

					if (sql_expr_length != null) {

						sb.append(", ");

						if (sql_expr_length.predicate != null) {

							try {

								BigDecimal length_value = new BigDecimal(sql_expr_length.predicate);
								int length = length_value.setScale(0, RoundingMode.HALF_EVEN).toBigInteger().intValue();

								sb.append(length);

							} catch (NumberFormatException e) {
								throw new PgSchemaException(tree);
							}

						}

						else {

							sb.append("round( ");

							appendSqlColumnName(sql_expr_length, sb);

							sb.append(" )");

						}

					}

					sb.append(" )");

				}
				break;
			case "string-length":
				if (pred_size > 1)
					throw new PgSchemaException(tree);

				if (sql_predicates == null) {

					sb.append("length( ");

					appendSqlColumnName(getXPathSqlExprOfPath(src_path_expr.path, src_path_expr.terminus), sb);

					sb.append(" )");

				}

				else {

					sql_expr_str = sql_predicates.get(0);

					if (sql_expr_str.predicate != null) {

						String first_arg = sql_expr_str.predicate;
						first_arg = first_arg.substring(1, first_arg.length() - 1);

						sb.append(first_arg.length());

					}

					else {

						sb.append("length( ");

						appendSqlColumnName(sql_expr_str, sb);

						sb.append(" )");

					}

				}
				break;
			case "normalize-space":
				if (pred_size > 1)
					throw new PgSchemaException(tree);

				if (sql_predicates == null) {

					sb.append("btrim( regexp_replace( regexp_replace( ");

					appendSqlColumnName(getXPathSqlExprOfPath(src_path_expr.path, src_path_expr.terminus), sb);

					sb.append(" , E'[\\t\\n\\r]+', ' ', 'g' ), E'\\s+', ' ', 'g' ) )");

				}

				else {

					sql_expr_str = sql_predicates.get(0);

					if (sql_expr_str.predicate != null) {

						String first_arg = sql_expr_str.predicate;
						first_arg = first_arg.substring(1, first_arg.length() - 1);

						sb.append("'" + PgSchemaUtil.collapseWhiteSpace(first_arg) + "'");

					}

					else {

						sb.append("btrim( regexp_replace( regexp_replace( ");

						appendSqlColumnName(sql_expr_str, sb);

						sb.append(" , E'[\\t\\n\\r]+', ' ', 'g' ), E'\\s+', ' ', 'g' ) )");

					}

				}
				break;
			case "translate":
				if (pred_size != 3)
					throw new PgSchemaException(tree);

				sql_expr_str = sql_predicates.get(0);
				XPathSqlExpr sql_expr_from = sql_predicates.get(1);
				XPathSqlExpr sql_expr_to = sql_predicates.get(2);

				if (pred_size == sizeOfPredicate(sql_predicates)) {

					sb.append("'");

					String first_arg = sql_expr_str.predicate;
					first_arg = first_arg.substring(1, first_arg.length() - 1);

					String second_arg = sql_expr_from.predicate;
					second_arg = second_arg.substring(1, second_arg.length() - 1);

					String third_arg = sql_expr_to.predicate;
					third_arg = third_arg.substring(1, third_arg.length() - 1);

					char[] srcs = first_arg.toCharArray();
					char[] froms = second_arg.toCharArray();
					char[] tos = third_arg.toCharArray();

					char src;

					for (int l = 0; l < srcs.length; l++) {

						src = srcs[l];

						for (int m = 0; m < froms.length; m++) {

							if (src == froms[m]) {

								if (m < tos.length)
									src = tos[m];
								else
									src = 0;

								break;
							}

						}

						if (src != 0)
							sb.append(src);

					}

					sb.append("'");

				}

				else {

					sb.append("translate( ");

					if (sql_expr_str.predicate != null)
						sb.append(sql_expr_str.predicate);
					else
						appendSqlColumnName(sql_expr_str, sb);

					sb.append(", ");

					if (sql_expr_from.predicate != null)
						sb.append(sql_expr_from.predicate);
					else
						appendSqlColumnName(sql_expr_from, sb);

					sb.append(", ");

					if (sql_expr_to.predicate != null)
						sb.append(sql_expr_to.predicate);
					else
						appendSqlColumnName(sql_expr_to, sb);

					sb.append(" )");

				}
				break;
			default:
				throw new PgSchemaException(tree);
			}

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, sb.toString(), XPathCompType.text, parent, tree));

		} finally {
			sb.setLength(0);
		}

		return false;
	}

	/**
	 * Test BooleanFunctionCallContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param func_name function name
	 * @param sql_predicates list of SQL expression of current node
	 * @return boolean whether to delegate to other function
	 * @throws PgSchemaException the pg schema exception
	 */
	private boolean testBooleanFunctionCallContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, String func_name, List<XPathSqlExpr> sql_predicates) throws PgSchemaException {

		int pred_size = sql_predicates != null ? sql_predicates.size() : 0;

		switch (func_name) {
		case "boolean":
			if (sql_predicates == null)
				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, "FALSE", XPathCompType.text, parent, tree));

			if (pred_size != 1)
				throw new PgSchemaException(tree);

			XPathSqlExpr sql_expr_str = sql_predicates.get(0);

			if (sql_expr_str.predicate != null) {

				String first_arg = sql_expr_str.predicate;

				if (((first_arg.startsWith("'") && first_arg.endsWith("'")) ||
						(first_arg.startsWith("\"") && first_arg.endsWith("\""))))
					first_arg = first_arg.substring(1, first_arg.length() - 1);

				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, first_arg.isEmpty() ? "FALSE" : "TRUE", XPathCompType.text, parent, tree));

			}

			else {

				StringBuilder sb = new StringBuilder();

				sb.append("( ");

				appendSqlColumnName(sql_expr_str, sb);

				sb.append(" IS NOT NULL )");

				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, sb.toString(), XPathCompType.text, parent, tree));

				sb.setLength(0);

			}
			break;
		case "not":
			return true;
		case "true":
			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, "TRUE", XPathCompType.text, parent, tree));
			break;
		case "false":
			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, "FALSE", XPathCompType.text, parent, tree));
			break;
		case "lang":
			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, "TRUE", XPathCompType.text, parent, tree));
		default:
			throw new PgSchemaException(tree);
		}

		return false;
	}

	/**
	 * Test NumberFunctionCallContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param func_name function name
	 * @param sql_predicates list of SQL expression of current node
	 * @return boolean whether to delegate to other function
	 * @throws PgSchemaException the pg schema exception
	 */
	private boolean testNumberFunctionCallContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, String func_name, List<XPathSqlExpr> sql_predicates) throws PgSchemaException {

		int pred_size = sql_predicates != null ? sql_predicates.size() : 0;

		StringBuilder sb = new StringBuilder();

		try {

			XPathSqlExpr sql_expr_str;

			switch (func_name) {
			case "number":
				if (pred_size > 1)
					throw new PgSchemaException(tree);

				if (sql_predicates == null)
					sb.append("'NaN'");

				else {

					sql_expr_str = sql_predicates.get(0);

					if (sql_expr_str.predicate != null) {

						String first_arg = sql_expr_str.predicate;

						if (first_arg.equals("TRUE"))
							sb.append(1);
						else if (first_arg.equals("FALSE"))
							sb.append(0);

						else {

							if (((first_arg.startsWith("'") && first_arg.endsWith("'")) ||
									(first_arg.startsWith("\"") && first_arg.endsWith("\""))))
								first_arg = first_arg.substring(1, first_arg.length() - 1).trim();

							try {

								BigDecimal value = new BigDecimal(first_arg.trim());
								sb.append(value.toString());

							} catch (NumberFormatException e) {
								sb.append("'NaN'");
							}

						}

					}

					else
						appendSqlColumnName(sql_expr_str, sb);

				}
				break;
			case "sum":
				if (pred_size != 1)
					throw new PgSchemaException(tree);

				sql_expr_str = sql_predicates.get(0);

				if (sql_expr_str.predicate != null)
					throw new PgSchemaException(tree);

				if (!document_key) {
					try {
						throw new PgSchemaException(tree, "document key", document_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}

				sb.append("( SELECT sum( ");

				appendSqlColumnName(sql_expr_str, sb);

				sb.append(" ) FROM ");

				String alias_name = "s" + (++sub_alias_id);

				sb.append(schema.getPgNameOf(sql_expr_str.table) + " as " + alias_name);

				sb.append(" WHERE " + alias_name + "." + PgSchemaUtil.avoidPgReservedWords(schema.getDocKeyName(sql_expr_str.table)) + " = t1." + PgSchemaUtil.avoidPgReservedWords(schema.getDocKeyName(main_aliases.entrySet().parallelStream().filter(entry -> entry.getValue().equals("t1")).findFirst().get().getKey())));

				sb.append(" )");
				break;
			case "floor":
			case "ceiling":
			case "round":
				if (pred_size != 1)
					throw new PgSchemaException(tree);

				sql_expr_str = sql_predicates.get(0);

				if (sql_expr_str.predicate != null) {

					BigDecimal value = new BigDecimal(sql_expr_str.predicate);

					switch (func_name) {
					case "floor":
						sb.append(value.setScale(0, RoundingMode.FLOOR).toBigInteger().intValue());
						break;
					case "ceiling":
						sb.append(value.setScale(0, RoundingMode.CEILING).toBigInteger().intValue());
						break;
					case "round":
						sb.append(value.setScale(0, RoundingMode.HALF_EVEN).toBigInteger().intValue());
						break;
					}

				}

				else {

					switch (func_name) {
					case "floor":
						sb.append("floor( ");
						break;
					case "ceiling":
						sb.append("ceiling( ");
						break;
					case "round":
						sb.append("round( ");
						break;
					}

					appendSqlColumnName(sql_expr_str, sb);

					sb.append(" )");

				}
				break;
			default:
				throw new PgSchemaException(tree);
			}

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, null, sb.toString(), XPathCompType.text, parent, tree));

		} finally {
			sb.setLength(0);
		}

		return false;
	}

	/**
	 * Return last name of current path.
	 *
	 * @param path current path
	 * @return String last name of the path
	 */
	private String getLastNameOfPath(String path) {

		String[] _path = path.split(" ").length < 2 ? path.replaceFirst("//$", "").split("/") : path.replaceFirst("//$", "").split(" ");

		int position = _path.length - 1;

		if (position < 0)
			return null;

		return _path[position];
	}

	/**
	 * Return total number of predicates in XPath SQL expression
	 *
	 * @param sql_predicates list of XPath SQL expression
	 * @return int the total number of predicates in the list
	 */
	private int sizeOfPredicate(List<XPathSqlExpr> sql_predicates) {
		return (int) sql_predicates.parallelStream().filter(sql_expr -> sql_expr.predicate != null).count();
	}

	/**
	 * Trim duplicated SQL predicate of any element.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param sql_predicate list of target SQL predicates
	 */
	private void trimAnySqlPredicate(XPathExpr src_path_expr, List<XPathSqlExpr> sql_predicates) {

		if (!option.wild_card || !sql_predicates.parallelStream().anyMatch(sql_predicate -> sql_predicate.terminus.equals(XPathCompType.any_element)))
			return;

		List<XPathSqlExpr> any_sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_predicate -> sql_predicate.terminus.equals(XPathCompType.any_element)).collect(Collectors.toList());

		XPathSqlExpr sql_expr_1, sql_expr_2;

		for (int i = 0; i < any_sql_predicates.size() - 1; i++) {

			sql_expr_1 = any_sql_predicates.get(i);

			for (int j = i + 1; j < any_sql_predicates.size(); j++) {

				sql_expr_2 = any_sql_predicates.get(j);

				if (sql_expr_1.path.contains(sql_expr_2.path))
					sql_predicates.remove(sql_expr_2);

				else if (sql_expr_2.path.contains(sql_expr_1.path))
					sql_predicates.remove(sql_expr_1);

			}

		}

	}

	/**
	 * Return where successive predicates start in XPath SQL expression
	 *
	 * @param sql_predicates list of XPath SQL expression
	 * @param offset offset id
	 * @return int where successive predicates start in the list or not (-1)
	 */
	private int startIdOfSuccessivePredicate(List<XPathSqlExpr> sql_predicates, int offset) {

		boolean successive = false;

		XPathSqlExpr sql_expr;

		for (int expr_id = offset; expr_id < sql_predicates.size(); expr_id++) {

			sql_expr = sql_predicates.get(expr_id);

			if (sql_expr.predicate != null) {

				if (successive)
					return expr_id - 1;

				successive = true;

			}

			else
				successive = false;

		}

		return -1;
	}

	/**
	 * Return where successive predicates end in XPath SQL expression
	 *
	 * @param sql_predicates list of XPath SQL expression
	 * @param offset offset id
	 * @return int where successive predicates end in the list or not (-1)
	 */
	private int endIdOfSuccessivePredicate(List<XPathSqlExpr> sql_predicates, int offset) {

		boolean successive = false;

		XPathSqlExpr sql_expr;

		for (int expr_id = offset; expr_id < sql_predicates.size(); expr_id++) {

			sql_expr = sql_predicates.get(expr_id);

			if (sql_expr.predicate != null) {

				if (!successive)
					successive = true;

			}

			else {

				if (successive)
					return expr_id;

				successive = false;

			}

		}

		return successive ? sql_predicates.size() : -1;
	}

	/**
	 * Translate predicate expression to SQL implementation.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr source path expression
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translatePredicateTree2SqlImpl(XPathComp src_comp, XPathExpr src_path_expr, StringBuilder sb) throws PgSchemaException {

		LinkedList<StringBuilder> sb_list = new LinkedList<StringBuilder>();

		translatePredicateTree(src_path_expr, src_comp.tree, false, sb, sb_list);

		translatePredicateContext(src_path_expr, src_comp.tree, sb);

		sb_list.clear();

	}

	/**
	 * Return whether XPath parse tree of predicate is effective.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param tree current XPath parse tree
	 * @param has_children whether parent has children
	 * @param sb StringBuilder to store SQL expression
	 * @param sb_list LinkedList<StringBuilder> for each function call context
	 * @return boolean whether valid predicate
	 * @throws PgSchemaException the pg schema exception
	 */
	private boolean translatePredicateTree(XPathExpr src_path_expr, ParseTree tree, boolean has_children, StringBuilder sb, LinkedList<StringBuilder> sb_list) throws PgSchemaException {

		boolean valid = false, _has_children;

		ParseTree child;
		Class<?> childClass;

		for (int i = 0; i < tree.getChildCount(); i++) {

			child = tree.getChild(i);
			childClass = child.getClass();

			if (childClass.equals(TerminalNodeImpl.class))
				continue;

			else if (childClass.equals(FunctionCallContext.class))
				sb_list.addFirst(new StringBuilder());

			_has_children = !child.getText().isEmpty() && (hasEffectiveChildren(child) || hasChildOfTerminalNodeImpl(child));

			if (translatePredicateTree(src_path_expr, child, _has_children, sb, sb_list) || _has_children)
				valid = true;

		}

		if (has_children) {

			ParseTree parent = tree.getParent();

			while (!(!parent.getText().isEmpty() && (hasEffectiveChildren(parent) || hasChildOfTerminalNodeImpl(parent))))
				parent = parent.getParent();

			Class<?> parentClass = parent.getClass();

			// PathContext node has already been validated

			if (!isPathContextClass(parentClass)) {

				Class<?> currentClass = tree.getClass();

				// OrExprContext node

				if (currentClass.equals(OrExprContext.class))
					translateOrExprContext(src_path_expr, parent, tree, sb_list.size() == 0 ? sb : sb_list.peek());

				// AndExprContext node

				else if (currentClass.equals(AndExprContext.class))
					translateAndExprContext(src_path_expr, parent, tree, sb_list.size() == 0 ? sb : sb_list.peek());

				// AdditiveExprContext node

				else if (currentClass.equals(AdditiveExprContext.class))
					translateAdditiveExprContext(src_path_expr, parent, tree, sb_list.size() == 0 ? sb : sb_list.peek());

				// MultiplicativeExprContext node

				else if (currentClass.equals(MultiplicativeExprContext.class))
					translateMultiplicativeExprContext(src_path_expr, parent, tree, sb_list.size() == 0 ? sb : sb_list.peek());

				// FunctionCallContext node

				else if (currentClass.equals(FunctionCallContext.class)) {
					translateFunctionCallContext(src_path_expr, parent, tree, sb_list.size() > 1 ? sb_list.get(1) : sb, sb_list.peek());
					sb_list.removeFirst();
				}

			}

		}

		return valid;
	}

	/**
	 * Translate OrExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateOrExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, StringBuilder sb) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size == 0)
			return;

		if (pred_size == 1)
			sb.append(" OR ");

		sql_predicates.forEach(sql_expr -> {

			appendSqlColumnName(sql_expr, sb);
			sb.append(" " + sql_expr.binary_operator + " " + sql_expr.value);

			sb.append(" OR ");

		});

		sb.setLength(sb.length() - 4); // remove last " OR "

	}

	/**
	 * Translate AndExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateAndExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, StringBuilder sb) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size == 0)
			return;

		if (pred_size == 1)
			sb.append(" AND ");

		sql_predicates.forEach(sql_expr -> {

			switch (sql_expr.terminus) {
			case element:
			case simple_content:
			case attribute:
			case any_element:
			case any_attribute:
				appendSqlColumnName(sql_expr, sb);
				sb.append(" " + sql_expr.binary_operator + " " + sql_expr.value);
				break;
			case text:
				PgTable parent_table = getParentTable(src_path_expr);
				if (!serial_key) {
					try {
						throw new PgSchemaException(tree, "serial key", serial_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}
				appendSqlColumnName(parent_table, serial_key_name, sb);
				sb.append(" = " + sql_expr.predicate);
				break;
			default:
				try {
					throw new PgSchemaException(tree);
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
			}

			sb.append(" AND ");

		});

		sb.setLength(sb.length() - 5); // remove last " AND "

	}

	/**
	 * Translate AdditiveExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateAdditiveExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, StringBuilder sb) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size == 0)
			return;

		String[] terminal_codes = getTextArrayOfChildTerminalNodeImpl(tree);

		if (parent.getClass().equals(PredicateContext.class)) {

			appendSqlColumnName(getTable(src_path_expr), serial_key_name, sb);
			sb.append(" = ");

		}

		XPathSqlExpr sql_expr;

		for (int expr_id = 0; expr_id < pred_size; expr_id++) {

			sql_expr = sql_predicates.get(expr_id);

			switch (sql_expr.terminus) {
			case element:
			case simple_content:
			case attribute:
			case any_element:
			case any_attribute:
				appendSqlColumnName(sql_expr, sb);
				break;
			case text:
				if (!sql_expr.predicate.equals("0")) {
					if (!serial_key) {
						try {
							throw new PgSchemaException(tree, "serial key", serial_key);
						} catch (PgSchemaException e) {
							e.printStackTrace();
						}
					}
					sb.append(sql_expr.predicate);
				}
				break;
			default:
				try {
					throw new PgSchemaException(tree);
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
			}

			if (expr_id < pred_size - 1) {

				switch (terminal_codes[expr_id]) {
				case "+":
					sb.append(" + ");
					break;
				case "-":
					sb.append(" - ");
					break;
				default:
					throw new PgSchemaException(tree);
				}

			}

		}

	}

	/**
	 * Translate MultiplicativeExprContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateMultiplicativeExprContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, StringBuilder sb) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size < 2)
			return;

		String[] terminal_codes = getTextArrayOfChildTerminalNodeImpl(tree);

		if (parent.getClass().equals(PredicateContext.class)) {

			appendSqlColumnName(getTable(src_path_expr), serial_key_name, sb);
			sb.append(" = ");

		}

		XPathSqlExpr sql_expr;

		for (int expr_id = 0; expr_id < pred_size; expr_id++) {

			sql_expr = sql_predicates.get(expr_id);

			switch (sql_expr.terminus) {
			case element:
			case simple_content:
			case attribute:
			case any_element:
			case any_attribute:
				appendSqlColumnName(sql_expr, sb);
				break;
			case text:
				if (!serial_key) {
					try {
						throw new PgSchemaException(tree, "serial key", serial_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}
				sb.append(sql_expr.predicate);
				break;
			default:
				try {
					throw new PgSchemaException(tree);
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
			}

			if (expr_id < pred_size - 1) {

				switch (terminal_codes[expr_id]) {
				case "*":
					sb.append(" * ");
					break;
				case "div":
					sb.append(" / ");
					break;
				case "mod":
					sb.append(" % ");
					break;
				default:
					throw new PgSchemaException(tree);
				}

			}

		}

	}

	/**
	 * Translate FunctionCallContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param sb StringBuilder to store SQL expression
	 * @param sb_func_call StringBuilder for function call context
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateFunctionCallContext(XPathExpr src_path_expr, ParseTree parent, ParseTree tree, StringBuilder sb, StringBuilder sb_func_call) throws PgSchemaException {

		try {

			List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

			String func_name = null;

			ParseTree child;

			for (int i = 0; i < tree.getChildCount(); i++) {

				child = tree.getChild(i);

				if (child.getClass().equals(TerminalNodeImpl.class))
					continue;

				func_name = child.getText();

				break;
			}

			int pred_size = sql_predicates.size();

			if (pred_size == 0) {

				switch (func_name) {
				case "not":
					if (sb.length() > 0 && parent.getClass().equals(OrExprContext.class))
						sb.append(" OR ");

					if (sb.length() > 0 && parent.getClass().equals(AndExprContext.class))
						sb.append(" AND ");

					switch (sb_func_call.toString()) {
					case "TRUE":
						sb.append("FALSE");
						break;
					case "FALSE":
						sb.append("TRUE");
						break;
					default:
						sb.append("NOT ( " + sb_func_call + " )");
					}
					break;
				default:
				}

				return;
			}

			if (pred_size != 1)
				throw new PgSchemaException(tree);

			XPathSqlExpr sql_expr = sql_predicates.get(0);

			switch (func_name) {
			case "not":
				if (sb.length() > 0 && parent.getClass().equals(OrExprContext.class))
					sb.append(" OR ");

				if (sb.length() > 0 && parent.getClass().equals(AndExprContext.class))
					sb.append(" AND ");

				sb.append("NOT ( ");

				switch (sql_expr.terminus) {
				case element:
				case simple_content:
				case attribute:
				case any_element:
				case any_attribute:
					appendSqlColumnName(sql_expr, sb);
					sb.append(" " + sql_expr.binary_operator + " " + sql_expr.value);
					break;
				case text:
					PgTable parent_table = getParentTable(src_path_expr);
					if (!serial_key) {
						try {
							throw new PgSchemaException(tree, "serial key", serial_key);
						} catch (PgSchemaException e) {
							e.printStackTrace();
						}
					}
					appendSqlColumnName(parent_table, serial_key_name, sb);
					sb.append(" = " + sql_expr.predicate);
					break;
				default:
					throw new PgSchemaException(tree);
				}

				sb.append(" )");
				break;
			default:
			}

		} finally {
			sb_func_call.setLength(0);
		}

	}

	/**
	 * Translate PredicateContext node.
	 *
	 * @param src_path_expr path expression of source predicate
	 * @param tree current XPath parse tree
	 * @param sb StringBuilder to stor:e SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translatePredicateContext(XPathExpr src_path_expr, ParseTree tree, StringBuilder sb) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size == 0)
			return;

		if (pred_size != 1)
			throw new PgSchemaException(tree);

		XPathSqlExpr sql_expr = sql_predicates.get(0);

		switch (sql_expr.terminus) {
		case element:
		case simple_content:
		case attribute:
		case any_element:
		case any_attribute:
			appendSqlColumnName(sql_expr, sb);
			sb.append(" " + sql_expr.binary_operator + " " + sql_expr.value);
			break;
		case text:
			if (_predicateContextClass != null && (_predicateContextClass.equals(EqualityExprContext.class) || _predicateContextClass.equals(RelationalExprContext.class) || _predicateContextHasBooleanFunc))
				sb.append(sql_expr.predicate);

			else {
				PgTable parent_table = getParentTable(src_path_expr);
				if (!serial_key) {
					try {
						throw new PgSchemaException(tree, "serial key", serial_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}
				appendSqlColumnName(parent_table, serial_key_name, sb);
				sb.append(" = " + sql_expr.predicate);
			}
			break;
		default:
			throw new PgSchemaException(tree);
		}

	}

	/**
	 * Test SQL JOIN clause for simple type attribute.
	 *
	 * @param table current table
	 * @param ref_path reference node path
	 * @param linking_tables additional linking SQL tables
	 * @param linking_order order of linking SQL tables
	 */
	private void testJoinClauseForSimpleTypeAttr(PgTable table, String ref_path, HashMap<PgTable, String> linking_tables, LinkedList<PgTable> linking_order) {

		boolean has_nested_key_as_attr = table.has_nested_key_as_attr;

		if (!has_nested_key_as_attr)
			has_nested_key_as_attr = table.fields.parallelStream().anyMatch(field -> field.nested_key_as_attr);

		if (!linking_tables.containsKey(table))
			linking_tables.put(table, getAbsoluteXPathOfTable(table, ref_path));

		linking_order.push(table);

		if (!table.has_simple_attribute && !has_nested_key_as_attr)
			return;

		Optional<PgTable> opt;

		opt = tables.parallelStream().filter(foreign_table -> foreign_table.nested_fields > 0 && foreign_table.fields.stream().anyMatch(field -> field.nested_key_as_attr && schema.getTable(field.foreign_table_id).equals(table) && (ref_path == null || (ref_path != null && ((foreign_table.virtual && field.containsParentNodeName(ref_path)) || (!foreign_table.virtual && (foreign_table.has_nested_key_as_attr || ref_path.contains(foreign_table.xname)))))))).findFirst();

		if (opt.isPresent())
			testJoinClauseForSimpleTypeAttr(opt.get(), ref_path, linking_tables, linking_order);

		opt = tables.parallelStream().filter(foreign_table -> foreign_table.nested_fields > 0 && foreign_table.fields.stream().anyMatch(field -> field.nested_key && !field.nested_key_as_attr && schema.getTable(field.foreign_table_id).equals(table) && (ref_path == null || (ref_path != null && ((foreign_table.virtual && field.containsParentNodeName(ref_path)) || (!foreign_table.virtual && (foreign_table.has_nested_key_as_attr || ref_path.contains(foreign_table.xname)))))))).findFirst();

		if (opt.isPresent())
			testJoinClauseForSimpleTypeAttr(opt.get(), ref_path, linking_tables, linking_order);

	}

	/**
	 * Test SQL JOIN clause.
	 *
	 * @param target_tables target SQL tables
	 * @param joined_tables joined SQL tables
	 * @param linking_tables additional linking SQL tables
	 * @param linking_orders order of linking SQL tables
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testJoinClause(HashMap<PgTable, String> target_tables, HashMap<PgTable, String> joined_tables, HashMap<PgTable, String> linking_tables, LinkedList<LinkedList<PgTable>> linking_orders) throws PgSchemaException {

		joined_tables.keySet().stream().filter(_joined_table -> target_tables.containsKey(_joined_table)).forEach(_joined_table -> target_tables.remove(_joined_table));

		if (target_tables.isEmpty())
			return;

		PgTable dst_table;

		LinkedList<PgTable> linking_order = linking_orders.poll();

		if (linking_order != null && linking_order.size() > 0) {

			while ((dst_table = linking_order.poll()) != null) {

				if (!joined_tables.containsKey(dst_table))
					joined_tables.put(dst_table, target_tables.get(dst_table));

				target_tables.remove(dst_table);

			}

			testJoinClause(target_tables, joined_tables, linking_tables, linking_orders);

			return;
		}

		PgTable target_table = null, _target_table;
		String target_table_path = null, _target_path;

		PgTable joined_table = null, _joined_table;
		String joined_table_path = null, _joined_path;

		int min_distance = -1, distance;

		for (Entry<PgTable, String> joined : joined_tables.entrySet()) {

			_joined_table = joined.getKey();
			_joined_path = joined.getValue();

			for (Entry<PgTable, String> target : target_tables.entrySet()) {

				_target_table = target.getKey();
				_target_path = target.getValue();

				distance = getDistanceOfTables(_joined_path, _target_path);

				if (distance > 0) {

					if (min_distance == -1 || distance < min_distance) {

						target_table = _target_table;
						target_table_path = _target_path;

						joined_table = _joined_table;
						joined_table_path = _joined_path;

						min_distance = distance;

					}

				}

				// the same depth

				else if (distance == 0 && min_distance == -1) {

					target_table = _target_table;
					target_table_path = _target_path;

					joined_table = _joined_table;
					joined_table_path = _joined_path;

					min_distance = distance;

				}

			}

		}

		// branched case

		if (min_distance != 1) {

			char[] target_char, joined_char;
			int last_match;

			String common_path;

			for (Entry<PgTable, String> joined : joined_tables.entrySet()) {

				_joined_table = joined.getKey();
				_joined_path = joined.getValue();

				joined_char = _joined_path.toCharArray();

				for (Entry<PgTable, String> target : target_tables.entrySet()) {

					_target_table = target.getKey();
					_target_path = target.getValue();

					target_char = _target_path.toCharArray();

					last_match = 0;

					for (int l = 0; l < joined_char.length && l < target_char.length; l++) {

						if (joined_char[l] == target_char[l])
							last_match = l;
						else
							break;

					}

					if (last_match == 0 || last_match == target_char.length - 1)
						continue;

					common_path = _target_path.substring(0, last_match + 1);

					distance = getDistanceOfTables(joined.getValue(), common_path);

					if (distance > 0) {

						if (min_distance == -1 || distance < min_distance) {

							target_table = _target_table;
							target_table_path = common_path;

							joined_table = _joined_table;
							joined_table_path = _joined_path;

							min_distance = distance;

						}

					}

					else if (distance == 0) {

						XPathExpr target_path_expr = new XPathExpr(_target_path, XPathCompType.table);

						String parent_path = target_path_expr.getParentPath();

						XPathExpr parent_path_expr = new XPathExpr(parent_path, XPathCompType.table);

						PgTable parent_table = getTable(parent_path_expr);

						if (parent_table != null && !target_tables.containsKey(parent_table)) {

							target_tables.put(parent_table, parent_path);

							if (!linking_tables.containsKey(parent_table)) {

								linking_tables.put(parent_table, parent_path);

								testJoinClause(target_tables, joined_tables, linking_tables, linking_orders);

								return;
							}	

						}

					}

				}

			}

		}

		PgTable src_table;

		String src_path;
		String dst_path;

		// subject table is parent

		if (joined_table_path.split("/").length < target_table_path.split("/").length || min_distance == 0) {

			src_table = joined_table;
			dst_table = target_table;

			src_path = joined_table_path;
			dst_path = target_table_path;

		}

		// subject table is child

		else {

			src_table = target_table;
			dst_table = joined_table;

			src_path = target_table_path;
			dst_path = joined_table_path;

		}

		if (src_table.bridge) {

			src_table.fields.stream().filter(field -> field.nested_key).forEach(field -> {

				PgTable foreign_table = schema.getForeignTable(field);

				if (!target_tables.containsKey(foreign_table))
					target_tables.put(foreign_table, src_path);

			});

		}

		LinkedList<PgTable> touched_tables = new LinkedList<PgTable>();

		touched_tables.add(src_table);

		HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

		Integer[] ft_ids = src_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
		Integer[] _ft_ids = null;

		boolean found_table = false;

		int _touched_size;
		PgTable foreign_table;

		while (ft_ids != null && ft_ids.length > 0 && !found_table) {

			_touched_size = touched_ft_ids.size();

			for (Integer foreign_table_id : ft_ids) {

				if (!touched_ft_ids.add(foreign_table_id))
					continue;

				foreign_table = schema.getTable(foreign_table_id);

				if (foreign_table.equals(dst_table)) {

					touched_tables.add(dst_table);

					found_table = true;

					break;
				}

				else if (foreign_table.virtual && !found_table) {

					touched_tables.add(foreign_table);

					Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

					if (__ft_ids != null && __ft_ids.length > 0)
						_ft_ids = __ft_ids;

				}

				else if (foreign_table.bridge && !found_table) {

					PgTable _dst_table = dst_table;

					if (foreign_table.fields.parallelStream().anyMatch(field -> field.nested_key && schema.getForeignTable(field).equals(_dst_table))) {

						if (!target_tables.containsKey(foreign_table))
							target_tables.put(foreign_table, dst_path);

						touched_tables.add(foreign_table);

						Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

						if (__ft_ids != null && __ft_ids.length > 0)
							_ft_ids = __ft_ids;

					}

				}

			}

			ft_ids = _ft_ids;

			if (touched_ft_ids.size() == _touched_size)
				break;

		}

		touched_ft_ids.clear();

		if (!found_table) {

			XPathExpr dst_path_expr = new XPathExpr(dst_path, XPathCompType.table);

			String parent_path = dst_path_expr.getParentPath();

			XPathExpr parent_path_expr = new XPathExpr(parent_path, XPathCompType.table);

			PgTable parent_table = getTable(parent_path_expr);

			if (parent_table != null && !target_tables.containsKey(parent_table)) {

				target_tables.put(parent_table, parent_path);

				if (!linking_tables.containsKey(parent_table)) {

					linking_tables.put(parent_table, parent_path);

					testJoinClause(target_tables, joined_tables, linking_tables, linking_orders);

					return;
				}

			}

			throw new PgSchemaException("Not found path from " + src_table.pname + " to " + dst_table.pname + ".");

		}

		// no need to join stop over tables

		while ((dst_table = touched_tables.poll()) != null) {

			String dst_table_path = target_tables.get(dst_table);

			if (dst_table_path != null) {

				if (!joined_tables.containsKey(dst_table))
					joined_tables.put(dst_table, dst_table_path);

				target_tables.remove(dst_table);

			}

		}

		testJoinClause(target_tables, joined_tables, linking_tables, linking_orders);

	}	

	/**
	 * Append SQL JOIN clause.
	 *
	 * @param target_tables target SQL tables
	 * @param joined_tables joined SQL tables
	 * @param linking_orders order of linking SQL tables
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void appendJoinClause(HashMap<PgTable, String> target_tables, HashMap<PgTable, String> joined_tables, LinkedList<LinkedList<PgTable>> linking_orders, StringBuilder sb) throws PgSchemaException {

		if (target_tables.isEmpty())
			return;

		PgTable src_table;
		PgTable dst_table;

		LinkedList<PgTable> linking_order = linking_orders.poll();

		if (linking_order != null && linking_order.size() > 0) {

			src_table = linking_order.poll();

			if (!joined_tables.containsKey(src_table))
				joined_tables.put(src_table, target_tables.get(src_table));

			target_tables.remove(src_table);

			while ((dst_table = linking_order.poll()) != null) {

				if (!joined_tables.containsKey(dst_table))
					joined_tables.put(dst_table, target_tables.get(dst_table));

				target_tables.remove(dst_table);

				PgTable _dst_table = dst_table;

				PgField nested_key = src_table.fields.stream().filter(field -> field.nested_key && schema.getTable(field.foreign_table_id).equals(_dst_table)).findFirst().get();

				appendSqlColumnName(src_table, nested_key.pname, sb);

				sb.append(" = ");

				appendSqlColumnName(schema.getTable(nested_key.foreign_table_id), nested_key.foreign_field_pname, sb);

				sb.append(" AND ");

				src_table = _dst_table;

			}

			appendJoinClause(target_tables, joined_tables, linking_orders, sb);

			return;
		}

		PgTable target_table = null, _target_table;
		String target_table_path = null, _target_path;

		PgTable joined_table = null, _joined_table;
		String joined_table_path = null, _joined_path;

		int min_distance = -1, distance;

		for (Entry<PgTable, String> joined : joined_tables.entrySet()) {

			_joined_table = joined.getKey();
			_joined_path = joined.getValue();

			for (Entry<PgTable, String> target : target_tables.entrySet()) {

				_target_table = target.getKey();
				_target_path = target.getValue();

				distance = getDistanceOfTables(_joined_path, _target_path);

				if (distance > 0) {

					if (min_distance == -1 || distance < min_distance) {

						target_table = _target_table;
						target_table_path = _target_path;

						joined_table = _joined_table;
						joined_table_path = _joined_path;

						min_distance = distance;

					}

				}

				// the same depth

				else if (distance == 0 && min_distance == -1) {

					target_table = _target_table;
					target_table_path = _target_path;

					joined_table = _joined_table;
					joined_table_path = _joined_path;

					min_distance = distance;

				}

			}

		}

		// branched case

		if (min_distance != 1) {

			char[] target_char, joined_char;
			int last_match;

			String common_path;

			for (Entry<PgTable, String> joined : joined_tables.entrySet()) {

				_joined_table = joined.getKey();
				_joined_path = joined.getValue();

				joined_char = _joined_path.toCharArray();

				for (Entry<PgTable, String> target : target_tables.entrySet()) {

					_target_table = target.getKey();
					_target_path = target.getValue();

					target_char = _target_path.toCharArray();

					last_match = 0;

					for (int l = 0; l < joined_char.length && l < target_char.length; l++) {

						if (joined_char[l] == target_char[l])
							last_match = l;
						else
							break;

					}

					if (last_match == 0 || last_match == target_char.length - 1)
						continue;

					common_path = _target_path.substring(0, last_match + 1);

					distance = getDistanceOfTables(joined.getValue(), common_path);

					if (distance > 0) {

						if (min_distance == -1 || distance < min_distance) {

							target_table = _target_table;
							target_table_path = common_path;

							joined_table = _joined_table;
							joined_table_path = _joined_path;

							min_distance = distance;

						}

					}

				}

			}

		}

		// subject table is parent

		if (joined_table_path.split("/").length < target_table_path.split("/").length || min_distance == 0) {

			src_table = joined_table;
			dst_table = target_table;

		}

		// subject table is child

		else {

			src_table = target_table;
			dst_table = joined_table;

		}

		LinkedList<PgTable> touched_tables = new LinkedList<PgTable>();

		touched_tables.add(src_table);

		HashSet<Integer> touched_ft_ids = new HashSet<Integer>();

		Integer[] ft_ids = src_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
		Integer[] _ft_ids = null;

		boolean found_table = false;

		while (ft_ids != null && ft_ids.length > 0 && !found_table) {

			int _touched_size = touched_ft_ids.size();

			PgTable foreign_table;

			for (Integer foreign_table_id : ft_ids) {

				if (!touched_ft_ids.add(foreign_table_id))
					continue;

				foreign_table = schema.getTable(foreign_table_id);

				if (foreign_table.equals(dst_table)) {

					touched_tables.add(dst_table);

					found_table = true;

					break;
				}

				else if ((foreign_table.virtual || foreign_table.has_nested_key_as_attr) && !found_table) {

					touched_tables.add(foreign_table);

					Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

					if (__ft_ids != null && __ft_ids.length > 0)
						_ft_ids = __ft_ids;

				}

				else if (foreign_table.bridge && !found_table) {

					PgTable _dst_table = dst_table;

					if (foreign_table.fields.parallelStream().anyMatch(field -> field.nested_key && schema.getForeignTable(field).equals(_dst_table))) {

						touched_tables.add(foreign_table);

						Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

						if (__ft_ids != null && __ft_ids.length > 0)
							_ft_ids = __ft_ids;

					}

				}

			}

			ft_ids = _ft_ids;

			if (touched_ft_ids.size() == _touched_size)
				break;

		}

		touched_ft_ids.clear();

		if (!found_table)
			throw new PgSchemaException("Not found path from " + src_table.pname + " to " + dst_table.pname + ".");

		src_table = touched_tables.poll();

		if (!joined_tables.containsKey(src_table))
			joined_tables.put(src_table, target_tables.get(src_table));

		target_tables.remove(src_table);

		while ((dst_table = touched_tables.poll()) != null) {

			if (!joined_tables.containsKey(dst_table))
				joined_tables.put(dst_table, target_tables.get(dst_table));

			target_tables.remove(dst_table);

			PgTable _dst_table = dst_table;

			PgField nested_key = src_table.fields.stream().filter(field -> field.nested_key && schema.getTable(field.foreign_table_id).equals(_dst_table)).findFirst().get();

			appendSqlColumnName(src_table, nested_key.pname, sb);

			sb.append(" = ");

			appendSqlColumnName(schema.getTable(nested_key.foreign_table_id), nested_key.foreign_field_pname, sb);

			sb.append(" AND ");

			src_table = _dst_table;

		}

		appendJoinClause(target_tables, joined_tables, linking_orders, sb);

	}

	/**
	 * Return distance between table paths
	 *
	 * @param path1 first table path
	 * @param path2 second table path
	 * @return int the distance between the paths, return -1 when invalid case
	 */
	private int getDistanceOfTables(String path1, String path2) {

		if (!path1.contains("/") || !path2.contains("/"))
			return -1;

		if (!path1.contains(path2) && !path2.contains(path1))
			return -1;

		return Math.abs(path1.split("/").length - path2.split("/").length);
	}

	/**
	 * Return XPath SQL expression of current path.
	 *
	 * @param path current path
	 * @param terminus current terminus type
	 * @return XPathSqlExpr XPath SQL expression
	 */
	private XPathSqlExpr getXPathSqlExprOfPath(String path, XPathCompType terminus) {

		String[] _path = path.replaceFirst("//$", "").split("/");

		int position = _path.length - 1;

		if (position < 0)
			return null;

		PgTable table = null;

		String table_xname = null;
		String _table_xname;
		String field_xname;

		String pg_xpath_code = null;
		boolean has_prefix = true;
		boolean attr_in_any = false;

		switch (terminus) {
		case element:
			if (position - 1 < 0)
				return null;
			table_xname = _path[position - 1];
			field_xname = _path[position];
			break;
		case simple_content:
			table_xname = _path[position];
			field_xname = PgSchemaUtil.simple_content_name;
			break;
		case attribute:
			if (position - 1 < 0)
				return null;
			table_xname = _path[position - 1];
			field_xname = _path[position].replaceFirst("^@", "");
			break;
		case any_element:
			if (position - 1 < 0)
				return null;
			has_prefix = schema.getNamespaceUriForPrefix("") != null;
			attr_in_any = getLastNameOfPath(_path[position]).startsWith("@");
			table_xname = _path[position - 1];
			_table_xname = "/" + table_xname;
			table = getTable(new XPathExpr(path.substring(0, path.lastIndexOf(_table_xname)) + _table_xname, XPathCompType.table));
			field_xname = PgSchemaUtil.any_name;
			pg_xpath_code = "xpath('/" + (has_prefix ? pg_xpath_prefix_ : "") + table_xname + "/" + (has_prefix ? pg_xpath_prefix_ : "") + _path[position].replace(" @", "/@").replace(" ", "/" + (has_prefix ? pg_xpath_prefix_: "")) + (attr_in_any ? "" : "/text()") + "', "
					+ schema.getPgNameOf(table) + "." + PgSchemaUtil.avoidPgReservedWords(field_xname) + (has_prefix ? ", ARRAY[ARRAY['" + pg_xpath_prefix + "', '" + schema.getNamespaceUriForPrefix("") + "']]" : "") + ")::text[]";
			break;
		case any_attribute:
			if (position - 1 < 0)
				return null;
			has_prefix = schema.getNamespaceUriForPrefix("") != null;
			table_xname = _path[position - 1];
			_table_xname = "/" + table_xname;
			table = getTable(new XPathExpr(path.substring(0, path.lastIndexOf(_table_xname)) + _table_xname, XPathCompType.table));
			field_xname = PgSchemaUtil.any_attribute_name;
			pg_xpath_code = "xpath('/" + (has_prefix ? pg_xpath_prefix_ : "") + table_xname + "/" + _path[position].replace(" ", "/" + (has_prefix ? pg_xpath_prefix_: "")) + "', "
					+ schema.getPgNameOf(table) + "." + PgSchemaUtil.avoidPgReservedWords(field_xname) + (has_prefix ? ", ARRAY[ARRAY['" + pg_xpath_prefix + "', '" + schema.getNamespaceUriForPrefix("") + "']]" : "") + ")::text[]";
			break;
		default:
			return null;
		}

		if (table == null) {

			_table_xname = "/" + table_xname;
			table = getTable(new XPathExpr(path.substring(0, path.lastIndexOf(_table_xname)) + _table_xname, XPathCompType.table));

		}

		if (table == null)
			return null;

		try {

			return new XPathSqlExpr(schema, path, table, field_xname, pg_xpath_code, null, terminus);

		} catch (PgSchemaException e) {

			String _field_xname = field_xname;

			HashSet<Integer> touched_ft_ids = null;

			Integer[] ft_ids = null;
			Integer[] _ft_ids = null;

			switch (terminus) {
			case element:
				touched_ft_ids = new HashSet<Integer>();

				ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				_ft_ids = null;

				while (ft_ids != null && ft_ids.length > 0) {

					int _touched_size = touched_ft_ids.size();

					PgTable foreign_table;

					for (Integer foreign_table_id : ft_ids) {

						if (!touched_ft_ids.add(foreign_table_id))
							continue;

						foreign_table = schema.getTable(foreign_table_id);

						// check foreign element

						if (foreign_table.fields.parallelStream().anyMatch(field -> field.element && field.xname.equals(_field_xname))) {

							try {
								return new XPathSqlExpr(schema, path, foreign_table, _field_xname, null, null, terminus);
							} catch (PgSchemaException e2) {
							}

						}

						// check foreign nested_key

						if (foreign_table.virtual) {

							Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__ft_ids != null && __ft_ids.length > 0)
								_ft_ids = __ft_ids;

						}

					}

					ft_ids = _ft_ids;

					if (touched_ft_ids.size() == _touched_size)
						break;

				}

				touched_ft_ids.clear();
				break;
			case simple_content:
				touched_ft_ids = new HashSet<Integer>();

				ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				_ft_ids = null;

				while (ft_ids != null && ft_ids.length > 0) {

					int _touched_size = touched_ft_ids.size();

					PgTable foreign_table;

					for (Integer foreign_table_id : ft_ids) {

						if (!touched_ft_ids.add(foreign_table_id))
							continue;

						foreign_table = schema.getTable(foreign_table_id);

						// check foreign simple_cont

						if (foreign_table.fields.parallelStream().anyMatch(field -> field.simple_content && !field.simple_attribute)) {

							try {
								return new XPathSqlExpr(schema, path, foreign_table, _field_xname, null, null, terminus);
							} catch (PgSchemaException e2) {
							}

						}

						// check foreign nested_key

						if (foreign_table.virtual) {

							Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__ft_ids != null && __ft_ids.length > 0)
								_ft_ids = __ft_ids;

						}

					}

					ft_ids = _ft_ids;

					if (touched_ft_ids.size() == _touched_size)
						break;

				}

				touched_ft_ids.clear();
				break;
			case attribute:
				touched_ft_ids = new HashSet<Integer>();

				ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				_ft_ids = null;

				while (ft_ids != null && ft_ids.length > 0) {

					int _touched_size = touched_ft_ids.size();

					PgTable foreign_table;
					Optional<PgField> opt;

					for (Integer foreign_table_id : ft_ids) {

						if (!touched_ft_ids.add(foreign_table_id))
							continue;

						foreign_table = schema.getTable(foreign_table_id);

						// check foreign attribute

						opt = foreign_table.fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond) && (field.attribute ? field.xname.equals(_field_xname) : field.foreign_table_xname.equals(_field_xname))).findFirst();

						if (opt.isPresent()) {

							try {
								return new XPathSqlExpr(schema, path, foreign_table, _field_xname, null, null, terminus);
							} catch (PgSchemaException e2) {
							}

						}

						// check foreign nested_key

						if (foreign_table.virtual || foreign_table.has_nested_key_as_attr) {

							Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__ft_ids != null && __ft_ids.length > 0)
								_ft_ids = __ft_ids;

						}

					}

					ft_ids = _ft_ids;

					if (touched_ft_ids.size() == _touched_size)
						break;

				}

				touched_ft_ids.clear();
				break;
			case any_element:
				touched_ft_ids = new HashSet<Integer>();

				ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				_ft_ids = null;

				while (ft_ids != null && ft_ids.length > 0) {

					int _touched_size = touched_ft_ids.size();

					PgTable foreign_table;

					for (Integer foreign_table_id : ft_ids) {

						if (!touched_ft_ids.add(foreign_table_id))
							continue;

						foreign_table = schema.getTable(foreign_table_id);

						// check foreign attribute

						if (foreign_table.fields.parallelStream().anyMatch(field -> field.any)) {

							pg_xpath_code = "xpath('/" + (has_prefix ? pg_xpath_prefix_ : "") + foreign_table.pname + "/" + (has_prefix ? pg_xpath_prefix_ : "") + _path[position].replace(" @", "/@").replace(" ", "/" + (has_prefix ? pg_xpath_prefix_: "")) + (attr_in_any ? "" : "/text()") + "', "
									+ schema.getPgNameOf(foreign_table) + "." + PgSchemaUtil.avoidPgReservedWords(_field_xname) + (has_prefix ? ", ARRAY[ARRAY['" + pg_xpath_prefix + "', '" + schema.getNamespaceUriForPrefix("")+ "']]" : "") + ")::text[]";

							try {
								return new XPathSqlExpr(schema, path, foreign_table, _field_xname, pg_xpath_code, null, terminus);
							} catch (PgSchemaException e2) {
							}

						}

						// check foreign nested_key

						if (foreign_table.virtual || foreign_table.bridge) {

							Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__ft_ids != null && __ft_ids.length > 0)
								_ft_ids = __ft_ids;

						}

					}

					ft_ids = _ft_ids;

					if (touched_ft_ids.size() == _touched_size)
						break;

				}

				touched_ft_ids.clear();
				break;
			case any_attribute:
				touched_ft_ids = new HashSet<Integer>();

				ft_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				_ft_ids = null;

				while (ft_ids != null && ft_ids.length > 0) {

					int _touched_size = touched_ft_ids.size();

					PgTable foreign_table;

					for (Integer foreign_table_id : ft_ids) {

						if (!touched_ft_ids.add(foreign_table_id))
							continue;

						foreign_table = schema.getTable(foreign_table_id);

						// check foreign attribute

						if (foreign_table.fields.parallelStream().anyMatch(field -> field.any_attribute)) {

							pg_xpath_code = "xpath('/" + (has_prefix ? pg_xpath_prefix_ : "") + foreign_table.pname + "/" + _path[position].replace(" ", "/" + (has_prefix ? pg_xpath_prefix_: "")) + "', "
									+ schema.getPgNameOf(foreign_table) + "." + PgSchemaUtil.avoidPgReservedWords(_field_xname) + (has_prefix ? ", ARRAY[ARRAY['" + pg_xpath_prefix + "', '" + schema.getNamespaceUriForPrefix("")+ "']]" : "") + ")::text[]";

							try {
								return new XPathSqlExpr(schema, path, foreign_table, _field_xname, pg_xpath_code, null, terminus);
							} catch (PgSchemaException e2) {
							}

						}

						// check foreign nested_key

						if (foreign_table.virtual || foreign_table.bridge) {

							Integer[] __ft_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__ft_ids != null && __ft_ids.length > 0)
								_ft_ids = __ft_ids;

						}

					}

					ft_ids = _ft_ids;

					if (touched_ft_ids.size() == _touched_size)
						break;

				}

				touched_ft_ids.clear();
				break;
			default:
				return null;
			}

		}

		return null;
	}

	/**
	 * Show SQL string expressions.
	 */
	public void showSqlExpr() {

		path_exprs.forEach(path_expr -> System.out.println(path_expr.getReadablePath() + " (terminus type: " + path_expr.terminus.name() + ") -> " + path_expr.sql));

	}

	// XPath parser for identity constraint definition of XML Schema

	/**
	 * Instance of XPath parse tree serializer (XPath parser for identity constraint definition of XML Schema).
	 *
	 * @param tree XPath parse tree
	 */
	public XPathCompList(ParseTree tree) {

		comps = new ArrayList<XPathComp>();

		if (!testParserTree(tree, " "))
			return;

		union_counter = step_counter = 0;

		serializeTree(tree);

	}

	/**
	 * Return array of the last QName component.
	 *
	 * @return XPathComp[] array of the last Qname component
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathComp[] getLastQNameComp() throws PgSchemaException {

		int union_size = getLastUnionId() + 1;

		XPathComp[] last_named_comp = new XPathComp[union_size], comps;
		Class<?> anyClass;

		for (int union_id = 0; union_id < union_size; union_id++) {

			for (int step_id = 0; step_id <= getLastStepId(union_id); step_id++) {

				comps = arrayOf(union_id, step_id);

				for (XPathComp comp : comps) {

					anyClass = comp.tree.getClass();

					// TerminalNodeImpl node

					if (anyClass.equals(TerminalNodeImpl.class))
						continue;

					// AbbreviatedStepContext node

					else if (anyClass.equals(AbbreviatedStepContext.class))
						continue;

					// AxisSpecifierContext node

					else if (anyClass.equals(AxisSpecifierContext.class))
						continue;

					// NCNameContext node

					else if (anyClass.equals(NCNameContext.class))
						last_named_comp[union_id] = comp;

					// NodeTestContext node

					else if (anyClass.equals(NodeTestContext.class))
						continue;

					// NameTestContext node

					else if (anyClass.equals(NameTestContext.class))
						last_named_comp[union_id] = comp;

					// PredicateContext node

					else if (anyClass.equals(PredicateContext.class))
						break;

					else
						throw new PgSchemaException(comp.tree);

				}

			}

		}

		return last_named_comp;
	}

}
