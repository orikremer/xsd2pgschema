/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017 Masashi Yokochi

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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;

import com.github.antlr.grammars_v4.xpath.xpathParser.AbsoluteLocationPathNorootContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.AdditiveExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.AndExprContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.EqualityExprContext;
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

/**
 * XPath component list.
 *
 * @author yokochi
 */
public class XPathCompList {

	/** Serialized XPath parse tree. */
	public List<XPathComp> comps = null;

	/** Instance of path expression. */
	public List<XPathExpr> path_exprs = null;

	/** Instance of XPath predicate expression. */
	public List<XPathPredicateExpr> predicates = null;

	/** XPath variable reference. */
	public HashMap<String, String> variables = null;

	/** Whether UnionExprNoRootContext node exists or not. */
	private boolean union_expr = false;

	/** Whether add serial key in PostgreSQL DDL. */
	private boolean serial_key = false;

	/** Whether retain case sensitive name in PostgreSQL DDL. */
	private boolean case_sense = true;

	/** The PostgreSQL data model. */
	private PgSchema schema = null;

	/** Instance of path expression for stacking while UnionExprNoRootContext node. */
	private List<XPathExpr> path_exprs_union = null;

	/**
	 * Serialize XPath parse tree.
	 *
	 * @param schema PostgreSQL data model
	 * @param tree XPath parse tree
	 * @param variables XPath variable reference
	 * @param verbose whether outputs result or not
	 */
	public XPathCompList(PgSchema schema, ParseTree tree, HashMap<String, String> variables, boolean verbose) {

		this.schema = schema;

		serial_key = schema.option.serial_key;
		case_sense = schema.option.case_sense;

		comps = new ArrayList<XPathComp>();

		path_exprs = new ArrayList<XPathExpr>();

		if (verbose)
			System.out.println("Abstract syntax tree of query: '" + tree.getText() + "'");

		if (!testPathTree(tree, verbose, " "))
			return;

		union_counter = step_counter = 0;
		wild_card = false;

		if (verbose)
			System.out.println("\nSerialized axis and node-test of query: '"+ tree.getText() + "'");

		serializeTree(tree, verbose);

		if (verbose)
			System.out.println();

		this.variables = variables;

	}

	/**
	 * Serialize XPath parse tree (temporary use only).
	 */
	public XPathCompList() {

		path_exprs = new ArrayList<XPathExpr>();

	}

	/**
	 * Clear XPathCompList.
	 */
	public void clear() {

		if (comps != null)
			comps.clear();

		if (path_exprs != null)
			path_exprs.clear();

		if (path_exprs_union != null)
			path_exprs_union.clear();

		if (predicates != null)
			predicates.clear();

	}

	/**
	 * Clear path expressions.
	 */
	protected void clearPathExprs() {

		if (path_exprs != null)
			path_exprs.clear();

	}

	/**
	 * Return whether XPath parse tree is effective.
	 *
	 * @param tree XPath parse tree
	 * @param verbose whether outputs XPath parse tree or not
	 * @param indent indent code for output
	 * @return boolean whether valid or not
	 */
	private boolean testPathTree(ParseTree tree, boolean verbose, String indent) {

		boolean valid = false;

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			boolean has_children = child.getChildCount() > 1;

			if (verbose)
				System.out.println(indent + child.getClass().getSimpleName() + " '" + child.getText() + "' " + child.getSourceInterval().toString());

			if (testPathTree(child, verbose, indent + " ") || has_children)
				valid = true;

		}

		return valid;
	}

	/** The union counter. */
	private int union_counter;

	/** The step counter. */
	private int step_counter;

	/** Whether wild card follows or not. */
	private boolean wild_card;

	/**
	 * Serialize XPath parse tree to XPath component list.
	 *
	 * @param tree XPath parse tree
	 * @param verbose whether outputs XPath component list or not
	 */
	private void serializeTree(ParseTree tree, boolean verbose) {

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			Class<?> anyClass = child.getClass();
			String text = child.getText();

			if (anyClass.equals(TerminalNodeImpl.class)) {

				if (text.equals("*") && tree.getClass().equals(MultiplicativeExprContext.class))
					continue;

				if (!text.equals("/") && !text.equals("//") && !text.equals("|") && !text.equals("*")) {
					union_counter++;
					continue;
				}

				boolean union_expr = text.equals("|");

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

				traceChildOfStepContext(child, verbose);

				if (verbose)
					System.out.println();

				if (!wild_card)
					step_counter++;

				continue;
			}

			serializeTree(child, verbose);

		}

	}

	/**
	 * Trace child node of StepContext node.
	 *
	 * @param tree XPath parse tree
	 * @param verbose whether outputs XPath component list or not
	 */
	private void traceChildOfStepContext(ParseTree tree, boolean verbose) {

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			Class<?> anyClass = child.getClass();
			String text = child.getText();

			if (hasEffectiveChildOfQNameContext(child)) {

				XPathComp comp = new XPathComp(union_counter, step_counter, child);

				comps.add(comp);

				if (verbose)
					System.out.print(" " + anyClass.getSimpleName() + " '" + text + "'");

				// no need to trace more

				break;
			}

			else if (hasChildOfTerminalNodeImpl(child)) {

				XPathComp comp = new XPathComp(union_counter, step_counter, child);

				comps.add(comp);

				if (verbose)
					System.out.print(" " + anyClass.getSimpleName() + " '" + text + "'");

				// no need to trace prefix

				if (anyClass.equals(NameTestContext.class))
					break;

			}

			traceChildOfStepContext(child, verbose);

		}

	}

	/**
	 * Return whether current tree has effective QNameContext child node.
	 *
	 * @param tree XPath parse tree
	 * @return boolean whether curren tree has effective QNameContext child node
	 */
	private boolean hasEffectiveChildOfQNameContext(ParseTree tree) {

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

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

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

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

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

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

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

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

			for (int i = 0; i < tree.getChildCount(); i++) {

				ParseTree child = tree.getChild(i);

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
	protected int getLastUnionId() {
		return comps.size() > 0 ? comps.get(comps.size() - 1).union_id : -1;
	}

	/**
	 * Return the last step id.
	 *
	 * @param union_id union id
	 * @return int the last step id
	 */
	protected int getLastStepId(int union_id) {

		if (comps.stream().filter(comp -> comp.union_id == union_id).count() == 0)
			return -1;

		return comps.stream().filter(comp -> comp.union_id == union_id).max(Comparator.comparingInt(comp -> comp.step_id)).get().step_id;
	}

	/**
	 * Return array of given XPath component.
	 *
	 * @param union_id current union id
	 * @return XPathComp[] array of XPath component
	 */
	protected XPathComp[] arrayOf(int union_id) {
		return comps.stream().filter(comp -> comp.union_id == union_id).toArray(XPathComp[]::new);
	}

	/**
	 * Return array of given XPath component.
	 *
	 * @param union_id current union id
	 * @param step_id current step id
	 * @return XPathComp[] array of XPath component
	 */
	protected XPathComp[] arrayOf(int union_id, int step_id) {
		return comps.stream().filter(comp -> comp.union_id == union_id && comp.step_id == step_id).toArray(XPathComp[]::new);
	}

	/**
	 * Return array of PredicateContext of given XPath component.
	 *
	 * @param union_id current union id
	 * @param step_id current step id
	 * @return XPathComp[] array of PredicateContext XPath component
	 */
	protected XPathComp[] arrayOfPredicateContext(int union_id, int step_id) {
		return comps.stream().filter(comp -> comp.union_id == union_id && comp.step_id == step_id && comp.tree.getClass().equals(PredicateContext.class)).toArray(XPathComp[]::new);
	}

	/**
	 * Return size of path expression in XPath component list.
	 *
	 * @return int size of path expression in XPath component list
	 */
	protected int sizeOfPathExpr() {
		return getLastUnionId() + 1 - (int) comps.stream().filter(comp -> comp.tree.getClass().equals(TerminalNodeImpl.class) && comp.tree.getText().equals("|")).count();
	}

	/**
	 * Return previous step of XPath component.
	 *
	 * @param comp current XPath component in list
	 * @return XPathComp parent XPath component
	 */
	protected XPathComp previousOf(XPathComp comp) {

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
	protected void add(XPathExpr path_expr) {

		path_exprs.add(path_expr);

	}

	/**
	 * Add all path expressions.
	 *
	 * @param list XPath component list
	 */
	protected void addAll(XPathCompList list) {

		path_exprs.addAll(list.path_exprs);

	}

	/**
	 * Test TerminalNodeImpl node.
	 *
	 * @param comp current XPath component
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void testTerminalNodeImpl(XPathComp comp, boolean predicate) throws PgSchemaException {

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
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void testAbbreviateStepContext(XPathComp comp, boolean predicate) throws PgSchemaException {

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
	protected void testAxisSpecifierContext(XPathComp comp, XPathComp[] comps) throws PgSchemaException {

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
	 * Test NCNameContext node having parent axis.
	 *
	 * @param comp current XPath component
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void testNCNameContextWithParentAxis(XPathComp comp, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

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
	 * @param inc_self whether include self node or not
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void testNCNameContextWithAncestorAxis(XPathComp comp, boolean inc_self, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

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
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNCNameContext(XPathComp comp, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = comp.tree.getText();

		Iterator<XPathExpr> iter = path_exprs.iterator();

		while (iter.hasNext()) {

			XPathExpr path_expr = iter.next();

			String[] _path = path_expr.path.split("/");
			String last_path = _path[_path.length - 1];

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
	 * Test NodeTestContext node having parent axis.
	 *
	 * @param comp current XPath component
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void testNodeTestContextWithParentAxis(XPathComp comp, boolean predicate) throws PgSchemaException {

		if (selectParentPath() == 0) {

			if (predicate)
				return;

			throw new PgSchemaException(comp.tree, previousOf(comp).tree);

		}

	}

	/**
	 * Test NodeTestContext node having ancestor axis.
	 *
	 * @param comp current XPath component
	 * @param inc_self whether include self node or not
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void testNodeTestContextWithAncestorAxis(XPathComp comp, boolean inc_self, boolean predicate) throws PgSchemaException {

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
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void testNameTestContextWithParentAxis(XPathComp comp, String namespace_uri, String local_part, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

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
	 * @param inc_self whether include self node or not
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void testNameTestContextWithAncestorAxis(XPathComp comp, String namespace_uri, String local_part, boolean inc_self, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

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
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @param predicate whether XPath component in predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testNameTestContext(XPathComp comp, String namespace_uri, String local_part, boolean wild_card, String composite_text, boolean predicate) throws PgSchemaException {

		String text = wild_card ? composite_text : local_part;

		Iterator<XPathExpr> iter = path_exprs.iterator();

		while (iter.hasNext()) {

			XPathExpr path_expr = iter.next();

			String[] name = path_expr.path.split("/");

			int len = name.length;

			PgTable table;

			switch (path_expr.terminus) {
			case table:
				if (len < 1)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				table = schema.getTable(name[name.length - 1]);

				if (table == null)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				if (!table.target_namespace.equals(namespace_uri) || !schema.matchesNodeName(table.name, text, wild_card))
					iter.remove();
				break;
			case element:
			case simple_content:
			case attribute:
				if (len < 2)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				table = schema.getTable(name[name.length - 2]);

				if (table == null)
					throw new PgSchemaException(comp.tree, previousOf(comp).tree);

				String field_name = name[name.length - 1];

				PgField field = table.getField(field_name);

				if (field != null) {

					if (!field.target_namespace.equals(namespace_uri) || !schema.matchesNodeName(field.xname, text, wild_card))
						iter.remove();

					continue;
				}

				Integer[] foreign_table_ids = table.fields.stream().filter(_field -> _field.nested_key).map(_field -> _field.foreign_table_id).toArray(Integer[]::new);
				Integer[] _foreign_table_ids = null;

				boolean found_field = false;

				while (foreign_table_ids != null && foreign_table_ids.length > 0 && !found_field) {

					for (Integer foreign_table_id : foreign_table_ids) {

						PgTable foreign_table = schema.getTable(foreign_table_id);

						PgField foreign_field = foreign_table.getField(field_name);

						if (foreign_field != null) {

							if (!foreign_field.target_namespace.equals(namespace_uri) || !schema.matchesNodeName(foreign_field.xname, text, wild_card))
								iter.remove();

							found_field = true;

							break;
						}

						if (foreign_table.virtual && !found_field) {

							Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(_field -> _field.nested_key).map(_field -> _field.foreign_table_id).toArray(Integer[]::new);

							if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
								_foreign_table_ids = __foreign_table_ids;

						}

					}

					foreign_table_ids = _foreign_table_ids;

				}

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
	 * Return whether any path ends with table node.
	 *
	 * @return boolean whether any path ends with table node
	 */
	public boolean hasPathEndsWithTableNode() {
		return path_exprs.stream().anyMatch(path_expr -> path_expr.terminus.equals(XPathCompType.table));
	}

	/**
	 * Return whether any path ends with text node.
	 *
	 * @return boolean whether any path ends with text node
	 */
	public boolean hasPathEndsWithTextNode() {
		return path_exprs.stream().anyMatch(path_expr -> !path_expr.terminus.equals(XPathCompType.table) && !path_expr.terminus.equals(XPathCompType.element) && !path_expr.terminus.equals(XPathCompType.simple_content) && !path_expr.terminus.equals(XPathCompType.attribute));
	}

	/**
	 * Return whether any path ends without text node.
	 *
	 * @return boolean whether any path ends without text node
	 */
	public boolean hasPathEndsWithoutTextNode() {
		return path_exprs.stream().anyMatch(path_expr -> path_expr.terminus.equals(XPathCompType.table) || path_expr.terminus.equals(XPathCompType.element) || path_expr.terminus.equals(XPathCompType.simple_content) || path_expr.terminus.equals(XPathCompType.attribute));
	}

	/**
	 * Remove any path that ends with table node.
	 *
	 * @return int the number of survived paths
	 */
	protected int removePathEndsWithTableNode() {

		path_exprs.removeIf(path_expr -> path_expr.terminus.equals(XPathCompType.table));

		return path_exprs.size();
	}

	/**
	 * Remove any path that ends with field node.
	 *
	 * @return int the number of survived paths
	 */
	protected int removePathEndsWithFieldNode() {

		path_exprs.removeIf(path_expr -> path_expr.terminus.equals(XPathCompType.element) || path_expr.terminus.equals(XPathCompType.simple_content) || path_expr.terminus.equals(XPathCompType.attribute));

		return path_exprs.size();
	}

	/**
	 * Remove any path that ends with text node.
	 *
	 * @return int the number of survived paths
	 */
	protected int removePathEndsWithTextNode() {

		path_exprs.removeIf(path_expr -> path_expr.terminus.equals(XPathCompType.simple_content) || path_expr.terminus.equals(XPathCompType.text) || path_expr.terminus.equals(XPathCompType.comment) || path_expr.terminus.equals(XPathCompType.processing_instruction));

		return path_exprs.size();
	}

	/**
	 * Remove any path that ends without table node.
	 *
	 * @return int the number of survived paths
	 */
	protected int removePathEndsWithoutTableNode() {

		path_exprs.removeIf(path_expr -> !path_expr.terminus.equals(XPathCompType.table));

		return path_exprs.size();
	}

	/**
	 * Remove any path that ends without field node.
	 *
	 * @return int the number of survived paths
	 */
	protected int removePathEndsWithoutFieldNode() {

		path_exprs.removeIf(path_expr -> !path_expr.terminus.equals(XPathCompType.element) && !path_expr.terminus.equals(XPathCompType.simple_content) && !path_expr.terminus.equals(XPathCompType.attribute));

		return path_exprs.size();
	}

	/**
	 * Remove any path that ends without text node.
	 *
	 * @return int the number of survived paths
	 */
	protected int removePathEndsWithoutTextNode() {

		path_exprs.removeIf(path_expr -> path_expr.terminus.equals(XPathCompType.table) || path_expr.terminus.equals(XPathCompType.element) || path_expr.terminus.equals(XPathCompType.simple_content) || path_expr.terminus.equals(XPathCompType.attribute));

		return path_exprs.size();
	}

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

			if (!parent_path_exprs.stream().anyMatch(src_path_expr -> path_expr.path.startsWith(src_path_expr.path)))
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

			if (path_exprs.stream().filter(path_expr -> path_expr.path.equals(_path_expr.path)).count() > 1) {

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
	protected boolean isAbsolutePath(int union_id) {

		if (path_exprs.isEmpty()) {

			XPathComp first_comp = comps.stream().filter(comp -> comp.union_id == union_id && comp.step_id == 0).findFirst().get();

			return first_comp.tree.getClass().equals(TerminalNodeImpl.class) && first_comp.tree.getText().equals("/");
		}

		return !path_exprs.get(0).path.endsWith("//");
	}

	/**
	 * Append abbreviation path of all paths.
	 */
	protected void appendAbbrevPath() {

		path_exprs.forEach(path_expr -> path_expr.path = path_expr.path + "//");

	}

	/**
	 * Append text node.
	 */
	protected void appendTextNode() {

		List<XPathExpr> _path_exprs = new ArrayList<XPathExpr>(path_exprs);

		path_exprs.clear();

		_path_exprs.forEach(path_expr -> {

			switch (path_expr.terminus) {
			case element:
			case simple_content:
			case attribute:
				path_exprs.add(new XPathExpr(path_expr.path + "/" + PgSchemaUtil.text_node_name, XPathCompType.text, path_expr.terminus));
				break;
			default:
				path_exprs.add(path_expr);
			}

		});

		_path_exprs.clear();

	}

	/**
	 * Append comment node.
	 */
	protected void appendCommentNode() {

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
	protected void appendProcessingInstructionNode(String expression) {

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

			path_expr.path = getParentPath(path_expr.path);
			path_expr.terminus = getParentTerminus(path_expr);

		});

		path_exprs.removeIf(path_expr -> path_expr.path.isEmpty());

		return path_exprs.size();
	}

	/**
	 * Return parent path.
	 *
	 * @param path current path
	 * @return String parent path
	 */
	private String getParentPath(String path) {

		String[] _path = path.split("/");

		StringBuilder sb = new StringBuilder();

		for (int i = 1; i < _path.length - 1; i++)
			sb.append("/" + _path[i]);

		return sb.toString();
	}

	/**
	 * Return parent terminus.
	 *
	 * @param path_expr current path expression
	 * @return XPathCompType parent terminus type
	 */
	private XPathCompType getParentTerminus(XPathExpr path_expr) {

		switch (path_expr.terminus) {
		case table:
		case element:
		case simple_content:
		case attribute:
			return XPathCompType.table;
		default:
			return path_expr.prev_term;
		}

	}

	/**
	 * Replace path expressions.
	 *
	 * @param src_list source XPath component list
	 */
	public void replacePathExprs(XPathCompList src_list) {

		replacePathExprs(src_list.path_exprs);

	}

	/**
	 * Replace path expressions.
	 *
	 * @param src_predicate source XPath predicate
	 */
	public void replacePathExprs(XPathPredicateExpr src_predicate) {

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
	protected void applyUnionExpr() {

		if (!union_expr)
			return;

		path_exprs_union.addAll(path_exprs);

		path_exprs = new ArrayList<XPathExpr>(path_exprs_union);

		path_exprs_union.clear();

		union_expr = false;

	}

	/**
	 * Show path expressions.
	 *
	 * @param indent indent code for output
	 */
	public void showPathExprs(String indent) {

		path_exprs.forEach(path_expr -> System.out.println(indent + path_expr.path + " (terminus type: " + path_expr.terminus.name() + ")"));

	}

	/**
	 * Translate to path expression to SQL expression.
	 *
	 * @param verbose whether output reversed XPath parse tree or not
	 */
	public void path2Sql(boolean verbose) {

		path_exprs.forEach(path_expr -> {

			String table_name;
			String column_name;

			String path = path_expr.path;
			XPathCompType terminus = path_expr.terminus;

			switch (terminus) {
			case text:
				path = getParentPath(path);
				terminus = getParentTerminus(path_expr);
			case element:
			case simple_content:
			case attribute:
				XPathSqlExpr sql_expr = schema.getXPathSqlExprOfPath(path, terminus);
				table_name = sql_expr.table_name;
				column_name = sql_expr.column_name;
				if (!case_sense) {
					table_name = table_name.toLowerCase();
					column_name = column_name.toLowerCase();
				}
				try {
					path_expr.setSubjectSql(new XPathSqlExpr(schema, path, table_name, column_name, null, terminus));
				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}
				break;
			case table:
				table_name = schema.getTableNameOfPath(path);
				if (!case_sense)
					table_name = table_name.toLowerCase();
				try {
					path_expr.setSubjectSql(new XPathSqlExpr(schema, path, table_name, "*", null, terminus));
				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}
				break;
			default:
				try {
					throw new PgSchemaException("Couldn't retrieve " + path_expr.terminus.name() + " via SQL.");
				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}

			Set<XPathComp> src_comps = null;

			if (predicates != null && predicates.size() > 0) {

				String _path = path; // finalized

				src_comps = predicates.stream().filter(predicate -> _path.equals(predicate.src_path_expr.path)).map(predicate -> predicate.src_comp).collect(Collectors.toSet());

				try {

					for (XPathComp src_comp : src_comps) {

						if (verbose)
							System.out.println("\nReversed abstract syntax tree of predicate: '" + src_comp.tree.getText() + "'");

						testPredicateTree2SqlExpr(src_comp, path_expr, verbose);

					}

				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}

			StringBuilder sb = new StringBuilder();

			try {

				HashMap<String, String> target_tables = new HashMap<String, String>(); // key = table name, value = table path
				HashMap<String, String> joined_tables = new HashMap<String, String>();

				target_tables.put(path_expr.sql_subject.table_name, path_expr.terminus.equals(XPathCompType.table) ? path_expr.sql_subject.path : getParentPath(path_expr.sql_subject.path));

				path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.table_name != null).forEach(sql_expr -> target_tables.put(sql_expr.table_name, sql_expr.terminus.equals(XPathCompType.table) ? sql_expr.path : getParentPath(sql_expr.path)));

				boolean single = target_tables.size() == 1;

				sb.append("SELECT ");

				appendSqlColumnName(single, path_expr.sql_subject, sb);

				sb.append(" FROM ");

				target_tables.forEach((_table_name_, _path_) -> appendSqlTableName(_table_name_, sb));

				sb.setLength(sb.length() - 2); // remove last ", "

				if (src_comps != null) {

					sb.append(" WHERE ");

					for (XPathComp src_comp : src_comps) {

						translatePredicateTree2SqlImpl(src_comp, path_expr, single, sb);
						sb.append(" AND ");

					}

					// remove subject table from target

					joined_tables.put(path_expr.sql_subject.table_name, target_tables.get(path_expr.sql_subject.table_name));
					target_tables.remove(path_expr.sql_subject.table_name);

					joinSqlTables(target_tables, joined_tables, sb);

					sb.setLength(sb.length() - 5); // remove last " AND "

				}

				target_tables.clear();
				joined_tables.clear();

				path_expr.sql = sb.toString();

			} catch (PgSchemaException e) {
				e.printStackTrace();
				System.exit(1);
			} finally {
				sb.setLength(0);
				path_expr.sql_predicates.clear();
			}

		});

	}

	/**
	 * Append SQL table name.
	 *
	 * @param table_name SQL table name
	 * @param sb StringBuilder to store SQL expression
	 */
	private void appendSqlTableName(String table_name, StringBuilder sb) {

		boolean lower_case = table_name.equals(table_name.toLowerCase());

		if (case_sense && !lower_case)
			sb.append("\"");

		sb.append(table_name);

		if (case_sense && !lower_case)
			sb.append("\"");

		sb.append(", ");

	}

	/**
	 * Append SQL column name.
	 *
	 * @param single whether single table expression
	 * @param sql_expr SQL expression
	 * @param sb StringBuilder to store SQL expression
	 */
	private void appendSqlColumnName(boolean single, XPathSqlExpr sql_expr, StringBuilder sb) {

		if (sql_expr.unary_oprator != null)
			sb.append(sql_expr.unary_oprator);

		if (!single) {

			boolean lower_case = sql_expr.table_name.equals(sql_expr.table_name.toLowerCase());

			if (case_sense && !lower_case)
				sb.append("\"");

			sb.append(sql_expr.table_name);

			if (case_sense && !lower_case)
				sb.append("\"");

			sb.append(".");

		}

		boolean lower_case = sql_expr.column_name.equals(sql_expr.column_name.toLowerCase());

		if (case_sense && !lower_case && !sql_expr.column_name.equals("*"))
			sb.append("\"");

		sb.append(sql_expr.column_name);

		if (case_sense && !lower_case && !sql_expr.column_name.equals("*"))
			sb.append("\"");

	}

	/**
	 * Append SQL column name.
	 *
	 * @param single whether single table expression
	 * @param table_name SQL table name
	 * @param column_name SQL column name
	 * @param sb StringBuilder to store SQL expression
	 */
	private void appendSqlColumnName(boolean single, String table_name, String column_name, StringBuilder sb) {

		if (!single) {

			boolean lower_case = table_name.equals(table_name.toLowerCase());

			if (case_sense && !lower_case)
				sb.append("\"");

			sb.append(table_name);

			if (case_sense && !lower_case)
				sb.append("\"");

			sb.append(".");

		}

		boolean lower_case = column_name.equals(column_name.toLowerCase());

		if (case_sense && !lower_case && !column_name.equals("*"))
			sb.append("\"");

		sb.append(column_name);

		if (case_sense && !lower_case && !column_name.equals("*"))
			sb.append("\"");

	}

	/** path expression counter in predicate. */
	private int path_expr_counter;

	/**
	 * Translate predicate expression to SQL expression.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr source path expression
	 * @param verbose whether outputs reversed XPath parse tree or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testPredicateTree2SqlExpr(XPathComp src_comp, XPathExpr src_path_expr, boolean verbose) throws PgSchemaException {

		path_expr_counter = 0;

		if (!testPredicateTree(src_comp, src_path_expr, src_comp.tree, false, verbose, " "))
			src_path_expr.sql_predicates.clear();

	}

	/**
	 * Return whether XPath parse tree of predicate is effective.
	 *
	 * @param path_expr current path
	 * @param src_comp XPath component of source predicate
	 * @param tree XPath parse tree
	 * @param has_children whether parent has children or not
	 * @param verbose whether outputs reversed XPath parse tree or not
	 * @param indent indent code for output
	 * @return boolean whether valid predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private boolean testPredicateTree(XPathComp src_comp, XPathExpr src_path_expr, ParseTree tree, boolean has_children, boolean verbose, String indent) throws PgSchemaException {

		boolean valid = false;

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class))
				continue;

			boolean _has_children = !child.getText().isEmpty() && (hasEffectiveChildren(child) || hasChildOfTerminalNodeImpl(child));

			if (testPredicateTree(src_comp, src_path_expr, child, _has_children, verbose, indent + " ") || _has_children)
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

				// arbitrary PathContet node

				if (isPathContextClass(currentClass))
					testPathContext(src_comp, src_path_expr, parent, tree);

				// PrimaryExprContext node

				else if (currentClass.equals(PrimaryExprContext.class))
					testPrimaryExprContext(src_comp, src_path_expr, parent, tree);

				// VariableReferenceContext node

				else if (currentClass.equals(VariableReferenceContext.class))
					testVariableReferenceContext(src_comp, src_path_expr, parent, tree);

				// EqualityExprContext node

				else if (currentClass.equals(EqualityExprContext.class))
					testEqualityExprContext(src_comp, src_path_expr, parent, tree);

				// RelationalExprContext node

				else if (currentClass.equals(RelationalExprContext.class))
					testRelationalExprContext(src_comp, src_path_expr, parent, tree);

				// AdditiveExprContext node

				else if (currentClass.equals(AdditiveExprContext.class))
					testAdditiveExprContext(src_comp, src_path_expr, parent, tree, 0);

				// MultiplicativeExprContext node

				else if (currentClass.equals(MultiplicativeExprContext.class))
					testMultiplicativeExprContext(src_comp, src_path_expr, parent, tree);

				// UnaryExprNoRootContext node

				else if (currentClass.equals(UnaryExprNoRootContext.class))
					testUnaryExprNoRootContext(src_comp, src_path_expr, parent, tree);

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

		int pred_path_size = (int) predicates.stream().filter(predicate -> predicate.src_comp.equals(src_comp)).count();

		if (path_expr_counter >= pred_path_size)
			throw new PgSchemaException(tree);

		// designation of predicate

		XPathPredicateExpr predicate = predicates.stream().filter(_predicate -> _predicate.src_comp.equals(src_comp)).toArray(XPathPredicateExpr[]::new)[path_expr_counter];

		predicate.dst_path_exprs.forEach(path_expr -> {

			String table_name;
			String column_name;

			String path = path_expr.path;
			XPathCompType terminus = path_expr.terminus;

			switch (terminus) {
			case text:
				path = getParentPath(path);
				terminus = getParentTerminus(path_expr);
			case element:
			case simple_content:
			case attribute:
				XPathSqlExpr sql_expr = schema.getXPathSqlExprOfPath(path, terminus);
				table_name = sql_expr.table_name;
				column_name = sql_expr.column_name;
				if (!case_sense) {
					table_name = table_name.toLowerCase();
					column_name = column_name.toLowerCase();
				}
				try {
					src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, path, table_name, column_name, null, terminus, parent, tree));
				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}
				break;
			case table:
				table_name = schema.getTableNameOfPath(path);
				if (!case_sense)
					table_name = table_name.toLowerCase();
				try {
					src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, path, table_name, "*", null, terminus, parent, tree));
				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}
				break;
			default:
				try {
					throw new PgSchemaException("Couldn't retrieve " + path_expr.terminus.name() + " via SQL.");
				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}

		});

		path_expr_counter++;

	}

	/**
	 * Test PrimaryExprContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testPrimaryExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, tree.getText(), XPathCompType.text, parent, tree));

	}

	/**
	 * Test VariableReferenceContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testVariableReferenceContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		String text = tree.getText();

		if (variables == null || !text.startsWith("$"))
			throw new PgSchemaException(tree);

		String value = variables.get(text.substring(1));

		if (value == null)
			throw new PgSchemaException(tree);

		src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, value, XPathCompType.text, parent, tree));

	}

	/**
	 * Test EqualityExprContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testEqualityExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		if (sql_predicates.size() != 2)
			throw new PgSchemaException(tree);

		String terminal_code = getTextOfChildTerminalNodeImpl(tree);

		boolean equality = terminal_code.equals("=");

		int predicate_count = sizeOfPredicate(sql_predicates);

		int first_sql_id;

		switch (predicate_count) {
		case 2:
			boolean _equality = sql_predicates.get(0).predicate.equals(sql_predicates.get(1).predicate);

			if (equality != _equality)
				throw new PgSchemaException(tree);

			src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			break;
		case 1:
			first_sql_id = sql_predicates.get(0).predicate == null || sql_predicates.get(1).predicate != null ? 0 : 1;

			XPathSqlExpr sql_relation = sql_predicates.get(first_sql_id);
			XPathSqlExpr sql_predicate = sql_predicates.get((first_sql_id + 1) % 2);

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, sql_relation.path, sql_relation.table_name, sql_relation.column_name, sql_predicate.predicate, sql_relation.terminus, parent, tree, null, terminal_code));

			src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			break;
		case 0:
			if (sql_predicates.get(0).equalsRelationally(sql_predicates.get(1))) {

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
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testRelationalExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		if (sql_predicates.size() != 2)
			throw new PgSchemaException(tree);

		String terminal_code = getTextOfChildTerminalNodeImpl(tree);

		int predicate_count = sizeOfPredicate(sql_predicates);

		int first_sql_id;

		switch (predicate_count) {
		case 2:
			try {

				BigDecimal value0 = new BigDecimal(sql_predicates.get(0).predicate);
				BigDecimal value1 = new BigDecimal(sql_predicates.get(1).predicate);

				int comp = value0.compareTo(value1);

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
			first_sql_id = sql_predicates.get(0).predicate == null || sql_predicates.get(1).predicate != null ? 0 : 1;

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

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, sql_relation.path, sql_relation.table_name, sql_relation.column_name, sql_predicate.predicate, sql_relation.terminus, parent, tree, null, terminal_code));

			src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

			break;
		case 0:
			if (sql_predicates.get(0).equalsRelationally(sql_predicates.get(1))) {

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
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param offset offset id
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testAdditiveExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree, int offset) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int start_id = startIdOfSuccessivePredicate(sql_predicates, offset);

		if (start_id < 0)
			return;

		int end_id = endIdOfSuccessivePredicate(sql_predicates, start_id);

		if (end_id < 0)
			throw new PgSchemaException(tree);

		String[] terminal_codes = getTextArrayOfChildTerminalNodeImpl(tree);

		BigDecimal result = null;

		for (int expr_id = start_id; expr_id < end_id; expr_id++) {

			if (result == null)
				result = new BigDecimal(sql_predicates.get(expr_id).predicate);

			else {

				XPathSqlExpr sql_expr = sql_predicates.get(expr_id);

				if (sql_expr.predicate.equals("0"))
					continue;

				BigDecimal value = new BigDecimal(sql_expr.predicate);

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

		}

		// withdraw all successive predicates by calculated one

		if (end_id - start_id == sql_predicates.size()) {

			src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, result.toString(), XPathCompType.text, parent, tree));

			src_path_expr.sql_predicates.removeIf(sql_expr -> sql_expr.parent_tree.equals(tree));

		}

		// otherwise, reduce successive predicates

		else {

			// store calculated value to the starting predicate

			src_path_expr.sql_predicates.get(start_id).predicate = result.toString();

			// set zero to the other predicates

			for (int expr_id = start_id + 1; expr_id < end_id; expr_id++)
				src_path_expr.sql_predicates.get(expr_id).predicate = "0";

			// find next successive predicates

			testAdditiveExprContext(src_comp, src_path_expr, parent, tree, end_id);

		}

	}

	/**
	 * Test MultiplicativeExprContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testMultiplicativeExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size < 2)
			return;

		if (pred_size != 2)
			throw new PgSchemaException(tree);

		String terminal_code = getTextOfChildTerminalNodeImpl(tree);

		int predicate_count = sizeOfPredicate(sql_predicates);

		switch (predicate_count) {
		case 2:
			try {

				BigDecimal value0 = new BigDecimal(sql_predicates.get(0).predicate);
				BigDecimal value1 = new BigDecimal(sql_predicates.get(1).predicate);

				switch (terminal_code) {
				case "*":
					value0 = value0.multiply(value1);
					break;
				case "div":
					value0 = value0.divide(value1);
					break;
				case "mod":
					value0 = value0.remainder(value1);
					break;
				default:
					throw new PgSchemaException(tree);
				}

				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, value0.toString(), XPathCompType.text, parent, tree));

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
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void testUnaryExprNoRootContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

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

				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, null, null, null, result.toString(), XPathCompType.text, parent, tree));

				src_path_expr.sql_predicates.removeIf(_sql_expr -> _sql_expr.parent_tree.equals(tree));

			} catch (NumberFormatException e) {
				throw new PgSchemaException(tree);
			}
			break;
		default:
			switch (terminal_code) {
			case "-":
				src_path_expr.appendPredicateSql(new XPathSqlExpr(schema, sql_expr.path, sql_expr.table_name, sql_expr.column_name, null, sql_expr.terminus, parent, tree, terminal_code, null));

				src_path_expr.sql_predicates.removeIf(_sql_expr -> _sql_expr.parent_tree.equals(tree));
				break;
			default:
				throw new PgSchemaException(tree);
			}

		}

	}

	/**
	 * Return total number of predicates in XPath SQL expression
	 *
	 * @param sql_predicates list of XPath SQL expression
	 * @return int total number of predicates in the list
	 */
	private int sizeOfPredicate(List<XPathSqlExpr> sql_predicates) {
		return (int) sql_predicates.stream().filter(sql_expr -> sql_expr.predicate != null).count();
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

		for (int expr_id = offset; expr_id < sql_predicates.size(); expr_id++) {

			XPathSqlExpr sql_expr = sql_predicates.get(expr_id);

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

		for (int expr_id = offset; expr_id < sql_predicates.size(); expr_id++) {

			XPathSqlExpr sql_expr = sql_predicates.get(expr_id);

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
	 * @param single whether single table expression
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translatePredicateTree2SqlImpl(XPathComp src_comp, XPathExpr src_path_expr, boolean single, StringBuilder sb) throws PgSchemaException {

		translatePredicateTree(src_comp, src_path_expr, src_comp.tree, false, single, sb);

		translatePredicateContext(src_comp, src_path_expr, src_comp.tree, single, sb);

	}

	/**
	 * Return whether XPath parse tree of predicate is effective.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param tree current XPath parse tree
	 * @param has_children whether parent has children or not
	 * @param single whether single table expression
	 * @param sb StringBuilder to store SQL expression
	 * @return boolean whether valid predicate or not
	 * @throws PgSchemaException the pg schema exception
	 */
	private boolean translatePredicateTree(XPathComp src_comp, XPathExpr src_path_expr, ParseTree tree, boolean has_children, boolean single, StringBuilder sb) throws PgSchemaException {

		boolean valid = false;

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class))
				continue;

			boolean _has_children = !child.getText().isEmpty() && (hasEffectiveChildren(child) || hasChildOfTerminalNodeImpl(child));

			if (translatePredicateTree(src_comp, src_path_expr, child, _has_children, single, sb) || _has_children)
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

				if (currentClass.equals(OrExprContext.class))
					translateOrExprContext(src_comp, src_path_expr, parent, tree, single, sb);

				// AndExprContext node

				else if (currentClass.equals(AndExprContext.class))
					translateAndExprContext(src_comp, src_path_expr, parent, tree, single, sb);

				// AdditiveExprContext node

				else if (currentClass.equals(AdditiveExprContext.class))
					translateAdditiveExprContext(src_comp, src_path_expr, parent, tree, single, sb);

				// MultiplicativeExprContext node

				else if (currentClass.equals(MultiplicativeExprContext.class))
					translateMultiplicativeExprContext(src_comp, src_path_expr, parent, tree, single, sb);

			}

		}

		return valid;
	}

	/**
	 * Translate OrExprContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param single whether single table expression
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateOrExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree, boolean single, StringBuilder sb) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size == 0)
			return;

		if (pred_size == 1)
			sb.append(" OR ");

		sql_predicates.forEach(sql_expr -> {

			appendSqlColumnName(single, sql_expr, sb);
			sb.append(" " + sql_expr.binary_operator + " " + sql_expr.value);

			sb.append(" OR ");

		});

		sb.setLength(sb.length() - 4); // remove last " OR "

	}

	/**
	 * Translate AndExprContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param single whether single table expression
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateAndExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree, boolean single, StringBuilder sb) throws PgSchemaException {

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
				appendSqlColumnName(single, sql_expr, sb);
				sb.append(" " + sql_expr.binary_operator + " " + sql_expr.value);
				break;
			case text:
				if (!serial_key) {
					try {
						throw new PgSchemaException(tree, "serial key", serial_key);
					} catch (PgSchemaException e) {
						e.printStackTrace();
					}
				}
				String table_name = schema.getTableNameOfPath(src_path_expr.path);
				if (!case_sense)
					table_name = table_name.toLowerCase();
				appendSqlColumnName(single, table_name, PgSchemaUtil.serial_key_name, sb);
				sb.append(" = " + sql_expr.predicate);
				break;
			default:
				try {
					throw new PgSchemaException(tree);
				} catch (PgSchemaException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}

			sb.append(" AND ");

		});

		sb.setLength(sb.length() - 5); // remove last " AND "

	}

	/**
	 * Translate AdditiveExprContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param single whether single table expression
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateAdditiveExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree, boolean single, StringBuilder sb) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size == 0)
			return;

		String[] terminal_codes = getTextArrayOfChildTerminalNodeImpl(tree);

		if (parent.getClass().equals(PredicateContext.class)) {

			String table_name = schema.getTableNameOfPath(src_path_expr.path);
			if (!case_sense)
				table_name = table_name.toLowerCase();
			appendSqlColumnName(single, table_name, PgSchemaUtil.serial_key_name, sb);
			sb.append(" = ");

		}

		for (int expr_id = 0; expr_id < pred_size; expr_id++) {

			XPathSqlExpr sql_expr = sql_predicates.get(expr_id);

			switch (sql_expr.terminus) {
			case element:
			case simple_content:
			case attribute:
				appendSqlColumnName(single, sql_expr, sb);
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
					System.exit(1);
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
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param parent XPath parse tree of parent
	 * @param tree current XPath parse tree
	 * @param single whether single table expression
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translateMultiplicativeExprContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree parent, ParseTree tree, boolean single, StringBuilder sb) throws PgSchemaException {

		List<XPathSqlExpr> sql_predicates = src_path_expr.sql_predicates.stream().filter(sql_expr -> sql_expr.parent_tree.equals(tree)).collect(Collectors.toList());

		int pred_size = sql_predicates.size();

		if (pred_size < 2)
			return;

		String[] terminal_codes = getTextArrayOfChildTerminalNodeImpl(tree);

		if (parent.getClass().equals(PredicateContext.class)) {

			String table_name = schema.getTableNameOfPath(src_path_expr.path);
			if (!case_sense)
				table_name = table_name.toLowerCase();
			appendSqlColumnName(single, table_name, PgSchemaUtil.serial_key_name, sb);
			sb.append(" = ");

		}

		for (int expr_id = 0; expr_id < pred_size; expr_id++) {

			XPathSqlExpr sql_expr = sql_predicates.get(expr_id);

			switch (sql_expr.terminus) {
			case element:
			case simple_content:
			case attribute:
				appendSqlColumnName(single, sql_expr, sb);
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
					System.exit(1);
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
	 * Translate PredicateContext node.
	 *
	 * @param src_comp XPath component of source predicate
	 * @param src_path_expr path expression of source predicate
	 * @param tree current XPath parse tree
	 * @param single whether single table expression
	 * @param sb StringBuilder to stor:e SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void translatePredicateContext(XPathComp src_comp, XPathExpr src_path_expr, ParseTree tree, boolean single, StringBuilder sb) throws PgSchemaException {

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
			appendSqlColumnName(single, sql_expr, sb);
			sb.append(" " + sql_expr.binary_operator + " " + sql_expr.value);
			break;
		case text:
			if (!serial_key) {
				try {
					throw new PgSchemaException(tree, "serial key", serial_key);
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}
			}
			String table_name = schema.getTableNameOfPath(src_path_expr.path);
			if (!case_sense)
				table_name = table_name.toLowerCase();
			appendSqlColumnName(single, table_name, PgSchemaUtil.serial_key_name, sb);
			sb.append(" = " + sql_expr.predicate);
			break;
		default:
			throw new PgSchemaException(tree);
		}

	}

	/**
	 * Append SQL JOIN clause.
	 *
	 * @param target_tables target SQL tables
	 * @param joined_tables joined SQL tables
	 * @param sb StringBuilder to store SQL expression
	 * @throws PgSchemaException the pg schema exception
	 */
	private void joinSqlTables(HashMap<String, String> target_tables, HashMap<String, String> joined_tables, StringBuilder sb) throws PgSchemaException {

		if (target_tables.isEmpty())
			return;

		String target_table_name = null;
		String target_table_path = null;

		String joined_table_name = null;
		String joined_table_path = null;

		int min_distance = -1;

		for (Entry<String, String> joined : joined_tables.entrySet()) {

			String joined_name = joined.getKey();
			String joined_path = joined.getValue();

			for (Entry<String, String> target : target_tables.entrySet()) {

				String target_name = target.getKey();
				String target_path = target.getValue();

				int distance = getDistanceOfTables(joined.getValue(), target.getValue());

				if (distance > 0) {

					if (min_distance == -1 || distance < min_distance) {

						target_table_name = target_name;
						target_table_path = target_path;

						joined_table_name = joined_name;
						joined_table_path = joined_path;

						min_distance = distance;

					}

				}

			}

		}

		// branched case

		if (min_distance != -1) {

			for (Entry<String, String> joined : joined_tables.entrySet()) {

				String joined_name = joined.getKey();
				String joined_path = joined.getValue();

				char[] joined_char = joined_path.toCharArray();

				for (Entry<String, String> target : target_tables.entrySet()) {

					String target_path = target.getValue();

					char[] target_char = target_path.toCharArray();

					int last_match = 0;

					for (int l = 0; l < target_char.length; l++) {

						if (joined_char[l] == target_char[l])
							last_match = l;
						else
							break;

					}

					if (last_match == 0 || last_match == target_char.length - 1)
						continue;

					String common_path = target_path.substring(0, last_match + 1);

					int distance = getDistanceOfTables(joined.getValue(), common_path);

					if (distance > 0) {

						if (min_distance == -1 || distance < min_distance) {

							target_table_name = schema.getTableNameOfPath(target_path);
							target_table_path = common_path;

							joined_table_name = joined_name;
							joined_table_path = joined_path;

							min_distance = distance;

						}

					}


				}

			}

		}

		String src_table_name;
		String dst_table_name;

		// subject table is parent

		if (joined_table_path.split("/").length < target_table_path.split("/").length) {

			src_table_name = joined_table_name;
			dst_table_name = target_table_name;

		}

		// subject table is child

		else {

			src_table_name = target_table_name;
			dst_table_name = joined_table_name;

		}

		PgTable src_table = schema.getTable(src_table_name);

		List<String> table_path = new ArrayList<String>();
		table_path.add(src_table_name);

		Integer[] foreign_table_ids = src_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
		Integer[] _foreign_table_ids = null;

		boolean found_table = false;

		while (foreign_table_ids != null && foreign_table_ids.length > 0 && !found_table) {

			for (Integer foreign_table_id : foreign_table_ids) {

				PgTable foreign_table = schema.getTable(foreign_table_id);

				if (foreign_table.name.equals(dst_table_name)) {

					table_path.add(dst_table_name);

					found_table = true;

					break;
				}

				else if (foreign_table.virtual && !found_table) {

					table_path.add(foreign_table.name);

					Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

					if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
						_foreign_table_ids = __foreign_table_ids;

				}

			}

			foreign_table_ids = _foreign_table_ids;

		}

		if (!found_table)
			throw new PgSchemaException("Not found path from " + src_table_name + " to " + dst_table_name + ".");

		for (int l = 1; l < table_path.size(); l++) {

			PgTable dst_table = schema.getTable(table_path.get(l));

			PgField nested_key = src_table.fields.stream().filter(field -> field.nested_key && field.foreign_table.equals(dst_table.name)).findFirst().get();

			appendSqlColumnName(false, src_table.name, nested_key.name, sb);

			sb.append(" = ");

			appendSqlColumnName(false, nested_key.foreign_table, nested_key.foreign_field, sb);

			sb.append(" AND ");

			src_table = dst_table;

		}

		// no need to join stop over tables

		table_path.forEach(table_name -> {

			String _table_path = target_tables.get(table_name);

			if (_table_path != null)
				joined_tables.put(table_name, _table_path);

			target_tables.remove(table_name);

		});

		joinSqlTables(target_tables, joined_tables, sb);

	}

	/**
	 * Return distance between table paths
	 *
	 * @param path1 first table path
	 * @param path2 second table path
	 * @return int distance between the paths, return -1 when invalid case
	 */
	private int getDistanceOfTables(String path1, String path2) {

		if (!path1.contains("/") || !path2.contains("/"))
			return -1;

		if (!path1.contains(path2) && !path2.contains(path1))
			return -1;

		return Math.abs(path1.split("/").length - path2.split("/").length);
	}

	/**
	 * Show SQL string expressions.
	 *
	 * @param indent indent code for output
	 */
	public void showSql(String indent) {

		path_exprs.forEach(path_expr -> System.out.println(indent + path_expr.path + " (terminus type: " + path_expr.terminus.name() + ") -> " + indent + path_expr.sql));

	}

}
