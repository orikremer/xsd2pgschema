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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;

import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.NCNameContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.NameTestContext;
import com.github.antlr.grammars_v4.xpath.xpathParser.NodeTestContext;

/**
 * XPath component list.
 *
 * @author yokochi
 */
public class XPathCompList {

	/** Serialized XPath parse tree. */
	public List<XPathComp> comps = null;

	/** Instance of XML path. */
	public List<String> paths = null;

	/** Terminal type of path. */
	public List<XPathCompType> termini = null;

	/** Whether UnionExprNoRootContext node exists or not. */
	private boolean union_expr = false;

	/** Instance of XML paths for stacking while UnionExprNoRootContext. */
	private List<String> paths_union = null;

	/** Terminal type of paths for stacking while UnionExprNoRootContext. */
	private List<XPathCompType> termini_union = null;

	/**
	 * Serialize XPath parse tree to XPath component list.
	 *
	 * @param tree XPath parse tree
	 * @param output whether outputs result or not
	 */
	public XPathCompList(ParseTree tree, boolean output) {

		comps = new ArrayList<XPathComp>();

		paths = new ArrayList<String>();
		termini = new ArrayList<XPathCompType>();

		if (!testTree(tree, output, ""))
			return;

		expr_counter = step_counter = 0;
		wild_card = false;

		serializeTree(tree, output);

	}

	/**
	 * Serialize XPath parse tree to XPath component list (temporary use only).
	 */
	public XPathCompList() {

		paths = new ArrayList<String>();
		termini = new ArrayList<XPathCompType>();

	}

	/**
	 * Clear XPathCompList.
	 */
	public void clear() {

		if (comps != null)
			comps.clear();

		if (paths != null)
			paths.clear();

		if (termini != null)
			termini.clear();

		if (paths_union != null)
			paths_union.clear();

		if (termini_union != null)
			termini_union.clear();

	}

	/**
	 * Return whether XPath parse tree is effective.
	 *
	 * @param tree XPath parse tree
	 * @param output whether outputs XPath parse tree or not
	 * @param indent indent code for output
	 * @return boolean whether valid or not
	 */
	private boolean testTree(ParseTree tree, boolean output, String indent) {

		boolean valid = false;

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			boolean has_children = child.getChildCount() > 1;

			if (output)
				System.out.println(indent + child.getClass().getSimpleName() + " '" + child.getText() + "' " + child.getSourceInterval().toString());

			if (testTree(child, output, indent + " ") || has_children)
				valid = true;

		}

		return valid;
	}

	/** The expr counter. */
	private int expr_counter;

	/** The step counter. */
	private int step_counter;

	/** Whether wild card follows or not. */
	private boolean wild_card;

	/**
	 * Serialize XPath parse tree to XPath component list.
	 *
	 * @param tree XPath parse tree
	 * @param output whether outputs XPath component list or not
	 */
	private void serializeTree(ParseTree tree, boolean output) {

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class)) {

				boolean union_expr = child.getText().equals("|");

				if (union_expr) {

					expr_counter++;
					step_counter = 0;

				}

				if (child.getText().equals("*")) {

					step_counter--;
					wild_card = true;

				}

				else if (wild_card) {

					step_counter++;
					wild_card = false;

				}

				XPathComp comp = new XPathComp(expr_counter, step_counter, child);

				comps.add(comp);

				if (output)
					System.out.println("expr.#" + expr_counter + ", step.#" + step_counter + ": " + child.getClass().getSimpleName() + " '" + child.getText() + "'");

				if (!wild_card)
					step_counter++;

				if (union_expr) {

					expr_counter++;
					step_counter = 0;

				}

				continue;
			}

			else if (child.getClass().equals(xpathParser.StepContext.class)) {

				if (output)
					System.out.println("expr.#" + expr_counter + ", step.#" + step_counter + ": '" + child.getText() + "' ->");

				traceStepContextNode(child, output);

				if (output)
					System.out.println();

				if (!wild_card)
					step_counter++;

				continue;
			}

			serializeTree(child, output);

		}

	}

	/**
	 * Trace child node of StepContext node.
	 *
	 * @param tree XPath parse tree
	 * @param output whether outputs XPath component list or not
	 */
	private void traceStepContextNode(ParseTree tree, boolean output) {

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			if (isTerminalNode(child)) {

				XPathComp comp = new XPathComp(expr_counter, step_counter, child);

				comps.add(comp);

				if (output)
					System.out.print(" " + child.getClass().getSimpleName() + " '" + child.getText() + "'");

				// no need to trace prefix

				if (child.getClass().equals(NameTestContext.class))
					break;

			}

			traceStepContextNode(child, output);

		}

	}

	/**
	 * Return whether child node is TerminalNodeImpl.
	 *
	 * @param tree XPath parse tree
	 * @return boolean whether child node is TerminalNodeImpl or not
	 */
	private boolean isTerminalNode(ParseTree tree) {

		for (int i = 0; i < tree.getChildCount(); i++) {

			ParseTree child = tree.getChild(i);

			if (child.getClass().equals(TerminalNodeImpl.class))
				return true;

		}

		return false;
	}

	/**
	 * Return the last expression id.
	 *
	 * @return int the last expression id
	 */
	public int getLastExpr() {
		return comps.get(comps.size() - 1).expr_id;
	}

	/**
	 * Return the last step id.
	 *
	 * @param expr_id expression id
	 * @return int the last step id
	 */
	public int getLastStep(int expr_id) {
		return comps.stream().filter(comp -> comp.expr_id == expr_id).max(Comparator.comparingInt(comp -> comp.step_id)).get().step_id;
	}

	/**
	 * Return array of XPath component.
	 *
	 * @param expr_id current expression id
	 * @param step_id current step id
	 * @return XPathComp[] array of XPath component
	 */
	public XPathComp[] getXPathComp(int expr_id, int step_id) {
		return comps.stream().filter(comp -> comp.expr_id == expr_id && comp.step_id == step_id).toArray(XPathComp[]::new);
	}

	/**
	 * Return previous step of XPath component.
	 *
	 * @param comp current XPath component in list
	 * @return XPathComp parent XPath component
	 */
	public XPathComp getPreviousStep(XPathComp comp) {

		int step_id = comp.step_id - (comp.tree.getClass().equals(TerminalNodeImpl.class) ? 1 : 2);

		if (step_id < 0)
			return null;

		XPathComp[] prev_comps = comps.stream().filter(_comp -> _comp.expr_id == comp.expr_id && _comp.step_id == step_id).toArray(XPathComp[]::new);

		if (prev_comps == null || prev_comps.length == 0)
			return null;

		return prev_comps[prev_comps.length - 1];
	}

	/**
	 * Add a path.
	 *
	 * @param path current path
	 * @param terminus current terminus type of path
	 */
	public void add(String path, XPathCompType terminus) {

		paths.add(path);
		termini.add(terminus);

	}

	/**
	 * Add all paths.
	 *
	 * @param list XPath component list
	 */
	public void addAll(XPathCompList list) {

		paths.addAll(list.paths);
		termini.addAll(list.termini);

	}

	/**
	 * Validate XPath component with TerminalNodeImpl class.
	 *
	 * @param comp current XPath component
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateTerminalNodeImpl(XPathComp comp) throws PgSchemaException {

		String text = comp.tree.getText();
		int step_id = comp.step_id;

		switch (text) {
		case "/":
		case "//":
			// first TerminalNodeImpl node

			if (step_id == 0) {} // nothing to do

			// check terminus of paths

			else {

				if (hasPathEndsWithTextNode()) {

					XPathComp[] child_comps = getXPathComp(comp.expr_id, step_id + 1);

					if (child_comps == null || child_comps.length == 0) {

						if (removePathEndsWithTextNode() == 0)
							throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

					}

					else {

						XPathComp child_last_comp = child_comps[child_comps.length - 1];

						if (!child_last_comp.tree.getClass().equals(NodeTestContext.class) || !child_last_comp.tree.getText().equals("text()")) {

							if (removePathEndsWithTextNode() == 0)
								throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

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
			union_expr = true;

			if (paths_union == null)
				paths_union = new ArrayList<String>();

			if (termini_union == null)
				termini_union = new ArrayList<XPathCompType>();

			paths_union.addAll(paths);
			termini_union.addAll(termini);

			paths.clear();
			termini.clear();
			break;
		default:
			throw new PgSchemaException(comp.tree);
		}

	}

	/**
	 * Validate XPath component with AbbreviateStepContext class.
	 *
	 * @param comp current XPath component
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateAbbreviateStepContext(XPathComp comp) throws PgSchemaException {

		switch (comp.tree.getText()) {
		case ".":
			break;
		case "..":
			if (selectParentPath() == 0) {

				XPathComp prev_comp = getPreviousStep(comp);

				if (prev_comp != null)
					throw new PgSchemaException(comp.tree, prev_comp.tree);
				else
					throw new PgSchemaException(comp.tree);
			}
			break;
		default:
			throw new PgSchemaException(comp.tree);
		}

	}

	/**
	 * Validate XPath component with AxisSpecifierContext class.
	 *
	 * @param comp current XPath component
	 * @param comps array of XPath component of the same step
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateAxisSpecifierContext(XPathComp comp, XPathComp[] comps) throws PgSchemaException {

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
	 * Validate XPath component with NCNameContext class having parent axis.
	 *
	 * @param comp current XPath component
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateNCNameContextWithParentAxis(XPathComp comp, boolean wild_card, String composite_text) throws PgSchemaException {

		if (selectParentPath() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

		validateNCNameContext(comp, wild_card, composite_text);

		if (paths.size() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

	}

	/**
	 * Validate XPath component with NCNameContext class having ancestor axis.
	 *
	 * @param comp current XPath component
	 * @param inc_self whether include self node or not
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateNCNameContextWithAncestorAxis(XPathComp comp, boolean inc_self, boolean wild_card, String composite_text) throws PgSchemaException {

		List<String> _paths = new ArrayList<String>();
		List<XPathCompType> _termini = new ArrayList<XPathCompType>();

		if (inc_self) {

			_paths.addAll(paths);
			_termini.addAll(termini);

		}

		while (selectParentPath() > 0) {

			_paths.addAll(paths);
			_termini.addAll(termini);

		}

		replacePath(_paths, _termini);

		validateNCNameContext(comp, wild_card, composite_text);

		if (paths.size() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

	}

	/**
	 * Validate XPath component with NCNameContext class.
	 *
	 * @param comp current XPath component
	 * @param wild_card whether wild card follows or not
	 * @param composite_text composite text including wild card
	 * @throws PgSchemaException the pg schema exception
	 */
	private void validateNCNameContext(XPathComp comp, boolean wild_card, String composite_text) throws PgSchemaException {

		String text = comp.tree.getText();

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			String path = iter_path.next();
			iter_term.next();

			String[] _path = path.split("/");
			String last_path = _path[_path.length - 1];

			if (!(wild_card ? last_path.matches(composite_text) : last_path.equals(text))) {

				iter_path.remove();
				iter_term.remove();

			}

		}

		if (paths.size() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

	}

	/**
	 * Validate XPath component with NodeTestContext class having parent axis.
	 *
	 * @param comp current XPath component
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateNodeTestContextWithParentAxis(XPathComp comp) throws PgSchemaException {

		if (selectParentPath() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

	}

	/**
	 * Validate XPath component with NodeTestContext class having ancestor axis.
	 *
	 * @param comp current XPath component
	 * @param inc_self whether include self node or not
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateNodeTestContextWithAncestorAxis(XPathComp comp, boolean inc_self) throws PgSchemaException {

		List<String> _paths = new ArrayList<String>();
		List<XPathCompType> _termini = new ArrayList<XPathCompType>();

		if (inc_self) {

			_paths.addAll(paths);
			_termini.addAll(termini);

		}

		while (selectParentPath() > 0) {

			_paths.addAll(paths);
			_termini.addAll(termini);

		}

		replacePath(_paths, _termini);

		if (paths.size() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

	}

	/**
	 * Validate XPath component with NameTestContext class having parent axis.
	 *
	 * @param comp current XPath component
	 * @param namespace_uri current namespace URI
	 * @param schema PostgreSQL data model
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateNameTestContextWithParentAxis(XPathComp comp, String namespace_uri, PgSchema schema) throws PgSchemaException {

		if (selectParentPath() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

		validateNameTestContext(comp, namespace_uri, schema);

		if (paths.size() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

	}

	/**
	 * Validate XPath component with NameTestContext class having ancestor axis.
	 *
	 * @param comp current XPath component
	 * @param namespace_uri current namespace URI
	 * @param schema PostgreSQL data model
	 * @param inc_self whether include self node or not
	 * @throws PgSchemaException the pg schema exception
	 */
	public void validateNameTestContextWithAncestorAxis(XPathComp comp, String namespace_uri, PgSchema schema, boolean inc_self) throws PgSchemaException {

		List<String> _paths = new ArrayList<String>();
		List<XPathCompType> _termini = new ArrayList<XPathCompType>();

		if (inc_self) {

			_paths.addAll(paths);
			_termini.addAll(termini);

		}

		while (selectParentPath() > 0) {

			_paths.addAll(paths);
			_termini.addAll(termini);

		}

		replacePath(_paths, _termini);

		validateNameTestContext(comp, namespace_uri, schema);

		if (paths.size() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

	}

	/**
	 * Validate XPath component with NameTestContext class.
	 *
	 * @param comp current XPath component
	 * @param namespace_uri current namespace URI
	 * @param schema PostgreSQL data model
	 * @throws PgSchemaException the pg schema exception
	 */
	private void validateNameTestContext(XPathComp comp, String namespace_uri, PgSchema schema) throws PgSchemaException {

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			String path = iter_path.next();
			XPathCompType terminus = iter_term.next();

			String[] name = path.split("/");

			int len = name.length;
			int t;

			switch (terminus) {
			case table:

				if (len < 1)
					throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

				t = schema.getTableId(name[name.length - 1]);

				if (t < 0)
					throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

				if (!schema.getTable(t).target_namespace.equals(namespace_uri)) {

					iter_path.remove();
					iter_term.remove();

				}
				break;
			case field:
				if (len < 2)
					throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

				t = schema.getTableId(name[name.length - 2]);

				if (t < 0)
					throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

				PgTable table = schema.getTable(t);
				String field_name = name[name.length - 1];

				int f = table.getFieldId(field_name);

				if (f >= 0) {

					if (!table.fields.get(f).target_namespace.equals(namespace_uri)) {

						iter_path.remove();
						iter_term.remove();

					}

					continue;
				}

				Integer[] foreign_table_ids = table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);
				Integer[] _foreign_table_ids = null;

				boolean found_field = false;

				while (foreign_table_ids != null && foreign_table_ids.length > 0 && !found_field) {

					for (Integer foreign_table_id : foreign_table_ids) {

						PgTable foreign_table = schema.getTable(foreign_table_id);

						f = table.getFieldId(field_name);

						if (f >= 0) {

							if (!table.fields.get(f).target_namespace.equals(namespace_uri)) {

								iter_path.remove();
								iter_term.remove();

							}

							found_field = true;

							break;
						}

						if (foreign_table.virtual && !found_field) {

							Integer[] __foreign_table_ids = foreign_table.fields.stream().filter(field -> field.nested_key).map(field -> field.foreign_table_id).toArray(Integer[]::new);

							if (__foreign_table_ids != null && __foreign_table_ids.length > 0)
								_foreign_table_ids = __foreign_table_ids;

						}

					}

					foreign_table_ids = _foreign_table_ids;

				}

				if (!found_field)
					throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

				break;
			default:
				throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);
			}

		}

		if (paths.size() == 0)
			throw new PgSchemaException(comp.tree, getPreviousStep(comp).tree);

	}

	/**
	 * Return whether any path ends with table node.
	 *
	 * @return boolean whether any path ends with table node
	 */
	public boolean hasPathEndsWithTableNode() {
		return termini.stream().anyMatch(terminus -> terminus.equals(XPathCompType.table));
	}

	/**
	 * Return whether any path ends with text node.
	 *
	 * @return boolean whether any path ends with text node
	 */
	public boolean hasPathEndsWithTextNode() {
		return termini.stream().anyMatch(terminus -> !terminus.equals(XPathCompType.table) && !terminus.equals(XPathCompType.field));
	}

	/**
	 * Return whether any path ends without text node.
	 *
	 * @return boolean whether any path ends without text node
	 */
	public boolean hasPathEndsWithoutTextNode() {
		return termini.stream().anyMatch(terminus -> terminus.equals(XPathCompType.table) || terminus.equals(XPathCompType.field));
	}

	/**
	 * Remove any path that ends with table node.
	 *
	 * @return int the number of survived paths
	 */
	public int removePathEndsWithTableNode() {

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			iter_path.next();
			XPathCompType terminus = iter_term.next();

			switch (terminus) {
			case table:
				iter_path.remove();
				iter_term.remove();
				break;
			default:
				break;
			}

		}

		return paths.size();
	}

	/**
	 * Remove any path that ends with field node.
	 *
	 * @return int the number of survived paths
	 */
	public int removePathEndsWithFieldNode() {

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			iter_path.next();
			XPathCompType terminus = iter_term.next();

			switch (terminus) {
			case field:
				iter_path.remove();
				iter_term.remove();
				break;
			default:
				break;
			}

		}

		return paths.size();
	}

	/**
	 * Remove any path that ends with text node.
	 *
	 * @return int the number of survived paths
	 */
	public int removePathEndsWithTextNode() {

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			iter_path.next();
			XPathCompType terminus = iter_term.next();

			switch (terminus) {
			case text:
			case comment:
			case processing_instruction:
				iter_path.remove();
				iter_term.remove();
				break;
			default:
				break;
			}

		}

		return paths.size();
	}

	/**
	 * Remove any path that ends without table node.
	 *
	 * @return int the number of survived paths
	 */
	public int removePathEndsWithoutTableNode() {

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			iter_path.next();
			XPathCompType terminus = iter_term.next();

			if (!terminus.equals(XPathCompType.table)) {

				iter_path.remove();
				iter_term.remove();

			}

		}

		return paths.size();
	}

	/**
	 * Remove any path that ends without field node.
	 *
	 * @return int the number of survived paths
	 */
	public int removePathEndsWithoutFieldNode() {

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			iter_path.next();
			XPathCompType terminus = iter_term.next();

			if (!terminus.equals(XPathCompType.field)) {

				iter_path.remove();
				iter_term.remove();

			}

		}

		return paths.size();
	}

	/**
	 * Remove any path that ends without text node.
	 *
	 * @return int the number of survived paths
	 */
	public int removePathEndsWithoutTextNode() {

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			iter_path.next();
			XPathCompType terminus = iter_term.next();

			switch (terminus) {
			case table:
			case field:
				iter_path.remove();
				iter_term.remove();
				break;
			default:
				break;
			}

		}

		return paths.size();
	}

	/**
	 * Remove any orphan path.
	 *
	 * @param parent_paths list of parental paths
	 * @return int the number of survived paths
	 */
	public int removeOrphanPath(List<String> parent_paths) {

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			String path = iter_path.next();
			iter_term.next();

			if (!parent_paths.stream().anyMatch(src_path -> path.startsWith(src_path))) {

				iter_path.remove();
				iter_term.remove();

			}

		}

		return paths.size();
	}

	/**
	 * Append abbreviation path of all paths.
	 */
	public void appendAbbrevPath() {

		paths = paths.stream().map(path -> path + "//").collect(Collectors.toList());

	}

	/**
	 * Append text node.
	 */
	public void appendTextNode() {

		List<String> _paths = new ArrayList<String>();
		List<XPathCompType> _termini = new ArrayList<XPathCompType>();

		_paths.addAll(paths);
		_termini.addAll(termini);

		paths.clear();
		termini.clear();

		Iterator<String> iter_path = _paths.iterator();
		Iterator<XPathCompType> iter_term = _termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			String path = iter_path.next();
			XPathCompType terminus = iter_term.next();

			if (terminus.equals(XPathCompType.field)) {

				paths.add(path + "/" + PgSchemaUtil.text_node_name);
				termini.add(XPathCompType.text);

			}

			else {

				paths.add(path);
				termini.add(terminus);

			}

		}

		_paths.clear();
		_termini.clear();

	}

	/**
	 * Append comment node.
	 */
	public void appendCommentNode() {

		List<String> _paths = new ArrayList<String>();
		List<XPathCompType> _termini = new ArrayList<XPathCompType>();

		_paths.addAll(paths);
		_termini.addAll(termini);

		paths.clear();
		termini.clear();

		Iterator<String> iter_path = _paths.iterator();
		Iterator<XPathCompType> iter_term = _termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			String path = iter_path.next();
			XPathCompType terminus = iter_term.next();

			if (terminus.equals(XPathCompType.table) || terminus.equals(XPathCompType.field)) {

				paths.add(path + "/" + PgSchemaUtil.comment_node_name);
				termini.add(XPathCompType.comment);

			}

			else {

				paths.add(path);
				termini.add(terminus);

			}

		}

		_paths.clear();
		_termini.clear();

	}

	/**
	 * Append processing-instruction node.
	 *
	 * @param expression expression of processing-instruction()
	 */
	public void appendProcessingInstructionNode(String expression) {

		if (paths.isEmpty()) {

			paths.add("/" + expression);
			termini.add(XPathCompType.processing_instruction);

		}

		else {

			List<String> _paths = new ArrayList<String>();
			List<XPathCompType> _termini = new ArrayList<XPathCompType>();

			_paths.addAll(paths);
			_termini.addAll(termini);

			paths.clear();
			termini.clear();

			Iterator<String> iter_path = _paths.iterator();
			Iterator<XPathCompType> iter_term = _termini.iterator();

			while (iter_path.hasNext() && iter_term.hasNext()) {

				String path = iter_path.next();
				XPathCompType terminus = iter_term.next();

				if (terminus.equals(XPathCompType.table) || terminus.equals(XPathCompType.field)) {

					paths.add(path + "/" + expression);
					termini.add(XPathCompType.processing_instruction);

				}

				else {

					paths.add(path);
					termini.add(terminus);

				}

			}

			_paths.clear();
			_termini.clear();

		}

	}

	/**
	 * Return whether absolute path used.
	 *
	 * @param expr_id expression id
	 * @return boolean whether absolute path used
	 */
	public boolean isAbsolutePath(int expr_id) {

		if (paths.isEmpty()) {

			XPathComp first_comp = comps.stream().filter(comp -> comp.expr_id == expr_id && comp.step_id == 0).findFirst().get();

			return first_comp.tree.getClass().equals(TerminalNodeImpl.class) && first_comp.tree.getText().equals("/");
		}

		return !paths.get(0).endsWith("//");
	}

	/**
	 * Select parent path.
	 *
	 * @return int the number of survived paths
	 */
	public int selectParentPath() {

		paths = paths.stream().map(path -> getParentPath(path)).collect(Collectors.toList());
		termini = termini.stream().map(terminus -> getParentTerminus(terminus)).collect(Collectors.toList());

		Iterator<String> iter_path = paths.iterator();
		Iterator<XPathCompType> iter_term = termini.iterator();

		while (iter_path.hasNext() && iter_term.hasNext()) {

			String path = iter_path.next();
			iter_term.next();

			if (path.isEmpty()) {

				iter_path.remove();
				iter_term.remove();

			}

		}

		return paths.size();
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
	 * @param terminus current terminus
	 * @return XPathCompType parent terminus
	 */
	private XPathCompType getParentTerminus(XPathCompType terminus) {

		switch (terminus) {
		case table:
		case field:
			return XPathCompType.table;
		case text:
			return XPathCompType.field;
		default: // comment, processing_instruction
			return null;
		}

	}

	/**
	 * Replace paths.
	 *
	 * @param list XPath component list
	 */
	public void replacePath(XPathCompList list) {

		replacePath(list.paths, list.termini);

	}

	/**
	 * Replace paths.
	 *
	 * @param paths new paths
	 * @param termini new termini
	 */
	private void replacePath(List<String> paths, List<XPathCompType> termini) {

		this.paths.clear();
		this.paths = paths;

		this.termini.clear();
		this.termini = termini;

	}

	/**
	 * Apply union expression.
	 */
	public void applyUnionExpr() {

		if (!union_expr)
			return;

		paths_union.addAll(paths);
		termini_union.addAll(termini);

		paths.clear();
		termini.clear();

		paths.addAll(paths_union);
		termini.addAll(termini_union);

		paths_union.clear();
		termini_union.clear();

		union_expr = false;

	}

	/**
	 * Show paths.
	 *
	 * @param indent indent code for output
	 */
	public void showPath(String indent) {

		paths.forEach(path -> System.out.println(indent + path));

	}

	/**
	 * Show termini.
	 *
	 * @param indent indent code for output
	 */
	public void showTerminus(String indent) {

		termini.forEach(terminus -> System.out.println(indent + terminus.name()));

	}

	/**
	 * Unify paths.
	 */
	public void unifyPath() {

		HashSet<String> _paths = new HashSet<String>();

		paths.forEach(path -> _paths.add(path));

		paths.clear();

		paths.addAll(_paths);

		_paths.clear();

	}

}
