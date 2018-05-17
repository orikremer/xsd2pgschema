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

import java.util.HashSet;

/**
 * XML post editor.
 *
 * @author yokochi
 */
public class XmlPostEditor {

	/** Whether fill @default value. */
	public boolean fill_default_value = false;

	/** The list of --filt-in option. */
	protected HashSet<String> filt_ins = null;

	/** The list of --filt-out option. */
	protected HashSet<String> filt_outs = null;

	/** The list of --fill-this option. */
	protected HashSet<String> fill_these = null;

	/** Whether the --file-in options have been resolved. */
	protected boolean filt_in_resolved = false;	

	/** Whether the --file-out options have been resolved. */
	protected boolean filt_out_resolved = false;

	/** Whether the --fill-this options have been resolved. */
	protected boolean fill_this_resolved = false;

	/**
	 * Instance of XmlPostEditor.
	 */
	public XmlPostEditor() {

		filt_ins = new HashSet<String>();
		filt_outs = new HashSet<String>();
		fill_these = new HashSet<String>();

	}

	/**
	 * Add a --filt-in option.
	 *
	 * @param filt_in argument value
	 * @return boolean result of addition
	 */
	public boolean addFiltIn(String filt_in) {
		return filt_ins.add(filt_in);
	}

	/**
	 * Add a --filt-out option.
	 *
	 * @param filt_out argument value
	 * @return boolean result of addition
	 */
	public boolean addFiltOut(String filt_out) {
		return filt_outs.add(filt_out);
	}

	/**
	 * Add a --fill-this option.
	 *
	 * @param fill_this argument value
	 * @return boolean result of addition
	 */
	public boolean addFillThis(String fill_this) {
		return fill_these.add(fill_this);
	}

}
