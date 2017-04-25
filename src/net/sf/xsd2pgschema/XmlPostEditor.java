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

import java.util.ArrayList;
import java.util.List;

/**
 * XML post editor.
 *
 * @author yokochi
 */
public class XmlPostEditor {

	/** The filt-in option. */
	List<String> filt_ins = null;

	/** The filt-out option. */
	List<String> filt_outs = null;

	/** The fill-this option. */
	List<String> fill_these = null;

	/**
	 * Instance of XmlPostEditor.
	 */
	public XmlPostEditor() {

		filt_ins = new ArrayList<String>();
		filt_outs = new ArrayList<String>();
		fill_these = new ArrayList<String>();

	}

	/**
	 * Add filt-in option.
	 *
	 * @param filt_in argument value
	 * @return result of addition
	 */
	public boolean addFiltIn(String filt_in) {
		return filt_ins.add(filt_in);
	}

	/**
	 * Add filt-out option.
	 *
	 * @param filt_out argument value
	 * @return result of addition
	 */
	public boolean addFiltOut(String filt_out) {
		return filt_outs.add(filt_out);
	}

	/**
	 * Add fill-this option.
	 *
	 * @param fill_this argument value
	 * @return result of addition
	 */
	public boolean addFillThis(String fill_this) {
		return fill_these.add(fill_this);
	}

}
