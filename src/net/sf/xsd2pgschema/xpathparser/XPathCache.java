/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

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

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

import net.sf.xsd2pgschema.PgTable;

/**
 * Cache for XPath parser.
 *
 * @author yokochi
 */
public class XPathCache implements Serializable {

	/** The default serial version ID. */
	private static final long serialVersionUID = 1L;

	/** Matched path. */
	public HashMap<String, PgTable> matched_paths = new HashMap<String, PgTable>();

	/** Unmatched path. */
	public HashSet<String> unmatched_paths = new HashSet<String>();

}
