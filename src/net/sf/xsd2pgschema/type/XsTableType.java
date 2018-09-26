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

package net.sf.xsd2pgschema.type;

/**
 * Enumerator of table type.
 *
 * @author yokochi
 */
public enum XsTableType {

	/** The root table. */
	xs_root,
	/** The child table of the root table. */
	xs_root_child,
	/** The administrative root table. */
	xs_admin_root,
	/** The child table of the administrative table. */
	xs_admin_child,

	/** The attribute group. */
	xs_attr_group,
	/** The model group. */
	xs_model_group

}
