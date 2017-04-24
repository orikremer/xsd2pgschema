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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

// this code was derived from:
// http://stackoverflow.com/questions/11945728/how-to-use-termvector-lucene-4-0

/**
 * The Class NoIdxStringField.
 */
public class NoIdxStringField extends Field {

	/** The Constant TYPE_NOT_STORED. */
	/* Not stored. */
	public static final FieldType TYPE_NOT_STORED = new FieldType();

	/** The Constant TYPE_STORED. */
	/* Stored. */
	public static final FieldType TYPE_STORED = new FieldType();

	static {

		TYPE_NOT_STORED.setIndexOptions(IndexOptions.NONE);
		TYPE_NOT_STORED.setTokenized(false);
		TYPE_NOT_STORED.setStored(false);
		TYPE_NOT_STORED.setStoreTermVectors(false);
		TYPE_NOT_STORED.setStoreTermVectorPositions(false);
		TYPE_NOT_STORED.freeze();

		TYPE_STORED.setIndexOptions(IndexOptions.NONE);
		TYPE_STORED.setTokenized(false);
		TYPE_STORED.setStored(true);
		TYPE_STORED.setStoreTermVectors(false);
		TYPE_STORED.setStoreTermVectorPositions(false);
		TYPE_STORED.freeze();

	}

	/**
	 *  Creates a new StringField with String value.
	 *
	 * @param name the name
	 * @param value the value
	 * @param store the store
	 */
	public NoIdxStringField(String name, String value, Store store) {
		super(name, value, store == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);
	}

}
