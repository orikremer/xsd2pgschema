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

import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

// this code was taken from:
// http://stackoverflow.com/questions/11945728/how-to-use-termvector-lucene-4-0

/**
 * The Class VecTextField.
 */
public class VecTextField extends Field {

	/** The Constant TYPE_NOT_STORED. */
	public static final FieldType TYPE_NOT_STORED = new FieldType();

	/** The Constant TYPE_STORED. */
	public static final FieldType TYPE_STORED = new FieldType();

	static {

		TYPE_NOT_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		TYPE_NOT_STORED.setTokenized(true);
		TYPE_NOT_STORED.setStored(false);
		TYPE_NOT_STORED.setStoreTermVectors(true);
		TYPE_NOT_STORED.setStoreTermVectorPositions(true);
		TYPE_NOT_STORED.freeze();

		TYPE_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		TYPE_STORED.setTokenized(true);
		TYPE_STORED.setStored(true);
		TYPE_STORED.setStoreTermVectors(true);
		TYPE_STORED.setStoreTermVectorPositions(true);
		TYPE_STORED.freeze();

	}

	/**
	 * Creates a new TextField with Reader value.
	 *
	 * @param name the name
	 * @param reader the reader
	 * @param store the store
	 */
	public VecTextField(String name, Reader reader, Store store) {

		super(name, reader, store == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);

	}

	/**
	 * Creates a new TextField with String value.
	 *
	 * @param name the name
	 * @param value the value
	 * @param store the store
	 */
	public VecTextField(String name, String value, Store store) {

		super(name, value, store == Store.YES ? TYPE_STORED : TYPE_NOT_STORED);

	}

	/**
	 * Creates a new un-stored TextField with TokenStream value.
	 *
	 * @param name the name
	 * @param stream the stream
	 */
	public VecTextField(String name, TokenStream stream) {

		super(name, stream, TYPE_NOT_STORED);

	}

}
