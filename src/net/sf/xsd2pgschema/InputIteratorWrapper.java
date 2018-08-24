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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;

/**
 * The Class InputIteratorWrapper.
 */
public class InputIteratorWrapper implements InputIterator {

	/** The iters. */
	private List<InputIterator> iters = null;

	/** The cur. */
	private int cur = -1;

	/**
	 * Instantiates a new input iterator wrapper.
	 */
	public InputIteratorWrapper() {

		iters = new ArrayList<InputIterator>();

	}

	/**
	 * Adds the iter.
	 *
	 * @param iter the iter
	 */
	public void add(InputIterator iter) {

		if (iter == null)
			return;

		iters.add(iter);
		cur = 0;

	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.util.BytesRefIterator#next()
	 */
	@Override
	public BytesRef next() throws IOException {

		if (cur < 0 || cur >= iters.size())
			return null;

		InputIterator iter = iters.get(cur);

		if (iter != null) {

			BytesRef term = iter.next();

			if (term != null)
				return term;

		}

		++cur;

		return next();
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.search.suggest.InputIterator#contexts()
	 */
	@Override
	public Set<BytesRef> contexts() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.search.suggest.InputIterator#hasContexts()
	 */
	@Override
	public boolean hasContexts() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.search.suggest.InputIterator#hasPayloads()
	 */
	@Override
	public boolean hasPayloads() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.search.suggest.InputIterator#payload()
	 */
	@Override
	public BytesRef payload() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.search.suggest.InputIterator#weight()
	 */
	@Override
	public long weight() {
		return 0;
	}

}
