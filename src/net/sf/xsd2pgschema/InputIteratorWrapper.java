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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;

public class InputIteratorWrapper implements InputIterator {

	private List<InputIterator> iters = null;
	private int cur = -1;

	public InputIteratorWrapper() {

		iters = new ArrayList<InputIterator>();

	}

	public void add(InputIterator iter) {

		if (iter == null)
			return;

		iters.add(iter);
		cur = 0;

	}

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

	@Override
	public Set<BytesRef> contexts() {
		return null;
	}

	@Override
	public boolean hasContexts() {
		return false;
	}

	@Override
	public boolean hasPayloads() {
		return false;
	}

	@Override
	public BytesRef payload() {
		return null;
	}

	@Override
	public long weight() {
		return 0;
	}

}
