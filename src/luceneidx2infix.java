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

import net.sf.xsd2pgschema.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.LuceneDictionary;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Generate Lucene analyzed infix suggester from index.
 * 
 * @author yokochi
 */
public class luceneidx2infix {

	/** The index directory name. */
	public static String idx_dir_name = xml2luceneidx.idx_dir_name;
	
	/** The analyzing infix suggester directory name. */
	public static String infix_dir_name = "lucene_infix";

	/** The field list. */
	public static List<String> fields = new ArrayList<String>();

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--idx-dir"))
				idx_dir_name = args[++i];

			else if (args[i].equals("--field"))
				fields.add(args[++i]);

			else if (args[i].equals("--infix-dir"))
				infix_dir_name = args[++i];

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		File idx_dir = new File(idx_dir_name);

		if (!idx_dir.isDirectory()) {
			System.err.println("Couldn't find directory '" + idx_dir_name + "'.");
			System.exit(1);
		}

		File infix_dir = new File(infix_dir_name);

		if (!infix_dir.isDirectory()) {

			if (!infix_dir.mkdir()) {
				System.err.println("Couldn't create directory '" + infix_dir_name + "'.");
				System.exit(1);
			}

		}

		try {

			Directory idx = FSDirectory.open(idx_dir.toPath());

			Analyzer analyzer = new StandardAnalyzer();

			IndexReader reader = DirectoryReader.open(idx);

			// Lucene dictionary for AnalyzingInfixSuggester

			Directory infix = FSDirectory.open(infix_dir.toPath());

			AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(infix, analyzer);

			if (fields.size() == 0) {

				System.out.println("field: " + PgSchemaUtil.simple_cont_name);

				Dictionary dictionary = new LuceneDictionary(reader, PgSchemaUtil.simple_cont_name);
				suggester.build(dictionary);

			}

			else {

				boolean[] contains = new boolean[fields.size()];
				Arrays.fill(contains, false);

				InputIteratorWrapper iters = new InputIteratorWrapper();

				List<LeafReaderContext> leaves = reader.leaves();

				for (LeafReaderContext leaf : leaves) {

					Fields fields_in = leaf.reader().fields();

					for (String field_in : fields_in) {

						if (!fields.contains(field_in))
							continue;

						System.out.println("field: " + field_in);

						contains[fields.indexOf(field_in)] = true;

						Dictionary dictionary = new LuceneDictionary(reader, field_in);

						iters.add(dictionary.getEntryIterator());

					}

				}

				for (int f = 0; f < fields.size(); f++) {

					if (contains[f])
						continue;

					System.err.println("field: " + fields.get(f) + " not found");

				}

				suggester.build(iters);

			}

			suggester.commit();
			suggester.close();

			System.out.println("Done index -> infix (" + infix_dir.getPath() + ").");

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("luceneidx2infix: Lucene index -> Lucene analyzed infix suggester");
		System.err.println("Usage:  --idx-dir DIRECTORY (default=\"" + xml2luceneidx.idx_dir_name + "\") --infix-dir DIRECTORY (default=\"" + infix_dir_name + "\")");
		System.err.println("Option: --field FIELD_NAME (default=\"" + PgSchemaUtil.simple_cont_name + "\")");
		System.exit(1);

	}

}
