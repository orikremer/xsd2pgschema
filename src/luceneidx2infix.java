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

import net.sf.xsd2pgschema.*;
import net.sf.xsd2pgschema.luceneutil.InputIteratorWrapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
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

	/** The analyzing infix suggester directory name. */
	private static String infix_dir_name = "lucene_infix";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The index directory name. */
		String idx_dir_name = xml2luceneidx.idx_dir_name;

		/** The field list. */
		List<String> fields = new ArrayList<String>();

		/** The threshold frequency for index dictionary. */
		int freq_threshold = PgSchemaUtil.freq_threshold;

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--idx-dir") && i + 1 < args.length)
				idx_dir_name = args[++i];

			else if (args[i].equals("--field") && i + 1 < args.length)
				fields.add(args[++i]);

			else if (args[i].equals("--infix-dir") && i + 1 < args.length)
				infix_dir_name = args[++i];

			else if (args[i].equals("--freq") && i + 1 < args.length)
				freq_threshold = Integer.valueOf(args[++i]);

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		Path idx_dir_path = Paths.get(idx_dir_name);

		if (!Files.isDirectory(idx_dir_path)) {
			System.err.println("Couldn't find directory '" + idx_dir_name + "'.");
			showUsage();
		}

		Path infix_dir_path = Paths.get(infix_dir_name);

		if (!Files.isDirectory(infix_dir_path)) {

			try {
				Files.createDirectory(infix_dir_path);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		try {

			Directory idx = FSDirectory.open(idx_dir_path);

			Analyzer analyzer = new StandardAnalyzer();

			IndexReader reader = DirectoryReader.open(idx);

			// Lucene dictionary for AnalyzingInfixSuggester

			Directory infix = FSDirectory.open(infix_dir_path);

			AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(infix, analyzer);

			if (fields.size() == 0) {

				System.out.println("field: " + PgSchemaUtil.simple_content_name);

				Dictionary dictionary = new LuceneDictionary(reader, PgSchemaUtil.simple_content_name);
				suggester.build(dictionary);

			}

			else {

				boolean[] contains = new boolean[fields.size()];
				Arrays.fill(contains, false);

				InputIteratorWrapper iters = new InputIteratorWrapper();

				List<LeafReaderContext> leaves = reader.leaves();

				for (LeafReaderContext leaf : leaves) {

					for (String field_in : fields) {

						if (leaf.reader().getDocCount(field_in) >= freq_threshold) {

							System.out.println("field: " + field_in);

							contains[fields.indexOf(field_in)] = true;

							Dictionary dictionary = new LuceneDictionary(reader, field_in);

							iters.add(dictionary.getEntryIterator());

						}

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

			System.out.println("Done index -> infix (" + infix_dir_path.toString() + ").");

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
		System.err.println("Option: --field FIELD_NAME (default=\"" + PgSchemaUtil.simple_content_name + "\")");
		System.err.println("        --freq FREQ_THRESHOLD (default=" + PgSchemaUtil.freq_threshold + ")");
		System.exit(1);

	}

}
