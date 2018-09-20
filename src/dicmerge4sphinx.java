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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.text.StringEscapeUtils;

/**
 * Merge Sphinx dictionary index.
 *
 * @author yokochi
 */
public class dicmerge4sphinx {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The data source directory name. */
		String ds_dir_name = xml2sphinxds.ds_dir_name;

		/** The dictionary file list. */
		List<String> dic_file_list = new ArrayList<String>();

		/** The threshold frequency for index dictionary. */
		int freq_threshold = PgSchemaUtil.freq_threshold;

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--ds-dir") && i + 1 < args.length)
				ds_dir_name = args[++i];

			else if (args[i].equals("--dic") && i + 1 < args.length)
				dic_file_list.add(args[++i]);

			else if (args[i].equals("--freq") && i + 1 < args.length)
				freq_threshold = Integer.valueOf(args[++i]);

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (dic_file_list.size() == 0) {
			System.err.println("There is no source dictionary to merge.");
			showUsage();
		}

		Path ds_dir_path = Paths.get(ds_dir_name);

		if (!Files.isDirectory(ds_dir_path)) {

			try {
				Files.createDirectory(ds_dir_path);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		try {

			HashMap<String, Integer> dictionary = new HashMap<String, Integer>();

			int dic_files = 0;

			for (String dic_file_name : dic_file_list) {

				Path dic_file_path = Paths.get(dic_file_name);

				if (!Files.isRegularFile(dic_file_path)) {
					System.err.println("Not a file '" + dic_file_name + "'.");
					System.exit(1);
				}

				BufferedReader buffr = Files.newBufferedReader(dic_file_path);

				String line = null, keyword;
				String[] parsed_line;
				int freq;

				while ((line = buffr.readLine()) != null) {

					parsed_line = line.split("[\\s,]+");

					if (parsed_line.length != 2)
						continue;

					keyword = parsed_line[0];

					if (keyword.isEmpty())
						continue;

					freq = Integer.valueOf(parsed_line[1]);

					if (dic_files > 0 && dictionary.containsKey(keyword))
						freq += dictionary.get(keyword);

					dictionary.put(keyword, freq);

				}

				buffr.close();

				dic_files++;

			}

			Path sphinx_data_source_path = Paths.get(ds_dir_name, PgSchemaUtil.sph_data_source_name);

			BufferedWriter buffw = Files.newBufferedWriter(sphinx_data_source_path);

			buffw.write("<?xml version=\"" + PgSchemaUtil.def_xml_version + "\" encoding=\"" + PgSchemaUtil.def_encoding + "\"?>\n");

			buffw.write("<sphinx:docset>\n");

			buffw.write("<sphinx:schema>\n");

			buffw.write("<sphinx:attr name=\"keyword\" type=\"string\"/>\n");
			buffw.write("<sphinx:attr name=\"freq\" type=\"int\" bits=\"32\"/>\n");
			buffw.write("<sphinx:field name=\"trigrams\"/>\n"); // default field

			buffw.write("</sphinx:schema>\n");

			int id = 0, freq;
			String keyword;

			for (Entry<String, Integer> entry : dictionary.entrySet()) {

				keyword = (String) entry.getKey();
				freq = (int) entry.getValue();

				if (freq < freq_threshold)
					continue;

				buffw.write("<sphinx:document id=\"" + (++id) + "\">\n");
				buffw.write("<keyword>" + StringEscapeUtils.escapeXml10(keyword) + "</keyword>\n");
				buffw.write("<freq>" + freq + "</freq>\n");
				buffw.write("<trigrams>" + toTrigram(keyword) + "</trigrams>\n");
				buffw.write("</sphinx:document>\n");

			}

			buffw.write("</sphinx:docset>\n");

			buffw.close();

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/**
	 * Convert keyword to trigram.
	 *
	 * @param keyword keyword
	 * @return String trigram
	 */
	private static String toTrigram(String keyword) {

		if (keyword == null || keyword.isEmpty())
			return "";

		keyword = "__" + keyword + "__";

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < keyword.length() - 2; i++)
			sb.append(keyword.substring(i, i + 3) + " ");

		int len = sb.length();

		if (len == 0)
			return "";

		return sb.substring(0, len - 1);
	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("dicmerge4sphinx: Sphinx data source -> Sphinx dictionary index");
		System.err.println("Usage:  --ds-dir DIRECTORY (default=\"" + xml2sphinxds.ds_dir_name + "\")");
		System.err.println("        --dic DIC_FILE (repeat until you specify all dictionaries)");
		System.err.println("Option: --freq FREQ_THRESHOLD (default=" + PgSchemaUtil.freq_threshold + ")");
		System.exit(1);

	}

}
