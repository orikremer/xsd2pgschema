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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringEscapeUtils;

/**
 * Sphinx data source -> Sphinx dictionary index
 */
public class dicmerge4sphinx {

	public static int freq_threshold = 10;

	public static List<String> dic_file_list = new ArrayList<String>();

	public static void main(String[] args) {

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--dic"))
				dic_file_list.add(args[++i]);

			else if (args[i].equals("--freq"))
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

		try {

			HashMap<String, Integer> dictionary = new HashMap<String, Integer>();

			int dic_files = 0;

			for (String dic_file_name : dic_file_list) {

				File dic_file = new File(dic_file_name);

				if (!dic_file.isFile()) {
					System.err.println("Not a file '" + dic_file_name + "'.");
					System.exit(1);
				}

				FileReader filer = new FileReader(dic_file);
				BufferedReader bufferr = new BufferedReader(filer);

				String line = null;

				while ((line = bufferr.readLine()) != null) {

					String[] parsed_line = line.split("[\\s,]+");

					if (parsed_line.length != 2)
						continue;

					String keyword = parsed_line[0];

					if (keyword.isEmpty())
						continue;

					int freq = Integer.valueOf(parsed_line[1]);

					if (dic_files > 0 && dictionary.containsKey(keyword))
						freq += dictionary.get(keyword);

					dictionary.put(keyword, freq);

				}

				bufferr.close();
				filer.close();

				dic_files++;

			}

			File sphinx_data_source = new File(PgSchemaUtil.sphinx_data_source_name);

			FileWriter filew = new FileWriter(sphinx_data_source);

			filew.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");

			filew.write("<sphinx:docset>\n");

			filew.write("<sphinx:schema>\n");

			filew.write("<sphinx:attr name=\"keyword\" type=\"string\"/>\n");
			filew.write("<sphinx:attr name=\"freq\" type=\"int\" bits=\"32\"/>\n");
			filew.write("<sphinx:field name=\"" + PgSchemaUtil.trigram_field_name + "\"/>\n"); // default field

			filew.write("</sphinx:schema>\n");

			int id = 0;

			for (Entry<String, Integer> entry : dictionary.entrySet()) {

				String keyword = (String) entry.getKey();
				int freq = (int) entry.getValue();

				if (freq < freq_threshold)
					continue;

				filew.write("<sphinx:document id=\"" + (++id) + "\">\n");
				filew.write("<keyword>" + StringEscapeUtils.escapeXml10(keyword) + "</keyword>\n");
				filew.write("<freq>" + freq + "</freq>\n");
				filew.write("<" + PgSchemaUtil.trigram_field_name + ">" + toTrigram(keyword) + "</" + PgSchemaUtil.trigram_field_name + ">\n");
				filew.write("</sphinx:document>\n");

			}

			filew.write("</sphinx:docset>\n");
			filew.close();

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

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

	private static void showUsage() {

		System.err.println("dicmerge4sphinx: Sphinx data source -> Sphinx dictionary index");
		System.err.println("Usage:  --dic DIC_FILE (repeat until you specify all dictionaries)");
		System.err.println("Option: --freq FREQ_THRESHOLD (default=" + freq_threshold + ")");
		System.exit(1);

	}

}
