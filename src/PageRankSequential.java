import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PageRankSequential {
	static Map<String, ArrayList<String>> dict;
	static Map<String, ArrayList<String>> outDict;
	static int iteration = 1;
	static Set<String> words;
	static LinkedHashMap<String, Double> sortedFinalPageRanks;
	static HashMap<String, Double> pageRanks;
	static HashMap<String, Double> pageRanksLeaked;
	static HashMap<String, Double> pageRanksUpdated;
	static int numNodes;
	static boolean converged = false;
	static double beta;
	static double epsilon = 0.005;
	private static HashMap<String, String> pageDict;

	public static void main(String[] args) {
		beta = Double.parseDouble(args[1]);
		if (args.length == 4) {
			readData(args[0], args[3]);
		} else {
			readData(args[0], null);
		}
		initializePageRanks();
		while (!converged) {
			updatePageRanks();
			checkForConvergence();
		}
		sortedFinalPageRanks = sortHashMapByValuesD(pageRanksUpdated);
		writeRankstoFile(args[2]);
	}

	private static void writeRankstoFile(String path) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(
					new File(path)));
			for (Iterator<String> iterator = sortedFinalPageRanks.keySet()
					.iterator(); iterator.hasNext();) {
				String link = iterator.next();
				int inLinks = dict.get(link) == null ? 0 : dict.get(link)
						.size();
				int outLinks = outDict.get(link) == null ? 0 : outDict
						.get(link).size();
				String toWrite = pageDict.get(link) + " : "
						+ sortedFinalPageRanks.get(link) + " , " + inLinks
						+ ", " + outLinks;
				bw.write(toWrite);
				if (iterator.hasNext()) {
					bw.write("\n");
				}
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void checkForConvergence() {
		System.out.println("Checking for convergence");
		double error = 0.0;
		for (Iterator<String> iterator = pageRanks.keySet().iterator(); iterator
				.hasNext();) {
			String link = iterator.next();
			error += Math.abs(pageRanksUpdated.get(link) - pageRanks.get(link));
		}
		// error = error / numNodes;
		if (error <= epsilon) {
			converged = true;
			System.out.println("Converged with error " + error);
		} else {
			iteration++;
			System.out.println("Error = " + error
					+ ". Continuing to iteration " + iteration);

		}
	}

	private static void updatePageRanks() {
		System.out.println("Running Iteration " + iteration);
		double nodes = 0.0 + numNodes;
		pageRanks = pageRanksUpdated;
		pageRanksUpdated = new HashMap<String, Double>();
		pageRanksLeaked = new HashMap<String, Double>();
		double totLeakedRank = 0.0;
		for (Iterator<String> iterator = pageRanks.keySet().iterator(); iterator
				.hasNext();) {
			String link = iterator.next();
			if (dict.containsKey(link)) {
				ArrayList<String> inLinks = dict.get(link);
				double leakedPageRank = 0.0;
				for (Iterator<String> iterator2 = inLinks.iterator(); iterator2
						.hasNext();) {
					String inLink = iterator2.next();
					ArrayList<String> outLinks = outDict.get(inLink);
					double numOutLinks = outLinks.size() + 0.0;
					leakedPageRank += beta * pageRanks.get(inLink);
					leakedPageRank = leakedPageRank / numOutLinks;
				}
				pageRanksLeaked.put(link, leakedPageRank);
				totLeakedRank += leakedPageRank;

			} else {
				pageRanksLeaked.put(link, 0.0);
			}

		}
		double unLeakedComponent = 1 - totLeakedRank;
		unLeakedComponent = unLeakedComponent / nodes;
		System.out.println("Completed calculating leaked ranks for iteration "
				+ iteration + " Unleaked Component " + unLeakedComponent);
		for (Iterator<String> iterator = pageRanks.keySet().iterator(); iterator
				.hasNext();) {
			String link = iterator.next();
			double updatedRank = pageRanksLeaked.get(link) + unLeakedComponent;
			pageRanksUpdated.put(link, updatedRank);
		}
		System.out.println("Updated Pageranks for iteration " + iteration);

	}

	private static void initializePageRanks() {
		numNodes = words.size();
		double nodes = numNodes + 0.0;
		System.out.println("Numnodes " + nodes);
		double initalRank = 1 / nodes;
		pageRanks = new HashMap<String, Double>();
		pageRanksUpdated = new HashMap<String, Double>();
		for (Iterator<String> iterator = words.iterator(); iterator.hasNext();) {
			String word = iterator.next();
			pageRanks.put(word, initalRank);
			pageRanksUpdated.put(word, initalRank);

		}
		System.out.println("Initialized Pageranks");
	}

	private static void readData(String inputFile, String dictFile) {
		File f = new File(inputFile + "/InListData/part-00000");
		words = new HashSet<String>();
		dict = new HashMap<String, ArrayList<String>>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();
			while (line != null) {
				String[] parts = line.split("\\t");
				words.add(parts[0]);
				String[] inLinksArray = parts[1].split(",");
				ArrayList<String> inLinks = new ArrayList<String>();
				for (int i = 0; i < inLinksArray.length; i++) {
					inLinks.add(inLinksArray[i]);
					words.add(inLinksArray[i]);
				}
				dict.put(parts[0], inLinks);
				line = br.readLine();
			}
			outDict = new HashMap<String, ArrayList<String>>();
			File f2 = new File(inputFile + "/OutListData/part-00000");
			BufferedReader br2 = new BufferedReader(new FileReader(f2));
			String line2 = br2.readLine();
			while (line2 != null) {
				String[] parts = line2.split("\\t");
				words.add(parts[0]);
				String[] outLinksArray = parts[1].split(",");
				ArrayList<String> outLinks = new ArrayList<String>();
				for (int i = 0; i < outLinksArray.length; i++) {
					outLinks.add(outLinksArray[i]);
				}
				outDict.put(parts[0], outLinks);
				line2 = br2.readLine();
			}
			pageDict = new HashMap<String, String>();
			if (dictFile != null) {
				File f3 = new File(dictFile);
				BufferedReader br3 = new BufferedReader(new FileReader(f3));
				String line3 = br3.readLine();
				while (line3 != null) {
					String[] parts = line3.split("\\s+");
					pageDict.put(parts[0], parts[1]);
					line3 = br3.readLine();
				}
			} else {
				for (Iterator<String> iterator = words.iterator(); iterator
						.hasNext();) {
					String node = iterator.next();
					pageDict.put(node, node);

				}
			}
			System.out.println("Done Reading Data");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static LinkedHashMap<String, Double> sortHashMapByValuesD(
			HashMap<String, Double> passedMap) {
		List<String> mapKeys = new ArrayList<String>(passedMap.keySet());
		List<Double> mapValues = new ArrayList<Double>(passedMap.values());
		Collections.sort(mapValues, new Comparator<Double>() {

			@Override
			public int compare(Double o1, Double o2) {
				if (o1 < o2) {
					return 1;
				} else if (o1 > o2) {
					return -1;
				} else {
					return 0;
				}
			}
		});
		Collections.sort(mapKeys, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});

		LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<String, Double>();

		Iterator<Double> valueIt = mapValues.iterator();
		while (valueIt.hasNext()) {
			double val = valueIt.next();
			Iterator<String> keyIt = mapKeys.iterator();

			while (keyIt.hasNext()) {
				String key = keyIt.next();
				double comp1 = passedMap.get(key);
				double comp2 = val;

				if (comp1 == comp2) {
					passedMap.remove(key);
					mapKeys.remove(key);
					sortedMap.put(key, val);
					break;
				}

			}

		}
		return sortedMap;
	}
}
