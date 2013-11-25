package de.unimannheim.master.datamining;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class CopyOfWikiList {

	/*
	 * Class variables
	 */
	public static String rdfTag = "";
	public static String rdfTagPrefix = "";
	public static String wikiListURL = "";
	public static String wikiListName = "";
	public static String regexInstances = "";
	public static int columnInstance = -1;
	public static String captureGroup = "";
	public static int columnPosition = 0;
	public static String pathToResult = "C:/Users/d049650/Documents/Private_Stuff/Data Mining/";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) {

		// Logger log = Logger.getLogger(CopyOfWikiList.class);

		/*****************************
		 * Data Collection
		 *****************************/

		/*
		 * Read List of Instances Read list of attributes available in one list
		 * of instances
		 */
		System.out.println("Read table list ..");

		ReaderResource myCSVRes = new ReaderResource(pathToResult
				+ "Unternehmen.txt");

		ListPageCSVReader myListsReader = new ListPageCSVReader();
		List<List<String>> myListsList = new ArrayList<List<String>>();

		try {
			myListsReader.openInput(myCSVRes);
			myListsList = myListsReader.readInput();
			myListsReader.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		System.out.println("done!");

		// System.out.println("Read instance list ..");
		//
		// ReaderResource myCSVRes2 = new ReaderResource(
		// "C:/Users/d049650/Documents/Uni_Workspace/LinkstoInstances.csv");
		//
		// ListPageCSVReader myInstancesReader = new
		// ListPageCSVReader();
		// myInstancesReader.openInput(myCSVRes2);
		// List<List<String>> myInstancesList = myInstancesReader
		// .readInput();
		// myInstancesReader.close();
		//
		// System.out.println("done!");

		/*
		 * Obtain plain Wiki Mark Up (formerly Test Data Set)
		 */
		System.out.println("Obtain DBPedia Company URLs ..");

		/*
		 * Find corresponding list of DBPedia instances
		 */
		List<String> dbpediaResources = new ArrayList<String>();
		HashMap<String, String> companyNames = new HashMap<String, String>();

		for (List<String> company : myListsList) {
			String wikilink = wiki2dbpLink(company.get(1));
			if (wikilink.equals("")) {
				companyNames.put(company.get(0), company.get(0));
				dbpediaResources.add(company.get(0));
			} else {
				companyNames.put(wikilink, company.get(0));
				dbpediaResources.add(wikilink);
			}
		}
		
		System.out.println("Read Company list ..");

		myCSVRes = new ReaderResource(pathToResult
				+ "company_names.txt");

		myListsReader = new ListPageCSVReader();
		List<List<String>> myCompanyList = new ArrayList<List<String>>();

		try {
			myListsReader.openInput(myCSVRes);
			myCompanyList = myListsReader.readInput();
			myListsReader.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		HashMap<String, Integer> mySeqOfCompanies = sequenceOfCompanies(myCompanyList);

		System.out.println("done!");

		/*
		 * General settings for this run
		 */

		for (List<String> list : myListsList) {

			if (!list.get(0).contains("ACINO"))
				continue;

			wikiListURL = list.get(1);
			wikiListName = wikiListURL.replace("http://de.wikipedia.org/wiki/",
					"");

			System.out.println("Start processing of table " + wikiListName);

			/*
			 * Read list of DBPedia Attributes
			 */
			List<String> dbpediaAttributes = new ArrayList<String>();
			for (int i = 2; i < list.size(); i++) {
				dbpediaAttributes.add(list.get(i));
			}

			//
			// for (List<String> instanceList : myInstancesList) {
			//
			// if (instanceList.get(0).equals(wikiListURL)) {
			// regexInstances = instanceList.get(1);
			// captureGroup = instanceList.get(2);
			// for (int i = 3; i < instanceList.size(); i++) {
			// dbpediaResources.add(instanceList.get(i));
			// }
			//
			// }
			// }

			/*
			 * If size > 0 iterate over the instances for each DBPedia
			 * attribute.
			 */
			if (dbpediaResources.size() == 0 || dbpediaAttributes.size() == 0)
				continue;

			/*
			 * Reset column counter.
			 */
			columnPosition = 0;

			for (String string : dbpediaAttributes) {

				/*
				 * If column is empty, continue
				 */
				if (string == null || string.trim().equals("")) {
					columnPosition++;
					continue;
				}

				/*
				 * If column is instance column, continue
				 */
				if (columnPosition == columnInstance
						|| string.equals("Column of Entity")) {
					columnPosition++;
					continue;
				}

				System.out.println("Start processing of attribute " + string);

				String[] dbpediaAttribute = string.split(":");
				rdfTag = dbpediaAttribute[1];
				rdfTagPrefix = dbpediaAttribute[0];

				/****************************************************************
				 * Obtain Evaluation Data Set with JWPL and DBPedia Values
				 * merged
				 ****************************************************************/

				System.out.println("Obtain Evaluation Data Set ..");

				ReaderResource myEvalRes = new ReaderResource(dbpediaResources,
						rdfTag, rdfTagPrefix);

				ListPageDBPediaReader myDBPReader = new ListPageDBPediaReader();
				myDBPReader.companyNames = companyNames;
				myDBPReader.openInput(myEvalRes);

				HashMap<String, String> myDBPValues;
				// try {
				myDBPValues = myDBPReader.readInput();
				// } catch (Exception e) {
				// e.printStackTrace();
				// columnPosition++;
				// continue;
				// }
				myDBPReader.close();

				List<List<String>> myExtractedDBPValues = processXML(myDBPValues);



				writeOutputToCsv(mySeqOfCompanies, pathToResult
						+ "dbpedia/values_" + rdfTag + ".csv",
						myExtractedDBPValues);

				System.out.println("done!");
				columnPosition++;
			}
		}
	}

	/**
	 * Wiki2dbp link.
	 * 
	 * @param s
	 *            the s
	 * @return the string
	 */
	public static String wiki2dbpLink(String s) {
		if (s.equals(""))
			return "";
		try {
			s = URLDecoder.decode(s, "UTF-8");
			s = s.replace("http://de.wikipedia.org/wiki/", "");
			return "<http://de.dbpedia.org/resource/" + s.replaceAll(" ", "_")
					+ ">";
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
	}

	public static List<List<String>> processXML(
			HashMap<String, String> dbpValues) {

		List<List<String>> result = new ArrayList<List<String>>();

		/*
		 * Initiate XML Parser
		 */
		for (String link : dbpValues.keySet()) {

			List<String> lineList = new ArrayList<String>();

			lineList.add(link);

			StringBuilder resLine = new StringBuilder();

			InputSource source = new InputSource(new StringReader(
					dbpValues.get(link)));

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db;

			try {
				db = dbf.newDocumentBuilder();
				Document document = db.parse(source);

				XPathFactory xpathFactory = XPathFactory.newInstance();
				XPath xpath = xpathFactory.newXPath();

				/*
				 * Extract Uri values from DBPedia and transform to comparable
				 * format
				 */

				XPathExpression expr = xpath.compile("//binding[@name = \""
						+ rdfTag + "\"]/uri/text()");

				// "/Employees/Employee[gender='Female']/name/text()");

				List<String> uris = new ArrayList<String>();
				NodeList nodes = (NodeList) expr.evaluate(document,
						XPathConstants.NODESET);
				for (int i = 0; i < nodes.getLength(); i++)
					uris.add(nodes.item(i).getNodeValue());

				if (uris.size() > 0) {

					for (String dbpUri : uris) {
						resLine.append(" "
								+ dbpUri.replace(
										"http://de.dbpedia.org/resource/", "")
										.replace("_", " "));
					}
				}

				/*
				 * Extract Literal values from DBPedia and transform to
				 * comparable format
				 */

				expr = xpath.compile("//binding[@name = \"" + rdfTag
						+ "\"]/literal/text()");

				List<String> literals = new ArrayList<String>();
				nodes = (NodeList) expr.evaluate(document,
						XPathConstants.NODESET);
				for (int i = 0; i < nodes.getLength(); i++)
					literals.add(nodes.item(i).getNodeValue());

				if (literals.size() > 0) {

					for (String dbpLiteral : literals) {
						if (rdfTag.equals("mitarbeiterzahl")) {
							dbpLiteral = parseNumbers(dbpLiteral);
						}
						resLine.append(" " + dbpLiteral);
					}
				}

				lineList.add(resLine.toString().trim());
				result.add(lineList);

			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (XPathExpressionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				lineList.add(resLine.toString().trim());
				result.add(lineList);
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;

	}

	public static HashMap<String, Integer> sequenceOfCompanies(
			List<List<String>> list) {

		HashMap<String, Integer> result = new HashMap<String, Integer>();

		for (List<String> list2 : list) {

			if (!result.containsKey(list2.get(0))) {
				result.put(list2.get(0), 1);
			} else {
				result.put(list2.get(0), result.get(list2.get(0)) + 1);
			}
		}
		return result;
	}

	/**
	 * Write output to csv.
	 * 
	 * @param path
	 *            the path
	 * @param data
	 *            the data
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void writeOutputToCsv(
			HashMap<String, Integer> seqOfCompanies, String path,
			List<List<String>> data) {

		// generate CSVPrinter Object
		// FileOutputStream csvBAOS = new FileOutputStream(new File(path));
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(new File(path)));

			// OutputStreamWriter csvWriter = new OutputStreamWriter(csvBAOS);
			CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);

			csvPrinter.print("sep=,");
			csvPrinter.println();

			csvPrinter.print("Companies");
			csvPrinter.print(rdfTag);
			csvPrinter.println();

			for (List<String> list : data) {

				System.out.print(list.get(0) + " : ");
				System.out.println(seqOfCompanies.get(list.get(0)));
				try {

					for (int i = 0; i < seqOfCompanies.get(list.get(0)); i++) {
						for (String string : list) {
							csvPrinter.print(string);
						}
						csvPrinter.println();
					}
				} catch (NullPointerException e) {
					for (String string : list) {
						System.out.println(string);
					}
				}

			}

			csvPrinter.flush();
			csvPrinter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block

			e.printStackTrace();
		}
	}

	public static String parseNumbers(String s) {
		StringBuilder result = new StringBuilder();
		StreamTokenizer t = new StreamTokenizer(new StringReader(s));
		t.resetSyntax();
		t.parseNumbers();
		try {
			if (t.nextToken() == StreamTokenizer.TT_NUMBER) {
				result.append(t.nval);
			}
		} catch (IOException e) {
			e.printStackTrace();
			result.append(s);
		}
		return result.toString();
	}
}
