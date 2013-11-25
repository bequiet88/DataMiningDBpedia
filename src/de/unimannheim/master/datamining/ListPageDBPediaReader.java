package de.unimannheim.master.datamining;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;

/**
 * The Class ListPageDBPediaReader.
 */
public class ListPageDBPediaReader implements
		IListPageReader<HashMap<String, String>> {

	/** The writer. */
	private BufferedWriter writer = null;

	/** The dbp res. */
	private ReaderResource dbpRes = null;

	/** The query str. */
	private String queryStr = null;

	/** The query. */
	private Query query = null;

	/** The qexec. */
	private QueryExecution qexec = null;

	/** The prefix. */
	private String prefix = "PREFIX owl: <http://www.w3.org/2002/07/owl#>"
			+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
			+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
			+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>"
			+ "PREFIX : <http://dbpedia.org/resource/>"
			+ "PREFIX dbpprop: <http://dbpedia.org/property/>"
			+ "PREFIX dbpedia: <http://dbpedia.org/>"
			+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"
			+ "PREFIX owl: <http://dbpedia.org/ontology/>"
			+ "PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>"
			+ "PREFIX dbpprop-de: <http://de.dbpedia.org/property/>";
	
	public HashMap<String, String> companyNames;
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.unimannheim.dws.wikilist.reader.IListPageReader#openInput(de.unimannheim
	 * .dws.wikilist.reader.ReaderResource)
	 */
	@Override
	public void openInput(ReaderResource resource) {

		dbpRes = resource;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.unimannheim.dws.wikilist.reader.IListPageReader#readInput()
	 */
	@Override
	public HashMap<String, String> readInput() {

		HashMap<String, String> result = new HashMap<String, String>();

		if (dbpRes.getResources() != null) {

			for (String dbpInstance : dbpRes.getResources()) {

				try {

					if (!dbpInstance.equals("")) {

						queryStr = prefix
								+

								/*
								 * Example Query for
								 * http://en.wikipedia.org/wiki
								 * /List_of_Peers_1330 -1339 Instances
								 */
								"SELECT * WHERE {"
								// +"<http://dbpedia.org/resource/Henry_Plantagenet,_3rd_Earl_of_Leicster_and_Lancaster> dbpprop:title ?title. }";
								+ dbpInstance + " " + dbpRes.getAttrPrefix()
								+ ":" + dbpRes.getAttribute() + " ?"
								+ dbpRes.getAttribute() + ". }";

						query = QueryFactory.create(queryStr);

						// Remote execution.
						qexec = QueryExecutionFactory.sparqlService(
								"http://de.dbpedia.org/sparql", query);
						// Set the DBpedia specific timeout.
						((QueryEngineHTTP) qexec).addParam("timeout", "10000");

						// Execute.
						ResultSet rs = qexec.execSelect();

						// ResultSetFormatter.out(System.out, rs, query);
						// Add result to HashMap.
						result.put(companyNames.get(dbpInstance),
								ResultSetFormatter.asXMLString(rs));
						System.out.println("DBPedia Query successful for "
								+ dbpInstance);
						this.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
					result.put(companyNames.get(dbpInstance),
							"");
					continue;
				}
			}
		} else if (dbpRes.getTriples() != null) {
			for (Triple<String, String, String> dbpQuery : dbpRes.getTriples()) {

				try {

					queryStr = prefix +

					/*
					 * Example Query for http://en.wikipedia.org/wiki
					 * /List_of_Peers_1330 -1339 Instances
					 */
					"SELECT * WHERE {"
							// +"<http://dbpedia.org/resource/Henry_Plantagenet,_3rd_Earl_of_Leicster_and_Lancaster> dbpprop:title ?title. }";
							+ dbpQuery.getFirst() + " " + dbpQuery.getSecond()
							+ " " + dbpQuery.getThird() + ". }";

					query = QueryFactory.create(queryStr);

					// Remote execution.
					qexec = QueryExecutionFactory.sparqlService(
							"http://dbpedia.org/sparql", query);
					// Set the DBpedia specific timeout.
					((QueryEngineHTTP) qexec).addParam("timeout", "10000");

					// Execute.
					ResultSet rs = qexec.execSelect();

					// ResultSetFormatter.out(System.out, rs, query);
					// Add result to HashMap.
					result.put(dbpQuery.toString(),
							ResultSetFormatter.asXMLString(rs));
					System.out.println("DBPedia Query successful for "
							+ dbpQuery.toString());
					this.close();

				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.unimannheim.dws.wikilist.reader.IListPageReader#close()
	 */
	@Override
	public void close() {
		if (qexec != null)
			qexec.close();
	}

	/**
	 * Write output to file.
	 * 
	 * @param path
	 *            the path
	 * @param text
	 *            the text
	 */
	public void writeOutputToFile(String path, String text) {
		try {
			writer = new BufferedWriter(new FileWriter(new File(path)));
			writer.write(text);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
