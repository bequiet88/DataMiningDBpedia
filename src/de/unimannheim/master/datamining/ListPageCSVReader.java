package de.unimannheim.master.datamining;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

/**
 * The Class ListPageCSVReader.
 */
public class ListPageCSVReader implements IListPageReader<List<List<String>>> {

	/** The reader. */
	private BufferedReader reader = null;

	/** The writer. */
	private BufferedWriter writer = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.unimannheim.dws.wikilist.reader.IListPageReader#openInput(de.unimannheim
	 * .dws.wikilist.reader.ReaderResource)
	 */
	@Override
	public void openInput(ReaderResource resource) throws Exception {

		reader = new BufferedReader(new FileReader(new File(
				resource.getResource())));
		;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.unimannheim.dws.wikilist.reader.IListPageReader#readInput()
	 */
	@Override
	public List<List<String>> readInput() throws Exception {

		List<List<String>> result = new ArrayList<List<String>>();
		Iterable<CSVRecord> parser = CSVFormat.newFormat('\t').parse(reader);
//				CSVFormat.EXCEL.parse(reader);
		boolean firstRecord = true;
		for (CSVRecord record : parser) {
			if (firstRecord)
				firstRecord = false;
			else {
				List<String> recordFields = new ArrayList<String>();
				Iterator<String> iter = record.iterator();
				while (iter.hasNext()) {
					recordFields.add(iter.next());
				}
				result.add(recordFields);
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
	public void close() throws Exception {
		if (reader != null)
			reader.close();
		if(writer != null)
			writer.close();
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
