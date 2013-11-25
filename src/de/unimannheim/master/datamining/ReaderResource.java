package de.unimannheim.master.datamining;

import java.util.List;


/**
 * The Class ReaderResource.
 */
public class ReaderResource {

	/** The resource. */
	private String resource;
	
	/** The resources. */
	private List<String> resources;
	
	/** The attribute. */
	private String attribute;
	
	/** The attr prefix. */
	private String attrPrefix;
	
	/** The attr prefix. */
	private List<Triple<String, String, String>> triples;
	
	/**
	 * Constructor for DBPedia Reader.
	 *
	 * @param resources the resources
	 * @param attribute the attribute
	 * @param attrPrefix the attr prefix
	 */
	public ReaderResource(List<String> resources, String attribute,
			String attrPrefix) {
		super();
		this.resources = resources;
		this.attribute = attribute;
		this.attrPrefix = attrPrefix;
	}

	/**
	 * Constructor for DBPedia Reader II.
	 *
	 * @param resources the resources
	 */	
	public ReaderResource(List<Triple<String, String, String>> resources) {
		this.triples = resources;
	}

	/**
	 * Constructor for Text-File, WikiMarkUp, CSV.
	 *
	 * @param resource is path to file or title of wiki page
	 */
	public ReaderResource(String resource) {
		super();
		this.resource = resource;
	}

	/**
	 * Gets the resource.
	 *
	 * @return the resource
	 */
	public String getResource() {
		return resource;
	}

	/**
	 * Gets the resources.
	 *
	 * @return the resources
	 */
	public List<String> getResources() {
		return resources;
	}

	/**
	 * Gets the attribute.
	 *
	 * @return the attribute
	 */
	public String getAttribute() {
		return attribute;
	}

	/**
	 * Gets the attr prefix.
	 *
	 * @return the attr prefix
	 */
	public String getAttrPrefix() {
		return attrPrefix;
	}

	/**
	 * Gets the triples.
	 *
	 * @return the triples
	 */
	public List<Triple<String, String, String>> getTriples() {
		return triples;
	}
}
