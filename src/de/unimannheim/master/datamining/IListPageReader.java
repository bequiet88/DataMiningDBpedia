package de.unimannheim.master.datamining;

// TODO: Auto-generated Javadoc
/**
 * The Interface IListPageReader.
 *
 * @param <T> the generic type
 */
public interface IListPageReader<T> {

	/**
	 * Open input.
	 *
	 * @param resource the resource
	 */
	public void openInput(ReaderResource resource) throws Exception;

	/**
	 * Read input.
	 *
	 * @return the t
	 * @throws Exception the exception
	 */
	public T readInput() throws Exception;

	/**
	 * Close.
	 */
	public void close() throws Exception;

}
