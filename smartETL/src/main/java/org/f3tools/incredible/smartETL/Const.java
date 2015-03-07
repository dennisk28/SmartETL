package org.f3tools.incredible.smartETL;

public class Const {

	public final static String DATASET_BUFFER_SIZE_PARAM = "dataset_buffer_size";
	public final static int DATASET_BUFFER_SIZE_DEFAULT = 5000;	
	
	/**
	 * Sleep time waiting when buffer is empty (the default)
	 */
	public static final int TIMEOUT_GET_MILLIS = 50;


	/**
	 * Sleep time waiting when buffer is full (the default)
	 */
	public static final int TIMEOUT_PUT_MILLIS = 50;

	  /**
	   * The name of the variable that optionally contains an alternative rowset get timeout (in ms).
	   * This only makes a difference for extremely short lived transformations.
	   */
	  public static final String DATASET_GET_TIMEOUT = "DATASET_GET_TIMEOUT";

	  /**
	   * The name of the variable that optionally contains an alternative rowset put timeout (in ms).
	   * This only makes a difference for extremely short lived transformations.
	   */
	  public static final String DATASET_PUT_TIMEOUT = "DATASET_PUT_TIMEOUT";	
	
	/**
	 * Convert a String into an integer.  If the conversion fails, assign a default value.
	 * @param str The String to convert to an integer 
	 * @param def The default value
	 * @return The converted value or the default.
	 */
	public static final int toInt(String str, int def)
	{
		int retval;
		try
		{
			if (str == null) return def;
			retval = Integer.parseInt(str);
		} catch (Exception e)
		{
			retval = def;
		}		
		return retval;
	}	
	
	public static final boolean toBoolean(String str)
	{
		if (str == null) return false;
		
		if (str.equalsIgnoreCase("true"))
			return true;
		else
			return false;
	}
}
