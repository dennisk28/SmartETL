package org.f3tools.incredible.utilities;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Generic utility functions
 * @author Dennis
 *
 */

public class Utl 
{
	public static void check(boolean condition, String errMsg) throws ETLException
	{
		if (condition) throw new ETLException(errMsg);
	}
	
	public static String stackTrack(Exception e)
	{
		ByteArrayOutputStream os = new ByteArrayOutputStream(1000);
		
		e.printStackTrace(new PrintStream(os));
		return os.toString();
	}
}
