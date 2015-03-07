package org.f3tools.incredible.smartETL.utilities;

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
	
	public static final int getFreeMemoryPercentage()
	{
		Runtime runtime = Runtime.getRuntime();
		long maxMemory = runtime.maxMemory();
		long allocatedMemory = runtime.totalMemory();
		long freeMemory = runtime.freeMemory();
		long totalFreeMemory = (freeMemory + (maxMemory - allocatedMemory));
		
		int percentage = (int)Math.round(100*(double)totalFreeMemory / (double)maxMemory);
		
		return percentage;
	}	
}
