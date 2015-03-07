package org.f3tools.incredible.smartETL.utilities;

import java.io.File;
import java.io.IOException;


public class FileUtl
{

	public static File createTempFile(String prefix, String suffix, String directory) throws ETLException
	{
		try
		{
			File dir = new File(directory);
			if (!dir.exists()) dir.mkdirs();
				
			return File.createTempFile(prefix, suffix, dir);
		} catch (IOException e)
		{
			throw new ETLException(e);
		}
	}

}
