package org.f3tools.incredible.smartETL.utilities;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

/**
 * Utility classes on file operations
 * @author Desheng Kang
 * @since 2015/3/8
 */

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
	
	/**
	 * Find a list of files based on the pattern. The pattern only applies
	 * to file names. This is not as flexible as Spring which can do folder
	 * matching as well.
	 * @param filePattern
	 * @return
	 */
	public static File[] findFiles(String filePattern)
	{
		String folder;
		final String pattern;
		
		int idx1 = filePattern.lastIndexOf("/");
		int idx2 = filePattern.lastIndexOf("\\");
		
		int idx = Math.max(idx1, idx2);
		
		if (idx < 0)
		{
			folder = ".";
			pattern = filePattern;
		}
		else
		{
			folder = filePattern.substring(0, idx);
			pattern = filePattern.substring(idx + 1);
		}

		File dir = new File(folder);
		
		if (!dir.exists()) return null;
		
		return dir.listFiles(new FilenameFilter() 
			{
				public boolean accept (File theDir, String name) 
				{
					AntPathStringMatcher matcher = new AntPathStringMatcher(pattern, name, null);
					return matcher.matchStrings();
				}
			});
	}
}
