package org.f3tools.incredible.smartETL.utilities;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.f3tools.incredible.smartETL.DataDef;
import org.f3tools.incredible.utilities.ETLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVFile
{
	private Logger logger = LoggerFactory.getLogger(CSVFile.class);
	
	private BufferedReader br;
	private String delimiter;
	private String quote;
	private String path;
	private DataDef dataDef;
	
	public CSVFile(DataDef dataDef, String path, String delimiter, String quote, boolean hasTitle) throws ETLException
	{
		try
		{
			this.dataDef = dataDef;
			this.delimiter = delimiter;
			this.quote = quote;
			this.path = path;
			
			br = new BufferedReader(new InputStreamReader(new FileInputStream(path)), 
				5000);
			
			if (hasTitle) readRow(false);
			
		} catch (Exception e)
		{
			throw new ETLException(e);
		}
	}
	
	public void setDataDef(DataDef dataDef)
	{
		this.dataDef = dataDef;
	}

	public Object[] readRow(boolean conversion)
	{
		String line = null;
		Object[] row = null;
		int fieldCount = this.dataDef.getFieldCount();
		
		try
		{
			line = br.readLine();
		
			while (line != null)
			{
				row = createElements(line, this.delimiter, (char)0, this.quote, conversion);
				
				// shall add code to log problematic rows
				if (row != null)
				{
					if (row.length != fieldCount)
					{
						logger.error("actual field amount {} is less than required amount {}, line:{}", row.length,
								fieldCount, line);
					}
					else
						return row;
				}
				else
					logger.error("return null for line{}", line);
				
				line = br.readLine();
			}
			
			return null;
				
		} catch (Exception e)
		{
			if (line != null)
			{
				logger.info("processing line: {}", line);
			}
			
			logger.error("can't create row elements", e);
			return null;
		}
	}
	
	public void close()
	{
		try
		{
			if (br != null) br.close();
		} catch (Exception e)
		{
			logger.error("Can't close file {}", path, e);
		}
	}
	
    /**
     * This method returns a ArrayList which contains tokens from the input line.
     * Empty element which occurs when there is nothing between two delimiters
     * will be added in the row ArrayList as null.
     *
     * Since java.util.StringTokenizer doesn't work well for empty element,
     * we need write our own tokenizer
     *
     * @param aLine
     * @param delimiters
     * @param escape if escape is not 0, escape is supported. for example, if delimiter is ",", "\," will be treated as ","
     * @return
     */
    private Object[] createElements(String aLine, String delimiter, char escape, String quoteStr, boolean conversion) throws ETLException
    {
        int nPos = 0;
        int nLen = aLine.length();
        char c;
        char[] eBuf = new char[nLen];
        int ePos = 0;
        char dl = delimiter.charAt(0);
        boolean startQ = false;
        
        int fldCount = this.dataDef.getFieldCount();
        
        Object[] vRow = new Object[fldCount];
        int i = 0;
        
        char quote = 0;
        
        if (quoteStr != null) quote = quoteStr.charAt(0);
        
        //ArrayList<String> vRow = new ArrayList<String>();

        while(nPos < nLen)
        {
            c = aLine.charAt(nPos);

        	if (quote != 0)
        	{
        		if (startQ)
        		{
        			if (c == quote)
                    	startQ = false;
        			else
        				eBuf[ePos++] = c;

        			nPos++;
    				continue;
        		}
        		else
        		{
        			if (c == quote)
        			{
        				startQ = true;
        				nPos++;
        				continue;
        			}
        		}
        	}
        	
            if(dl == c)
            {
                if(ePos == 0)
                {
                    vRow[i++] = null;
                }
                else
                {
                	char preC = aLine.charAt(nPos - 1);
                	
                	if (preC == escape && escape != 0 )
                	{
                		eBuf[ePos - 1] = c;
                	}
                	else
                	{
                		String value = new String(eBuf, 0, ePos);
                		if (conversion)
                			vRow[i] = this.dataDef.getFieldValue(i, value);
                		else
                			vRow[i] = value;
                		i++;
                		ePos = 0;
                	}
                }
            }
            else
            {
            	if (quote == 0) eBuf[ePos++] = c;
            }

            nPos++;
        }
        
        // currently nPos == nLen, end of the line

        if(ePos != 0)
        {
    		String value = new String(eBuf, 0, ePos);
    		
    		if (conversion)
    			vRow[i] = this.dataDef.getFieldValue(i, value);
    		else
    			vRow[i] = value;
        }
        else
        {
            // the last character is a delimiter
            vRow[i] = null;
        }
        
        // if actual element larger than total field count, resize returned row
        if (fldCount > i + 1) vRow = Arrays.copyOf(vRow, i + 1);
        
        return vRow;
    }	
	
	
}
