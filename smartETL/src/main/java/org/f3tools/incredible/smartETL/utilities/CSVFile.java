package org.f3tools.incredible.smartETL.utilities;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.DataDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVFile
{
	private Logger logger = LoggerFactory.getLogger(CSVFile.class);
	private final static int MIN_BUFFER_SIZE = 1000;
	
	private BufferedReader br;
	private String delimiter;
	private String quote;
	private String path;
	private DataDef dataDef;
	private int bottomSkipCount;
	private int bufSize;
	private String[] lineBuf;
	private String[] titles;
	private int curPos;
	private int bufBottomPos;
	private boolean eof;
	
	public String[] getTitles()
	{
		return this.titles;
	}
	
	public CSVFile(DataDef dataDef, String path, String delimiter, String quote, boolean hasTitle) throws ETLException
	{
		this(dataDef, path, delimiter, quote, hasTitle, 0, 0);
	}
	
	
	public CSVFile(DataDef dataDef, String path, String delimiter, String quote, boolean hasTitle, 
			int topSkipCount, int bottomSkipCount) throws ETLException
	{
		try
		{
			this.dataDef = dataDef;
			this.delimiter = delimiter;
			this.quote = quote;
			this.path = path;
			this.bottomSkipCount = bottomSkipCount;
			
			br = new BufferedReader(new InputStreamReader(new FileInputStream(path)), 
				5000);
			
			if (bottomSkipCount > 0)
			{
				bufSize = Math.max(bottomSkipCount * 10, MIN_BUFFER_SIZE);
				lineBuf = new String[bufSize];
				eof = false;
			}
			
			// skip lines if required, TODO shall we save it? Dennis 2015/3/13	
			for (int i = 0; i < topSkipCount; i++) readLine(); 
			if (hasTitle)
			{
				Object[] row = readRow(false);
				
				if (row != null)
				{
					titles = new String[row.length];
					for (int i = 0; i < row.length; i++) titles[i] = (String)row[i];
				}
			}
			
		} catch (Exception e)
		{
			throw new ETLException(e);
		}
	}
	
	private String readLine() throws IOException
	{
		if (lineBuf == null) return br.readLine();
		if (bufBottomPos - curPos <= this.bottomSkipCount) refill();
		if (bufBottomPos - curPos <= bottomSkipCount) return null;
		
		return lineBuf[curPos++];
	}

	private void refill() throws IOException
	{
		if (eof) return;

		int len = bufBottomPos - curPos;
		
		if (curPos > 0) 
		{
			System.arraycopy(lineBuf,  curPos, lineBuf, 0, len);
			bufBottomPos = len;
			curPos = 0;
		}
		
		for(; bufBottomPos < bufSize; bufBottomPos++)
		{
			String s = br.readLine();
			
			if (s == null)
			{
				eof = true;
				return;
			}
			else
				lineBuf[bufBottomPos] = s;
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
		int fieldCount = dataDef == null ? this.dataDef.getFieldCount() : 0;
		
		try
		{
			line = readLine();
		
			while (line != null)
			{
				row = createElements(line, this.delimiter, (char)0, this.quote, conversion);
				
				// shall add code to log problematic rows
				if (row != null)
				{
					if (fieldCount > 0 && row.length != fieldCount)
					{
						logger.error("actual field amount {} is less than required amount {}, line:{}", row.length,
								fieldCount, line);
					}
					else
						return row;
				}
				else
					logger.error("return null for line{}", line);
				
				line = readLine();
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
        char[] inputBuf = aLine.toCharArray();
        int ePos = 0;
        char dl = delimiter.charAt(0);
        
        List<Object> vRow = new ArrayList<Object>();
        
        char quote = 0;
        
        if (quoteStr != null) quote = quoteStr.charAt(0);
        
        while(nPos < nLen)
        {
            c = inputBuf[nPos];

            if (quote != 0 && c == quote)
            {
            	nPos++;
            	
            	if (nPos == nLen) continue;
            	
            	c = inputBuf[nPos];

                while(c != quote)
                {
                	eBuf[ePos++] = c;
                	nPos++;
                	
                	if (nPos == nLen) break;
                	
                	c = inputBuf[nPos];
                }
                
                if (c == quote) nPos++;
                
                continue;
            }
            
        	
            if(dl == c)
            {
                if(ePos == 0)
                {
                    vRow.add(null);
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
                			vRow.add(this.dataDef.getFieldValue(vRow.size(), value));
                		else
                			vRow.add(value);

                		ePos = 0;
                	}
                }
            }
            else
            {
            	eBuf[ePos++] = c;
            }

            nPos++;
        }
        
        // currently nPos == nLen, end of the line

        if(ePos != 0)
        {
    		String value = new String(eBuf, 0, ePos);
    		
    		if (conversion)
    			vRow.add(this.dataDef.getFieldValue(vRow.size(), value));
    		else
    			vRow.add(value);
        }
        else
        {
            // the last character is a delimiter
            vRow.add(null);
        }
        
        return vRow.toArray();
    }	
}
