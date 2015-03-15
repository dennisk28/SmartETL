package org.f3tools.incredible.smartETL.steps.csvinput;

import java.io.File;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.FileUtl;
import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.DataDef;
import org.f3tools.incredible.smartETL.DataDefRegistry;
import org.f3tools.incredible.smartETL.DataRow;
import org.f3tools.incredible.smartETL.Job;
import org.f3tools.incredible.smartETL.utilities.CSVFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVInput extends AbstractStep 
{
	private Logger logger = LoggerFactory.getLogger(CSVInput.class);
	
	private CSVInputDef csvInputDef;
	private DataDef dataDef;
	private CSVFile csvFile;
	private File[] csvFiles;
	private int fileIdx;
	
	public DataDef getDataDef() {
		return dataDef;
	}
	
	public CSVInputDef getCvsInputDef() {
		return csvInputDef;
	}

	public void setCvsInputDef(CSVInputDef csvInputDef) {
		this.csvInputDef = csvInputDef;
	}

	public CSVInput(String name, Job job)
	{
		super(name, job);
	}
	
	@Override
	public boolean init()
	{
		if (!super.init()) return false;
		
		this.csvInputDef = (CSVInputDef)this.getStepDef();
		
		if (this.csvInputDef == null) return false;
		
		try
		{
			this.dataDef = DataDefRegistry.getInstance().findDataDef(csvInputDef.getDataDefRef());
			
			if (this.dataDef == null)
			{
				throw new ETLException("Can't find data def " + csvInputDef.getDataDefRef());
			}

			csvFiles = FileUtl.findFiles(csvInputDef.getFile());
			
			if (csvFiles == null || csvFiles.length == 0)
			{
				throw new ETLException("Can't find any csv file for:" + csvInputDef.getFile());
			}
			
			fileIdx = 0;
			
			csvFile = getNextCSVFile();;
		}
		catch (Exception e)
		{
			logger.error("Can't open csv file {}", this.csvInputDef.getFile(), e);
			return false;
		}
		
		return true;
	}
	
	private CSVFile getNextCSVFile() throws ETLException
	{
		if (fileIdx < csvFiles.length)
			return new CSVFile(dataDef, csvFiles[fileIdx].getAbsolutePath(), 
				csvInputDef.getDelimiter(), csvInputDef.getQuote(), csvInputDef.hasTitle(),
				csvInputDef.getTopSkipCount(), csvInputDef.getBottomSkipCount());
		else
			return null;
	}
	
	@Override
	public boolean processRow() throws ETLException
	{
		Object[] row = csvFile.readRow(true);

		if (row == null)
		{
			if (fileIdx == csvFiles.length - 1)
			{
				this.setOutputDone();
				return false;
			}
			else
			{
				fileIdx++;
				csvFile.close();
				csvFile = getNextCSVFile();
				return processRow();
			}
		}
		
		if (row != null)
		{
			DataRow r = new DataRow();
			r.setDataDef(dataDef);
			r.setRow(row);
			getStats().addLinesInput();
			
			this.setCurrentInputRow(r);

			// if the row is filtered out, return
			if (this.filterRow()) return true;
			
			try
			{
				this.putRow(r);
			}
			catch (ETLException e)
			{
				logger.error("Error processing csvFile ", e);
				return false;
			}
		}
		
		return true;
	}
	
	@Override
	public boolean dispose()
	{
		try
		{
			if (csvFile != null) csvFile.close();
		}
		catch (Exception e)
		{
			logger.error("can't close file reader", e);
			return false;
		}
		
		return true;
	}
}
