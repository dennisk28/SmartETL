package org.f3tools.incredible.smartETL.steps.csvinput;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.DataDef;
import org.f3tools.incredible.smartETL.DataDefRegistry;
import org.f3tools.incredible.smartETL.Job;
import org.f3tools.incredible.smartETL.utilities.CSVFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVInput extends AbstractStep 
{
	private Logger logger = LoggerFactory.getLogger(CSVInput.class);
	
	private CSVInputDef cvsInputDef;
	private DataDef dataDef;
	private CSVFile csvFile;
	
	public DataDef getDataDef() {
		return dataDef;
	}
	
	public CSVInputDef getCvsInputDef() {
		return cvsInputDef;
	}

	public void setCvsInputDef(CSVInputDef cvsInputDef) {
		this.cvsInputDef = cvsInputDef;
	}

	public CSVInput(String name, Job job)
	{
		super(name, job);
	}
	
	@Override
	public boolean init()
	{
		if (!super.init()) return false;
		
		if (this.cvsInputDef == null) return false;
		
		try
		{
			this.dataDef = DataDefRegistry.getInstance().findDataDef(cvsInputDef.getDataDefRef());
			
			if (this.dataDef == null)
			{
				throw new ETLException("Can't find data def " + cvsInputDef.getDataDefRef());
			}

			csvFile = new CSVFile(dataDef, cvsInputDef.getFile(), cvsInputDef.getDelimiter(), 
					cvsInputDef.getQuote(), cvsInputDef.hasTitle());
		}
		catch (Exception e)
		{
			logger.error("Can't open csv file {}", this.cvsInputDef.getFile(), e);
			return false;
		}
		
		return true;
	}
	
	@Override
	public boolean processRow()
	{
		Object[] row = csvFile.readRow(true);
		
		if (row != null)
		{
			try
			{
				this.putRow(this.dataDef, row);
			}
			catch (ETLException e)
			{
				logger.error("Error processing csvFile ", e);
				return false;
			}
		}
		else
		{
			this.setOutputDone();
			return false;
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
