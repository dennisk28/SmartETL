package org.f3tools.incredible.smartETL.steps.csvoutput;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.DataDef;
import org.f3tools.incredible.smartETL.DataDefRegistry;
import org.f3tools.incredible.smartETL.DataRow;
import org.f3tools.incredible.smartETL.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This step receives data and save it to CSV output file. If DataDef reference is
 * not defined, the input data DataDef will be used. 
 * @author Dennis
 *
 */

public class CSVOutput extends AbstractStep
{
	private Logger logger = LoggerFactory.getLogger(CSVOutput.class);
	private boolean firstRow = true;
	private CSVOutputDef csvOutputDef;
	private BufferedWriter bw;
	private DataDef dataDef;
	
	public void setCsvOutputDef(CSVOutputDef csvOutputDef)
	{
		this.csvOutputDef = csvOutputDef;
	}
	
	public CSVOutput(String name, Job job)
	{
		super(name, job);
	}
	
	@Override
	public boolean init()
	{
		if (!super.init()) return false;
		
		this.csvOutputDef = (CSVOutputDef)this.getStepDef();
		
		if (this.csvOutputDef == null) return false;
		
		try
		{
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.csvOutputDef.getFile())), 
				5000);
			
			if (csvOutputDef.getDataDefRef() != null)
			{
				this.dataDef = DataDefRegistry.getInstance().findDataDef(csvOutputDef.getDataDefRef());
				
				if (this.dataDef == null)
				{
					throw new ETLException("Can't find data def " + csvOutputDef.getDataDefRef());
				}
			}
		}
		catch (Exception e)
		{
			logger.error("Can't open csv file {}", this.csvOutputDef.getFile(), e);
			return false;
		}
		
		return true;
		
	}
	
	public boolean processRow() throws ETLException
	{
		DataRow r = getRow();
		
		if (r == null)  // no more input to be expected...
		{
			setOutputDone();
			return false;
		}
		
		putRow(r);     // copy row to possible alternate rowset(s).

		try
		{
			if (firstRow && this.csvOutputDef.hasTitle())
			{
				this.dataDef = r.getDataDef();
				bw.write(this.printTitle(this.dataDef.getFieldNames(), this.csvOutputDef.getDelimiter(), this.csvOutputDef.getQuote()));
				bw.write("\n");
				firstRow = false;
			}
			
			bw.write(this.printRow(r, this.csvOutputDef.getDelimiter(), this.csvOutputDef.getQuote()));
			bw.write("\n");
			this.getStats().addLinesOutput();
		}
		catch (IOException e)
		{
			throw new ETLException("Can't write output csv file to " + this.csvOutputDef.getFile(), e);
		}
			
		return true;	
	}
	
	@Override
	public boolean dispose()
	{
		try
		{
			if (bw != null) 
			{
				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e)
		{
			logger.error("can't close file writer", e);
			return false;
		}
		
		return true;
	}
}
