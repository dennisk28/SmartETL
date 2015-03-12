package org.f3tools.incredible.smartETL.steps.smarttrans;

import java.util.HashMap;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.Utl;
import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.Context;
import org.f3tools.incredible.smartETL.DataDef;
import org.f3tools.incredible.smartETL.DataDefRegistry;
import org.f3tools.incredible.smartETL.DataRow;
import org.f3tools.incredible.smartETL.Job;
import org.f3tools.incredible.smartETL.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartTrans extends AbstractStep
{

	private Logger logger = LoggerFactory.getLogger(SmartTrans.class);

	private SmartTransDef smartTransDef;
	private DataDef outputDataDef;
	
	public void setSmartTransDef(SmartTransDef smartTransDef)
	{
		this.smartTransDef = smartTransDef;
	}
	
	public SmartTrans(String name, Job job)
	{
		super(name, job);
	}
	
	@Override
	public boolean init()
	{
		if (!super.init()) return false;
		
		smartTransDef = (SmartTransDef)getStepDef();
		if (this.smartTransDef == null) return false;
		
		try
		{
			this.outputDataDef = DataDefRegistry.getInstance().findDataDef(smartTransDef.getDataDefRef());
			
			Utl.check(this.outputDataDef == null, "Can't find data def " + smartTransDef.getDataDefRef());
		}
		catch (Exception e)
		{
			//logger.severe("Can't open csv file " + this.csvOutputDef.getFile() + ", err:" + e);
			logger.error("Failed initializing SmartTrans step", e);
			return false;
		}
		
		return true;		
	}
	
	/**
	 * This is the core function of SmartTransStep. 
	 * @param inputRow
	 * @return
	 */
	public DataRow map(DataRow inputRow) throws ETLException
	{
		DataRow outputRow = new DataRow(this.outputDataDef);
		
		for (SmartTransDef.Mapping mapping : this.smartTransDef.getMappings())
		{
			int idx = this.outputDataDef.getFieldIndex(mapping.getField());
			
			Utl.check(idx < 0, "Mapping field " + mapping.getField() + " does not exist in datadef");
			
			outputRow.setFieldValue(idx, this.getContext().eval(mapping.getFormula()));
		}
		
		return outputRow;
	}
	
	public boolean processRow() throws ETLException
	{
		DataRow r = getRow();
		
		if (r == null)  // no more input to be expected...
		{
			setOutputDone();
			return false;
		}

		this.setCurrentInputRow(r);

		// if the row is filtered out, return
		if (this.filterRow()) return true;
		
		putRow(map(r));

		return true;	
	}
	
	@Override
	public boolean dispose()
	{
		try
		{
		}
		catch (Exception e)
		{
			logger.error("error in dispose", e);
			return false;
		}
		
		return true;
	}	
}
