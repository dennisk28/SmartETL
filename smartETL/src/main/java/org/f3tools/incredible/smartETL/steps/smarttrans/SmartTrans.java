package org.f3tools.incredible.smartETL.steps.smarttrans;

import java.util.HashMap;

import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.Context;
import org.f3tools.incredible.smartETL.DataDef;
import org.f3tools.incredible.smartETL.DataDefRegistry;
import org.f3tools.incredible.smartETL.DataRow;
import org.f3tools.incredible.smartETL.Job;
import org.f3tools.incredible.smartETL.Variable;
import org.f3tools.incredible.utilities.ETLException;
import org.f3tools.incredible.utilities.Utl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartTrans extends AbstractStep
{

	private Logger logger = LoggerFactory.getLogger(SmartTrans.class);

	private SmartTransDef smartTransDef;
	private DataDef dataDef;
	private HashMap<String, Variable> variables;
	
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
		
		if (this.smartTransDef == null) return false;
		
		try
		{
			this.variables = new HashMap<String, Variable>();
			this.dataDef = DataDefRegistry.getInstance().findDataDef(smartTransDef.getDataDefRef());
			
			Utl.check(this.dataDef == null, "Can't find data def " + smartTransDef.getDataDefRef());

			for(SmartTransDef.VarDef varDef : this.smartTransDef.getVarDefs())
			{
				Variable var = new Variable(varDef.getName(), varDef.getFormula());
				this.variables.put(varDef.getName(), var);
				getContext().addVariable(varDef.getName(), var);
			}
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
		DataRow outputRow = new DataRow(this.dataDef);
		this.getContext().setCurrentInputRow(inputRow);
		
		recalculateVariables();
		
		for (SmartTransDef.Mapping mapping : this.smartTransDef.getMappings())
		{
			int idx = this.dataDef.getFieldIndex(mapping.getField());
			
			Utl.check(idx < 0, "Mapping field " + mapping.getField() + " does not exist in datadef");
			
			outputRow.setFieldValue(idx, this.getContext().eval(mapping.getFormula()));
		}
		
		return outputRow;
	}
	
	private void recalculateVariables() throws ETLException
	{
		Context context = getContext();
		
		for(Variable v : this.variables.values())
		{
			v.setValue(context.eval(v.getFormula()));
		}
	}
	
	public boolean processRow() throws ETLException
	{
		DataRow r = getRow();
		
		if (r == null)  // no more input to be expected...
		{
			setOutputDone();
			return false;
		}
		
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
