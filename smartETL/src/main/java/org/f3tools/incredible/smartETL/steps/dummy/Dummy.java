package org.f3tools.incredible.smartETL.steps.dummy;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.DataRow;
import org.f3tools.incredible.smartETL.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Dummy extends AbstractStep
{
	private Logger logger = LoggerFactory.getLogger(Dummy.class);
	
	public Dummy(String name, Job job)
	{
		super(name, job);
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

		if (this.isDebug()) logger.info(this.printRow(r, ";", null));
			
		return true;	
	}
	
}
