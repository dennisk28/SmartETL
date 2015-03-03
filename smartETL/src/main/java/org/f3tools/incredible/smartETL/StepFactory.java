package org.f3tools.incredible.smartETL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.f3tools.incredible.smartETL.steps.csvinput.CSVInput;
import org.f3tools.incredible.smartETL.steps.csvinput.CSVInputDef;
import org.f3tools.incredible.smartETL.steps.csvoutput.CSVOutput;
import org.f3tools.incredible.smartETL.steps.csvoutput.CSVOutputDef;
import org.f3tools.incredible.smartETL.steps.dummy.Dummy;
import org.f3tools.incredible.smartETL.steps.smarttrans.SmartTrans;
import org.f3tools.incredible.smartETL.steps.smarttrans.SmartTransDef;
import org.f3tools.incredible.utilities.ETLException;
import org.f3tools.incredible.utilities.XMLUtl;

public class StepFactory 
{
	private static Logger logger = LoggerFactory.getLogger(StepFactory.class);
	private static StepFactory instance;
	
	public static StepFactory getInstance()
	{
		if (instance == null)
		{
			instance = new StepFactory();
		}
		
		return instance;
	}
	
	public Step createStep(Node stepDefNode, Job job)
	{

		String stepType = XMLUtl.getTagValue(stepDefNode, "type");
		String name = XMLUtl.getTagValue(stepDefNode, "name");
		String debugStr = XMLUtl.getTagValue(stepDefNode, "debug");
		boolean debug = false;
		
		if (debugStr != null && debugStr.equalsIgnoreCase("TRUE")) debug = true;
		
		if (stepType == null)
		{
			logger.error("Missing step type!");
			return null;
		}		
		else if (stepType.equalsIgnoreCase("CSVInputStep"))
		{
			try
			{
				CSVInputDef csvInputDef = new CSVInputDef(stepDefNode);
				CSVInput csvInput = new CSVInput(name, job);
				csvInput.setCvsInputDef(csvInputDef);
				csvInput.setDebug(debug);
			
				return csvInput;
			} catch (ETLException e)
			{
				logger.error("Can't create step, type is {} ", stepType);
				return null;
			}
		}
		else if (stepType.equalsIgnoreCase("CSVOutputStep"))
		{
			try
			{
				CSVOutputDef csvOutputDef = new CSVOutputDef(stepDefNode);
				CSVOutput csvOutput = new CSVOutput(name, job);
				csvOutput.setCsvOutputDef(csvOutputDef);
				csvOutput.setDebug(debug);
			
				return csvOutput;
			} catch (ETLException e)
			{
				logger.error("Can't create step, type is {}", stepType);
				return null;
			}
		}
		else if (stepType.equalsIgnoreCase("SmartTransStep"))
		{
			try
			{
				SmartTransDef smartTransDef = new SmartTransDef(stepDefNode);
				SmartTrans smartTrans = new SmartTrans(name, job);
				smartTrans.setSmartTransDef(smartTransDef);
				smartTrans.setDebug(debug);
			
				return smartTrans;
			} catch (ETLException e)
			{
				logger.error("Can't create step, type is {} ", stepType, e);
				return null;
			}
		}
		else if (stepType.equalsIgnoreCase("DummyStep"))
		{
			Dummy dummy = new Dummy(name, job);
			dummy.setDebug(debug);
			return dummy;
		}
		
		
		return null;
	}
}
