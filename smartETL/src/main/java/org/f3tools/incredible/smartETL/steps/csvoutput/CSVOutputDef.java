package org.f3tools.incredible.smartETL.steps.csvoutput;

import org.w3c.dom.Node;
import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.steps.csvinput.CSVInputDef;

public class CSVOutputDef extends CSVInputDef
{
	public CSVOutputDef(Node node) throws ETLException
	{
		super(node);
	}
}
