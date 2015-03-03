package org.f3tools.incredible.smartETL.steps.join;

import org.f3tools.incredible.smartETL.Const;
import org.f3tools.incredible.smartETL.StepDef;
import org.f3tools.incredible.utilities.ETLException;
import org.f3tools.incredible.utilities.XMLUtl;
import org.w3c.dom.Node;

public class JoinDef extends StepDef
{


	private String file;
	private String delimiter;
	private String dataDefRef;
	private String quote;
	
    private String[] leftKeys;;
	private String[] rightKeys;	
	private String leftStepName;
	private String rightStepName;
	private String joinType;
	
	
	public String getJoinType()
	{
		return joinType;
	}

	public void setJoinType(String joinType)
	{
		this.joinType = joinType;
	}

	public String[] getLeftKeys()
	{
		return leftKeys;
	}

	public void setLeftKeys(String[] leftKeys)
	{
		this.leftKeys = leftKeys;
	}

	public String[] getRightKeys()
	{
		return rightKeys;
	}

	public void setRightKeys(String[] rightKeys)
	{
		this.rightKeys = rightKeys;
	}

	public String getLeftStepName()
	{
		return leftStepName;
	}

	public void setLeftStepName(String leftStepName)
	{
		this.leftStepName = leftStepName;
	}

	public String getRightStepName()
	{
		return rightStepName;
	}

	public void setRightStepName(String rightStepName)
	{
		this.rightStepName = rightStepName;
	}

	public String getDataDefRef() {
		return dataDefRef;
	}

	private boolean hasTitle;
	
	public boolean hasTitle()
	{
		return this.hasTitle;
	}
	
	public String getFile() 
	{
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}


	public JoinDef(Node defNode) throws ETLException
	{
		super(defNode);
		
		this.file = XMLUtl.getTagValue(defNode, "file");
		this.delimiter = XMLUtl.getTagValue(defNode, "delimiter");
		this.quote = XMLUtl.getTagValue(defNode,  "quote");
		
		String hasTitle = XMLUtl.getTagValue(defNode, "title");
		
		if (hasTitle != null)
		{
			this.hasTitle = Const.toBoolean(hasTitle);
		}
		
		this.dataDefRef = XMLUtl.getTagValueWithAttribute(defNode, "datadef", "ref");
		
		if (this.dataDefRef == null)
		{
			throw new ETLException("No data def found for CSVInput Step " + this.getName());
		}
	}

	public String getQuote()
	{
		return quote;
	}
	
}
