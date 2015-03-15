package org.f3tools.incredible.smartETL.steps.csvinput;

import org.w3c.dom.Node;
import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.f3tools.incredible.smartETL.Const;
import org.f3tools.incredible.smartETL.StepDef;

public class CSVInputDef extends StepDef
{
	private String file;
	private String delimiter;
	private String dataDefRef;
	private String quote;
	private int topSkipCount;
	private int bottomSkipCount;
	
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

	public int getTopSkipCount()
	{
		return topSkipCount;
	}

	public int getBottomSkipCount()
	{
		return bottomSkipCount;
	}

	public CSVInputDef(Node defNode) throws ETLException
	{
		super(defNode);
		
		this.file = XMLUtl.getTagValue(defNode, "file");
		this.delimiter = XMLUtl.getTagValue(defNode, "delimiter");
		this.quote = XMLUtl.getTagValue(defNode,  "quote");
		this.topSkipCount = Const.toInt(XMLUtl.getTagValue(defNode,  "topskip"), 0);
		this.bottomSkipCount = Const.toInt(XMLUtl.getTagValue(defNode,  "bottomskip"), 0);
		
		String hasTitle = XMLUtl.getTagValue(defNode, "title");
		if (hasTitle != null)
		{
			this.hasTitle = Const.toBoolean(hasTitle);
		}
		
		this.dataDefRef = XMLUtl.getTagValueWithAttribute(defNode, "datadef", "ref");
		
		if (this.dataDefRef == null && this.getClass().getName().equalsIgnoreCase("CSVInputDef"))
		{
			throw new ETLException("No data def found for CSVInput Step " + this.getName());
		}
	}

	public String getQuote()
	{
		return quote;
	}
	
}
