package org.f3tools.incredible.smartETL.steps.smarttrans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;
import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.Utl;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.f3tools.incredible.smartETL.StepDef;

public class SmartTransDef extends StepDef
{
	private String dataDefRef;
	private List<Mapping> mappings;
	private String tagValue;
	private String tagFormula;
	private static final String NULL_TAG = "DEFAULT";

	public String getTagValue()
	{
		return tagValue;
	}

	public String getTagFormula()
	{
		return tagFormula;
	}

	public List<Mapping> getMappings() 
	{
		return mappings;
	}
	
	public String getDataDefRef() 
	{
		return dataDefRef;
	}
	
	public SmartTransDef(Node defNode) throws ETLException
	{
		super(defNode);
		
		this.mappings = new ArrayList<Mapping>();

		Node mapsNode = XMLUtl.getSubNode(defNode, "mappings");
		
		if (mapsNode != null)
		{
			List<Node> maps = XMLUtl.getNodes(mapsNode, "mapping");
			
			for(Node mapNode : maps)
			{
				String field = XMLUtl.getTagValue(mapNode, "field");

				Utl.check(field == null, "Mapping field name has to be defined.");
				
				List<Node> formulaNodes = XMLUtl.getNodes(mapNode, "formula");
				
				Utl.check(formulaNodes == null, "Mapping formuls(s) need to be defined");
				
				HashMap<String, String> formulaMap = new HashMap<String, String>();
				
				for (Node formulaNode : formulaNodes)
				{
					String formula = XMLUtl.getNodeValue(formulaNode);
					String tag = XMLUtl.getTagAttribute(formulaNode, "tag");
					String ref = XMLUtl.getTagAttribute(formulaNode, "ref");
					
					if (tag == null) tag = NULL_TAG;
					
					Utl.check(formula == null && ref == null, "Either formula content or reference shall be defined");
					
					if (formula == null)
					{
						String refFormula = formulaMap.get(ref);
						Utl.check(refFormula == null, "Can't find formula reference");
						formula = refFormula;
					}
					
					formulaMap.put(tag, formula);
				}
				
				this.mappings.add(new Mapping(field, formulaMap));
			}
		}
		
		this.dataDefRef = XMLUtl.getTagValueWithAttribute(defNode, "datadef", "ref");
		
		Node tagNode = XMLUtl.getSubNode(defNode, "tag");

		if (tagNode != null)
		{
			tagValue = XMLUtl.getTagAttribute(tagNode, "value");
			tagFormula = XMLUtl.getTagAttribute(tagNode, "formula");
			
			Utl.check(tagValue == null && tagFormula == null, "Must define at least one of tag value and formula");
		}
		
		Utl.check(this.dataDefRef == null, "No data def found for CSVInput Step " + this.getName());
	}

	
    public static class Mapping 
    {
    	private String field;
    	private Map<String, String> formulaMap;
    	
    	public Mapping(String field, Map<String, String> formulaMap)
    	{
    		this.field = field;
    		this.formulaMap = formulaMap;
    	}
    	
		public String getField() 
		{
			return field;
		}
		
		public void setField(String field)
		{
			this.field = field;
		}
		
		public String getFormula(String tag)
		{
			if (tag == null)
				return formulaMap.get(NULL_TAG);
			else
				return formulaMap.get(tag);
		}
		
    }
}
