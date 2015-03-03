package org.f3tools.incredible.smartETL.steps.smarttrans;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Node;

import org.f3tools.incredible.smartETL.StepDef;
import org.f3tools.incredible.utilities.ETLException;
import org.f3tools.incredible.utilities.Utl;
import org.f3tools.incredible.utilities.XMLUtl;

public class SmartTransDef extends StepDef
{
	private String dataDefRef;
	private List<VarDef> varDefs;
	private List<Mapping> mappings;

	public List<VarDef> getVarDefs() {
		return varDefs;
	}

	public List<Mapping> getMappings() {
		return mappings;
	}
	
	public String getDataDefRef() {
		return dataDefRef;
	}
	
	public SmartTransDef(Node defNode) throws ETLException
	{
		super(defNode);
		
		this.varDefs = new ArrayList<VarDef>();
		this.mappings = new ArrayList<Mapping>();
		
		Node varsNode = XMLUtl.getSubNode(defNode, "variables");
		
		if (varsNode != null)
		{
			List<Node> vars = XMLUtl.getNodes(varsNode, "variable");
			
			for(Node varNode : vars)
			{
				String name = XMLUtl.getTagValue(varNode, "name");
				String formula = XMLUtl.getTagValue(varNode, "formula");
				
				Utl.check(name == null, "Variable name has to be defined.");
				Utl.check(formula == null, "Variable formula has to be defined.");
				
				this.varDefs.add(new VarDef(name, formula));
			}
		}

		Node mapsNode = XMLUtl.getSubNode(defNode, "mappings");
		
		if (mapsNode != null)
		{
			List<Node> maps = XMLUtl.getNodes(mapsNode, "mapping");
			
			for(Node mapNode : maps)
			{
				String field = XMLUtl.getTagValue(mapNode, "field");
				String formula = XMLUtl.getTagValue(mapNode, "formula");
				
				Utl.check(field == null, "Mapping field name has to be defined.");
				Utl.check(formula == null, "Mapping formula has to be defined.");
				
				this.mappings.add(new Mapping(field, formula));
			}
		}
		
		this.dataDefRef = XMLUtl.getTagValueWithAttribute(defNode, "datadef", "ref");
		
		Utl.check(this.dataDefRef == null, "No data def found for CSVInput Step " + this.getName());
	}

	
    public static class Mapping 
    {
    	private String field;
    	private String formula;
    	
    	public Mapping(String field, String formula)
    	{
    		this.field = field;
    		this.formula = formula;
    	}
    	
		public String getField() {
			return field;
		}
		
		public void setField(String field) {
			this.field = field;
		}
		
		public String getFormula() {
			return formula;
		}
		
		public void setFormula(String forluma) {
			this.formula = forluma;
		}
    }
	
    public static class VarDef 
    {
    	private String name;
    	private String formula;
    	
    	public VarDef(String name, String formula)
    	{
    		this.name = name;
    		this.formula = formula;
    	}
    	
    	public String getName() {
    		return name;
    	}
    	
    	public void setName(String name) {
    		this.name = name;
    	}
    	
    	public String getFormula() {
    		return formula;
    	}
    	
    	public void setFormula(String formula) {
    		this.formula = formula;
    	}
    }    
}
