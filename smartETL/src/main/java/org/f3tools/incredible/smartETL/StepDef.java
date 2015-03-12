package org.f3tools.incredible.smartETL;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Node;
import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.Utl;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;

public class StepDef 
{
	private String name;
	private String type;
	private List<VarDef> varDefs;
	private String filterFormula;

	public StepDef(Node defNode) throws ETLException
	{
		name = XMLUtl.getTagAttribute(defNode, "name");
		type = XMLUtl.getTagValue(defNode, "type");
		filterFormula = XMLUtl.getTagValue(defNode, "filter");
				
		this.varDefs = new ArrayList<VarDef>();

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
	}
	
	public List<VarDef> getVarDefs() 
	{
		return varDefs;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
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
	
	public String getFilterFormula()
	{
		return filterFormula;
	}
}
