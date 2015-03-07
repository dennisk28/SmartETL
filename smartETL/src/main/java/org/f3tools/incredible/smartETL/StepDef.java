package org.f3tools.incredible.smartETL;

import org.w3c.dom.Node;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;

public class StepDef 
{
	private String name;
	private String type;
	
	public StepDef(Node defNode)
	{
		name = XMLUtl.getTagValue(defNode, "name");
		type = XMLUtl.getTagValue(defNode, "type");
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
}
