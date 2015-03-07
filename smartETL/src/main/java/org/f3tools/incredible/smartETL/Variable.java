package org.f3tools.incredible.smartETL;

public class Variable 
{
	private String name;
	private String formula;
	//private boolean calculated = false;
	private Object value;
	
	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Variable(String name)
	{
		this.name = name;
	}
	
	public Variable(String name, String formula)
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
	
	public String toString()
	{
		return this.name + ":" + this.value;
	}
}
