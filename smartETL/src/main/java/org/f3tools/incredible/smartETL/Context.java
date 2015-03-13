package org.f3tools.incredible.smartETL;

import java.util.HashMap;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.formula.ICFormula;
import org.f3tools.incredible.smartETL.formula.ICFormulaContext;
import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;


/**
 * @TODO cache parent variables to gain better performance
 * @author Dennis
 *
 */
public class Context
{
	private Context parent;
	private HashMap<String, Variable> variables;
	private DataRow currentInputRow;
	private ICFormula formula;
	private ICFormulaContext formulaContext;
	
	public Context(Context parent)
	{
		this.parent = parent;
		this.variables = new HashMap<String, Variable>();
		this.formula = new ICFormula();
		this.formulaContext = new ICFormulaContext(this);
	}
	
	public void setCurrentInputRow(DataRow row)
	{
		this.currentInputRow = row;
	}
	
	public DataRow getCurrentInputRow()
	{
		return this.currentInputRow;
	}
	
	/**
	 * Variable is not a hard copy. external class can modify it. 
	 * @param varName
	 * @param var
	 */
	public void addVariable(String varName, Variable var)
	{
		this.variables.put(varName, var);
	}
	
	public Object eval(String formula) throws ETLException
	{
		try
		{
			return this.formula.evaluate(formula, this.formulaContext);
		} catch (Exception e)
		{
			throw new ETLException("Error in evaluate formula " + formula + ", err:", e);
		}
	}
	
	public Object resolveVariable(String varName) throws EvaluationException
	{
		if (hasVariable(varName)) return this.getVariableValue(varName);
		
		DataDef dataDef = this.currentInputRow.getDataDef();
		int idx = dataDef.getFieldIndex(varName);

		if (idx >= 0)
			return this.currentInputRow.getFieldValue(idx);
		else
			//TODO shall throw a message "can't resolve the variable" 
			throw EvaluationException.getInstance(LibFormulaErrorValue.ERROR_NOTDEFINED_VALUE); 
	}
	
	public boolean hasVariable(String varName)
	{
		Variable var = this.variables.get(varName);
		
		if (var != null)
			return true;
		else if (this.parent != null)
			return this.parent.hasVariable(varName);
		else
			return false;
	}
	
	public Object getVariableValue(String varName)
	{
		Variable var = this.variables.get(varName);
		
		if (var != null)
			return var.getValue();
		else
		{
			if (this.parent != null) 
				return this.parent.getVariableValue(varName);
			else
				return null;
		}
	}
	
	public void setVariable(String varName, Object value)
	{
		Variable var = this.variables.get(varName);
		
		if (var == null)
		{
			var = new Variable(varName);
			var.setValue(value);
		}
	}
}
