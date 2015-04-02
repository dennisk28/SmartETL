package org.f3tools.incredible.smartETL;

import java.util.HashMap;

import org.f3tools.incredible.smartETL.formula.FormulaException;
import org.f3tools.incredible.smartETL.formula.ICFormula;
import org.f3tools.incredible.smartETL.formula.ICFormulaContext;
import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @TODO cache parent variables to gain better performance
 * @author Dennis
 *
 */
public class Context
{
	private Logger logger = LoggerFactory.getLogger(Context.class);
	private Context parent;
	private HashMap<String, Variable> variables;
	private DataRow currentInputRow;
	private DataRow currentOutputRow;
	private ICFormula formula;
	private ICFormulaContext formulaContext;
	
	public Context(Context parent)
	{
		this.parent = parent;
		this.variables = new HashMap<String, Variable>();
		this.formula = new ICFormula();
		this.formulaContext = new ICFormulaContext(this);
	}
	
	public DataRow getCurrentOutputRow()
	{
		return currentOutputRow;
	}

	public void setCurrentOutputRow(DataRow currentOutputRow)
	{
		this.currentOutputRow = currentOutputRow;
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
		this.variables.put(varName.toUpperCase(), var);
	}
	
	public Object eval(String formula) throws FormulaException
	{
			return this.formula.evaluate(formula, this.formulaContext);
	}
	
	public Object resolveVariable(String varName) throws EvaluationException
	{
		String ucaseVarName = varName.toUpperCase();
		
		if (hasVariable(ucaseVarName)) return this.getVariableValue(ucaseVarName);
		
		int dotPos = ucaseVarName.indexOf(".");
		String prefix = null;
		String realVarName = null;
		
		if (dotPos > 0)
		{
			prefix = ucaseVarName.substring(0,  dotPos);
			realVarName = ucaseVarName.substring(dotPos + 1);
		}
		else
			realVarName = ucaseVarName;
		
		if (realVarName == null) 
		{
			logger.error("Can't recognize var " + varName);
			throw EvaluationException.getInstance(LibFormulaErrorValue.ERROR_NOTDEFINED_VALUE);
		}
		
		if ((prefix != null && prefix.equalsIgnoreCase("IN")) || prefix == null)
		{
			DataDef dataDef = this.currentInputRow.getDataDef();
			int idx = dataDef.getFieldIndex(realVarName);

			if (idx >= 0)
				return this.currentInputRow.getFieldValue(idx);
			else
				logger.error("Can't recognize var " + varName);
				//TODO shall throw a message "can't resolve the variable" 
				throw EvaluationException.getInstance(LibFormulaErrorValue.ERROR_NOTDEFINED_VALUE); 
		}
		else if (prefix.equalsIgnoreCase("OUT"))
		{
			DataDef dataDef = this.currentOutputRow.getDataDef();
			int idx = dataDef.getFieldIndex(realVarName);

			if (idx >= 0)
				return this.currentOutputRow.getFieldValue(idx);
			else
				logger.error("Can't recognize var " + varName);
				//TODO shall throw a message "can't resolve the variable" 
				throw EvaluationException.getInstance(LibFormulaErrorValue.ERROR_NOTDEFINED_VALUE); 
			
		}
		else
		{
			logger.error("Can't recognize var " + varName);
			throw EvaluationException.getInstance(LibFormulaErrorValue.ERROR_NOTDEFINED_VALUE);
		}
	}
	
	public boolean hasVariable(String varName)
	{
		String ucaseVarName = varName.toUpperCase();
		
		Variable var = this.variables.get(ucaseVarName);
		
		if (var != null)
			return true;
		else if (this.parent != null)
			return this.parent.hasVariable(ucaseVarName);
		else
			return false;
	}
	
	public Object getVariableValue(String varName)
	{
		String ucaseVarName = varName.toUpperCase();
		
		Variable var = this.variables.get(ucaseVarName);
		
		if (var != null)
			return var.getValue();
		else
		{
			if (this.parent != null) 
				return this.parent.getVariableValue(ucaseVarName);
			else
				return null;
		}
	}
	
	public void setVariable(String varName, Object value)
	{
		String ucaseVarName = varName.toUpperCase();

		Variable var = this.variables.get(ucaseVarName);
		
		if (var == null)
		{
			var = new Variable(ucaseVarName);
			var.setValue(value);
		}
	}
}
