package org.f3tools.incredible.smartETL.function;

import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.pentaho.reporting.libraries.formula.function.Function;
import org.pentaho.reporting.libraries.formula.function.ParameterCallback;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;

public class DropFunction implements Function
{

	private static final long serialVersionUID = 1684051629491513183L;

	public DropFunction()
	{
	}

	public String getCanonicalName()
	{
		return "DROP";
	}

	public TypeValuePair evaluate(final FormulaContext context,
			final ParameterCallback parameters) throws EvaluationException
	{
		throw EvaluationException.getInstance(LibFormulaErrorValue.DROP_VALUE);
	}
}
