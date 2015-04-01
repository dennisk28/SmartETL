package org.f3tools.incredible.smartETL.function;

import org.f3tools.incredible.smartETL.LookupManager;
import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.pentaho.reporting.libraries.formula.function.Function;
import org.pentaho.reporting.libraries.formula.function.ParameterCallback;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.LogicalType;

/**
* Find out whether keys exist in a lookup table
* @author Dennis Kang
* @since 2015/03/18
*/

public class ExistFunction implements Function
{
	private static final long serialVersionUID = 5632211214941005265L;

	private static final TypeValuePair RETURN_TRUE = new TypeValuePair(
			LogicalType.TYPE, Boolean.TRUE);
	private static final TypeValuePair RETURN_FALSE = new TypeValuePair(
			LogicalType.TYPE, Boolean.FALSE);	

	public ExistFunction()
	{

	}

	public String getCanonicalName()
	{
		return "EXIST";
	}

	public TypeValuePair evaluate(final FormulaContext context,
			final ParameterCallback parameters) throws EvaluationException
	{
		final int parameterCount = parameters.getParameterCount();
		
		if (parameterCount < 2)
		{
			throw EvaluationException
					.getInstance(LibFormulaErrorValue.ERROR_ARGUMENTS_VALUE);
		}

		final Type dataNameType = parameters.getType(0);
		final Object dataNameValue = parameters.getValue(0);

		int size = parameters.getParameterCount();

		Object[] keys = new Object[size - 1];
		
		for (int i = 1; i < size; i++)
		{
			keys[i - 1] = parameters.getValue(i);
		}

		boolean retValue = LookupManager.getInstance().exist(
				context.getTypeRegistry().convertToText(dataNameType, dataNameValue),
				keys);

		if (retValue == false)
			return RETURN_FALSE;
		else
			return RETURN_TRUE;
	}

}
