package org.f3tools.incredible.smartETL.function;

import org.f3tools.incredible.smartETL.LookupManager;
import org.f3tools.incredible.smartETL.formula.FormulaUtl;
import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.pentaho.reporting.libraries.formula.function.Function;
import org.pentaho.reporting.libraries.formula.function.ParameterCallback;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.AnyType;

/**
 * This function looks up a value from a dataset
 * Format: dlookup("lookup dataset name";key1;key2...) 
 * @since 2015/03/03
 * @author Desheng Kang
 */

public class LookupFunction implements Function
{
	private static final long serialVersionUID = 4291346556010521043L;

	public LookupFunction()
	{

	}

	public String getCanonicalName()
	{
		return "DLOOKUP";
	}

	public TypeValuePair evaluate(final FormulaContext context,
			final ParameterCallback parameters) throws EvaluationException
	{
		final int parameterCount = parameters.getParameterCount();
		
		if (parameterCount < 3)
		{
			throw EvaluationException
					.getInstance(LibFormulaErrorValue.ERROR_ARGUMENTS_VALUE);
		}

		final Type dataNameType = parameters.getType(0);
		final Object dataNameValue = parameters.getValue(0);
		final Type fieldNameType = parameters.getType(1);
		final Object fieldNameValue = parameters.getValue(1);
		int size = parameters.getParameterCount();

		Object[] keys = new Object[size - 2];
		
		for (int i = 2; i < size; i++)
		{
			keys[i - 2] = parameters.getValue(i);
		}

		Object retValue = LookupManager.getInstance().lookup(
				context.getTypeRegistry().convertToText(dataNameType, dataNameValue),
				context.getTypeRegistry().convertToText(fieldNameType, fieldNameValue),
				keys);

		if (retValue == null)
			return new TypeValuePair(AnyType.TYPE, null);
		else
			return new TypeValuePair(FormulaUtl.guessTypeOfObject(retValue),
					retValue);
	}

}
