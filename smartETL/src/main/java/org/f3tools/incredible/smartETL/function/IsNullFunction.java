package org.f3tools.incredible.smartETL.function;

import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.pentaho.reporting.libraries.formula.function.Function;
import org.pentaho.reporting.libraries.formula.function.ParameterCallback;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.typing.coretypes.LogicalType;

/**
 * @since 2015/03/03
 * @author Desheng Kang
 */
public class IsNullFunction implements Function
{
	private static final long serialVersionUID = -2400121941298634442L;
	private static final TypeValuePair RETURN_TRUE = new TypeValuePair(
			LogicalType.TYPE, Boolean.TRUE);
	private static final TypeValuePair RETURN_FALSE = new TypeValuePair(
			LogicalType.TYPE, Boolean.FALSE);

	public IsNullFunction()
	{
	}

	public String getCanonicalName()
	{
		return "ISNULL";
	}

	public TypeValuePair evaluate(final FormulaContext context,
			final ParameterCallback parameters) throws EvaluationException
	{
		final int parameterCount = parameters.getParameterCount();
		
		if (parameterCount < 1)
		{
			throw EvaluationException
					.getInstance(LibFormulaErrorValue.ERROR_ARGUMENTS_VALUE);
		}
		
		try
		{
			Object value = parameters.getValue(0);

			if (value == null)
				return RETURN_TRUE;
			else
				return RETURN_FALSE;
		} catch (EvaluationException e)
		{
			throw e;
		}
	}
}
