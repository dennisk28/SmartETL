package org.f3tools.incredible.smartETL.function;

import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.pentaho.reporting.libraries.formula.function.Function;
import org.pentaho.reporting.libraries.formula.function.ParameterCallback;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.TextType;

/**
 * @since 2015/03/03
 * @author Desheng Kang
 */
public class NullEmptyFunction implements Function
{
	private static final long serialVersionUID = 7876544076770889892L;

	public NullEmptyFunction()
	{
	}

	public String getCanonicalName()
	{
		return "NULLEMPTY";
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
			Type type = parameters.getType(0);

			if (value == null)
				return new TypeValuePair(TextType.TYPE, "");
			else
				return new TypeValuePair(type, value);
		} catch (EvaluationException e)
		{
			throw e;
		}
	}
}
