package org.pentaho.reporting.libraries.formula.function.datetime;

import java.util.Calendar;
import java.util.Date;

import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.pentaho.reporting.libraries.formula.function.Function;
import org.pentaho.reporting.libraries.formula.function.ParameterCallback;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.typing.TypeRegistry;
import org.pentaho.reporting.libraries.formula.typing.coretypes.DateTimeType;
import org.pentaho.reporting.libraries.formula.util.DateUtil;

/**
 * This function add day, month, year to a date.
 * 
 * @author Desheng Kang
 */
public class DateAddFunction implements Function
{
	private static final long serialVersionUID = -4585982951848551086L;

	public DateAddFunction()
	{
	}

	public String getCanonicalName()
	{
		return "DATEADD";
	}

	public TypeValuePair evaluate(final FormulaContext context, final ParameterCallback parameters)
			throws EvaluationException
	{
		if (parameters.getParameterCount() != 3)
		{
			throw EvaluationException.getInstance(LibFormulaErrorValue.ERROR_ARGUMENTS_VALUE);
		}

		final TypeRegistry typeRegistry = context.getTypeRegistry();

		final String addType = typeRegistry.convertToText(parameters.getType(1), parameters.getValue(1));
		final int amount = typeRegistry.convertToNumber(parameters.getType(2), parameters.getValue(2)).intValue();

		final Date d = typeRegistry.convertToDate(parameters.getType(0), parameters.getValue(0));
		if (d == null)
		{
			throw EvaluationException.getInstance(LibFormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);
		}

		final Calendar gc = DateUtil.createCalendar(d, context.getLocalizationContext());

		if (addType.equalsIgnoreCase("D"))
			gc.add(Calendar.DAY_OF_YEAR, amount);
		else if (addType.equalsIgnoreCase("Y"))
			gc.add(Calendar.YEAR, amount);
		else if (addType.equalsIgnoreCase("M"))
			gc.add(Calendar.MONTH, amount);
		else
			throw EvaluationException.getInstance(LibFormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE);

		return new TypeValuePair(DateTimeType.DATE_TYPE, gc.getTime());
	}
}
