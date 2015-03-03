package org.f3tools.incredible.smartETL.formula;

import java.sql.Time;
import java.util.Date;

import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.AnyType;
import org.pentaho.reporting.libraries.formula.typing.coretypes.DateTimeType;
import org.pentaho.reporting.libraries.formula.typing.coretypes.LogicalType;
import org.pentaho.reporting.libraries.formula.typing.coretypes.NumberType;
import org.pentaho.reporting.libraries.formula.typing.coretypes.TextType;

public class FormulaUtl
{

	public static Type guessTypeOfObject(final Object o) 
	{
		if (o instanceof Number) {
			return NumberType.GENERIC_NUMBER;
		} else if (o instanceof Time) {
			return DateTimeType.TIME_TYPE;
		} else if (o instanceof java.sql.Date) {
			return DateTimeType.DATE_TYPE;
		} else if (o instanceof Date) {
			return DateTimeType.DATETIME_TYPE;
		} else if (o instanceof Boolean) {
			return LogicalType.TYPE;
		} else if (o instanceof String) {
			return TextType.TYPE;
		}

		return AnyType.TYPE;
	}
}
