package org.pentaho.reporting.libraries.formula.operators;

import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.typing.TypeRegistry;
import org.pentaho.reporting.libraries.formula.typing.coretypes.LogicalType;

public class NotOperator implements PrefixOperator
{
	private static final long serialVersionUID = -4955033594642088788L;
	private static final TypeValuePair RETURN_TRUE = new TypeValuePair(LogicalType.TYPE, Boolean.TRUE);
	private static final TypeValuePair RETURN_FALSE = new TypeValuePair(LogicalType.TYPE, Boolean.FALSE);

	public NotOperator()
	{
	}

	public TypeValuePair evaluate(final FormulaContext context, final TypeValuePair value)
			throws EvaluationException
	{
		final TypeRegistry typeRegistry = context.getTypeRegistry();

		Boolean valBool = typeRegistry.convertToLogical(value.getType(), value.getValue());

		if (!valBool.booleanValue())
		{
			return RETURN_TRUE;
		} else
		{
			return RETURN_FALSE;
		}
	}

	public int getLevel()
	{
		return 450;
	}

	public String toString()
	{
		return "Not";
	}
}
