package org.pentaho.reporting.libraries.formula.operators;

import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.typing.TypeRegistry;
import org.pentaho.reporting.libraries.formula.typing.coretypes.LogicalType;

public class OrOperator implements InfixOperator
{
	private static final long serialVersionUID = 4212710711697326512L;
	private static final TypeValuePair RETURN_TRUE = new TypeValuePair(LogicalType.TYPE, Boolean.TRUE);
	private static final TypeValuePair RETURN_FALSE = new TypeValuePair(LogicalType.TYPE, Boolean.FALSE);

	public OrOperator()
	{
	}

	public TypeValuePair evaluate(final FormulaContext context, final TypeValuePair value1, final TypeValuePair value2)
			throws EvaluationException
	{
		final TypeRegistry typeRegistry = context.getTypeRegistry();

		Boolean valBool1 = typeRegistry.convertToLogical(value1.getType(), value1.getValue());
		Boolean valBool2 = typeRegistry.convertToLogical(value2.getType(), value2.getValue());

		boolean result = valBool1.booleanValue() || valBool2.booleanValue();

		if (result)
		{
			return RETURN_TRUE;
		} else
		{
			return RETURN_FALSE;
		}
	}

	public int getLevel()
	{
		return 550;
	}

	public String toString()
	{
		return "Or";
	}

	/**
	 * Defines the bind-direction of the operator. That direction defines, in
	 * which direction a sequence of equal operators is resolved.
	 *
	 * @return true, if the operation is left-binding, false if right-binding
	 */
	public boolean isLeftOperation()
	{
		return true;
	}

	/**
	 * Defines, whether the operation is associative. For associative
	 * operations, the evaluation order does not matter, if the operation
	 * appears more than once in an expression, and therefore we can optimize
	 * them a lot better than non-associative operations (ie. merge constant
	 * parts and precompute them once).
	 *
	 * @return true, if the operation is associative, false otherwise
	 */
	public boolean isAssociative()
	{
		return false;
	}
}
