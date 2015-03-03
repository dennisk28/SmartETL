package org.f3tools.incredible.smartETL.function;

import org.pentaho.reporting.libraries.formula.function.AbstractFunctionDescription;
import org.pentaho.reporting.libraries.formula.function.FunctionCategory;
import org.pentaho.reporting.libraries.formula.function.information.InformationFunctionCategory;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.AnyType;

/**
 * @since 2015/03/03
 * @author Desheng Kang
 */
public class LookupFunctionDescription extends AbstractFunctionDescription
{
	private static final long serialVersionUID = -7029468408836648641L;

	public LookupFunctionDescription()
	{
		super("DLOOKUP", "org.f3tools.incredible.smartETL.function.information.Lookup-Function");
	}

	public Type getValueType()
	{
		return AnyType.TYPE;
	}

	public FunctionCategory getCategory()
	{
		return InformationFunctionCategory.CATEGORY;
	}

	public int getParameterCount()
	{
		return 3;
	}

	public Type getParameterType(final int position)
	{
		return AnyType.TYPE;
	}

	public boolean isParameterMandatory(final int position)
	{
		return true;
	}

	public boolean isInfiniteParameterCount()
	{
		return true;
	}
}
