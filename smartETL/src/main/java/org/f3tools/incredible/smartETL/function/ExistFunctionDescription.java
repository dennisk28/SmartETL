package org.f3tools.incredible.smartETL.function;

import org.pentaho.reporting.libraries.formula.function.AbstractFunctionDescription;
import org.pentaho.reporting.libraries.formula.function.FunctionCategory;
import org.pentaho.reporting.libraries.formula.function.information.InformationFunctionCategory;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.AnyType;
import org.pentaho.reporting.libraries.formula.typing.coretypes.LogicalType;

/**
 * 
 * @author Dennis Kang
 * @since 2015/03/18
 */
public class ExistFunctionDescription extends AbstractFunctionDescription
{
	private static final long serialVersionUID = -4566454559367954204L;

	public ExistFunctionDescription()
	{
		super("EXIST", "org.f3tools.incredible.smartETL.function.Exist-Function");
	}

	public Type getValueType()
	{
		return LogicalType.TYPE;
	}

	public int getParameterCount()
	{
		return 2;
	}

	public Type getParameterType(final int position)
	{
		return AnyType.TYPE;
	}

	public boolean isParameterMandatory(final int position)
	{
		return true;
	}

	public FunctionCategory getCategory()
	{
		return InformationFunctionCategory.CATEGORY;
	}
}