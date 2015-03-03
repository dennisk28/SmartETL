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
public class NullEmptyFunctionDescription extends AbstractFunctionDescription
{
	private static final long serialVersionUID = -1151454154609519311L;

	public NullEmptyFunctionDescription()
	{
		super("NULLEMPTY",
				"org.f3tools.incredible.smartETL.function.NullEmpty-Function");
	}

	public Type getValueType()
	{
		return AnyType.TYPE;
	}

	public int getParameterCount()
	{
		return 1;
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
