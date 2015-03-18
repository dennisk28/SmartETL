package org.f3tools.incredible.smartETL.function;

import org.pentaho.reporting.libraries.formula.function.AbstractFunctionDescription;
import org.pentaho.reporting.libraries.formula.function.FunctionCategory;
import org.pentaho.reporting.libraries.formula.function.information.InformationFunctionCategory;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.AnyType;
import org.pentaho.reporting.libraries.formula.typing.coretypes.LogicalType;

public class DropFunctionDescription extends AbstractFunctionDescription
{
  private static final long serialVersionUID = 3439147216891768842L;

  public DropFunctionDescription()
  {
    super("DROP", "org.f3tools.incredible.smartETL.function.Drop-Function");
  }

  public Type getValueType()
  {
    return LogicalType.TYPE;
  }

  public int getParameterCount()
  {
    return 0;
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