package org.f3tools.incredible.smartETL.formula;

import org.pentaho.reporting.libraries.formula.DefaultFormulaContext;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.pentaho.reporting.libraries.formula.typing.coretypes.AnyType;

import org.f3tools.incredible.smartETL.Context;

public class ICFormulaContext extends DefaultFormulaContext
{
	private Context etlContext;
	
	public ICFormulaContext(Context etlContext)
	{
		super();
		this.etlContext = etlContext;
	}
	
	@Override
	public Object resolveReference(Object name)
	{
		return etlContext.resolveVariable((String)name);
	}
	
	public Type resolveReferenceType(final Object name)
	{
		return AnyType.TYPE;
	}	
}
