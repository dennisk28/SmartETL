package org.f3tools.incredible.smartETL.formula;

import org.f3tools.incredible.utilities.ETLException;

public class FormulaException extends ETLException
{
	private static final long serialVersionUID = 462623274140149061L;

	public FormulaException(Throwable cause)
	{
		super(cause);
	}

	public FormulaException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public FormulaException(String message)
	{
		super(message);
	}
}
