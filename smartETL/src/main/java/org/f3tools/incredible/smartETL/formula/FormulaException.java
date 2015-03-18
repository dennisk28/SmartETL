package org.f3tools.incredible.smartETL.formula;

public class FormulaException extends Exception
{
	private static final long serialVersionUID = 462623274140149061L;
	public static final int EXCEPTION_CODE_NORMAL = 0;
	public static final int EXCEPTION_CODE_DROP = 100;

	private int exceptionCode;
	
	public FormulaException(Throwable cause)
	{
		super(cause);
	}

	public FormulaException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public FormulaException(int exceptionCode)
	{
		this.exceptionCode = exceptionCode;
	}
	
	public int getExceptionCode()
	{
		return this.exceptionCode;
	}
	
	public FormulaException(String message)
	{
		super(message);
	}
}
