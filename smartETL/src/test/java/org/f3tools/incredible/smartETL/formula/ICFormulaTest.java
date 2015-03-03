package org.f3tools.incredible.smartETL.formula;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ICFormulaTest extends TestCase 
{

	public static Test suite()
	{
		return new TestSuite( ICFormulaTest.class );
	}
	
	public ICFormulaTest(String name) {
		super(name);
	}

	public void testFormula()
	{
		ICFormula formula = new ICFormula();
		
	}
}
