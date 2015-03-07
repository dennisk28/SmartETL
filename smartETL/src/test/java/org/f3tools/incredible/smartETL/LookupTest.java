package org.f3tools.incredible.smartETL;

import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.w3c.dom.Node;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class LookupTest extends TestCase
{
	public static Test suite() 
	{
		return new TestSuite( LookupTest.class );
	}
	
	public LookupTest(String name) {
		super(name);
	}

	public void testLookup()
	{
		StringBuffer sb = new StringBuffer();
		
		sb.append("<lookup>");
		sb.append("<name>testlookup</name>");
		sb.append("<datadef ref=\"customer\"/>");
		sb.append("<sourcetype>file</sourcetype>");
		sb.append("<file>");
		sb.append("	<path>C:\\Work\\workspace\\incredible\\smartETL\\examples\\outputcustomers.txt</path>");
		sb.append("	<delimiter>;</delimiter>");
		sb.append("	<quote/>");
		sb.append("</file>");
		sb.append("<keys>");
		sb.append("	<field>id</field>");
		sb.append("	<field>name</field>");
		sb.append("</keys>");
		sb.append("<values>");
		sb.append("	<field>firstname</field>");
		sb.append("	<field>city</field>");
		sb.append("</values>");
		sb.append("</lookup>		");
		
		Node lookupNode = XMLUtl.loadXMLString(sb.toString()).getDocumentElement();
		
		Lookup lookup = new Lookup();
		
		lookup.init(lookupNode);
		
		Object[] key = new Object[2];
		key[0] = new Integer(3);
		key[1] = "xthfg-name";
		
		Object[] value = lookup.lookup(key);
		
		System.out.println(value);
		
	}
}
