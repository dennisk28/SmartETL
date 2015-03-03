package org.f3tools.incredible.smartETL;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataDefRegistry
{
	private static Logger logger = LoggerFactory.getLogger(DataDefRegistry.class);
	private static DataDefRegistry instance;
	private HashMap<String, DataDef> dataDefs;
	
	
	public static DataDefRegistry getInstance()
	{
		if (instance == null)
		{
			instance = new DataDefRegistry();
		}
		
		return instance;
	}
	
	private DataDefRegistry()
	{
		this.dataDefs = new HashMap<String, DataDef>();
	}
	
	public void add(DataDef dataDef)
	{
		this.dataDefs.put(dataDef.getName(), dataDef);
	}
	
	public DataDef findDataDef(String name)
	{
		return this.dataDefs.get(name);
	}
	
	/**
	 * This method rewire all dataDef objects with their parents
	 */
	public void rewireParents()
	{
		for(DataDef dataDef : dataDefs.values())
		{
			String parentName = dataDef.getParentName();
			
			if (parentName != null)
			{
				DataDef parentDef = this.findDataDef(parentName);
				
				if (parentDef == null)
				{
					logger.error("parent datadef {} doesn't exist", parentName);
				}
				else
				{
					if (isClosedParentLoop(dataDef, parentDef))
					{
						logger.error("Closed parent loop detected for {}", parentName);
					}
					else
					{
						dataDef.setParent(parentDef);
					}
				}
			}
		}
	}
		
	/**
	 * Check whether this s a closed parent loop
	 * @param def
	 * @param parentDef
	 * @return
	 */
	private boolean isClosedParentLoop(DataDef def, DataDef parentDef)
	{
		DataDef parentparent = parentDef.getParent();
		
		if (parentparent == null)
			return false;
		else if (parentparent == def) 
			return true;
		else
			return isClosedParentLoop(def, parentparent);
	}
}
