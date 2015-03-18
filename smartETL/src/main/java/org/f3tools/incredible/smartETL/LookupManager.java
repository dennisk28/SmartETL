package org.f3tools.incredible.smartETL;

import java.util.HashMap;
import java.util.List;

import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

public class LookupManager
{
	private Logger logger = LoggerFactory.getLogger(LookupManager.class);
	private static LookupManager lookupManager;
	private HashMap<String, Lookup> lookups;
	
	public static LookupManager getInstance()
	{
		if (lookupManager == null)
		{
			lookupManager = new LookupManager();
		}
		
		return lookupManager;
	}
	
	public boolean init(Node defNode)
	{
		if (defNode == null) return true;
		
		lookups = new HashMap<String, Lookup>();
		
		List<Node> lookupNodes = XMLUtl.getNodes(defNode, "lookup");
		
		if (lookupNodes == null)
		{
			logger.warn("can't find lookup definition");
		}
		
		for (Node node : lookupNodes)
		{
			Lookup lookup = new Lookup();
			lookup.init(node);
			this.lookups.put(lookup.getName(), lookup);
		}
		
		return true;
	}
	
	
	public Object[] lookup(String dataName, Object[] keys)
	{
		Lookup lookup = this.lookups.get(dataName);
		
		if (lookup == null)
		{
			logger.debug("can't find dataname {}", dataName);
			return null;
		}
		
		return lookup.lookup(keys);
	}
	
	public boolean exist(String dataName, Object[] keys)
	{
		Lookup lookup = this.lookups.get(dataName);
		
		if (lookup == null)
		{
			logger.debug("can't find dataname {}", dataName);
			return false;
		}
		
		return lookup.exist(keys);
	}
	
	public Object lookup(String dataName, String fieldName, Object[] keys)
	{
		Lookup lookup = this.lookups.get(dataName);
		
		if (lookup == null)
		{
			logger.debug("can't find dataname {}", dataName);
			return null;
		}
		
		return lookup.lookup(fieldName, keys);
	}
}
