package org.f3tools.incredible.smartETL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.f3tools.incredible.smartETL.utilities.CSVFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

public class Lookup
{
	private Logger logger = LoggerFactory.getLogger(Lookup.class);
	private static Lookup lookup;
	private String name;
	private DataDef dataDef;
	private List<String> keyFields;
	private List<String> valueFields;
	private HashMap<LKey, Object[]> data;
	private AtomicBoolean loaded = new AtomicBoolean(false);
	
	private String sourceType;

	public String getName()
	{
		return name;
	}

	public static Lookup getInstance()
	{
		if (lookup == null)
		{
			lookup = new Lookup();
		}
		
		return lookup;
	}

	public boolean init(Node defNode)
	{
		name = XMLUtl.getTagValue(defNode, "name");
		keyFields = new ArrayList<String>();
		valueFields = new ArrayList<String>();
		
		String dataDefRef = XMLUtl.getTagValueWithAttribute(defNode, "datadef", "ref");
		
		if (dataDefRef == null)
		{
			logger.error("lookup {} doesn't have datadef defined", name);
			return false;
		}
		
		dataDef = DataDefRegistry.getInstance().findDataDef(dataDefRef);
		
		if (dataDef == null)
		{
			logger.error("Can't find dataDef {}", dataDefRef);
			return false;
		}
		
		Node keysNode = XMLUtl.getSubNode(defNode, "keys");
		
		if (keysNode == null)
		{
			logger.error("Can't find key definition for lookup {}", name);
			return false;
		}
		
		List<Node> fields = XMLUtl.getNodes(keysNode, "field");
		
		for (Node field : fields)
		{
			keyFields.add(XMLUtl.getNodeValue(field));
		}

		Node valuesNode = XMLUtl.getSubNode(defNode, "values");
		
		if (valuesNode == null)
		{
			logger.error("Can't find value definition for lookup {}", name);
			return false;
		}
		
		fields = XMLUtl.getNodes(valuesNode, "field");
		
		for (Node field : fields)
		{
			valueFields.add(XMLUtl.getNodeValue(field));
		}
		
		sourceType = XMLUtl.getTagValue(defNode, "sourcetype");
		
		if (sourceType == null) sourceType = "file";
		
		if (sourceType.equalsIgnoreCase("file"))
		{
			Node fileNode = XMLUtl.getSubNode(defNode,  "file");
			
			if (fileNode == null)
			{
				logger.error("Can't find file definition for lookup {}", name);
				return false;
			}
			
			String path = XMLUtl.getTagValue(fileNode, "path");
			String delimiter = XMLUtl.getTagValue(fileNode, "delimiter");
			String quote = XMLUtl.getTagValue(fileNode, "quote");
			
			boolean success = false;
			
			try
			{
				success = loadDataFromFile(path, delimiter, quote);
			} catch (ETLException e)
			{
				logger.error("failed to load data from {} for lookuip {}", path, name, e);
				return false;
			}
			
			if (!success)
			{
				logger.error("failed to load data from {} for lookup {}", path, name);
				return false;
			}
		}

		this.loaded.set(true);
		
		return true;
	}
	
	private boolean loadDataFromFile(String path, String delimiter, String quote) throws ETLException
	{
		CSVFile csvFile = new CSVFile(dataDef, path, delimiter, quote, true);
		
		Object[] row = csvFile.readRow(true);
		int[] keyFieldIdx = new int[this.keyFields.size()];
		int[] valueFieldIdx = new int[this.valueFields.size()];
		
		data = new HashMap<LKey, Object[]>();
		
		for(int i = 0, size = keyFields.size(); i < size; i++)
		{
			String field = keyFields.get(i);
			int idx = dataDef.getFieldIndex(field);
			
			if (idx < 0)
			{
				throw new ETLException("Can't find field " + field + " in dataDef " + dataDef.getName());
			}
			
			keyFieldIdx[i] = idx;
		}
		
		for(int i = 0, size = valueFields.size(); i < size; i++)
		{
			String field = valueFields.get(i);
			int idx = dataDef.getFieldIndex(field);
			
			if (idx < 0)
			{
				throw new ETLException("Can't find field " + field + " in dataDef " + dataDef.getName());
			}
			
			valueFieldIdx[i] = idx;
		}
				
		while (row != null)
		{
			Object[] keyValue = new Object[keyFieldIdx.length];
			Object[] valueValue = new Object[valueFieldIdx.length];
			
			for(int i = 0, size = keyFieldIdx.length; i < size; i++)
			{
				keyValue[i] = row[keyFieldIdx[i]];
			}

			for (int i = 0, size = valueFieldIdx.length; i < size; i++)
			{
				valueValue[i] = row[valueFieldIdx[i]];
			}

			data.put(new LKey(keyValue),  valueValue);
			
			row = csvFile.readRow(true);
		}
		
		csvFile.close();
		
		return true;
	}
	
	public boolean exist(Object[] keys)
	{
		if (lookup(keys) != null) 
			return true;
		else
			return false;
	}
	
	public Object[] lookup(Object[] keys)
	{
		if (!loaded.get())
		{
			logger.info("Lookup failed before data is fulled loaded");
			return null;
		}
		
		if (keys == null) return null;
		
		return this.data.get(new LKey(keys));
	}
	
	
	public Object lookup(String fieldName, Object[] keys)
	{
		Object[] values = this.lookup(keys);
		
		if (values == null) return null;
		
		int idx = this.valueFields.indexOf(fieldName);
		
		if (idx < 0) 
		{
			logger.error("Field {} doesn't exist in loaded lookup data", fieldName);
			return null;
		}
		
		return values[idx];
	}
	
	public static class LKey
	{
		private Object[] key;
		
		public LKey(Object[] key)
		{
			this.key = key.clone();
		}
		
		public Object[] getKey()
		{
			return key;
		}
		
		@Override
		public int hashCode()
		{
			return Arrays.hashCode(key);
		}
		
		@Override
		public boolean equals(Object obj)
		{
			return Arrays.equals(key, ((LKey)obj).getKey());
		}
	}
}
