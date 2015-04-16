package org.f3tools.incredible.smartETL;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.IndexedList;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

public class DataDef
{
	private Logger logger = LoggerFactory.getLogger(DataDef.class);
	
	private IndexedList<String, Field> localfields;
	private String name;
	private DataDef parent;
	private String parentName;
	private List<String> excludedFields;
	private IndexedList<String, Field> allFields;
	
	public DataDef(Node defNode)
	{
		this();
		
		if (defNode == null) return;
		
		Node fieldsNode = XMLUtl.getSubNode(defNode, "fields");
		
		if (fieldsNode != null)
		{
			List<Node> fieldNodeList = XMLUtl.getNodes(fieldsNode, "field");
			
			if (fieldNodeList != null)
			{
				for (Node node: fieldNodeList)
				{
					Field field = new Field(
							XMLUtl.getTagValue(node, "name"),
							XMLUtl.getTagValue(node, "type"),
							Const.toInt(XMLUtl.getTagValue(node, "length"), 0),
							XMLUtl.getTagValue(node, "format"));

					this.localfields.add(field.getName().toUpperCase(), field);
				}
			}
		}
		
		Node excludesNode = XMLUtl.getSubNode(defNode, "excludes");
		
		if (excludesNode != null)
		{
			List<Node> fieldNodeList = XMLUtl.getNodes(excludesNode, "field");
			
			if (fieldNodeList != null)
			{
				for (Node fieldNode : fieldNodeList)
				{
					this.excludedFields.add(XMLUtl.getNodeValue(fieldNode).toUpperCase());
				}
			}
		}
		
		this.name = XMLUtl.getTagValue(defNode, "name");
		this.parentName = XMLUtl.getTagAttribute(defNode, "parent");
		
		retrieveAllFields();		
	}	
	
	public DataDef()
	{
		this.localfields = new IndexedList<String, Field>();
		this.excludedFields = new ArrayList<String>();		
	}
	
	public void copyDataDef(DataDef srcDataDef)
	{
		srcDataDef.copyFields(srcDataDef.getAllFields(), this.localfields);
		retrieveAllFields();
	}
	
	public String getParentName()
	{
		return this.parentName;
	}
	
	public DataDef getParent() {
		return parent;
	}

	public void removeField(String fieldName)
	{
		if (this.excludedFields.indexOf(fieldName.toUpperCase()) < 0)
		{
			excludedFields.add(fieldName.toUpperCase());
		}
		
		retrieveAllFields();
	}
	
	public void removeField(int fieldIndex)
	{
		Field field = allFields.get(fieldIndex);
		
		if (field == null) return;
		
		removeField(field.getName());
	}	
	
	private void copyFields(IndexedList<String, Field> src, IndexedList<String, Field> dest)
	{
		for (int i = 0, size = src.size(); i < size; i++)
		{
			Field fld = src.get(i);
			dest.add(fld.name.toUpperCase(), fld);
		}
	}
	
	public IndexedList<String, Field> getAllFields()
	{
		if (this.allFields == null)
		{
			retrieveAllFields();
		}
		
		return this.allFields;
	}
	
	private void retrieveAllFields()
	{
		allFields = new IndexedList<String, Field>();

		if (parent != null) copyFields(parent.getAllFields(), allFields);
		
		copyFields(this.localfields, this.allFields);

		for (String excludedField : this.excludedFields)
		{
			Field fld = this.allFields.remove(excludedField);
			
			if (fld == null)
			{
				logger.error("excluded field {} doesn't exist", excludedField);
			}
		}
	}
	
	public void setParent(DataDef parent) {
		this.parent = parent;
		retrieveAllFields();
	}

	public String getName()
	{
		return this.name;
	}
	
	public int getFieldCount()
	{
		return this.allFields.size();
	}
	
	public Field getField(int index)
	{
		return this.allFields.get(index);
	}
	
	/**
	 * Convert string into field value based on field type
	 * @param index
	 * @param value
	 * @return
	 */
	public Object getFieldValue(int index, String value) throws ETLException
	{
		if (value == null) return null;
		
		if (index >= this.allFields.size()) 
		{
			throw new ETLException("index " + index + " is out of bound!");
		}
		
		Field fld = this.allFields.get(index);
		
		String fldType = fld.getType();
		
		try
		{
			if (fldType.equalsIgnoreCase("String"))
			{
				return value;
			}
	
			if (value.trim().equalsIgnoreCase("")) return null;
			
			if (fldType.equalsIgnoreCase("Integer"))
			{
				return Integer.valueOf(value.trim());
			}
			else if (fldType.equalsIgnoreCase("Double"))
			{
				return Double.valueOf(value.trim());
			}
			else if (fldType.equalsIgnoreCase("Date"))
			{
				SimpleDateFormat sdf;
				
				if (fld.getFormat() != null)
					sdf = new SimpleDateFormat(fld.getFormat());
				else
					sdf = new SimpleDateFormat();
				
				return sdf.parse(value.trim());
			}
			else
			{
				throw new ETLException("Can't support field type: " + fldType);
			}
		}
		catch (Exception e)
		{
			throw new ETLException("Can't convert value " + value + " to type " + fldType, e);
		}
	}


	public String formatField(int index, Object value) throws ETLException
	{
		if (value == null) return "";
		if (value instanceof String) return (String)value;
		
		if (index >= this.allFields.size()) 
		{
			throw new ETLException("index " + index + " is out of bound!");
		}
		
		Field fld = this.allFields.get(index);
		
		String fldType = fld.getType();
		
		try
		{
			if (fldType.equalsIgnoreCase("String"))
			{
				return (String)value;
			}
			else if (fldType.equalsIgnoreCase("Integer"))
			{
				return value.toString();
			}
			else if (fldType.equalsIgnoreCase("Double"))
			{
				return value.toString();
			}
			else if (fldType.equalsIgnoreCase("Date"))
			{
				SimpleDateFormat sdf;
				
				if (fld.getFormat() != null)
					sdf = new SimpleDateFormat(fld.getFormat());
				else
					sdf = new SimpleDateFormat();
				
				return sdf.format(value);
			}
			else
			{
				throw new ETLException("Can't support field type: " + fldType);
			}
		}
		catch (Exception e)
		{
			throw new ETLException("Can't convert value " + value + " to type " + fldType, e);
		}
	}

	public String[] getFieldNames()
	{
		String[] fieldNames = new String[this.allFields.size()];
		
		for(int i = 0, size = this.allFields.size(); i < size; i++)
		{
			Field fld = this.allFields.get(i);
			fieldNames[i] = fld.getName();
		}
		
		return fieldNames;
	}
	
	public int getFieldIndex(String fieldName)
	{
		return this.allFields.indexOf(fieldName.toUpperCase());
	}
	
    public static class Field {

        protected String name;
        
        public Field(String name, String type, int length, String format) {
			this.name = name;
			this.type = type;
			this.length = length;
			this.format = format;
		}

		protected String type;
        protected int length;
        protected String format;

        public String getName() {
            return name;
        }

        public void setName(String value) {
            this.name = value;
        }

        public String getType() {
            return type;
        }

        public void setType(String value) {
            this.type = value;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int value) {
            this.length = value;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String value) {
            this.format = value;
        }
    }

    public int compare(DataRow row1, DataRow row2, int[] fieldIdx1, int[] fieldIdx2) throws ETLException
    {
    	Object[] rowData1 = row1.getRow();
    	Object[] rowData2 = row2.getRow();
    	
    	return compare(rowData1, rowData2, fieldIdx1, fieldIdx2);
    }
    
    public int compare(Object[] rowData1, Object[] rowData2, int[] fieldIdx) throws ETLException
    {
    	return compare(rowData1, rowData2, fieldIdx, fieldIdx);
    }

    public int compare(Object[] rowData1, Object[] rowData2, int[] fieldIdx1, int[] fieldIdx2) throws ETLException
    {
    	int compare = 0;
    	
        for (int i = 0; i < fieldIdx1.length; i++)
        {
        	Object v1 = rowData1[fieldIdx1[i]];
        	Object v2 = rowData2[fieldIdx2[i]];
        	
        	if ((v1 instanceof String) && (v2 instanceof String))
        	{
        		compare = ((String)v1).compareTo((String)v2);
        	}
        	else if ((v1 instanceof Integer) && (v2 instanceof Integer))
        	{
        		compare = ((Integer)v1).compareTo((Integer)v2);
        	}
        	else if ((v1 instanceof Double) && (v2 instanceof Double))
        	{
        		compare = ((Double)v1).compareTo((Double)v2);
        	}
        	else if ((v1 instanceof BigDecimal) && (v2 instanceof BigDecimal))
        	{
        		compare = ((BigDecimal)v1).compareTo((BigDecimal)v2);
        	}
        	else if ((v1 instanceof Date) && (v2 instanceof Date))
        	{
        		compare = ((Date)v1).compareTo((Date)v2);
        	}
        	else
        		compare = -1;    // if they have different type, return -1

        	if (compare != 0) break;
        }
        
        return compare;
    }   
}
