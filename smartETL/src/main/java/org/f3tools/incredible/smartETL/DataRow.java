package org.f3tools.incredible.smartETL;

public class DataRow 
{
	private Object[] row;
	private DataDef dataDef;
	
	public DataRow()
	{
	}

	public DataRow(DataDef dataDef)
	{
		if (dataDef != null)
		{
			this.dataDef = dataDef;
			
			row = new Object[dataDef.getFieldCount()];
		}
	}
	
	public Object[] getRow() {
		return row;
	}
	
	public void setFieldValue(int index, Object value)
	{
		if (row != null && index < row.length)
		{
			row[index] = value;
		}
	}
	
	public void setRow(Object[] row) {
		this.row = row;
	}
	
	public DataDef getDataDef() {
		return dataDef;
	}
	
	public void setDataDef(DataDef dataDef) {
		this.dataDef = dataDef;
	}
	
	public Object getFieldValue(int index)
	{
		if (index >= row.length)
			return null;
		else
			return row[index];
	}
	
	public String toString()
	{
		return row == null? null: row.toString();
	}
}
