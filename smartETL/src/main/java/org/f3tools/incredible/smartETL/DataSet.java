package org.f3tools.incredible.smartETL;

import java.util.concurrent.TimeUnit;

public interface DataSet
{
	public String getName();
    public boolean putRow(DataDef dataDef, Object[] rowData);
    public DataDef getDataDef();
    public boolean putRowWait(DataDef dataDef, Object[] rowData, long time, TimeUnit tu);
    public Object[] getRow();
    public Object[] getRowImmediate();
    public Object[] getRowWait(long timeout, TimeUnit tu);    
    public int size();
	public void setDone();
	public boolean isDone();
}
