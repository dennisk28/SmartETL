package org.f3tools.incredible.smartETL;

import java.util.concurrent.TimeUnit;

public abstract class AbstractDataSet implements DataSet {

	
	protected int timeoutGet;
	protected int timeoutPut;
	boolean done;
	
	private String name;
	private Step fromStep;
	private Step toStep;
	private DataDef dataDef;
	
	public void setDataDef(DataDef dataDef) {
		this.dataDef = dataDef;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Step getFromStep() {
		return fromStep;
	}
	public void setFromStep(Step fromStep) {
		this.fromStep = fromStep;
	}
	public Step getToStep() {
		return toStep;
	}
	public void setToStep(Step toStep) {
		this.toStep = toStep;
	}
	
    public abstract boolean putRow(DataDef dataDef, Object[] rowData);
    public abstract boolean putRowWait(DataDef dataDef, Object[] rowData, long time, TimeUnit tu);
    public abstract Object[] getRow();
    public abstract Object[] getRowImmediate();
    public abstract Object[] getRowWait(long timeout, TimeUnit tu);    
    public abstract int size();	
    
	public void setDone()
	{
		this.done = true;
	}
	public boolean isDone()
	{
		return this.done;
	}
	
    public DataDef getDataDef()
    {
    	return this.dataDef;
    }
	
	
}
