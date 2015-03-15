package org.f3tools.incredible.smartETL;

import java.util.List;

import org.f3tools.incredible.smartETL.utilities.ETLException;

public interface Step 
{	
	public boolean init();

	public void setContext(Context context);
	public Context getContext();
	public boolean dispose();
	public boolean processRow() throws ETLException;

    public List<DataSet> getInputDataSets();
    public List<DataSet> getOutputDataSets();
    public String getName();
    
    public boolean isRunning();
	public void setRunning(boolean running);
	public boolean isStopped();
	public void setStopped(boolean stopped);
	public boolean isPaused();
	public void setPaused(boolean paused);
	public void stopRunning();	
    public void stopAll();
	public boolean isInitialized();	
	public void setInitialized(boolean initialized);
	public StepStats getStats();
	
    public void setOutputDone();		
}
