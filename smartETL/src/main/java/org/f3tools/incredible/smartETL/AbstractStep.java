package org.f3tools.incredible.smartETL;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.f3tools.incredible.utilities.ETLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStep implements Step
{
	private Logger logger = LoggerFactory.getLogger(AbstractStep.class);
	
	private List<DataSet> inputDataSets;
	private List<DataSet> outputDataSets;
	private String name;
	private boolean debug;
	private Context context;
	
    private AtomicBoolean running;
    private AtomicBoolean stopped;
	private AtomicBoolean paused;	
	private boolean initialized = false;
	private DataDef dataDef;
    
	private Job job;
	
	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public void setInitialized(boolean initialized)
	{
		this.initialized = initialized;
	}
	
	public boolean isInitialized()
	{
		return this.initialized;
	}
	
    public boolean isRunning() {
		return running.get();
	}

	public void setRunning(boolean running) {
		this.running.set(running);
	}

	public boolean isStopped() {
		return stopped.get();
	}

	public void setStopped(boolean stopped) {
		this.stopped.set(stopped);
	}

	public boolean isPaused() {
		return paused.get();
	}

	public void setPaused(boolean paused) {
		this.paused.set(paused);
	}

	public AbstractStep(String name, Job job)
	{
		this.name = name;
		this.inputDataSets = new ArrayList<DataSet>();
		this.outputDataSets = new ArrayList<DataSet>();
		this.job = job;
	    this.running = new AtomicBoolean();
	    this.stopped = new AtomicBoolean();
		this.paused = new AtomicBoolean();	
	}
	
	public String getName() {
		return name;
	}

    public void setInputDataDef(DataDef dataDef)
    {
        this.dataDef = dataDef;
    }
	
    public DataDef getInputDataDef()
    {
        return this.dataDef;
    }    
    
	public void setName(String name) {
		this.name = name;
	}

	public List<DataSet> getInputDataSets() {
		return inputDataSets;
	}

	public List<DataSet> getOutputDataSets() {
		return outputDataSets;
	}

	
	public boolean init() 
	{
		return true;
	}

	public abstract boolean processRow() throws ETLException;
	
	public boolean dispose() {return true;}


    //public String getName();
    	
    public void stopAll()
    {
        stopped.set(true);
        job.stopAll();
    }	
    
    public void stopRunning()
    {
    }
    
    /**
     * Get a row from input dataset. If no row is available, it will wait until all input datasets
     * are done.
     * @return
     * @throws ETLException
     */
    public DataRow getRow() throws ETLException 
    {
    	DataRow dataRow = new DataRow();
    	
        // Are we pausing the step? If so, stall forever...
        //
        while (paused.get() && !stopped.get()) 
        {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new ETLException(e);
          }
        }

        if (stopped.get()) 
        {
        	logger.debug("Stop looking for more rows");
            stopAll();
            
            return null;
        }

        Object[] row = null;

        while(row == null && !this.isStopped())
        {
        	int size = this.inputDataSets.size();
        	
        	if (size == 0) break;
        	
        	// @Todo not thread safe 2/20/2015 Dennis
        	for (int i = size - 1; i >= 0; i--)
        	{
        		DataSet ds = this.inputDataSets.get(i);
        		row = ds.getRowWait(1, TimeUnit.MILLISECONDS);
        		
        		if (row != null) 
        		{
        			dataRow.setDataDef(ds.getDataDef());
        			break;
        		}
        		
        		if (ds.isDone())
        		{
        			row = ds.getRowWait(1, TimeUnit.MILLISECONDS);
        			if (row == null)
        			{
        				this.inputDataSets.remove(i);
        			}
        			else
        			{
        				dataRow.setDataDef(ds.getDataDef());
        				break;
        			}
        		}
        	}
        }
          
        if (row == null) return null;
        		
        dataRow.setRow(row);
        
        return dataRow;  
    }    

    /**
     * Get a row from input dataset. If no row is available, it will wait until all input datasets
     * are done.
     * @return
     * @throws ETLException
     */
	public DataRow getRowFrom(DataSet dataSet) throws ETLException
	{
		DataRow dataRow = new DataRow();

		// Are we pausing the step? If so, stall forever...
		//
		while (paused.get() && !stopped.get())
		{
			try
			{
				Thread.sleep(100);
			} catch (InterruptedException e)
			{
				throw new ETLException(e);
			}
		}

		Object[] row = null;

		row = dataSet.getRow();
		while (row == null && !dataSet.isDone() && !stopped.get())
		{
			row = dataSet.getRow();
		}

		// Still nothing: no more rows to be had?
		//
		if (row == null && dataSet.isDone())
		{
			// Try one more time to get a row to make sure we don't get a
			// race-condition between the get and the isDone()
			//
			row = dataSet.getRow();
		}

		if (stopped.get())
		{
			logger.debug("Stop looking for more rows");
			stopAll();
			return null;
		}

		if (row == null && dataSet.isDone())
		{
			// Try one more time...
			//
			row = dataSet.getRow();
			if (row == null)
			{
				inputDataSets.remove(dataSet);
				return null;
			}
		}

		if (row == null) return null;

		dataRow.setRow(row);

		return dataRow;
	}    
    
    public void setOutputDone()
    {
        synchronized(this.outputDataSets)
        {
            for (int i = 0; i < outputDataSets.size(); i++)
            {
                DataSet ds = outputDataSets.get(i);
                ds.setDone();
            }
            //if (errorRowSet!=null) errorRowSet.setDone();
        }
    }
    
    public void putRow(DataRow dataRow) throws ETLException
    {
    	putRow(dataRow.getDataDef(), dataRow.getRow());
    }
    
    public void putRow(DataDef dataDef, Object[] row) throws ETLException
    {
      // Are we pausing the step? If so, stall forever...
      //
      while (paused.get() && !stopped.get()) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          throw new ETLException(e);
        }
      }
  
      // Right after the pause loop we have to check if this thread is stopped or
      // not.
      //
      if (stopped.get()) {
        stopAll();
        return;
      }
  
      for (DataSet ds : this.outputDataSets) 
	  {
    	  while (!ds.putRow(dataDef, row) && !isStopped()) ;
	  }
    }

    public String printTitle(String[] fieldNames, String delimiter, String quote)
    {
    	StringBuffer sb = new StringBuffer();
    	
    	int size = fieldNames.length;
    	
    	if (size > 0) 
    	{
    		if (quote != null) sb.append(quote);
    		sb.append(fieldNames[0]);
    		if (quote != null) sb.append(quote);
    	}
    		
    	for (int i = 1; i < size; i++)
    	{
    		sb.append(delimiter);
    		if (quote != null) sb.append(quote);
    		sb.append(fieldNames[i]);
    		if (quote != null) sb.append(quote);
    	}
    	
    	return sb.toString();
    }
    
    
    /**
     * Convert a row into a string delimited by a string
     */
    public String printRow(DataRow dataRow, String delimiter, String quote) throws ETLException
    {
    	StringBuffer sb = new StringBuffer();
    	Object[] row = dataRow.getRow();
    	DataDef dataDef = dataRow.getDataDef();
    	
    	int size = dataDef.getFieldCount();
    	
    	if (size > 0) 
    	{
    		if (quote != null) sb.append(quote);
    		sb.append(dataDef.formatField(0, row[0]));
    		if (quote != null) sb.append(quote);
    	}
    		
    	for (int i = 1; i < size; i++)
    	{
    		sb.append(delimiter);
    		if (quote != null) sb.append(quote);
    		sb.append(dataDef.formatField(i, row[i]));
    		if (quote != null) sb.append(quote);
    	}
    	
    	return sb.toString();
    }
}
