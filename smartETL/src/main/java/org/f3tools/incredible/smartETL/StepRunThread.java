package org.f3tools.incredible.smartETL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepRunThread implements Runnable
{
	private Step step;
	private Logger logger = LoggerFactory.getLogger(StepRunThread.class);
	
	public StepRunThread(Step step)
	{
		this.step = step;
	}

	public void run() {
		try
		{
			step.setRunning(true);
			logger.debug("Step {} started", step.getName());
			while (step.processRow() && !step.isStopped());
		}
		catch(Throwable t)
		{
		    try
		    {
		        if(t instanceof OutOfMemoryError) {
		        	logger.error("UnexpectedError ", t); 
		        } else {
		        	logger.error("System.Log.UnexpectedError ", t); 
		        }
		    }
		    catch(OutOfMemoryError e)
		    {
		        e.printStackTrace();
		    }
		    finally
		    {
//		        step.setErrors(1);
		    	t.printStackTrace();
		        step.stopAll();
		    }
		}
		finally
		{
			step.dispose();
			try {
			} catch(Throwable t) {
				//
				logger.error("UnexpectedError: ", t); 
			} finally {
				step.setRunning(false);
			}
		}
	}

}
