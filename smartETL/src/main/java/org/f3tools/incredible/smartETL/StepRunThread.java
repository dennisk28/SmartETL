package org.f3tools.incredible.smartETL;

import org.f3tools.incredible.smartETL.utilities.ETLException;

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
			
			boolean continueRun = true;
			
			while (continueRun && !step.isStopped())
			{
				try
				{
					continueRun = step.processRow();
				}
				catch (ETLException e)
				{
					logger.error("error processing step {} msg:{}", step.getName(), e.getStackTrack());
					step.getStats().addLinesErrored();
				}
			}
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
