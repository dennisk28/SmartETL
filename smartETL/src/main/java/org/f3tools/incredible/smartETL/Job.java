package org.f3tools.incredible.smartETL;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

public class Job {

	private static Logger logger = LoggerFactory.getLogger(Job.class);
	private JobDef jobDef;
	private LinkedHashMap<String, Step> steps;
	private LinkedHashMap<String, DataSet> dataSets;
    private AtomicBoolean stopped;
	private AtomicBoolean paused;	
	private Context context;
	
	public boolean execute(String[] parameters)
	{
		if (!init(parameters[0]))
		{
			logger.error("Failed to initialize job!");
			return false;
		}
		
		process();
		
		return true;
	}
	
	public Context getContext() {
		return context;
	}

	public void stopAll()
	{
		if (this.steps == null) return;
		
		for (Step step : this.steps.values())
		{
			step.setStopped(true);
			step.setPaused(false);

            try
            {
            	step.stopRunning();
            }
            catch(Exception e)
            {
                logger.error("Something went wrong while trying to stop the transformation: ", e);
            }
            
            //sid.data.setStatus(StepExecutionStatus.STATUS_STOPPED);
		}
		
		//if it is stopped it is not paused
		paused.set(false);
		stopped.set(true);
		
		// Fire the stopped listener...
		//
		//for (TransStoppedListener listener : transStoppedListeners) {
		//  listener.transStopped(this);
	}
	
	
	public boolean init(String jobDefFile)
	{
		this.jobDef = new JobDef();
		this.steps = new LinkedHashMap<String, Step>();
		this.dataSets = new LinkedHashMap<String, DataSet>();
		this.context = new Context(null);
		this.paused = new AtomicBoolean();
		this.stopped = new AtomicBoolean();
		
		jobDef.loadXML(jobDefFile);
		
		for (Node stepDefNode : jobDef.getSteps())
		{
			Step step = StepFactory.getInstance().createStep(stepDefNode, this);
			
			if (step != null) 
			{
				Context stepContext = new Context(this.context);
				step.setContext(stepContext);
				
				steps.put(step.getName(), step);
			}
		}
		
		if (steps.size() == 0)
		{
			logger.error("No step defined!");
			return false;
		}
		
		// init flows
		
		for (String flowStep : jobDef.getFlowSteps())
		{
			Step step = steps.get(flowStep);
			
			if (step == null)
			{
				logger.error("Can't find definition for step {}", flowStep);
				return false;
			}
		}
		
		for (Step step : this.steps.values())
		{
			
			for (String nextStepName : jobDef.getNextStepNames(step.getName()))
			{
				Step nextStep = this.findStep(nextStepName);
				
				if (nextStep == null)
				{
					logger.error("Can't find to step {}", nextStepName);
				}
				else
				{
					BlockingDataSet dataSet = new BlockingDataSet(jobDef.getPropertyInt(Const.DATASET_BUFFER_SIZE_PARAM,
							Const.DATASET_BUFFER_SIZE_DEFAULT));
					dataSet.setName(step.getName() + " to " + nextStep.getName());
					
					this.dataSets.put(dataSet.getName(), dataSet);
					step.getOutputDataSets().add(dataSet);
					nextStep.getInputDataSets().add(dataSet);
				}
			}
		}
		
		
		// load lookups
		
		LookupManager.getInstance().init(jobDef.getLookupDef());
		
		// init steps

		ArrayList<Thread> initThreads = new ArrayList<Thread>(this.steps.size());
		
		for (Step step : this.steps.values())
		{
			Thread thread = new Thread(new StepInitThread(step));
			thread.setName(step.getName() + " initializing");
			thread.start();
			initThreads.add(thread);
		}
		
		for (Thread thread : initThreads)
		{
			try
			{
				thread.join();
			} catch (InterruptedException ie)
			{
				logger.error("init thread is interrupted", ie);
			}
		}
		
		for (Step step : this.steps.values())
		{
			if (!step.isInitialized())
			{
				stopAll();
				return false;
			}
		}

		return true;
	}
	
	public Step findStep(String name)
	{
		return this.steps.get(name);
	}
	
	public void process()
	{
		ArrayList<Thread> runThreads = new ArrayList<Thread>(this.steps.size());
		
		for (Step step : this.steps.values())
		{
			Thread thread = new Thread(new StepRunThread(step));
			thread.setName(step.getName() + " processing");
			thread.start();
			runThreads.add(thread);
		}
		
		for (Thread thread : runThreads)
		{
			try
			{
				thread.join();
			} catch (InterruptedException ie)
			{
				
			}
		}
		
		// dispose steps
		for (Step step : this.steps.values())
		{
			step.dispose();
		}
		
	}
}
