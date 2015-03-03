package org.f3tools.incredible.smartETL;

public class StepInitThread implements Runnable
{
	private Step step;
	
	public void run()
	{
		step.setInitialized(step.init());
	}
	
	public StepInitThread(Step step)
	{
		this.step = step;
	}	
	
}
