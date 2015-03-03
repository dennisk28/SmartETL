package org.f3tools.incredible.smartETL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobRunner {
	
	private static Logger logger = LoggerFactory.getLogger(JobRunner.class);
	
	public static void main(String[] argc)
	{
		if (argc.length < 1)
		{
			logger.info("Usage: jobRunner jobDefFile");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.execute(argc);
	}

}
