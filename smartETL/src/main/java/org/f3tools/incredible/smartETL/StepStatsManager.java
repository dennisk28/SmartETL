package org.f3tools.incredible.smartETL;

import java.util.HashMap;
import java.util.Map;

public class StepStatsManager
{
	private static StepStatsManager instance;
	private Map<String, StepStats> stepStatsMap;
	
	private StepStatsManager()
	{
		stepStatsMap = new HashMap<String, StepStats>();
	}

	public static synchronized StepStatsManager getInstance()
	{
		if (instance == null) instance = new StepStatsManager();
		return instance;
	}
	
	public synchronized StepStats createStepStats(Step step)
	{
		StepStats stepStats = new StepStats(step);
		stepStatsMap.put(step.getName(),  stepStats);
		return stepStats;
	}
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		
		for(StepStats stats : stepStatsMap.values())
		{
			sb.append(stats.toString());
			sb.append("\n");
		}
		
		return sb.toString();
	}
}
