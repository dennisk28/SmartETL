package org.f3tools.incredible.smartETL;

/**
 * Capture all stats of each step
 * @author Dennis Kang
 * @since 2015/03/15
 *
 */
public class StepStats
{
	// number of lines read from previous steps
    private long linesRead;    
    // number of lines written to next step(s)
    private long linesWritten;
    // number of lines read from file or database
    private long linesInput;
    // number of lines written to file or database
    private long linesOutput;
    // number of updates in a database table or file
    private long linesUpdated;
    // number of lines skipped
    private long linesFiltered;
    // number of lines errored
    private long linesErrored;

    private Step step;
    
    public StepStats(Step step)
    {
    	this.step = step;
    }
    
    public long getLinesRead()
	{
		return linesRead;
	}
	
    public void addLinesRead()
	{
		linesRead++;
	}
	
    public long getLinesWritten()
	{
		return linesWritten;
	}
	
    public void addLinesWritten()
	{
		linesWritten++;
	}
	
    public long getLinesInput()
	{
		return linesInput;
	}
	
    public void addLinesInput()
	{
		linesInput++;
	}
	
    public long getLinesOutput()
	{
		return linesOutput;
	}
	
    public void addLinesOutput()
	{
		linesOutput++;
	}
	
    public long getLinesUpdated()
	{
		return linesUpdated;
	}
	
    public void addLinesUpdated()
	{
		linesUpdated++;
	}
	
    public long getLinesFiltered()
	{
		return linesFiltered;
	}
	
    public void addLinesFiltered()
	{
		linesFiltered++;
	}
	
    public long getLinesErrored()
	{
		return linesErrored;
	}
	
    public void addLinesErrored()
	{
		linesErrored++;
	}
    
    public String toString()
    {
    	StringBuffer sb = new StringBuffer();
    	
    	sb.append("Step statistics: " + step.getName() + "\n");
    	sb.append("Lines read: " + linesRead + "\n");
    	sb.append("Lines written: " + linesWritten + "\n");
    	sb.append("Lines inputed: " + linesInput + "\n");
    	sb.append("Lines outputed: " + linesOutput + "\n");
    	sb.append("Lines updated: " + linesUpdated + "\n");
    	sb.append("Lines filtered: " + linesFiltered + "\n");
    	sb.append("Lines with errs: " + linesErrored + "\n");
    	
    	return sb.toString();
    }
}
