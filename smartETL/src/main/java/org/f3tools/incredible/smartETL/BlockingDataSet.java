package org.f3tools.incredible.smartETL;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingDataSet extends AbstractDataSet implements DataSet 
{
    private BlockingQueue<Object[]> queArray;
    
    /**
     * Create new non-blocking-queue with maxSize capacity.
     * @param maxSize
     */
    public BlockingDataSet(int maxSize)
    {
    	// create an empty queue 
        queArray = new ArrayBlockingQueue<Object[]>(maxSize, false);
        
        timeoutGet = Const.toInt(System.getProperty(Const.DATASET_GET_TIMEOUT), Const.TIMEOUT_GET_MILLIS);
        timeoutPut = Const.toInt(System.getProperty(Const.DATASET_PUT_TIMEOUT), Const.TIMEOUT_PUT_MILLIS);
    }
    

    public boolean putRow(DataDef dataDef, Object[] rowData)
    {
    	return putRowWait(dataDef, rowData, timeoutPut, TimeUnit.MILLISECONDS);
    }
    
    public boolean putRowWait(DataDef dataDef, Object[] rowData, long time, TimeUnit tu) {
    	this.setDataDef(dataDef);
    	try{
    		
    		return queArray.offer(rowData, time, tu);
    	}
    	catch (InterruptedException e)
	    {
    		return false;
	    }
    	catch (NullPointerException e)
	    {
    		return false;
	    }    	
    	
    }
    
    public Object[] getRow(){
    	return getRowWait(timeoutGet, TimeUnit.MILLISECONDS);
    }
    
    
    public Object[] getRowImmediate(){

    	return queArray.poll();	    	
    }
    
    public Object[] getRowWait(long timeout, TimeUnit tu){

    	try{
    		return queArray.poll(timeout, tu);
    	}
    	catch(InterruptedException e){
    		return null;
    	}
    }
    
    public int size() {
    	return queArray.size();
    }	
	
}
