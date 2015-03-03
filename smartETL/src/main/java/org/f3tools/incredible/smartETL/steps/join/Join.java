package org.f3tools.incredible.smartETL.steps.join;



package org.pentaho.di.trans.steps.mergejoin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.DataRow;
import org.f3tools.incredible.smartETL.DataSet;
import org.f3tools.incredible.smartETL.Job;
import org.f3tools.incredible.utilities.ETLException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;


public class Join extends AbstractStep
{
	private JoinDef joinDef;
	private DataSet leftDataSet;
	private DataSet rightDataSet;
	private DataRow leftRow;
	private DataRow rightRow;
	private DataRow nextLeftRow;
	private DataRow nextRightRow;
	private boolean leftOptional = false;
	private boolean rightOptional = false;
	private int[] leftKeyIdx;
	private int[] rightKeyIdx;
	private List<DataRow> leftRows;
	private List<DataRow> rightRows;
	
	private boolean first;
	
	public JoinDef getJoinDef()
	{
		return joinDef;
	}

	public void setJoinDef(JoinDef joinDef)
	{
		this.joinDef = joinDef;
	}

	public Join(String name, Job job)
	{
		super(name, job);
	}
	
	private DataSet findInputDataSet(String name)
	{
		return null;
	}
	
	public boolean processRow() throws ETLException
	{
		meta=(MergeJoinMeta)smi;
		data=(MergeJoinData)sdi;
		int compare;

       if (first)
       {
           first = false;
           leftDataSet = findInputDataSet(joinDef.getLeftStepName());
           rightDataSet = findInputDataSet(joinDef.getRightStepName());

           if (leftDataSet == null)
           {
        	   throw new ETLException("Can't find left data set for name:" + joinDef.getLeftStepName() );
           }

           if (rightDataSet == null)
           {
        	   throw new ETLException("Can't find right data set for name:" + joinDef.getRightStepName() );
           }
           
           leftRow = getRowFrom(leftDataSet);
           rightRow = getRowFrom(rightDataSet);
 
           if (leftRow != null)
           {
               // Find the key indexes:
               data.keyNrs1 = new int[meta.getKeyFields1().length];
               for (int i=0;i<data.keyNrs1.length;i++)
               {
                   data.keyNrs1[i] = data.oneMeta.indexOfValue(meta.getKeyFields1()[i]);
                   if (data.keyNrs1[i]<0)
                   {
                       String message = BaseMessages.getString(PKG, "MergeJoin.Exception.UnableToFindFieldInReferenceStream",meta.getKeyFields1()[i]);  //$NON-NLS-1$ //$NON-NLS-2$
                       logError(message);
                       throw new KettleStepException(message);
                   }
               }
           }

           if (data.two!=null)
           {
               // Find the key indexes:
               data.keyNrs2 = new int[meta.getKeyFields2().length];
               for (int i=0;i<data.keyNrs2.length;i++)
               {
                   data.keyNrs2[i] = data.twoMeta.indexOfValue(meta.getKeyFields2()[i]);
                   if (data.keyNrs2[i]<0)
                   {
                       String message = BaseMessages.getString(PKG, "MergeJoin.Exception.UnableToFindFieldInReferenceStream",meta.getKeyFields2()[i]);  //$NON-NLS-1$ //$NON-NLS-2$
                       logError(message);
                       throw new KettleStepException(message);
                   }
               }
           }

           // Calculate one_dummy... defaults to null
           data.one_dummy=RowDataUtil.allocateRowData( data.oneMeta.size() + data.twoMeta.size() );
           
           // Calculate two_dummy... defaults to null
           //
           data.two_dummy=new Object[data.twoMeta.size()];
       }

       if (log.isRowLevel()) logRowlevel(BaseMessages.getString(PKG, "MergeJoin.Log.DataInfo",data.oneMeta.getString(data.one)+"")+data.twoMeta.getString(data.two)); //$NON-NLS-1$ //$NON-NLS-2$

       /*
        * We can stop processing if any of the following is true:
        *   a) Both streams are empty
        *   b) First stream is empty and join type is INNER or LEFT OUTER
        *   c) Second stream is empty and join type is INNER or RIGHT OUTER
        */
       if ((leftRow == null && rightRow == null) ||
       	(leftRow == null && leftOptional == false) ||
       	(rightRow == null && rightOptional == false))
       {
       	// Before we stop processing, we have to make sure that all rows from both input streams are depleted!
       	// If we don't do this, the transformation can stall.
       	//
       	while (leftRow !=null && !isStopped()) leftRow = getRowFrom(leftDataSet);
       	while (rightRow !=null && !isStopped()) rightRow = getRowFrom(rightDataSet);
       	
           setOutputDone();
           return false;
       }

       if (leftRow == null)
       {
       	compare = -1;
       }
       else 
       {
           if (rightRow == null)
           {
               compare = 1;
           }
           else
       	{
               int cmp = leftRow.getDataDef().compare(leftRow, rightRow, leftKeyIdx, rightKeyIdx);
               compare = cmp > 0 ? 1 : cmp < 0 ? -1 : 0;
           }
       }
       
       switch (compare)
       {
       case 0:
       	/*
       	 * We've got a match. This is what we do next (to handle duplicate keys correctly):
       	 *   Read the next record from both streams
       	 *   If any of the keys match, this means we have duplicates. We therefore
       	 *     Create an array of all rows that have the same keys
       	 *     Push a Cartesian product of the two arrays to output
       	 *   Else
       	 *     Just push the combined rowset to output
       	 */ 
       	nextLeftRow = getRowFrom(leftDataSet);
       	nextRightRow = getRowFrom(rightDataSet);
       	        	
       	int compare1 = (nextLeftRow == null) ? -1 : leftRow.getDataDef().compare(leftRow, nextLeftRow, leftKeyIdx, leftKeyIdx);
       	int compare2 = (nextRightRow == null) ? -1 : rightRow.getDataDef().compare(rightRow, nextRightRow, rightKeyIdx, rightKeyIdx);
       	if (compare1 == 0 || compare2 == 0) // Duplicate keys
       	{
           	if (leftRow == null)
           		leftRows = new ArrayList<DataRow>();
           	else
           		leftRows.clear();
           	
           	if (rightRows == null)
           		rightRows = new ArrayList<DataRow>();
           	else
           		rightRows.clear();
           	
           	leftRows.add(leftRow);
           	
           	if (compare1 == 0) // First stream has duplicates
           	{
           		leftRows.add(nextLeftRow);
           		for(;!isStopped();)
	            	{
	                	nextLeftRow = getRowFrom(leftDataSet);
	                	if (0 != ((nextLeftRow == null) ? -1 : leftRow.getDataDef().compare(leftRow, nextLeftRow, leftKeyIdx, leftKeyIdx))) {
	                		break;
	                	}
	                	leftRows.add(nextLeftRow);
	            	}
	            	if (isStopped()) return false;
           	}
           	
           	rightRows.add(rightRow);
           	
           	if (compare2 == 0) // Second stream has duplicates
           	{
           		rightRows.add(nextRightRow);
	            	for(;!isStopped();)
	            	{
	                	nextRightRow = getRowFrom(rightDataSet);
	                	if (0 != ((nextRightRow == null) ? -1 : rightRow.getDataDef().compare(rightRow, nextRightRow, rightKeyIdx, rightKeyIdx))) {
	                		break;
	                	}
	                	rightRows.add(nextRightRow);
	            	}
	            	if (isStopped()) return false;
           	}
           	
           	for (Iterator<Object[]> oneIter = data.ones.iterator(); oneIter.hasNext() && !isStopped(); ) {
           	  Object[] one = oneIter.next();
           	  for (Iterator<Object[]> twoIter = data.twos.iterator(); twoIter.hasNext() && !isStopped(); ) {
           	    Object[] two = twoIter.next();
           	    Object[] oneBig = RowDataUtil.createResizedCopy(one, data.oneMeta.size() + data.twoMeta.size());
                 Object[] combi = RowDataUtil.addRowData(oneBig, data.oneMeta.size(), two);
                 putRow(data.outputRowMeta, combi);
           	  }
               // Remove the rows as we merge them to keep the overall memory footprint minimal
           	  oneIter.remove();
           	}
           	data.twos.clear();
       	}
       	else // No duplicates
       	{
       		Object[] outputRowData = RowDataUtil.addRowData(data.one, data.oneMeta.size(), data.two);
	        	putRow(data.outputRowMeta, outputRowData);
       	}
       	data.one = data.one_next;
       	data.two = data.two_next;
       	break;
       case 1:
       	//if (log.isDebug()) logDebug("First stream has missing key");
       	/*
       	 * First stream is greater than the second stream. This means:
       	 *   a) This key is missing in the first stream
       	 *   b) Second stream may have finished
       	 *  So, if full/right outer join is set and 2nd stream is not null,
       	 *  we push a record to output with only the values for the second
       	 *  row populated. Next, if 2nd stream is not finished, we get a row
       	 *  from it; otherwise signal that we are done
       	 */
       	if (data.one_optional == true)
       	{
       		if (data.two != null)
       		{
       			Object[] outputRowData = RowDataUtil.createResizedCopy(data.one_dummy, data.outputRowMeta.size());
           		outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two);
	        		putRow(data.outputRowMeta, outputRowData);
	        		data.two = getRowFrom(data.twoRowSet);
       		}
       		else if (data.two_optional == false)
       		{
       			/*
       			 * If we are doing right outer join then we are done since
       			 * there are no more rows in the second set
       			 */
               	// Before we stop processing, we have to make sure that all rows from both input streams are depleted!
               	// If we don't do this, the transformation can stall.
               	//
               	while (data.one!=null && !isStopped()) data.one = getRowFrom(data.oneRowSet);
               	while (data.two!=null && !isStopped()) data.two = getRowFrom(data.twoRowSet);

       			setOutputDone();
       			return false;
       		}
       		else
       		{
       			/*
       			 * We are doing full outer join so print the 1st stream and
       			 * get the next row from 1st stream
       			 */
           		Object[] outputRowData = RowDataUtil.createResizedCopy(data.one, data.outputRowMeta.size());
           		outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two_dummy);
	        		putRow(data.outputRowMeta, outputRowData);
	        		data.one = getRowFrom(data.oneRowSet);
       		}
       	}
       	else if (data.two == null && data.two_optional == true)
       	{
       		/**
       		 * We have reached the end of stream 2 and there are records
       		 * present in the first stream. Also, join is left or full outer.
       		 * So, create a row with just the values in the first stream
       		 * and push it forward
       		 */
       		Object[] outputRowData = RowDataUtil.createResizedCopy(data.one, data.outputRowMeta.size());
       		outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two_dummy);
       		putRow(data.outputRowMeta, outputRowData);
       		data.one = getRowFrom(data.oneRowSet);
       	}
       	else if (data.two != null)
       	{
       		/*
       		 * We are doing an inner or left outer join, so throw this row away
       		 * from the 2nd stream
       		 */
       		data.two = getRowFrom(data.twoRowSet);
       	}
       	break;
       case -1:
       	//if (log.isDebug()) logDebug("Second stream has missing key");
       	/*
       	 * Second stream is greater than the first stream. This means:
       	 *   a) This key is missing in the second stream
       	 *   b) First stream may have finished
       	 *  So, if full/left outer join is set and 1st stream is not null,
       	 *  we push a record to output with only the values for the first
       	 *  row populated. Next, if 1st stream is not finished, we get a row
       	 *  from it; otherwise signal that we are done
       	 */
       	if (data.two_optional == true)
       	{
       		if (data.one != null)
       		{
           		Object[] outputRowData = RowDataUtil.createResizedCopy(data.one, data.outputRowMeta.size());
           		outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two_dummy);
       			putRow(data.outputRowMeta, outputRowData);
	        		data.one = getRowFrom(data.oneRowSet);
       		}
       		else if (data.one_optional == false)
       		{
       			/*
       			 * We are doing a left outer join and there are no more rows
       			 * in the first stream; so we are done
       			 */
               	// Before we stop processing, we have to make sure that all rows from both input streams are depleted!
               	// If we don't do this, the transformation can stall.
               	//
               	while (data.one!=null && !isStopped()) data.one = getRowFrom(data.oneRowSet);
               	while (data.two!=null && !isStopped()) data.two = getRowFrom(data.twoRowSet);

       			setOutputDone();
       			return false;
       		}
       		else
       		{
       			/*
       			 * We are doing a full outer join so print the 2nd stream and
       			 * get the next row from the 2nd stream
       			 */
           		Object[] outputRowData = RowDataUtil.createResizedCopy(data.one_dummy, data.outputRowMeta.size());
           		outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two);
       			putRow(data.outputRowMeta, outputRowData);
	        		data.two = getRowFrom(data.twoRowSet);
       		}
       	}
       	else if (data.one == null && data.one_optional == true)
       	{
       		/*
       		 * We have reached the end of stream 1 and there are records
       		 * present in the second stream. Also, join is right or full outer.
       		 * So, create a row with just the values in the 2nd stream
       		 * and push it forward
       		 */
       		Object[] outputRowData = RowDataUtil.createResizedCopy(data.one_dummy, data.outputRowMeta.size());
       		outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two);
       		putRow(data.outputRowMeta, outputRowData);
       		data.two = getRowFrom(data.twoRowSet);
       	}
       	else if (data.one != null)
       	{
       		/*
       		 * We are doing an inner or right outer join so a non-matching row
       		 * in the first stream is of no use to us - throw it away and get the
       		 * next row
       		 */
       		data.one = getRowFrom(data.oneRowSet);
       	}
       	break;
       default:
       	logDebug("We shouldn't be here!!");
       	// Make sure we do not go into an infinite loop by continuing to read data
       	data.one = getRowFrom(data.oneRowSet);
   	    data.two = getRowFrom(data.twoRowSet);
   	    break;
       }
       if (checkFeedback(getLinesRead())) logBasic(BaseMessages.getString(PKG, "MergeJoin.LineNumber")+getLinesRead()); //$NON-NLS-1$
		return true;
	}
		
	/**
    * @see StepInterface#init( org.pentaho.di.trans.step.StepMetaInterface , org.pentaho.di.trans.step.StepDataInterface)
    */
   public boolean init()
   {
	   
		if (!super.init()) return false;
		
		if (this.joinDef == null) return false;	   
	   
		meta=(MergeJoinMeta)smi;
		data=(MergeJoinData)sdi;

       if (super.init(smi, sdi))
       {
           List<StreamInterface> infoStreams = meta.getStepIOMeta().getInfoStreams();
           if (infoStreams.get(0).getStepMeta()==null || infoStreams.get(1).getStepMeta()==null)
           {
               logError(BaseMessages.getString(PKG, "MergeJoin.Log.BothTrueAndFalseNeeded")); //$NON-NLS-1$
               return false;
           }
           String joinType = meta.getJoinType();
           for (int i = 0; i < MergeJoinMeta.join_types.length; ++i)
           {
           	if (joinType.equalsIgnoreCase(MergeJoinMeta.join_types[i]))
           	{
           		data.one_optional = MergeJoinMeta.one_optionals[i];
           		data.two_optional = MergeJoinMeta.two_optionals[i];
           		return true;
           	}
           }
          	logError(BaseMessages.getString(PKG, "MergeJoin.Log.InvalidJoinType", meta.getJoinType())); //$NON-NLS-1$
              return false;
       }
       return true;
   }

   /**
    * Checks whether incoming rows are join compatible. This essentially
    * means that the keys being compared should be of the same datatype
    * and both rows should have the same number of keys specified
    * @param row1 Reference row
    * @param row2 Row to compare to
    * 
    * @return true when templates are compatible.
    */
   protected boolean isInputLayoutValid(RowMetaInterface row1, RowMetaInterface row2)
   {
       if (row1!=null && row2!=null)
       {
           // Compare the key types
       	String keyFields1[] = meta.getKeyFields1();
           int nrKeyFields1 = keyFields1.length;
       	String keyFields2[] = meta.getKeyFields2();
           int nrKeyFields2 = keyFields2.length;

           if (nrKeyFields1 != nrKeyFields2)
           {
           	logError("Number of keys do not match " + nrKeyFields1 + " vs " + nrKeyFields2);
           	return false;
           }

           for (int i=0;i<nrKeyFields1;i++)
           {
           	ValueMetaInterface v1 = row1.searchValueMeta(keyFields1[i]);
               if (v1 == null)
               {
               	return false;
               }
               ValueMetaInterface v2 = row2.searchValueMeta(keyFields2[i]);
               if (v2 == null)
               {
               	return false;
               }          
               if ( v1.getType()!=v2.getType() )
               {
               	return false;
               }
           }
       }
       // we got here, all seems to be ok.
       return true;
   }

}