package org.f3tools.incredible.smartETL.steps.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.DataDef;
import org.f3tools.incredible.smartETL.DataRow;
import org.f3tools.incredible.smartETL.DataSet;
import org.f3tools.incredible.smartETL.Job;
import org.f3tools.incredible.smartETL.Step;

/**
 * Join two input dataset based on keys. 
 * Join types:
 * inner: only left side key fields will be preserved
 * left outer: only left side key fields will be preserved
 * right outer: only right side key fields will be preserved
 * @author Desheng Kang
 * @since 2015/03/08
 *
 */
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
	private DataDef outputDataDef;
	private int[] leftExcludedIdx;
	private int [] rightExcludedIdx;
	
	private boolean first = true;

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

	/**
	 * Find input dataset for a given step name
	 * @param name
	 * @return
	 */
	private DataSet findInputDataSet(String stepName)
	{
		Step step = getJob().findStep(stepName);
		
		if (step == null) return null;
		
		for (DataSet ds : step.getOutputDataSets())
		{
			for (DataSet inputDs : this.getInputDataSets())
			{
				// shall we do hard copy compare? @TODO Dennis 2015/03/07
				if (ds == inputDs) return inputDs;
			}
		}
		
		return null;
	}

	@Override
	public boolean processRow() throws ETLException
	{
		int compare;

		if (first)
		{
			first = false;
			leftDataSet = findInputDataSet(joinDef.getLeftStepName());
			rightDataSet = findInputDataSet(joinDef.getRightStepName());

			if (leftDataSet == null)
			{
				throw new ETLException("Can't find left data set for name:" + joinDef.getLeftStepName());
			}

			if (rightDataSet == null)
			{
				throw new ETLException("Can't find right data set for name:" + joinDef.getRightStepName());
			}

			leftRow = getRowFrom(leftDataSet);
			rightRow = getRowFrom(rightDataSet);

			if (leftRow != null)
			{
				// Find the key indexes:
				leftKeyIdx = new int[joinDef.getLeftKeys().length];
				for (int i = 0; i < leftKeyIdx.length; i++)
				{
					leftKeyIdx[i] = leftRow.getDataDef().getFieldIndex(joinDef.getLeftKeys()[i]);
					if (leftKeyIdx[i] < 0)
					{
						throw new ETLException("can't find key " + joinDef.getLeftKeys()[i]);
					}
				}
			}

			if (rightRow != null)
			{
				// Find the key indexes:
				rightKeyIdx = new int[joinDef.getRightKeys().length];
				for (int i = 0; i < rightKeyIdx.length; i++)
				{
					rightKeyIdx[i] = rightRow.getDataDef().getFieldIndex(joinDef.getRightKeys()[i]);
					if (rightKeyIdx[i] < 0)
					{
						throw new ETLException("can't find key " + joinDef.getRightKeys()[i]);
					}
				}
			}

			outputDataDef = new DataDef();
			
			// remove key fields
			if (joinDef.getJoinType().equalsIgnoreCase("INNER") 
					|| joinDef.getJoinType().equalsIgnoreCase("LEFT OUTER"))
			{
				rightExcludedIdx = Arrays.copyOf(rightKeyIdx, rightKeyIdx.length);
				Arrays.sort(rightExcludedIdx);
				leftExcludedIdx = new int[0];

				if (leftRow != null) outputDataDef.copyDataDef(leftRow.getDataDef());
				
				if (rightRow != null)
				{
					DataDef newRightDataDef = new DataDef();
					newRightDataDef.copyDataDef(rightRow.getDataDef());
					
					for (int i = rightExcludedIdx.length - 1; i >= 0; i--)
					{
						newRightDataDef.removeField(rightExcludedIdx[i]);
					}
					
					outputDataDef.copyDataDef(newRightDataDef);
				}
			}
			else
			{
				leftExcludedIdx = Arrays.copyOf(leftKeyIdx, leftKeyIdx.length);
				Arrays.sort(leftExcludedIdx);
				rightExcludedIdx = new int[0];

				if (leftRow != null)
				{
					DataDef newLeftDataDef = new DataDef();
					newLeftDataDef.copyDataDef(leftRow.getDataDef());
					
					for (int i = leftExcludedIdx.length - 1; i >= 0; i--)
					{
						newLeftDataDef.removeField(leftExcludedIdx[i]);
					}
					
					outputDataDef.copyDataDef(newLeftDataDef);
				}
				
				if (rightRow != null) outputDataDef.copyDataDef(rightRow.getDataDef());
			}
		}

		//if (log.isRowLevel()) logRowlevel(BaseMessages.getString(PKG, "MergeJoin.Log.DataInfo",data.oneMeta.getString(data.one)+"")+data.twoMeta.getString(data.two)); //$NON-NLS-1$ //$NON-NLS-2$

		/*
		 * We can stop processing if any of the following is true: a) Both
		 * streams are empty b) First stream is empty and join type is INNER or
		 * LEFT OUTER c) Second stream is empty and join type is INNER or RIGHT
		 * OUTER
		 */
		if ((leftRow == null && rightRow == null) || (leftRow == null && leftOptional == false)
				|| (rightRow == null && rightOptional == false))
		{
			// Before we stop processing, we have to make sure that all rows
			// from both input streams are depleted!
			// If we don't do this, the transformation can stall.
			//
			while (leftRow != null && !isStopped())
				leftRow = getRowFrom(leftDataSet);
			while (rightRow != null && !isStopped())
				rightRow = getRowFrom(rightDataSet);

			setOutputDone();
			return false;
		}

		if (leftRow == null)
		{
			compare = -1;
		} else
		{
			if (rightRow == null)
			{
				compare = 1;
			} else
			{
				int cmp = leftRow.getDataDef().compare(leftRow, rightRow, leftKeyIdx, rightKeyIdx);
				compare = cmp > 0 ? 1 : cmp < 0 ? -1 : 0;
			}
		}

		switch (compare)
		{
		case 0:
			/*
			 * We've got a match. This is what we do next (to handle duplicate
			 * keys correctly): Read the next record from both streams If any of
			 * the keys match, this means we have duplicates. We therefore
			 * Create an array of all rows that have the same keys Push a
			 * Cartesian product of the two arrays to output Else Just push the
			 * combined rowset to output
			 */
			nextLeftRow = getRowFrom(leftDataSet);
			nextRightRow = getRowFrom(rightDataSet);

			int compare1 = (nextLeftRow == null) ? -1 : leftRow.getDataDef().compare(leftRow, nextLeftRow, leftKeyIdx,
					leftKeyIdx);
			int compare2 = (nextRightRow == null) ? -1 : rightRow.getDataDef().compare(rightRow, nextRightRow,
					rightKeyIdx, rightKeyIdx);
			if (compare1 == 0 || compare2 == 0) // Duplicate keys
			{
				if (leftRows == null)
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
					for (; !isStopped();)
					{
						nextLeftRow = getRowFrom(leftDataSet);
						if (0 != ((nextLeftRow == null) ? -1 : leftRow.getDataDef().compare(leftRow, nextLeftRow,
								leftKeyIdx, leftKeyIdx)))
						{
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
					for (; !isStopped();)
					{
						nextRightRow = getRowFrom(rightDataSet);
						if (0 != ((nextRightRow == null) ? -1 : rightRow.getDataDef().compare(rightRow, nextRightRow,
								rightKeyIdx, rightKeyIdx)))
						{
							break;
						}
						rightRows.add(nextRightRow);
					}
					if (isStopped()) return false;
				}

				for (Iterator<DataRow> leftIter = leftRows.iterator(); leftIter.hasNext() && !isStopped();)
				{
					DataRow left = leftIter.next();
					for (Iterator<DataRow> rightIter = rightRows.iterator(); rightIter.hasNext() && !isStopped();)
					{
						DataRow right = rightIter.next();
						putRow(mergeRow(left, right, outputDataDef));
					}
					// Remove the rows as we merge them to keep the overall
					// memory footprint minimal

					leftIter.remove();
				}
				
				rightRows.clear();
				
			} else // No duplicates
			{
				putRow(mergeRow(leftRow, rightRow, outputDataDef));
			}
			
			leftRow = nextLeftRow;
			rightRow = nextRightRow;
			break;
		case 1:
			// if (log.isDebug()) logDebug("First stream has missing key");
			/*
			 * First stream is greater than the second stream. This means: a)
			 * This key is missing in the first stream b) Second stream may have
			 * finished So, if full/right outer join is set and 2nd stream is
			 * not null, we push a record to output with only the values for the
			 * second row populated. Next, if 2nd stream is not finished, we get
			 * a row from it; otherwise signal that we are done
			 */
			if (leftOptional == true)
			{
				if (rightRow != null)
				{
					putRow(mergeRow(null, rightRow, outputDataDef));
					rightRow = getRowFrom(rightDataSet);
				} else if (rightOptional == false)
				{
					/*
					 * If we are doing right outer join then we are done since
					 * there are no more rows in the second set
					 */
					// Before we stop processing, we have to make sure that all
					// rows from both input streams are depleted!
					// If we don't do this, the transformation can stall.
					//
					while (leftRow != null && !isStopped())
						leftRow = getRowFrom(leftDataSet);
					while (rightRow != null && !isStopped())
						rightRow = getRowFrom(rightDataSet);

					setOutputDone();
					return false;
				} else
				{
					/*
					 * We are doing full outer join so print the 1st stream and
					 * get the next row from 1st stream
					 */
					putRow(mergeRow(leftRow, null, outputDataDef));
					leftRow = getRowFrom(leftDataSet);
				}
			} else if (rightRow == null && rightOptional == true)
			{
				/**
				 * We have reached the end of stream 2 and there are records
				 * present in the first stream. Also, join is left or full
				 * outer. So, create a row with just the values in the first
				 * stream and push it forward
				 */
				putRow(mergeRow(leftRow, null, outputDataDef));
				leftRow = getRowFrom(leftDataSet);
			} else if (rightRow != null)
			{
				/*
				 * We are doing an inner or left outer join, so throw this row
				 * away from the 2nd stream
				 */
				rightRow = getRowFrom(rightDataSet);
			}
			break;
		case -1:
			// if (log.isDebug()) logDebug("Second stream has missing key");
			/*
			 * Second stream is greater than the first stream. This means: a)
			 * This key is missing in the second stream b) First stream may have
			 * finished So, if full/left outer join is set and 1st stream is not
			 * null, we push a record to output with only the values for the
			 * first row populated. Next, if 1st stream is not finished, we get
			 * a row from it; otherwise signal that we are done
			 */
			if (rightOptional == true)
			{
				if (leftRow != null)
				{
					putRow(mergeRow(leftRow, null, outputDataDef));
					leftRow = getRowFrom(leftDataSet);
				} else if (leftOptional == false)
				{
					/*
					 * We are doing a left outer join and there are no more rows
					 * in the first stream; so we are done
					 */
					// Before we stop processing, we have to make sure that all
					// rows from both input streams are depleted!
					// If we don't do this, the transformation can stall.
					//
					while (leftRow != null && !isStopped())
						leftRow = getRowFrom(leftDataSet);
					while (rightRow != null && !isStopped())
						rightRow = getRowFrom(rightDataSet);

					setOutputDone();
					return false;
				} else
				{
					/*
					 * We are doing a full outer join so print the 2nd stream
					 * and get the next row from the 2nd stream
					 */
					putRow(mergeRow(null, rightRow, outputDataDef));
					rightRow = getRowFrom(rightDataSet);
				}
			} else if (leftRow == null && leftOptional == true)
			{
				/*
				 * We have reached the end of stream 1 and there are records
				 * present in the second stream. Also, join is right or full
				 * outer. So, create a row with just the values in the 2nd
				 * stream and push it forward
				 */
				putRow(mergeRow(null, rightRow, outputDataDef));
				rightRow = getRowFrom(rightDataSet);
			} else if (leftRow != null)
			{
				/*
				 * We are doing an inner or right outer join so a non-matching
				 * row in the first stream is of no use to us - throw it away
				 * and get the next row
				 */
				leftRow = getRowFrom(leftDataSet);
			}
			break;
		default:
			// Make sure we do not go into an infinite loop by continuing to
			// read data
			leftRow = getRowFrom(leftDataSet);
			rightRow = getRowFrom(rightDataSet);
			break;
		}
		return true;
	}

	private DataRow mergeRow(DataRow leftRow, DataRow rightRow, DataDef outDataDef)
	{
		if (outDataDef == null) return null;

		int count = outDataDef.getFieldCount();
		Object[] rowData = new Object[count];
		int leftCount = 0;

		if (leftRow != null) 
		{
			Object[] leftData = leftRow.getRow();
			leftCount = leftData.length;
			
			if (this.leftExcludedIdx.length > 0)
			{
				int pos = 0;
				int destPos = 0;
				int len = 0;
				
				for (int i = 0; i < leftExcludedIdx.length; i++)
				{
					int endPos = leftExcludedIdx[i];
					len = endPos - pos;
					
					if (len > 0)
					{
						System.arraycopy(leftData, pos, rowData, destPos, len);
						destPos += len;
					}
					
					pos = endPos + 1;
				}
				
				if (pos < leftCount)
				{
					len = leftCount - pos;
					System.arraycopy(leftData, pos, rowData, destPos, len);
				}
			}
			else
				System.arraycopy(leftData, 0, rowData, 0, leftCount);
		}

		if (rightRow != null)
		{
			Object[] rightData = rightRow.getRow();
			
			if (this.rightExcludedIdx.length > 0)
			{
				int pos = 0;
				int destPos = leftCount;
				int len = 0;
				
				for (int i = 0; i < rightExcludedIdx.length; i++)
				{
					int endPos = rightExcludedIdx[i];
					len = endPos - pos;
					
					if (len > 0)
					{
						System.arraycopy(rightData, pos, rowData, destPos, len);
						destPos += len;
					}
					
					pos = endPos + 1;
				}
				
				if (pos < rightData.length)
				{
					len = rightData.length - pos;
					System.arraycopy(rightData, pos, rowData, destPos, len);
				}	
			}
			else
				System.arraycopy(rightData, 0, rowData, count - rightData.length, rightData.length);
		}

		DataRow row = new DataRow();
		row.setDataDef(outDataDef);
		row.setRow(rowData);

		return row;
	}

	@Override
	public boolean init()
	{
		if (!super.init()) return false;

		this.joinDef = (JoinDef)getStepDef();
		if (this.joinDef == null) return false;

		String joinType = joinDef.getJoinType();

		for (int i = 0; i < JoinDef.JOIN_TYPES.length; ++i)
		{
			if (joinType.equalsIgnoreCase(JoinDef.JOIN_TYPES[i]))
			{
				leftOptional = JoinDef.LEFT_OPTIONALS[i];
				rightOptional = JoinDef.RIGHT_OPTIONALS[i];
				return true;
			}
		}

		return true;
	}
}