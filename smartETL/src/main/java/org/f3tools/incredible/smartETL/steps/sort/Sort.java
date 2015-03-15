package org.f3tools.incredible.smartETL.steps.sort;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.Utl;
import org.f3tools.incredible.smartETL.AbstractStep;
import org.f3tools.incredible.smartETL.DataRow;
import org.f3tools.incredible.smartETL.DataSetFile;
import org.f3tools.incredible.smartETL.Job;
import org.f3tools.incredible.smartETL.utilities.FileUtl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sort extends AbstractStep
{
	private Logger logger = LoggerFactory.getLogger(Sort.class);
	
	private boolean first = true;
	private SortDef sortDef;
	private int[] fieldnrs;
	public List<Object[]>   buffer;
	public List<File> files;
	public List<DataSetFile> dsis;
	private int freeCounter;
	private int sortSize;
	private int freeMemoryPct;
	private int memoryReporting;
	private int freeMemoryPctLimit;
	private int minSortSize;
	private List<Integer> bufferSizes;
	private int getBufferIndex;
	private List<Object[]> rowbuffer;
	private List<RowTempFile> tempRows;

	private Comparator<RowTempFile> comparator;
	
	public Sort(String name, Job job)
	{
		super(name, job);
		files= new ArrayList<File>();
		dsis  = new ArrayList<DataSetFile>();
        bufferSizes = new ArrayList<Integer>();		
	}

	private boolean addBuffer(DataRow r) throws ETLException
	{
		if (r != null)
		{
			buffer.add(r.getRow());
		}
		
		if (this.files.size() == 0 && r == null) // No more records: sort buffer
		{
			quickSort(this.buffer);
		}

		// Check the free memory every 1000 rows...
		//
		freeCounter++;
		if (sortSize <= 0 && this.freeCounter >= 1000)
		{
			freeMemoryPct = Utl.getFreeMemoryPercentage();
			freeCounter = 0;

			if (logger.isDebugEnabled())
			{
				memoryReporting++;
				if (memoryReporting >= 10)
				{
					logger.debug("Available memory : {}%" + freeMemoryPct);
					memoryReporting = 0;
				}
			}
		}

		boolean doSort = buffer.size() == sortSize; // Buffer is full: sort & dump to disk
		
		doSort |= files.size() > 0 && r == null && buffer.size() > 0; // No more records join from disk
		doSort |= freeMemoryPctLimit > 0 && freeMemoryPct < freeMemoryPctLimit
				&& buffer.size() >= minSortSize;

		// time to sort the buffer and write the data to disk...
		if (doSort)
		{
			sortExternalRows();
		}

		return true;
	}

	private void sortExternalRows() throws ETLException
	{
		// First sort the rows in buffer[]
		quickSort(buffer);

		// Then write them to disk...

		Object[] previousRow = null;

		try
		{
			File file = FileUtl.createTempFile(sortDef.getPrefix(), ".tmp", sortDef.getTempDirectory());
			DataSetFile dsf = new DataSetFile(sortDef.isCompress());
			dsf.setDataDef(dataDef);
			dsf.openForWrite(file.getAbsolutePath());

			files.add(file); // Remember the files!

			// Just write the data, nothing else
			if (sortDef.isUniquerows())
			{
				int index = 0;
				while (index < buffer.size())
				{
					Object[] row = buffer.get(index);
					if (previousRow != null)
					{
						int result = dataDef.compare(row, previousRow, fieldnrs);
						if (result == 0)
						{
							buffer.remove(index); // remove this duplicate
														// element as
														// requested
							//if (log.isRowLevel())
								//logRowlevel("Duplicate row removed: " + data.outputRowMeta.getString(row));
						} else
						{
							index++;
						}
					} else
					{
						index++;
					}
					previousRow = row;
				}
			}

			// How many records do we have left?
			this.bufferSizes.add(buffer.size());

			for (int i = 0, size = buffer.size(); i < size; i++)
			{
				dsf.writeData(buffer.get(i));
			}

			if (sortSize < 0)
			{
				if (buffer.size() > minSortSize)
				{
					minSortSize = buffer.size(); // if we did it once,
															// we can do
															// it again.

					// Memory usage goes up over time, even with garbage
					// collection
					// We need pointers, file handles, etc.
					// As such, we're going to lower the min sort size a bit
					//
					minSortSize = (int) Math.round((double) minSortSize * 0.90);
				}
			}

			// Clear the list
			buffer.clear();

			// Close temp-file
			dsf.close();

			// How much memory do we have left?
			//
			freeMemoryPct = Utl.getFreeMemoryPercentage();
			freeCounter = 0;
			if (sortSize <= 0)
			{
				logger.debug("Available memory: {}%", freeMemoryPct);
			}

		} catch (Exception e)
		{
			throw new ETLException("Error processing temp-file!", e);
		}

		getBufferIndex = 0;
	}

	private Object[] getBuffer() throws ETLException
	{
		Object[] retval;

		// Open all files at once and read one row from each file...
		if (files.size() > 0 && (this.dsis.size() == 0))
		{
			logger.info("Opening {} temp files...", files.size());

			try
			{
				for (int i = 0; i < files.size() && !isStopped(); i++)
				{
					File file = files.get(i);

					logger.info("Opening tmp-file: {}", file.getAbsolutePath());
					
					DataSetFile dsf = new DataSetFile(sortDef.isCompress());
					dsf.openForRead(file.getAbsolutePath());
					dsf.setDataDef(dataDef);
					
					this.dsis.add(dsf);

					// How long is the buffer?
					int buffersize = bufferSizes.get(i);

					logger.debug("{} expecting {} rows...", file.getName(), buffersize);

					if (buffersize > 0)
					{
						Object[] row = (Object[]) dsf.readData();
						this.rowbuffer.add(row); // new row from input stream
						this.tempRows.add(new RowTempFile(row, i));
					}
				}

				// Sort the data row buffer
				Collections.sort(tempRows, this.comparator);
			} 
			catch (Exception e)
			{
				logger.error("Error reading back tmp-files", e);
			}
		}

		if (files.size() == 0)
		{
			if (getBufferIndex < buffer.size())
			{
				retval = (Object[]) buffer.get(getBufferIndex);
				getBufferIndex++;
			} else
			{
				retval = null;
			}
		} else
		{
			if (rowbuffer.size() == 0)
			{
				retval = null;
			} 
			else
			{
				// We now have "filenr" rows waiting: which one is the smallest?
				//
				//if (log.isRowLevel())
				//{
				//	for (int i = 0; i < data.rowbuffer.size() && !isStopped(); i++)
				//	{
				//		Object[] b = (Object[]) data.rowbuffer.get(i);
				//		logRowlevel("--BR#" + i + ": " + data.outputRowMeta.getString(b));
				//	}
				//}

				RowTempFile rowTempFile = tempRows.remove(0);
				retval = rowTempFile.row;
				int smallest = rowTempFile.fileNumber;

				// now get another Row for position smallest

				File file = files.get(smallest);
				DataSetFile dsf = this.dsis.get(smallest);

				try
				{
					Object[] row2 = (Object[]) dsf.readData();
					
					if (row2 != null)
					{
						RowTempFile extra = new RowTempFile(row2, smallest);
	
						int index = Collections.binarySearch(tempRows, extra, comparator);
						if (index < 0)
						{
							tempRows.add(index * (-1) - 1, extra);
						} else
						{
							tempRows.add(index, extra);
						}
					}
					else
					{
						try
						{
							dsf.close();
							file.delete();

						} catch (Exception ioe)
						{
							logger.error("Unable to close/delete file #{}-->{}", smallest, file.getAbsolutePath());
						}

						files.remove(smallest);
						dsis.remove(smallest);

					// Also update all file numbers in in data.tempRows if they
					// are larger
					// than smallest.
					//
						for (RowTempFile rtf : tempRows)
						{
							if (rtf.fileNumber > smallest) rtf.fileNumber--;
						}
					}
				}
				catch (Exception e)
				{
					throw new ETLException("Error in get buffer", e);
				}
			}
		}

		return retval;
	}

	public boolean processRow() throws ETLException
	{
		boolean err = true;

		DataRow r = getRow(); 

		// initialize
		if (first && r != null)
		{
			first = false;
			
			dataDef = r.getDataDef();
			
			SortDef.SortField[] fields = sortDef.getFields();
			
			this.fieldnrs = new int[fields.length];
			for (int i = 0; i < fields.length; i++)
			{
				fieldnrs[i] = dataDef.getFieldIndex(fields[i].getName());

				if (fieldnrs[i] < 0)
				{
					throw new ETLException("Can't find sort field" + fields[i].getName() 
							+ " for step " + this.getName());
				}				
			}
		}

		err = addBuffer(r);
		if (!err)
		{
			setOutputDone(); // signal receiver we're finished.
			return false;
		}

		if (r == null) // no more input to be expected...
		{
			passBuffer();
			return false;
		}

		//if (checkFeedback(getLinesRead()))
		//{
		//	if (log.isBasic()) logBasic("Linenr " + getLinesRead());
		//}

		return true;
	}

	/**
	 * This method passes all rows in the buffer to the next steps.
	 * 
	 */
	private void passBuffer() throws ETLException
	{
		// Now we can start the output!
		//
		Object[] r = getBuffer();
		Object[] previousRow = null;
		while (r != null && !isStopped())
		{
			//if (log.isRowLevel()) logRowlevel("Read row: " + getInputRowMeta().getString(r));

			// Do another verification pass for unique rows...
			//
			if (sortDef.isUniquerows())
			{
				if (previousRow != null)
				{
					// See if this row is the same as the previous one as far as
					// the keys
					// are concerned.
					// If so, we don't put forward this row.
					int result = dataDef.compare(r, previousRow, fieldnrs);
					if (result != 0)
					{
						DataRow row = new DataRow();
						row.setDataDef(dataDef);
						row.setRow(r);
						putRow(row); // copy row to possible
														// alternate
														// rowset(s).
					}
				} else
				{
					DataRow row = new DataRow();
					row.setDataDef(dataDef);
					row.setRow(r);
					putRow(row); // copy row to possible
				}

				previousRow = r;
			} else
			{
				DataRow row = new DataRow();
				row.setDataDef(dataDef);
				row.setRow(r);
				putRow(row); // copy row to possible
			}

			r = getBuffer();
		}

		// Clear out the buffer for the next batch
		//
		clearBuffers();

		setOutputDone(); // signal receiver we're finished.
	}

	public boolean init()
	{
		if (super.init())
		{
			sortDef = (SortDef)getStepDef();
			
			sortSize = sortDef.getSortsize();
			freeMemoryPctLimit = sortDef.getFreememory();
			if (sortSize <= 0 && freeMemoryPctLimit <= 0)
			{
				// Prefer the memory limit as it should never fail
				//
				freeMemoryPctLimit = 25;
			}

			// In memory buffer
			//
			buffer = new ArrayList<Object[]>(5000);

			// Buffer for reading from disk
			//
			rowbuffer = new ArrayList<Object[]>(5000);

			//compressFiles = getBooleanValueOfVariable(meta.getCompressFilesVariable(), meta.getCompressFiles());

			comparator = new Comparator<RowTempFile>()
			{
				public int compare(RowTempFile o1, RowTempFile o2)
				{
					try
					{
						return dataDef.compare(o1.row, o2.row, fieldnrs);
					} catch (ETLException e)
					{
						logger.error("Error comparing rows: ", e);
						return 0;
					}
				}
			};

			tempRows = new ArrayList<RowTempFile>();

			minSortSize = 5000;

			return true;
		}
		return false;
	}

	@Override
	public boolean dispose()
	{
		clearBuffers();
		return super.dispose();
	}

	private void clearBuffers()
	{

		// Clean out the sort buffer
		//
		buffer = new ArrayList<Object[]>(1);
		getBufferIndex = 0;
		rowbuffer = new ArrayList<Object[]>(1);

		// close any open DataInputStream objects
		if ((this.dsis != null) && (this.dsis.size() > 0))
		{
			for (DataSetFile dsf : this.dsis)
			{
				try
				{
					dsf.close();
				}
				catch (ETLException e)
				{
					logger.error("Can't close file {}", dsf.getPath(), e);
				}
			}
		}

		// remove temp files
		for (int i = 0; i < files.size(); i++)
		{
			File fileToDelete = files.get(i);
			try
			{
				if (fileToDelete != null && fileToDelete.exists())
				{
					fileToDelete.delete();
				}
			} catch (Exception e)
			{
				logger.error("Can't close file {}", fileToDelete.getName(), e);
			}
		}
	}

	/**
	 * Sort the entire vector, if it is not empty.
	 */
	public void quickSort(List<Object[]> elements) throws ETLException
	{
		logger.debug("Starting quickSort algorithm...");
		
		if (elements.size() > 0)
		{

			Collections.sort(elements, new Comparator<Object[]>()
			{
				public int compare(Object[] o1, Object[] o2)
				{
					Object[] r1 = (Object[]) o1;
					Object[] r2 = (Object[]) o2;

					try
					{
						return dataDef.compare(r1, r2, fieldnrs);
					} catch (ETLException e)
					{
						logger.error("Error comparing rows: ", e);
						return 0;
					}
				}
			});

			/*
			long nrConversions = 0L;
			for (ValueMetaInterface valueMeta : data.outputRowMeta.getValueMetaList())
			{
				nrConversions += valueMeta.getNumberOfBinaryStringConversions();
				valueMeta.setNumberOfBinaryStringConversions(0L);
			}
			if (log.isDetailed())
				logDetailed("The number of binary string to data type conversions done in this sort block is "
						+ nrConversions);*/
		}
		
		logger.debug("QuickSort algorithm has finished.");
	}

	/**
	 * Calling this method will alert the step that we finished passing records
	 * to the step. Specifically for steps like "Sort Rows" it means that the
	 * buffered rows can be sorted and passed on.
	 */
	public void batchComplete() throws ETLException
	{
		if (files.size() > 0)
		{
			sortExternalRows();
		} else
		{
			quickSort(buffer);
		}
		passBuffer();
	}
	
	public void setSortDef(SortDef sortDef)
	{
		this.sortDef = sortDef;
	}
	
	public static class RowTempFile 
	{
		public Object[] row;
		public int fileNumber;
		
		public RowTempFile(Object[] row, int fileNumber) 
		{
			this.row = row;
			this.fileNumber=fileNumber;
		}
	}	
}