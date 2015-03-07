package org.f3tools.incredible.smartETL.utilities;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This utility class enables quicker search of list index. 
 * @author Dennis
 *
 * @param <E>
 */

public class IndexedList<K, E>  
{
	private ArrayList<E> list;
	private HashMap<K, Integer> keyMap;
	
	public IndexedList()
	{
		this.list = new ArrayList<E>();
		this.keyMap = new HashMap<K, Integer>();
	}
	
	public IndexedList(int initialCapacity)
	{
		this.list = new ArrayList<E>(initialCapacity);
		this.keyMap = new HashMap<K, Integer>(initialCapacity);
	}
	
	/**
	 * If e exists, no value is added
	 * @param e
	 * @return
	 */
	public boolean add(K k, E e) 
	{
		if (keyMap.get(e) != null) return false;
		
		boolean ret = list.add(e);
		
		if (ret)
		{
			keyMap.put(k, new Integer(list.size() - 1));
			return true;
		}
		else
			return ret;
	}
	
	public E get(int index)
	{
		return list.get(index);
	}

	public E get(K k)
	{
		int index = indexOf(k);
		
		if (index == -1)
			return null;
		else
			return list.get(index);
	}
	
	public int indexOf(K k)
	{
		Integer index = keyMap.get(k);
		
		if (index == null) 
			return -1;
		else
			return index.intValue();
	}
	
	public int size()
	{
		return this.list.size();
	}

	public E remove(K k)
	{
		Integer index = this.keyMap.remove(k);
		
		if (index == null) 
			return null;
		else
		{
			// reset index in the map, @TODO this is too expensive Dennis 2015/03/02
			for (K tmpK : keyMap.keySet())
			{
				Integer tIdx = keyMap.get(tmpK);
				
				if (tIdx.intValue() > index.intValue())
				{
					keyMap.put(tmpK, new Integer(tIdx.intValue() - 1));
				}
			}
			
			return this.list.remove(index.intValue());
		}
	}
	
	public E remove(int index)
	{
		if (index >= this.list.size()) return null;
		
		E e = this.list.remove(index);
		
		for (K k : keyMap.keySet())
		{
			if (keyMap.get(k).intValue() == index)
			{
				keyMap.remove(k);
				break;
			}
		}

		return e;
	}
}
