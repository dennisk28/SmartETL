package org.f3tools.incredible.smartETL.steps.sort;

import java.util.List;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.f3tools.incredible.smartETL.Const;
import org.f3tools.incredible.smartETL.StepDef;
import org.w3c.dom.Node;

public class SortDef extends StepDef
{
	private static int DEFAULT_SORT_SIZE = 10000;

	private String tempDirectory;
	private int sortsize;
	private String prefix;
	private SortField[] fields;
	private boolean compress;
	private boolean uniquerows;
	private int freememory;

	public int getFreememory()
	{
		return freememory;
	}

	public String getTempDirectory()
	{
		return tempDirectory;
	}

	public int getSortsize()
	{
		return sortsize;
	}

	public String getPrefix()
	{
		return prefix;
	}

	public SortField[] getFields()
	{
		return fields;
	}

	public boolean isCompress()
	{
		return compress;
	}

	public boolean isUniquerows()
	{
		return uniquerows;
	}

	public SortDef(Node defNode) throws ETLException
	{
		super(defNode);
		
		this.tempDirectory = XMLUtl.getTagValue(defNode, "tempdirectory");
		this.prefix = XMLUtl.getTagValue(defNode, "prefix");
		this.sortsize = Const.toInt(XMLUtl.getTagValue(defNode, "sortsize"), DEFAULT_SORT_SIZE);
		this.freememory = Const.toInt(XMLUtl.getTagValue(defNode, "freememory"), -1);
		this.compress = Const.toBoolean(XMLUtl.getTagValue(defNode, "compress"));
		this.uniquerows = Const.toBoolean(XMLUtl.getTagValue(defNode, "uniquerows"));
		
		Node fieldsNode = XMLUtl.getSubNode(defNode, "fields");
		
		if (fieldsNode == null) throw new ETLException("Fields need to be defined for sort step");
		
		List<Node> fieldNodes = XMLUtl.getNodes(fieldsNode, "field");

		if (fieldNodes == null || fieldNodes.size() == 0)
			throw new ETLException("At least one sort field shall be defined");
		
		fields = new SortField[fieldNodes.size()];
		
		for (int i = 0, size = fieldNodes.size(); i < size; i++)
		{
			Node node = fieldNodes.get(i);
			
			SortField field = new SortField(
					XMLUtl.getTagValue(node, "name"),
					Const.toBoolean(XMLUtl.getTagValue(defNode, "ascending")),
					Const.toBoolean(XMLUtl.getTagValue(defNode, "casesensitive")),
					Const.toBoolean(XMLUtl.getTagValue(defNode, "presorted")));

			fields[i] = field;
		}
	}

	public static class SortField
	{
		private String name;
		private boolean ascending;
		private boolean casesensitive;
		private boolean presorted;

		public SortField(String name, boolean ascending, boolean casesensitive, boolean presorted)
		{
			super();
			this.name = name;
			this.ascending = ascending;
			this.casesensitive = casesensitive;
			this.presorted = presorted;
		}

		public String getName()
		{
			return name;
		}

		public boolean isAscending()
		{
			return ascending;
		}

		public boolean isCasesensitive()
		{
			return casesensitive;
		}

		public boolean isPresorted()
		{
			return presorted;
		}
	}
}
