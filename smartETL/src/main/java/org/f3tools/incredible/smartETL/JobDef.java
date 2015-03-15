package org.f3tools.incredible.smartETL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;

public class JobDef 
{
	private Logger logger = LoggerFactory.getLogger(JobDef.class);
	
	private List<Node> steps;
	private Node lookupDef;
	
	public List<Node> getSteps() {
		return steps;
	}
	
	public Node getLookupDef()
	{
		return lookupDef;
	}

	private List<Flow> flows;
	private HashMap<String, String> properties;
	
	/**
	 * Return all steps in flow definition
	 * @return
	 */
	public Set<String> getFlowSteps()
	{
		HashSet<String> flowSteps = new HashSet<String>();
		
		for (Flow flow : this.flows)
		{
			flowSteps.add(flow.from);
			flowSteps.add(flow.to);
		}
		
		return flowSteps;
	}
	
	public List<Flow> getFlows() {
		return flows;
	}

	public boolean loadXML(String jobDefFile)
	{
		Document xmlDoc = XMLUtl.loadXMLFile(jobDefFile);
		
		if (xmlDoc == null)
		{
			logger.error("Can't load job def file {} ", jobDefFile);
			return false;
		}
		
		Node root = xmlDoc.getDocumentElement();
		
		this.properties = new HashMap<String, String>();
		
		Node propertiesNode = XMLUtl.getSubNode(root, "properties");
		
		if (propertiesNode != null)
		{
			List<Node> propertyNodeList = XMLUtl.getNodes(propertiesNode, "property");
		
			if (propertyNodeList != null)
			{
				for (Node node : propertyNodeList)
				{
					properties.put(XMLUtl.getTagAttribute(node, "name"),
							XMLUtl.getTagAttribute(node, "value"));
				}
			}
		}

		Node stepsNode = XMLUtl.getSubNode(root, "steps");
		if (stepsNode == null)
		{
			logger.error("steps node is missing in job definition file");
			return false;
		}
		
		this.steps = XMLUtl.getNodes(stepsNode, "step");
		if (steps == null)
		{
			logger.error("at least one step shall be defined");
			return false;
		}

		lookupDef = XMLUtl.getSubNode(root, "lookups");
		if (lookupDef == null)
		{
			logger.error("lookups node is missing in job definition file");
			return false;
		}
		
		Node datadefsNode = XMLUtl.getSubNode(root, "datadefs");
		if (datadefsNode == null)
		{
			logger.error("datadefs node is missing in job definition file");
			return false;
		}
		
		List<Node> datadefNodeList = XMLUtl.getNodes(datadefsNode, "datadef");

		if (datadefNodeList != null)
		{
			DataDefRegistry defRegistry = DataDefRegistry.getInstance();

			for (Node dataDefNode : datadefNodeList)
			{
				DataDef dataDef = new DataDef(dataDefNode);
				defRegistry.add(dataDef);
			}

			defRegistry.rewireParents();
		}

		this.flows = new ArrayList<Flow>();
		
		Node flowsNode = XMLUtl.getSubNode(root, "flows");
		
		if (flowsNode == null)
		{
			logger.error("flows node is missing in job definition file");
			return false;
		}
		
		List<Node> flowNodeList = XMLUtl.getNodes(flowsNode, "flow");
		
		if (flowNodeList == null)
		{
			logger.error("At least one flow shall be defined");
			return false;
		}
		
		for(Node flowNode : flowNodeList)
		{
			this.flows.add(new Flow(
					XMLUtl.getTagValue(flowNode, "from"),
					XMLUtl.getTagValue(flowNode, "to")));
		}
		
		return true;
	}
	
	
	public List<String> getNextStepNames(String stepName)
	{
		ArrayList<String> retList = new ArrayList<String>();
		
		for (Flow flow : flows)
		{
			if (flow.getFrom().equalsIgnoreCase(stepName))
			{
				retList.add(flow.getTo());
			}
		}
		
		return retList;
	}
		
	public int getPropertyInt(String property, int defaultValue)
	{
		String value = this.properties.get(property);
		
		if (value == null)
			return defaultValue;
		else
			return Const.toInt(value, defaultValue);
	}
	
    public static class Flow 
    {
        private String from;
        private String to;

        public Flow(String from, String to) {
			super();
			this.from = from;
			this.to = to;
		}

		public String getFrom() {
            return from;
        }

        public void setFrom(String value) {
            this.from = value;
        }

        public String getTo() {
            return to;
        }

        public void setTo(String value) {
            this.to = value;
        }
    }	
}
