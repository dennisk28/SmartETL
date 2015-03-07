package org.f3tools.incredible.smartETL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.f3tools.incredible.smartETL.xml.XMLDataDef;
import org.f3tools.incredible.smartETL.xml.XMLJob;

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
		XMLJob xmlJob = XMLUtl.JAXBUnmarshal(XMLJob.class, jobDefFile);
		
		if (xmlJob == null)
		{
			logger.error("Can't load job def file {} ", jobDefFile);
			return false;
		}
		
		this.properties = new HashMap<String, String>();
		
		for (XMLJob.Property prp : xmlJob.getProperty())
		{
			properties.put(prp.getName(), prp.getValue());
		}
		
		this.steps = new ArrayList<Node>();
		
		
		for (Object stepO : xmlJob.getStep())
		{
			if (stepO instanceof Node)
			{
				this.steps.add((Node)stepO);
			}
			else
			{
				logger.error("Step config is not a node type {}", stepO);
			}
		}

		lookupDef = (Node)xmlJob.getLookups();
		
		DataDefRegistry defRegistry = DataDefRegistry.getInstance();
		
		for (XMLDataDef xmlDataDef : xmlJob.getDatadef())
		{
			DataDef dataDef = new DataDef(xmlDataDef);
			defRegistry.add(dataDef);
		}

		defRegistry.rewireParents();
		
		this.flows = new ArrayList<Flow>();
		
		for(XMLJob.Flow xmlFlow : xmlJob.getFlow())
		{
			this.flows.add(new Flow(xmlFlow.getFrom(), xmlFlow.getTo()));
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
