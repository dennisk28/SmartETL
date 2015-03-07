package org.f3tools.incredible.smartETL.steps.join;

import java.util.List;

import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.f3tools.incredible.smartETL.StepDef;
import org.w3c.dom.Node;

public class JoinDef extends StepDef
{
	public static final String[] JOIN_TYPES = {"INNER", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER"}; 
	public static final boolean[] LEFT_OPTIONALS = {false, false, true, true};
	public static final boolean[] RIGHT_OPTIONALS = {false, true, false, true};

    private String[] leftKeys;;
	private String[] rightKeys;	
	private String leftStepName;
	private String rightStepName;
	private String joinType;
	
	public String getJoinType()
	{
		return joinType;
	}

	public void setJoinType(String joinType)
	{
		this.joinType = joinType;
	}

	public String[] getLeftKeys()
	{
		return leftKeys;
	}

	public void setLeftKeys(String[] leftKeys)
	{
		this.leftKeys = leftKeys;
	}

	public String[] getRightKeys()
	{
		return rightKeys;
	}

	public void setRightKeys(String[] rightKeys)
	{
		this.rightKeys = rightKeys;
	}

	public String getLeftStepName()
	{
		return leftStepName;
	}

	public void setLeftStepName(String leftStepName)
	{
		this.leftStepName = leftStepName;
	}

	public String getRightStepName()
	{
		return rightStepName;
	}

	public void setRightStepName(String rightStepName)
	{
		this.rightStepName = rightStepName;
	}


	public JoinDef(Node defNode) throws ETLException
	{
		super(defNode);
		
		this.joinType = XMLUtl.getTagValue(defNode, "jointype");
		
		Node leftNode = XMLUtl.getSubNode(defNode, "leftStep");
		Node rightNode = XMLUtl.getSubNode(defNode, "rightStep");
		
		if (leftNode == null || rightNode == null) 
			throw new ETLException("Both left and right step shall be defined.");
		
		this.leftStepName = XMLUtl.getTagValue(leftNode, "name");
		this.rightStepName = XMLUtl.getTagValue(rightNode, "name");
		
		if (leftStepName == null || rightStepName == null) 
			throw new ETLException("Both left and right step names shall be defined");
		
		Node leftKeysNode = XMLUtl.getSubNode(leftNode, "keys");
		Node rightKeysNode = XMLUtl.getSubNode(rightNode,  "keys");
		
		if (leftKeysNode == null || rightKeysNode == null)
			throw new ETLException("Both left and right step shall have keys defined");
		
		List<Node> leftFields = XMLUtl.getNodes(leftKeysNode, "field");
		List<Node> rightFields = XMLUtl.getNodes(rightKeysNode, "field");
		
		if ((leftFields == null || leftFields.size() == 0)
				|| (rightFields == null || rightFields.size() == 0))
			throw new ETLException("Both left and right keys shall have field defined");
		
		leftKeys = new String[leftFields.size()];
		
		for (int i = 0, size = leftFields.size(); i < size; i++)
		{
			Node node = leftFields.get(i);
			leftKeys[i] = XMLUtl.getNodeValue(node);
		}

		rightKeys = new String[rightFields.size()];
		
		for (int i = 0, size = rightFields.size(); i < size; i++)
		{
			Node node = rightFields.get(i);
			rightKeys[i] = XMLUtl.getNodeValue(node);
		}
	}
}
