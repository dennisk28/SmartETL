package org.f3tools.incredible.smartETL.utilities;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.f3tools.incredible.smartETL.utilities.XMLUtl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class XMLUtl {

	private static Logger logger = LoggerFactory.getLogger(XMLUtl.class);
	
	public static <T> T JAXBUnmarshal(Class<T> c, String inputFileName)
	{
		try
		{
			FileInputStream fis = new FileInputStream(inputFileName);
			return JAXBUnmarshal(c, fis);
		} catch (FileNotFoundException e)
		{
			logger.error("Can't load file {} ", inputFileName);
			return null;
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T JAXBUnmarshal(Class<T> c, InputStream inputStream)
	{
		try
		{
			JAXBContext context = JAXBContext.newInstance(c);
			Unmarshaller unms = context.createUnmarshaller();
			return (T)unms.unmarshal(inputStream);
		} catch (JAXBException e)
		{
			logger.error("can't unmarshall class {}", c.getName(), e);
			return null;
		}
	}
	
	public static String JAXBMarshal(Object obj)
	{
		try
		{
			JAXBContext context = JAXBContext.newInstance(obj.getClass());
			StringWriter writer = new StringWriter();
			context.createMarshaller().marshal(obj, writer);
			return writer.toString();
		} catch (JAXBException e)
		{
			logger.error("Can't marshall object {}", obj, e);
			return null;
		}
	}
	
	public static final Document loadXMLString(String string)
	{
		DocumentBuilderFactory dbf;
		DocumentBuilder db;
		Document doc;
		
		try
		{			
			// Check and open XML document
			dbf  = DocumentBuilderFactory.newInstance();
			db   = dbf.newDocumentBuilder();
			StringReader stringReader = new java.io.StringReader(string);
			InputSource inputSource = new InputSource(stringReader);
			try
			{
				doc  = db.parse(inputSource);
			}
			catch(IOException ef)
			{
				logger.error("Can't parse XML", ef);
				return null;
			}
			finally
			{
				stringReader.close();
			}
				
			return doc;
		}
		catch(Exception e)
		{
			logger.error("Can't parse XML", e);
			return null;
		}
	}
	
	public static final Document loadXMLFile(String xmlFileName)
	{
		DocumentBuilderFactory dbf;
		DocumentBuilder db;
		Document doc;
		
		try
		{			
			// Check and open XML document
			dbf  = DocumentBuilderFactory.newInstance();
			db   = dbf.newDocumentBuilder();
			FileReader fileReader = new FileReader(xmlFileName);
			InputSource inputSource = new InputSource(fileReader);
			try
			{
				doc  = db.parse(inputSource);
			}
			catch(IOException ef)
			{
				logger.error("Can't parse XML", ef);
				return null;
			}
			finally
			{
				fileReader.close();
			}
				
			return doc;
		}
		catch(Exception e)
		{
			logger.error("Can't parse XML", e);
			return null;
		}
	}
	
	public static final String getTagValue(Node n, String tag)
	{
		NodeList children;
		Node childnode;
		
		if (n==null) return null;
		
		children=n.getChildNodes();
		for (int i=0;i<children.getLength();i++)
		{
			childnode=children.item(i);
			if (childnode.getNodeName().equalsIgnoreCase(tag))
			{
				if (childnode.getFirstChild()!=null) return childnode.getFirstChild().getNodeValue();		
			}
		}
		return null;
	}
	
	public static final String getTagAttribute(Node node, String attribute)
	{
		if (node==null) return null;
		
		String retval = null;
		
		NamedNodeMap nnm = node.getAttributes();
		if (nnm!=null)
		{
			Node attr   = nnm.getNamedItem(attribute);
			if (attr!=null)
			{
				retval = attr.getNodeValue();
			}
		}
		return retval;
	}
	
	/**
	 * Get the value of a tag in a node
	 * @param n The node to look in
	 * @param tag The tag to look for
	 * @return The value of the tag or null if nothing was found.
	 */
	public static final String getTagValueWithAttribute(Node n, String tag,String attribute)
	{
		NodeList children;
		Node childnode;
		
		if (n==null) return null;
		
		children=n.getChildNodes();
		for (int i=0;i<children.getLength();i++)
		{
			childnode=children.item(i);
			if (childnode.getNodeName().equalsIgnoreCase(tag))
			{
				Node node = childnode.getAttributes().getNamedItem(attribute);
				
				if (node != null) return node.getNodeValue();
			}
		}
		return null;
	}
	
	/**
	 * Get nodes with a certain tag one level down
	 * 
	 * @param n The node to look in
	 * @param tag The tags to count
	 * @return The list of nodes found with the specified tag
	 */
	public static final List<Node> getNodes(Node n, String tag)
	{
		NodeList children;
		Node childnode;
		
		List<Node> nodes = new ArrayList<Node>();
		
		if (n==null) return nodes;
		
		children=n.getChildNodes();
		for (int i=0;i<children.getLength();i++)
		{
			childnode=children.item(i);
			if (childnode.getNodeName().equalsIgnoreCase(tag))  // <file>
			{
				nodes.add(childnode);
			}
		}
		return nodes;
	}

	/**
	 * Search for a subnode in the node with a certain tag.
	 * @param n The node to look in
	 * @param tag The tag to look for
	 * @return The subnode if the tag was found, or null if nothing was found.
	 */
	public static final Node getSubNode(Node n, String tag)
	{
		int i;
		NodeList children;
		Node childnode;
		
		if (n==null) return null;
		
		// Get the childres one by one out of the node, 
		// compare the tags and return the first found.
		//
		children=n.getChildNodes();
		for (i=0;i<children.getLength();i++)
		{
			childnode=children.item(i);
			if (childnode.getNodeName().equalsIgnoreCase(tag))
			{
				return childnode;
			}
		}
		return null;
	}
	
	public static final String getNodeValue(Node n)
	{
		if (n == null) return null;
		
		Node child = n.getFirstChild();
		
		if (child == null) 
			return null;
		else
			return n.getFirstChild().getNodeValue();
	}
}
