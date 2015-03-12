package org.f3tools.incredible.smartETL;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.f3tools.incredible.smartETL.DataDef.Field;
import org.f3tools.incredible.smartETL.utilities.ETLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataSetFile
{
	private Logger logger = LoggerFactory.getLogger(DataSetFile.class);
	
	private static final String ENCODING = "UTF-8";
	private DataDef dataDef;
	private String path;
	private DataInputStream is;
	private DataOutputStream os;
	private boolean compressed;
	
	
	public String getPath()
	{
		return path;
	}

	public DataSetFile(boolean compressed)
	{
		this.compressed = compressed;
	}
	
	public DataDef getDataDef()
	{
		return dataDef;
	}

	public void setDataDef(DataDef dataDef)
	{
		this.dataDef = dataDef;
	}

	public void openForRead(String path) throws ETLException
	{
		this.path = path;
		
		try
		{
			if (compressed)
			{
				is = new DataInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(path))));
			}
			else
				is = new DataInputStream(new BufferedInputStream(new FileInputStream(path), 50000));
		}
		catch (IOException e)
		{
			throw new ETLException("Can't open file for read " + path, e);
		}
	}
	
	public void openForWrite(String path) throws ETLException
	{
		this.path = path;

		try
		{
			if (compressed)
				os = new DataOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(path))));
			else
				os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path), 500000));
		}
		catch (IOException e)
		{
			throw new ETLException("Can't open file for write " + path, e);
		}
	}
	
    public void writeData(Object[] data) throws ETLException
    {
    	if (dataDef == null) 
    	{
    		logger.warn("dataDef is null, can't write anything, output file:{}", path);
    		return;
    	}

    	if (os == null) throw new ETLException("outputstream is null");
    	
    	int size = dataDef.getFieldCount();
    	
        for (int i = 0; i< size; i++) writeData(os, data[i]);
        
        if (size == 0)
        {
    		try 
    		{
				os.writeBoolean(true);
			} catch (IOException e) {
				throw new ETLException("Error writing marker flag", e);
			}
        }
    }
    
    private void writeData(DataOutputStream outputStream, Object data) throws ETLException
    {
    	try
    	{
    		writeBoolean(outputStream, data==null);
    		
    		if (data == null) return;
    		
	    	if (data instanceof String)
	    	{
	    		writeString(outputStream, (String)data);
	    	}
	    	else if (data instanceof Integer)
	    	{
	    		writeInteger(outputStream, (Integer)data);
	    	}
	    	else if (data instanceof Double)
	    	{
	    		writeDouble(outputStream, (Double)data);
	    	}
	    	else if (data instanceof Date)
	    	{
	    		writeDate(outputStream, (Date)data);
	    	}
	    	else if (data instanceof BigDecimal)
	    	{
	    		writeBigDecimal(outputStream, (BigDecimal)data);
	    	}
	    	else if (data instanceof Boolean)
	    	{
	    		writeBoolean(outputStream, (Boolean)data);
	    	}
	    	else if (data instanceof byte[])
	    	{
	    		writeBinary(outputStream, (byte[])data);
	    	}
	    	else
	    		throw new ETLException("Don't support data type: " + data.getClass().getName());
    	}
    	catch (IOException e)
    	{
    		throw new ETLException("Error write data to " + path, e);
    	}
    }
    
	public Object[] readData() throws ETLException
    {
    	if (dataDef == null) 
    	{
    		logger.warn("dataDef is null, can't read anything from file:{}", path);
    		return null;
    	}
    	
		if (is == null) throw new ETLException("inputstream is null, can't read from file " + path);
		
		int size = dataDef.getFieldCount();
		
        Object[] data = new Object[size];
        
        try
        {
	        for (int i = 0; i < size; i++)
	        {
	            data[i] = readData(i, is);
	        }
        } catch (EOFException e)
        {
        	return null;
        }
        
        if (size == 0) 
        {
        	try 
        	{
				is.readBoolean();
			}
        	catch(Exception e)
	        {
	            throw new ETLException("Can't read data from input file " + path, e);
	        }
        }
        
        return data;
    }    

    private Object readData(int index, DataInputStream inputStream) throws ETLException, EOFException
    {
    	if (dataDef == null) return null;
    	
    	Field field = dataDef.getField(index);
    	
    	String type = field.getType();
    	
    	try
    	{
    		boolean isNull = (Boolean)readBoolean(inputStream).booleanValue();
    		
    		if (isNull) return null;
    		
	    	if (type.equalsIgnoreCase("String"))
	    	{
	    		return readString(inputStream);
	    	}
	    	else if (type.equalsIgnoreCase("Integer"))
	    	{
	    		return readInteger(inputStream);
	    	}
	    	else if (type.equalsIgnoreCase("Double"))
	    	{
	    		return readDouble(inputStream);
	    	}
	    	else if (type.equalsIgnoreCase("Date"))
	    	{
	    		return readDate(inputStream);
	    	}
	    	else if (type.equalsIgnoreCase("BigDecimal"))
	    	{
	    		return readBigDecimal(inputStream);
	    	}
	    	else if (type.equalsIgnoreCase("Boolean"))
	    	{
	    		return readBoolean(inputStream);
	    	}
	    	else if (type.equalsIgnoreCase("byte"))
	    	{
	    		return readBinary(inputStream);
	    	}
	    	else
	    		throw new ETLException("Don't support data type: " + type);
    	}
    	catch(EOFException eofe)
    	{
    		throw eofe;
    	}
    	catch (IOException e)
    	{
    		throw new ETLException("Error read data from " + path, e);
    	}
    }
	
    private void writeString(DataOutputStream outputStream, String string) throws IOException
    {
        // Write the length and then the bytes
        if (string==null)
        {
            outputStream.writeInt(-1);
        }
        else
        {
            byte[] chars = string.getBytes(ENCODING);
            outputStream.writeInt(chars.length);
            outputStream.write(chars);
        }
    }

    private String readString(DataInputStream inputStream) throws IOException
    {
        // Read the length and then the bytes
        int length = inputStream.readInt();
        if (length<0) 
        {
            return null;
        }
        
        byte[] chars = new byte[length];
        inputStream.readFully(chars);

        String string = new String(chars, ENCODING);         
        // System.out.println("Read string("+getName()+"), length "+length+": "+string);
        return string;
    }
    
    private void writeDouble(DataOutputStream outputStream, Double number) throws IOException
    {
        outputStream.writeDouble(number.doubleValue());
    }
    
    private Double readDouble(DataInputStream inputStream) throws IOException
    {
        Double d = new Double( inputStream.readDouble() );
        // System.out.println("Read number("+getName()+") ["+d+"]");
        return d;
    }
    
    private void writeInteger(DataOutputStream outputStream, Integer number) throws IOException
    {
        outputStream.writeInt(number.intValue());
    }

    private Integer readInteger(DataInputStream inputStream) throws IOException
    {
        Integer l = new Integer( inputStream.readInt() );
        // System.out.println("Read integer("+getName()+") ["+l+"]");
        return l;
    }
    
    private void writeDate(DataOutputStream outputStream, Date date) throws IOException
    {
        outputStream.writeLong(date.getTime());
    }
    
    private Date readDate(DataInputStream inputStream) throws IOException
    {
        long time = inputStream.readLong();
        // System.out.println("Read Date("+getName()+") ["+new Date(time)+"]");
        return new Date(time);
    }
    
    private void writeBigDecimal(DataOutputStream outputStream, BigDecimal number) throws IOException
    {
        String string = number.toString();
        writeString(outputStream, string);
    }
    
    private BigDecimal readBigDecimal(DataInputStream inputStream) throws IOException
    {
        String string = readString(inputStream);
        // System.out.println("Read big number("+getName()+") ["+string+"]");
        return new BigDecimal(string);
    }
    
    private void writeBoolean(DataOutputStream outputStream, Boolean bool) throws IOException
    {
        outputStream.writeBoolean(bool.booleanValue());
    }

    private Boolean readBoolean(DataInputStream inputStream) throws IOException
    {
        Boolean bool = Boolean.valueOf( inputStream.readBoolean() );
        // System.out.println("Read boolean("+getName()+") ["+bool+"]");
        return bool;
    }
    
    private void writeBinary(DataOutputStream outputStream, byte[] binary) throws IOException
    {
        outputStream.writeInt(binary.length);
        outputStream.write(binary);
    }
    
    private byte[] readBinary(DataInputStream inputStream) throws IOException
    {
        int size = inputStream.readInt();
        byte[] buffer = new byte[size];
        inputStream.readFully(buffer);
        
        // System.out.println("Read binary("+getName()+") with size="+size);

        return buffer;
    }
    
    public void close() throws ETLException
    {
    	try
    	{
    		if (is != null) is.close();
    		
    		if (os != null)
    		{
    			os.flush();
    			os.close();
    		}
    	}
    	catch (IOException e)
    	{
    		throw new ETLException(e);
    	}
    }
}
