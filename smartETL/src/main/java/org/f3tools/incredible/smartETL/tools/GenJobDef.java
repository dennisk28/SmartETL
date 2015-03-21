package org.f3tools.incredible.smartETL.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.f3tools.incredible.smartETL.utilities.Utl;
import org.apache.commons.lang.StringEscapeUtils;

/**
 * This tool converts a Excel job definition (mainly mapping definitions) into xml which can be used by JobRunner 
 * @author Desheng Kang
 * @since 2015/03/21
 */
public class GenJobDef
{
	public static void main(String[] argc)
	{
		if (argc.length < 3)
		{
			System.out.println("Usage: GenJobDef excelJobFile xmlJobTemplateFile outputxmlfile");
			System.exit(-1);
		}
		
		GenJobDef gen = new GenJobDef();
		gen.gen(argc[0], argc[1], argc[2]);
	}
	
	public GenJobDef()
	{
		
	}
	
	private String copyStr(String srcStr, int copies)
	{
		if (copies <= 0) return null;
		
		StringBuffer sb = new StringBuffer(); 
		
		for (int i = 0; i < copies; i++)
		{
			sb.append(srcStr);
		}
		
		return sb.toString();
	}
	
	/**
	 * If a cell is null, return empty string. value is trimmed
	 * @param cell
	 * @return
	 */
	private String cellStrNullEmptyTrim(HSSFRow row, int colIndex)
	{
		if (row == null) return "";
		
		HSSFCell cell = row.getCell(colIndex);
		
		if (cell == null) 
			return "";
		else
			return cell.getStringCellValue().trim();
	}
	
	public void gen(String excelFile, String xmlTemplateFile, String outputXmlFile)
	{
		try
		{
			HSSFWorkbook hssfWorkbook = new HSSFWorkbook(new FileInputStream(excelFile));
			
			// TODO put the following in config file
			String mappingSheetName = "mappings";
			String variableSheetName = "mapping-variables";
			int cat1StartRow = 0;
			int mappingStartRow = 2;
			int fieldColumn = 0;
			int intentSpaces = 4;
			int startIntentSpaces = 3 * intentSpaces;
			int cat1DefColumnCount = 5;
			int cat2FlagColOffset = 0;
			int formulaColOffset = 3;
			int refDefaultColOffset = 2;
			String mappingsMark = "##mappings##";
			
			int curRow = mappingStartRow;
			
			HSSFSheet mappingSheet = hssfWorkbook.getSheet("mappings");
			Utl.check(mappingSheet == null, "can't find sheet " + mappingSheetName);
			
			StringBuffer sb = new StringBuffer();
			sb.append(copyStr(" ", startIntentSpaces));
			sb.append("<mappings>\n");
			
			HSSFRow row = mappingSheet.getRow(curRow);
			HSSFRow cat1Row = mappingSheet.getRow(cat1StartRow);
			String fieldName = cellStrNullEmptyTrim(row, fieldColumn);
			
			while (!fieldName.trim().equals(""))
			{
				int curCat1FlagColumn = 1;
				int curCat1DefIdx = 0;
				String cat1Flag = cellStrNullEmptyTrim(cat1Row, curCat1FlagColumn);
				
				sb.append(copyStr(" ", startIntentSpaces + intentSpaces));
				sb.append("<mapping>\n");
				sb.append(copyStr(" ", startIntentSpaces + intentSpaces * 2));
				sb.append("<field>");
				sb.append(fieldName);
				sb.append("</field>\n");
				
				while (!cat1Flag.trim().equals(""))
				{
					String cat2tag = cellStrNullEmptyTrim(row, curCat1FlagColumn + cat2FlagColOffset);
					String referDefault = cellStrNullEmptyTrim(row, curCat1FlagColumn + refDefaultColOffset);
					String formula = cellStrNullEmptyTrim(row, curCat1FlagColumn + formulaColOffset);
					
					if (!formula.equalsIgnoreCase("") || referDefault.equalsIgnoreCase("Y"))
					{
						sb.append(copyStr(" ", startIntentSpaces + intentSpaces * 2));
						
						sb.append("<formula");
						
						if (!cat2tag.equalsIgnoreCase(""))
							sb.append(" tag=\"" + cat1Flag + ":" + cat2tag + "\"");
						else
							sb.append(" tag=\"" + cat1Flag + "\"");

						if (referDefault.equalsIgnoreCase("Y"))
						{
							sb.append(" ref=\"DEFAULT\"/>");
						}
						else
						{
							sb.append(">");
							sb.append(StringEscapeUtils.escapeXml(formula));
							sb.append("</formula>");
						}
						
						sb.append("\n");
					}
					
					curCat1DefIdx++;
					curCat1FlagColumn = 1 + curCat1DefIdx * cat1DefColumnCount;
					cat1Flag = cellStrNullEmptyTrim(cat1Row, curCat1FlagColumn);
				}
				
				sb.append(copyStr(" ", startIntentSpaces + intentSpaces));
				sb.append("</mapping>\n");
				
				curRow++;
				row = mappingSheet.getRow(curRow);
				fieldName = cellStrNullEmptyTrim(row, fieldColumn);
			}
			
			sb.append(copyStr(" ", startIntentSpaces));
			sb.append("</mappings>");

			BufferedReader br = new BufferedReader(new FileReader(xmlTemplateFile));
			FileWriter fw = new FileWriter(outputXmlFile);
			
			String line = br.readLine();
			boolean mappingReplaced = false;
			
			while (line != null)
			{
				if (!mappingReplaced)
				{
					if (line.indexOf(mappingsMark) >= 0)
					{
						line = line.replaceFirst(mappingsMark, sb.toString());
						mappingReplaced = true;
					}
				}
				
				fw.write(line);
				fw.write("\n");
				
				line = br.readLine();
			}
			
			fw.close();
			br.close();
			hssfWorkbook.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
