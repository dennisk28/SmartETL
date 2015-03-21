package org.f3tools.incredible.smartETL.tools;

import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.f3tools.incredible.smartETL.utilities.Utl;
import org.pentaho.reporting.libraries.formula.parser.FormulaParser;
import org.pentaho.reporting.libraries.formula.parser.ParseException;

/**
 * This tool converts a Excel job definition (mainly mapping definitions) into xml which can be used by JobRunner 
 * @author Desheng Kang
 * @since 2015/03/21
 */
public class GrammarCheck
{
	public static void main(String[] argc)
	{
		if (argc.length < 2)
		{
			System.out.println("Usage: GrammarCheck excelJobFile outputFile");
			System.exit(-1);
		}
		
		GrammarCheck checker = new GrammarCheck();
		checker.check(argc[0], argc[1]);
	}
	
	public GrammarCheck()
	{
		
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
	
	public void check(String excelFile, String outputFile)
	{
		try
		{

			HSSFWorkbook hssfWorkbook = new HSSFWorkbook(new FileInputStream(excelFile));
			HSSFWorkbook outputWorkbook = new HSSFWorkbook();
			
			// TODO put the following in config file
			String mappingSheetName = "mappings";
			String variableSheetName = "mapping-variables";
			int cat1StartRow = 0;
			int mappingStartRow = 2;
			int fieldColumn = 0;
			int cat1DefColumnCount = 5;
			int formulaColOffset = 3;
			
			int curRow = mappingStartRow;
			int totalError = 0;
			
			HSSFSheet mappingSheet = hssfWorkbook.getSheet("mappings");
			Utl.check(mappingSheet == null, "can't find sheet " + mappingSheetName);
			
			HSSFRow row = mappingSheet.getRow(curRow);
			HSSFRow cat1Row = mappingSheet.getRow(cat1StartRow);
			String fieldName = cellStrNullEmptyTrim(row, fieldColumn);
			
			HSSFSheet grammarErrorSheet = outputWorkbook.createSheet("grammarError");
			
			while (!fieldName.trim().equals(""))
			{
				int curCat1FlagColumn = 1;
				int curCat1DefIdx = 0;
				String cat1Flag = cellStrNullEmptyTrim(cat1Row, curCat1FlagColumn);
				
				while (!cat1Flag.trim().equals(""))
				{
					int curColumn = curCat1FlagColumn + formulaColOffset;
					String formula = cellStrNullEmptyTrim(row, curColumn);
					
					if (!formula.equalsIgnoreCase(""))
					{
						try
						{
							FormulaParser parser = new FormulaParser();
							parser.parse(formula);
						}
						catch (ParseException e)
						{
							addGrammarError(grammarErrorSheet, curRow, curColumn, e, formula);
							totalError++;
						}
					}
					
					curCat1DefIdx++;
					curCat1FlagColumn = 1 + curCat1DefIdx * cat1DefColumnCount;
					cat1Flag = cellStrNullEmptyTrim(cat1Row, curCat1FlagColumn);
				}
				
				curRow++;
				row = mappingSheet.getRow(curRow);
				fieldName = cellStrNullEmptyTrim(row, fieldColumn);
			}
			
			hssfWorkbook.close();
			outputWorkbook.write(new FileOutputStream(outputFile));
			outputWorkbook.close();
			
			System.out.println("Total Error: " + totalError + ", created output file " + outputFile);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	
	private void addGrammarError(HSSFSheet logSheet, int row, int col, ParseException e, String formula)
	{
		int lastRow = logSheet.getLastRowNum();
		
		if (lastRow == 0)
		{
			HSSFRow logRow = logSheet.createRow(lastRow);

			HSSFCell cell = logRow.createCell(0);
			cell.setCellValue("Row");
			cell = logRow.createCell(1);
			cell.setCellValue("Col");
			cell = logRow.createCell(2);
			cell.setCellValue("Error");	
			cell = logRow.createCell(3);
			cell.setCellValue("Formula");	

			cell = logRow.createCell(4);
			cell.setCellValue("ErrBeginLine");	
			cell = logRow.createCell(5);
			cell.setCellValue("ErrEndLine");	
			cell = logRow.createCell(6);
			cell.setCellValue("ErrBeginRow");	
			cell = logRow.createCell(7);
			cell.setCellValue("ErrEndRow");	
			cell = logRow.createCell(8);
			cell.setCellValue("ErrToken");	

			lastRow++;
		}
		
		HSSFRow logRow = logSheet.createRow(lastRow);

		HSSFCell cell = logRow.createCell(0);
		cell.setCellValue(row);
		cell = logRow.createCell(1);
		cell.setCellValue(col);
		cell = logRow.createCell(2);
		cell.setCellValue(e.getMessage());	
		cell = logRow.createCell(3);
		cell.setCellValue(formula);
		
		cell = logRow.createCell(4);
		cell.setCellValue(e.currentToken.next.beginColumn);	
		cell = logRow.createCell(5);
		cell.setCellValue(e.currentToken.next.endColumn);	
		cell = logRow.createCell(6);
		cell.setCellValue(e.currentToken.next.beginLine);	
		cell = logRow.createCell(7);
		cell.setCellValue(e.currentToken.next.endLine);	
		cell = logRow.createCell(8);
		cell.setCellValue(e.currentToken.next.image);	
	}
}
