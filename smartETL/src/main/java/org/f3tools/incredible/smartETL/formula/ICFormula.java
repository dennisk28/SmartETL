package org.f3tools.incredible.smartETL.formula;

import java.io.Serializable;
import java.util.HashMap;

import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.LibFormulaBoot;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;
import org.pentaho.reporting.libraries.formula.lvalues.LValue;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.parser.FormulaParser;
import org.pentaho.reporting.libraries.formula.typing.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes a formula. Currently it uses pentaho formula engine and will be replaced by our own engine. 
 * @author Dennis
 *
 */
public class ICFormula implements Serializable, Cloneable
{

	private static final long serialVersionUID = 1969702659786312731L;
	private final Logger logger = LoggerFactory.getLogger(ICFormula.class);
	private HashMap<String, LValue> formulaCache;
	
	static { LibFormulaBoot.getInstance().start();}
	
	public ICFormula()
	{
		formulaCache = new HashMap<String, LValue>();
	}
	
	public Object evaluate(String formulaText, FormulaContext context) throws FormulaException
	{
		String sFormula = formulaText.trim();

		try
	    {
			LValue rootReference = formulaCache.get(sFormula); 
		
			if (rootReference == null)
			{
				FormulaParser parser = new FormulaParser();
			
				rootReference = parser.parse(sFormula);
				formulaCache.put(sFormula, rootReference);
			}
		
			rootReference.initialize(context);

			final TypeValuePair typeValuePair = rootReference.evaluate();
	      
			if (typeValuePair == null)
			{
				throw new FormulaException("NA error returned, formula=" + sFormula);
			}
	      
			final Type type = typeValuePair.getType();
	      
			if (type.isFlagSet(Type.ERROR_TYPE))
			{
				logger.info("Error: {}", typeValuePair.getValue());
				return null;
			}
			else
			{
				return typeValuePair.getValue();
			}
	    }catch (EvaluationException ee)
		{
	    	if (ee.getErrorValue() == LibFormulaErrorValue.DROP_VALUE)
	    	{
	    		throw new FormulaException(FormulaException.EXCEPTION_CODE_DROP);
	    	}
	    	else
	    	{
	    		throw new FormulaException("Evaluation failed unexpectedly, formula=" + sFormula, ee);
	    	}
		}
		catch (Exception e)
	    {
	      throw new FormulaException("Evaluation failed unexpectedly, formula=" + sFormula, e);
	    }
	}
	
	@SuppressWarnings("unchecked")
	public Object clone() throws CloneNotSupportedException
	{
		final ICFormula o = (ICFormula) super.clone();
		o.formulaCache = (HashMap<String, LValue>)formulaCache.clone();

		return o;
	}
}
