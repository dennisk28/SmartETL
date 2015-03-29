package org.pentaho.reporting.libraries.formula.statement;

import org.pentaho.reporting.libraries.formula.EvaluationException;
import org.pentaho.reporting.libraries.formula.FormulaContext;
import org.pentaho.reporting.libraries.formula.lvalues.AbstractLValue;
import org.pentaho.reporting.libraries.formula.lvalues.LValue;
import org.pentaho.reporting.libraries.formula.lvalues.TypeValuePair;
import org.pentaho.reporting.libraries.formula.typing.TypeRegistry;
import org.pentaho.reporting.libraries.formula.typing.coretypes.AnyType;

/**
 * Created by 1399903 on 3/7/2015.
 */
public class IfStatement extends AbstractLValue {
    private static final long serialVersionUID = 5203290270062342214L;
    private LValue condition;
    private LValue thenStatement;
    private LValue elseStatement;

    public void setCondition(LValue condition) {
        this.condition = condition;
    }

    public void setThenStatement(LValue thenStatement) {
        this.thenStatement = thenStatement;
    }

    public void setElseStatement(LValue elseStatement) {
        this.elseStatement = elseStatement;
    }

    public void initialize(final FormulaContext context) throws EvaluationException
    {
        this.context = context;
        if(condition!=null) condition.initialize(context);
        if(thenStatement!=null) thenStatement.initialize(context);
        if(elseStatement!=null) elseStatement.initialize(context);
    }
    
    public TypeValuePair evaluate() throws EvaluationException 
    {
        final TypeRegistry typeRegistry = context.getTypeRegistry();
        TypeValuePair result = new TypeValuePair(AnyType.TYPE, null);
        TypeValuePair conditionValue = condition.evaluate();
        boolean ifCondition = typeRegistry.convertToLogical(conditionValue.getType(), conditionValue.getValue());

        if (ifCondition == true)
        {
        	if (thenStatement != null) result = thenStatement.evaluate();
        }
        else
        {
            if (elseStatement != null) result = elseStatement.evaluate();
        }

        return result;
    }

    public boolean isConstant() 
    {
        return false;
    }
}
