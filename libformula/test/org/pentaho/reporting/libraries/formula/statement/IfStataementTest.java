/*
* This program is free software; you can redistribute it and/or modify it under the
* terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
* Foundation.
*
* You should have received a copy of the GNU Lesser General Public License along with this
* program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
* or from the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
* without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU Lesser General Public License for more details.
*
* Copyright (c) 2006 - 2013 Pentaho Corporation and Contributors.  All rights reserved.
*/

package org.pentaho.reporting.libraries.formula.statement;

import java.math.BigDecimal;

import org.pentaho.reporting.libraries.formula.FormulaTestBase;
import org.pentaho.reporting.libraries.formula.LibFormulaErrorValue;

/**
 * @author Cedric Pronzato
 */
public class IfStataementTest extends FormulaTestBase {
    public void testDefault() throws Exception {
        runDefaultTest();
    }

    public Object[][] createDataTest() {
        return new Object[][]
                {
                        {" IF 0 THEN 1 END IF", null},
                        {" IF 0 THEN MAX(2;4;1;-8) ELSE 1 END IF", new BigDecimal(4)},
//                        {" IF 0 THEN MAX(2;4;1;-8) ELSEIF 1 THEN 20  ELSEIF 2 then 12 ELSE 1  END IF", new BigDecimal(1)},
                        {" IF ABS(1)-1 THEN MAX(2;4;1;ABS(-8)) ELSE 11  END IF", new BigDecimal(11)},

                        {"IF false() then 7 else 8 end if", new BigDecimal(8)},
                        {"IF TRUE() then 7 else 8 end if", new BigDecimal(7)},
                        {"IF TRUE() then \"HI\" else 8 end if", "HI"},
                        {"IF 1 then 7 else 8 end if ", new BigDecimal(7)},

                        {"IF 0 then 7 else 8 end if ", new BigDecimal(8)},
                        {"IF TRUE() then [.B4] else 8 end if", new BigDecimal(2)},
                        {"IF TRUE() then [.B4]+5 else 8 end if", new BigDecimal(7)},
                        {"IF \"x\" then 7 else 8 end if", LibFormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE},
                        {"IF \"1\" then 7 else 8  end if", LibFormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE},
                        {"IF \"\"  then 7 else 8  end if", LibFormulaErrorValue.ERROR_INVALID_ARGUMENT_VALUE},
                        {"IF FALSE() then 7  end if", null},
//
//
//////                        //TODO { "IF(FALSE();7;)", new BigDecimal(0) }, we will not allow this syntax
                        {"IF TRUE() then 4 else 1/0 end if", new BigDecimal(4)},
                        {"IF FALSE() then 1/0 else 5 end if", new BigDecimal(5)},

                };
    }
}
