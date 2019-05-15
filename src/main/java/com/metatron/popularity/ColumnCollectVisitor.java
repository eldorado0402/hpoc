package com.metatron.popularity;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.TablesNamesFinder;


public  class ColumnCollectVisitor  extends TablesNamesFinder {

    private List<Column> columns = new ArrayList<Column>();

    public List<Column> getColumns (Expression expression) {
        this.init(false);
        expression.accept(this);
        return this.columns;
    }


    @Override
    public void visit(Function function) {
        ExpressionList params = function.getParameters();
        if(params != null){
            visit(params);
        }
    }

    @Override
    public void visit(ExpressionList el) {
        for (Iterator iter = el.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }
    }

    @Override
    public void visit(Column column) {
        columns.add(column);
    }

    @Override
    public void visit(Parenthesis prnths) {
        prnths.getExpression().accept(this);
    }

}
