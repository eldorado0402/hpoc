package com.metatron.sqlstatics;

import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ColumnsInfo extends TablesNamesFinder {
    private List<PlainSelect> selectBody ;
    private List<SubSelect> subSelect ;

    public ColumnsInfo() {
        this.selectBody = new ArrayList<PlainSelect>();
        this.subSelect = new ArrayList<SubSelect>();
    }

    public List<PlainSelect> getAllSelectList(Statement statement){
        this.init(false);
        statement.accept(this);
        return this.selectBody;
    }

    public List<SubSelect> getAllSubSelectList(Statement statement){
        this.init(false);
        statement.accept(this);
        return this.subSelect;
    }

    @Override
    public void visit(SubSelect subSelect) {
        this.subSelect.add(subSelect);

        //System.out.println("alaias : "+subSelect.getAlias());

        if (subSelect.getWithItemsList() != null) {
            Iterator var2 = subSelect.getWithItemsList().iterator();

            while(var2.hasNext()) {
                WithItem withItem = (WithItem)var2.next();
                withItem.accept(this);
            }
        }

        subSelect.getSelectBody().accept(this);
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        Iterator var2;

        //모든 select 구문
        this.selectBody.add(plainSelect);

        if (plainSelect.getSelectItems() != null) {
            var2 = plainSelect.getSelectItems().iterator();

            while(var2.hasNext()) {
                SelectItem item = (SelectItem)var2.next();
                item.accept(this);
            }
        }

        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }

        if (plainSelect.getJoins() != null) {
            var2 = plainSelect.getJoins().iterator();

            while(var2.hasNext()) {
                Join join = (Join)var2.next();
                join.getRightItem().accept(this);
            }
        }

        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }

        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(this);
        }

        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(this);
        }

    }

}
