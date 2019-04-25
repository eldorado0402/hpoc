package com.metatron.sqlstatics;


import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;

import net.sf.jsqlparser.statement.select.*;

import net.sf.jsqlparser.util.*;

import java.util.*;

public class QueryInfo extends TablesNamesFinder{

    private Map<String,String> selectItem = new HashMap<String,String>();
    private ArrayList<PlainSelect> selectBodyLists = new ArrayList<PlainSelect>();

    private static final String NOT_SUPPORTED_YET = "Not supported yet.";
    private HashMap<String,String> tables; // table name, table alias
    private boolean allowColumnProcessing = false;
    private List<String> otherItemNames;

    public QueryInfo() {
    }


    public HashMap<String,String> getTableInfoList(Statement statement) {
        this.init(false);
        statement.accept(this);
        return this.tables;
    }


    @Override
    public void visit(PlainSelect plainSelect) {

        //all select body
        this.selectBodyLists.add(plainSelect);

        Iterator var2;
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

    public ArrayList<PlainSelect> getSelectList(Statement statement){
        this.init(false);
        statement.accept(this);
        return this.selectBodyLists;
    }

    public HashMap<String,String> getTableInfoList(Expression expr) {
        this.init(true);
        expr.accept(this);
        return this.tables;
    }

    @Override
    public void visit(Table tableName) {
        String tableWholeName = this.extractTableName(tableName);
        if (!this.otherItemNames.contains(tableWholeName.toLowerCase()) && !this.tables.containsKey(tableWholeName)) {
            //this.tables.add(tableWholeName);

            if(tableName.getAlias() != null )
                this.tables.put(tableWholeName,tableName.getAlias().getName());
            else
                this.tables.put(tableWholeName,null);
        }

    }


    protected void init(boolean allowColumnProcessing) {
        this.otherItemNames = new ArrayList();
        this.tables = new HashMap <String, String>();
        this.allowColumnProcessing = allowColumnProcessing;
    }






}

