package com.metatron.popularity;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;


import java.util.*;

public class ColumnsFinder extends TablesNamesFinder {

    private static final String NOT_SUPPORTED_YET = "Not supported yet.";
    private Map <String, String> selectItem = new HashMap <String, String>();
    private ArrayList <PlainSelect> selectBodyLists = new ArrayList <PlainSelect>();
    private ArrayList <MetadataInfo> tables; // table name, table alias
    private boolean allowColumnProcessing = false;
    private List <String> otherItemNames;
    final static String defalutSchema = "polaris_dev";

    public ColumnsFinder() {
    }


    public ArrayList <MetadataInfo> getTableInfoList(Statement statement) {
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

            while (var2.hasNext()) {
                SelectItem item = (SelectItem) var2.next();
                item.accept(this);
            }
        }

        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }

        if (plainSelect.getJoins() != null) {
            var2 = plainSelect.getJoins().iterator();

            while (var2.hasNext()) {
                Join join = (Join) var2.next();
                join.getRightItem().accept(this);
            }
        }

//        if (plainSelect.getWhere() != null) {
//            plainSelect.getWhere().accept(this);
//        }
//
//        if (plainSelect.getHaving() != null) {
//            plainSelect.getHaving().accept(this);
//        }

        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(this);
        }

    }

    public ArrayList <PlainSelect> getSelectList(Statement statement) {
        this.init(false);
        statement.accept(this);
        return this.selectBodyLists;
    }

    public ArrayList <MetadataInfo>getTableInfoList(Expression expr) {
        this.init(true);
        expr.accept(this);
        return this.tables;
    }

    @Override
    public void visit(Table tableName) {
        String tableWholeName = this.extractTableName(tableName);
        MetadataInfo tableinfo = new MetadataInfo();

        if (!this.otherItemNames.contains(tableWholeName.toLowerCase()) && ! checkTable(tableName.getSchemaName(),tableName.getName())) {
            //this.tables.add(tableWholeName);

            if (tableName.getAlias() != null)
                tableinfo.setMetadata(tableName.getSchemaName(),tableName.getName(), null, tableName.getAlias().getName());
            else
                tableinfo.setMetadata(tableName.getSchemaName(),tableName.getName(), null, null);

            tables.add(tableinfo);
        }

    }


    protected void init(boolean allowColumnProcessing) {
        this.otherItemNames = new ArrayList();
        this.tables = new ArrayList <MetadataInfo>();
        this.allowColumnProcessing = allowColumnProcessing;
    }


    private boolean checkTable(String schema, String table){
        if(schema == null) {
            schema = this.defalutSchema;
        }

        for(MetadataInfo info :  tables){
            if(schema == info.getSchema() && table == info.getTable() )
                return true;
        }

        return false;
    }

}

