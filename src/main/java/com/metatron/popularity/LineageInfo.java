package com.metatron.popularity;

public class LineageInfo {
    String table;
    String tableAlias;
    String column;
    String columnAlias;
    String schema;
    int depth;
    String selectAlias;
    String expression;


    //getta
    public String getTable() {
        return table;
    }

    //setta
    public void setTable(String table) {
        this.table = table;
    }

    //getta
    public String getColumn() {
        return column;
    }

    //setta
    public void setColumn(String column) {
        this.column = column;
    }


    //getta
    public String getTableAlias() {
        return tableAlias;
    }

    //setta
    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    //getta
    public String getColumnAlias() {
        return columnAlias;
    }

    //setta
    public void setColumnAlias(String columnAlias) {
        this.columnAlias = columnAlias;
    }

    //getta
    public String getSchema() {
        return schema;
    }

    //setta
    public void setSchema(String schema) {
        this.schema = schema;
    }

    //getta
    public int getDepth() {
        return depth;
    }

    //setta
    public void setDepth(int depth) {
        this.depth = depth;
    }

    //getta
    public String getSelectAlias() {
        return selectAlias;
    }

    //setta
    public void setSelectAlias(String selectAlias) {
        this.selectAlias = selectAlias;
    }

    //getta
    public String getExpression() {
        return expression;
    }

    //setta
    public void setExpression(String expression) {
        this.expression = expression;
    }


    public String toString() {
        return "LineageInfo{" +
                "schema=" + schema +
                ", table=" + table +
                ", tableAlias='" + tableAlias + '\'' +
                ", column='" + column + '\'' +
                ", columnAlias=" + columnAlias + '\'' +
                ", expression='" + expression + '\'' +
                ", selectAlias=" + selectAlias + '\'' +
                ", depth =" + depth +
                '}';
    }


}
