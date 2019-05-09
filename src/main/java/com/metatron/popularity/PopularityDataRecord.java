package com.metatron.popularity;

import java.io.Serializable;

public class PopularityDataRecord implements Serializable {

    String engineType;
    long createdTime;
    String sqlId;
    String sql;
    String table;
    String tableAlias;
    String column;
    String columnAlias;
    String schema;
    int depth;
    String sqlType;



    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    public String getSqlId() {
        return sqlId;
    }

    public void setSqlId(String sqlId) {
        this.sqlId = sqlId;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

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

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }



    @Override
    public String toString() {
        return "PopularityDataRecord{" +
                "engineType='" + engineType + '\'' +
                ", createdTime='" + createdTime + '\'' +
                ", sqlId='" + sqlId + '\'' +
                ", sql='" + sql + '\'' +
                ", sqlType='" + sqlType + '\'' +
                ", schema=" + schema +
                ", table=" + table +
                ", tableAlias='" + tableAlias + '\'' +
                ", column='" + column + '\'' +
                ", columnAlias=" + columnAlias + '\'' +
                ", depth =" + depth +
                '}';
    }


}
