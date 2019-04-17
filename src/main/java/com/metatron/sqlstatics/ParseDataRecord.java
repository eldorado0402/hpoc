package com.metatron.sqlstatics;

import java.io.Serializable;

public class ParseDataRecord implements Serializable {

    String cluster;
    String engineType;
    long createdTime;
    String sqlId;
    String sql;
    String sourceTable;
    String targetTable;
    String sqlType;


    //getta
    public String getCluster() {
        return cluster;
    }

    //setta
    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

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

    public String getsql() {
        return sql;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public String toString() {
        return "ParseDataRecord{" +
                "cluster=" + cluster +
                ", engineType='" + engineType + '\'' +
                ", createdTime='" + createdTime + '\'' +
                ", sqlId='" + sqlId + '\'' +
                ", sql='" + sql + '\'' +
                ", sqlType='" + sqlType + '\'' +
                ", sourceTable='" + sourceTable + '\'' +
                ", targetTable=" + targetTable +
                '}';
    }


}
