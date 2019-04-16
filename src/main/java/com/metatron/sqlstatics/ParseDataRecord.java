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
    public String getEngineType() {
        return engineType;
    }
    public long getCreatedTime() {
        return createdTime;
    }
    public String getSqlId() {
        return sqlId;
    }
    public String getsql() {
        return sql;
    }
    public String getSourceTable() {
        return sourceTable;
    }
    public String getTargetTable() {
        return targetTable;
    }
    public String getSqlType() {
        return sqlType;
    }

    //setta
    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }
    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }
    public void setSqlId(String sqlId) {
        this.sqlId = sqlId;
    }
    public void setSql(String sql) {
        this.sql = sql;
    }
    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }
    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }
    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
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
