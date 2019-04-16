package com.metatron.sqlstatics;


//TODO: 필요시 사용
public class DruidLineageRecord {
    String cluster;
    String engineType;
    long createdTime;
    String sqlId;
    String sql;
    String sourceTable;
    String targetTable;
    String sqlType;


    public DruidLineageRecord(String cluster, String engineType, long createdTime, String sqlId, String getsql, String sourceTable, String targetTable, String sqlType) {
    }

    //편의용 생성자
    public DruidLineageRecord(ParseDataRecord parseDataRecord) {
        this(parseDataRecord.getCluster(), parseDataRecord.getEngineType(), parseDataRecord.getCreatedTime(),
                parseDataRecord.getSqlId(), parseDataRecord.getsql(), parseDataRecord.getSourceTable(), parseDataRecord.getTargetTable(),
                parseDataRecord.getSqlType());
    }

    public DruidLineageRecord(long timestamp, String cluster, String currentDatabase, String targetTableType,
                              boolean targetTableTemporary, String sql, String sqlType,
                              String owner, String workflowId, String jobId, String userIpAddress, String sqlHash, String sqlId,
                              String sqlFile) {
        this.createdTime = timestamp;
        this.cluster = cluster;
        this.engineType = currentDatabase;
        this.sqlId = targetTableType;
        this.sql = sql;
        this.sqlType = sqlType;
        this.sqlId = sqlId;
    }


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
