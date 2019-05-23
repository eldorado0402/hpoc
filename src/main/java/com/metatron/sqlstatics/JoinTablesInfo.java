package com.metatron.sqlstatics;

import java.util.HashMap;
import java.util.ArrayList;

public class JoinTablesInfo {
    String targetTable =null;
    ArrayList<HashMap<String,QueryParser.JoinType>> sourceTableInfos = new ArrayList<HashMap<String,QueryParser.JoinType>> (); //table name , jointype


    //getta
    public String getTargetTable() {
        return targetTable;
    }

    //setta
    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    //getta
    public ArrayList<HashMap<String,QueryParser.JoinType>>  getSourceTableInfos() {
        return sourceTableInfos;
    }

    //setta
    public void setSourceTableInfos(ArrayList<HashMap<String,QueryParser.JoinType>> sourceTableInfos) {
        this.sourceTableInfos = sourceTableInfos;
    }

    public void addSourceTableInfo(HashMap<String,QueryParser.JoinType> sourceTableInfo){
        this.sourceTableInfos.add(sourceTableInfo);
    }

}
