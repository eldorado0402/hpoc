package com.metatron.popularity;

import java.util.ArrayList;

public class MetadataInfo {
    private String schema;
    private String table;
    private String column;

    //getta
    public String getSchema() {
        return schema;
    }

    //setta
    public void setSchema(String schema) {
        this.schema = schema;
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

    public boolean checkMetadataInfo(ArrayList<MetadataInfo> list,MetadataInfo data) {

        for(MetadataInfo info :  list){
            if(data.schema == info.schema && data.table == info.table && data.column == info.column)
                return true;
        }

        return false;
    }


    public String searchSchemaName(ArrayList<MetadataInfo> list,MetadataInfo data){

    }

    public String searchTableName(ArrayList<MetadataInfo> list,MetadataInfo data){

    }

    public String searchColumnName(ArrayList<MetadataInfo> list,MetadataInfo data){

    }

}
