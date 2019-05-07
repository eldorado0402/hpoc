package com.metatron.popularity;

import java.util.ArrayList;

public class MetadataInfo {
    private String schema;
    private String table;
    private String column;
    private String tableAlias;
    final static String defalutSchema = "polaris_dev";


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
    public String getTableAlias() {
        return tableAlias;
    }

    //setta
    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    //getta
    public String getColumn() {
        return column;
    }

    //setta
    public void setColumn(String column) {
        this.column = column;
    }

    public void setMetadata(String schema,String table, String column, String tableAlias){
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.tableAlias = tableAlias;
    }

    public boolean checkMetadataInfo(ArrayList<MetadataInfo> list,MetadataInfo data, String defaultSchema) {
        if(data.schema == null) {
            data.schema = defaultSchema;
        }

        for(MetadataInfo info :  list){
            if(data.schema.equals(info.schema) && data.table.equals(info.table) && data.column.equals(info.column))
                return true;
        }

        return false;
    }


    public MetadataInfo checkTable(ArrayList<MetadataInfo> list,String schema, String table){
        if(schema == null) {
            schema = this.defalutSchema;
        }

        for(MetadataInfo info :  list){
            if(schema.equals(info.schema) && table.equals(info.table) )
                return info;
        }

        return null;
    }


    public MetadataInfo checkTable(ArrayList<MetadataInfo> list, String table){

        for(MetadataInfo info :  list){
            if(table.equals(info.table))
                return info;
        }

        return null;
    }

    public MetadataInfo checkTable(ArrayList<MetadataInfo> list,String schema, ArrayList<String> tables){

        for(String table : tables) {
            for (MetadataInfo info : list) {
                if (table.equals(info.table))
                    return info;
            }
        }

        return null;
    }

    public MetadataInfo checkTableAlias(ArrayList<MetadataInfo> list, String tableAlias){

        for(MetadataInfo info :  list){
            if(tableAlias.equals(info.tableAlias))
                return info;
        }

        return null;
    }



    public String searchSchemaName(ArrayList<MetadataInfo> list,String table){

        String schema=null;

        for(MetadataInfo info :  list){
            if(table.equals(info.table))
                return schema;
        }

        return schema;

    }


    public String searchTableName(ArrayList<MetadataInfo> list,MetadataInfo data){

        String table=null;

        return table;
    }

    public String searchColumnName(ArrayList<MetadataInfo> list,MetadataInfo data){

        String column=null;

        return column;
    }

}
