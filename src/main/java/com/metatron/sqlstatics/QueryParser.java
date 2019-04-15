package com.metatron.sqlstatics;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;

import org.json.simple.JSONObject;

import java.io.*;
import java.util.*;
import java.util.Iterator;
import net.sf.jsqlparser.statement.merge.*;
import net.sf.jsqlparser.statement.upsert.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.drop.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class QueryParser {

    public enum SqlType {
        //10개
        CREATE_TABLE,
        CREATE_VIEW,
        INSERT,
        SELECT,
        UPDATE,
        UPSERT,
        MERGE,
        DELETE,
        DROP,
        NONE
    }

    public void getQueryStatics(){

        //read log file
        BufferedReader br = null;
        FileWriter writer=null;
        JSONParser jsonParser = new JSONParser();

        //read config
        try {

            //TODO : 추후에는 변경 필요

            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            File inputFile = new File(sqlConfiguration.get("input_filename"));
            File outputFile= new File(sqlConfiguration.get("output_filename"));

            br = new BufferedReader(new FileReader(inputFile));
            String line;

            writer = new FileWriter(outputFile,true);

            while ((line = br.readLine()) != null) {
                    //inputdata
                JSONObject jsonData = (JSONObject)jsonParser.parse(line);

                QueryParser parser = new QueryParser();
                List<String> sourceTables = null;
                Statement statement;
                SqlType type;
                String targetTable;

                String query = jsonData.get("sql").toString();

                // sql 이 없으면 다음 라인 처리
                if(query == null || query.trim().equals(""))
                    continue;

                //sql 이 있으면 처리 시작
                statement = CCJSqlParserUtil.parse(query);
                type = parser.getSqlType(query,statement);
                targetTable = parser.getTargetTable(statement,type);

                //get source table list
                if( parser.getSourcrTables(statement,type) != null){
                    sourceTables = parser.getSourcrTables(statement,type);
                }


                if( sourceTables != null && sourceTables.size() >= 1){

                    for (String source :  sourceTables  ){
                        //output data
                        ParseDataRecord parseData = new ParseDataRecord();

                        //read from log file
                        parseData.setSql(jsonData.get("sql").toString());

                        if(jsonData.get("engineType") != null)
                            parseData.setEngineType(jsonData.get("engineType").toString());

                        if(jsonData.get("createTime") != null)
                            parseData.setCreatedTime((Long)jsonData.get("createTime"));

                        if(jsonData.get("cluster") != null)
                            parseData.setCluster(jsonData.get("cluster").toString());

                        if(jsonData.get("sqlId") != null)
                            parseData.setSqlId(jsonData.get("sqlId").toString());

                        //set sql Type
                        parseData.setSqlType(type.toString());
                        //set target Table
                        parseData.setTargetTable(targetTable);
                        //set source Table
                        parseData.setSourceTable(source);

                        //write
                        writeResultToFile(writer, parseData);

                    }

                }else{

                    //output data
                    ParseDataRecord parseData = new ParseDataRecord();

                    //read from log file
                    parseData.setSql(jsonData.get("sql").toString());

                    if(jsonData.get("engineType") != null)
                        parseData.setEngineType(jsonData.get("engineType").toString());

                    if(jsonData.get("createTime") != null)
                        parseData.setCreatedTime((Long)jsonData.get("createTime"));

                    if(jsonData.get("cluster") != null)
                        parseData.setCluster(jsonData.get("cluster").toString());

                    if(jsonData.get("sqlId") != null)
                        parseData.setSqlId(jsonData.get("sqlId").toString());

                    //set sql Type
                    parseData.setSqlType(type.toString());
                    //set target Table
                    parseData.setTargetTable(targetTable);
                    //set source Table
                    parseData.setSourceTable(null);

                    //System.out.println(parseData.toString());

                    //write
                    writeResultToFile(writer, parseData);

                    }

                }


        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }catch (ParseException e) {
            e.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
            //System.out.println(e);
        }
        finally {
            try {
                if(br != null) {
                    br.close();
                }

            }catch(IOException e){
                e.printStackTrace();
            }

            try {
                if(writer != null) {
                    writer.close();
                }

            }catch(IOException e){
                e.printStackTrace();
            }
        }

    }






    //Get source table
    public List<String> getSourcrTables(Statement statement,SqlType type){

        List<String> list = null;

        //todo : create view 일때 확인해 보기
        if((type == SqlType.CREATE_TABLE)
                || (type == SqlType.CREATE_VIEW)
                || (type == SqlType.INSERT)
                || (type == SqlType.SELECT)
                || (type == SqlType.UPDATE)
                || (type == SqlType.UPSERT)
                || (type == SqlType.MERGE)) {

            //get all tables
            list = new TablesNamesFinder().getTableList(statement);
            //get target table
            String target = getTargetTable(statement, type);

            //전체 테이블에서 타겟 테이블은 제외
            list.remove(target);
        }

        return list;
    }



    //get Target Table
    public String getTargetTable(Statement statement,SqlType type){
        String target = null;

        if( type ==  SqlType.CREATE_TABLE ) {
            CreateTable createTable = (CreateTable) statement;
            //Table name
            target = createTable.getTable().getName();

        }else if( type ==  SqlType.CREATE_VIEW ) {  //TODO : view 는 어떻게 처리 해야 하나...
            CreateView createView = (CreateView) statement;
            //Table name
            target = createView.getView().getName();

        }else if( type ==  SqlType.INSERT ) {
            Insert insert = (Insert) statement;
            //Table name
            target = insert.getTable().getName();

        }else if(type ==SqlType.MERGE){ //mergeUpdate , mergeInsert 2개 있음

            Merge merge = (Merge) statement;
            //Table name
            target = merge.getTable().getName();

        }else if(type ==SqlType.UPSERT){
            Upsert upsert  = (Upsert) statement;
            //Table name
            target = upsert.getTable().getName();
        }else if(type ==SqlType.DELETE){
            Delete delete  = (Delete) statement;
            //Table name
            target = delete.getTable().getName();

        }else if(type ==SqlType.DROP){
            Drop drop  = (Drop) statement;
            //Table name
            if(drop.getName() != null) //drop table 일 경우
                target = drop.getName().toString();

        }
        //TODO : update 처리 필요 !!

        return target;
    }



    //get SQL Type
    public SqlType getSqlType(String query,Statement stmt){

        SqlType sqlType = null;

        if(stmt.getClass().getSimpleName().equals("CreateTable")){
            sqlType = SqlType.CREATE_TABLE;

        }else if(stmt.getClass().getSimpleName().equals("CreateView")){
            sqlType = SqlType.CREATE_VIEW;

        }else if(stmt.getClass().getSimpleName().equals("Select")){
            sqlType = SqlType.SELECT;

        }else if(stmt.getClass().getSimpleName().equals("Insert")){
            sqlType = SqlType.INSERT;

        }else if(stmt.getClass().getSimpleName().equals("Merge")){
            sqlType = SqlType.MERGE;

        }else if(stmt.getClass().getSimpleName().equals("Update")){
            sqlType = SqlType.UPDATE;

        }else if(stmt.getClass().getSimpleName().equals("Upsert")){
            sqlType = SqlType.UPSERT;

        }else if(stmt.getClass().getSimpleName().equals("Delete")){
            sqlType = SqlType.DELETE;

        }else if(stmt.getClass().getSimpleName().equals("Drop")){
            sqlType = SqlType.DROP;

        }else{
            sqlType = SqlType.NONE;

        }


        return sqlType;

    }


    private void writeResultToFile(FileWriter writer,ParseDataRecord parseData){
        StringBuilder sb = new StringBuilder();
        ObjectMapper mapper = new ObjectMapper();
        
        try {
            sb.append(mapper.writer().writeValueAsString(parseData)).append("\n");

            if(sb.length() > 1) {
                writer.write(sb.toString().substring(0, sb.length() - 1));
                writer.write("\n");
                writer.flush();
            }

        }catch (IOException e) {
            e.printStackTrace();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

}

