package com.metatron.sqlstatics;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Iterator;
import net.sf.jsqlparser.statement.merge.*;
import net.sf.jsqlparser.statement.upsert.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.drop.*;

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


//    public SqlType getSqlType(String query,Statement stmt){
//        SqlType type= SqlType.NONE;
//
//        //System.out.println("class Name : " + stmt.getClass().getSimpleName());
//        //TODO: 복잡한 구문의 파싱이 되지 않음
//        try {
//            //stmt 파싱이 안되면 구문상에 문제가 있는 것으로 간주하고 lineage를 그릴 필요 없음
//            if(stmt.getClass().getSimpleName().equals("CreateTable")){
//                //check create stmt
//                CreateTable cts = (CreateTable) stmt;
//                if(query.toUpperCase().contains("SELECT")){
//                    //check select stmt
//                    Select select = cts.getSelect();
//                    type =  SqlType.CREATE_TABLE_AS_SELECT;
//                }else{
//                    type =  SqlType.CREATE_TABLE;
//                }
//
//            }else if(stmt.getClass().getSimpleName().equals("CreateView")){
//                CreateView ctv = (CreateView) stmt;
//                if(query.toUpperCase().contains("SELECT")){
//                    //check select stmt
//                    Select select = ctv.getSelect();
//                    type =  SqlType.CREATE_VIEW_AS_SELECT;
//                }else{
//                    type =  SqlType.CREATE_VIEW;
//                }
//
//            }else if(stmt.getClass().getSimpleName().equals("Insert")){
//                Insert inst = (Insert) stmt;
//                if(query.toUpperCase().contains("SELECT")){
//                    //check select stmt
//                    Select select = inst.getSelect();
//                    type =  SqlType.INSERT_AS_SELECT;
//                }else{
//                    type =  SqlType.INSERT_VALUE;
//                }
//
//            }else if(stmt.getClass().getSimpleName().equals("Update")){
//                Update upt = (Update) stmt;
//                type =  SqlType.UPDATE;
//
//            }else if(stmt.getClass().getSimpleName().equals("Select")){
//                Select select = (Select) stmt;
//                type =  SqlType.SELECT;
//
//            }else{
//                type =  SqlType.NONE;
//            }
//
//        }catch(Exception e){
//            type = SqlType.NONE;
//            System.out.println("sql type parser error : " + e);
//
//        }
//
//        //System.out.println("sql type : " +  type.toString() + " , sql : " + query);
//
//        return type;
//
//    }


}

