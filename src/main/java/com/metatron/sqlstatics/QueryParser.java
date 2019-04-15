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

public class QueryParser {

    public enum SqlType {

        CREATE_TABLE,
        CREATE_TABLE_AS_SELECT,
        CREATE_VIEW,
        CREATE_VIEW_AS_SELECT,
        INSERT_VALUE,
        INSERT_AS_SELECT ,
        UPDATE ,
        SELECT,
        NONE
    }

    //Get source table
    public List<String> getSourcrTables(Statement statement,SqlType type){

        List<String> list;
        String target = getTargetTable(statement,type);
        list = new TablesNamesFinder().getTableList(statement);

        list.remove(target);

        return list;
    }



    //get Target Table
    public String getTargetTable(Statement statement,SqlType type){
        String target = null;

        if( type ==  SqlType.CREATE_TABLE || type ==  SqlType.CREATE_TABLE_AS_SELECT ) {
            CreateTable cts = (CreateTable) statement;
            //Table name
            target = cts.getTable().getName();

        }else if( type ==  SqlType.INSERT_AS_SELECT || type ==  SqlType.INSERT_VALUE ){
            Insert inst = (Insert) statement;
            //Table name
            target = inst.getTable().getName();

        }
//        else if( type ==  SqlType.UPDATE){
//            Update update = (Update) statement;
//            target = update.getTables();
//
//        }

        return target;
    }


    //get Selected columns
    public ArrayList<String> SelectedColumns(Statement statement,SqlType type){

        ArrayList<String> columns = new ArrayList<String>();
        //JSONObject column = new JSONObject();
        Select select = null;
        PlainSelect plainSelect = null;


        if(type == SqlType.CREATE_TABLE_AS_SELECT || type == SqlType.INSERT_AS_SELECT || type ==SqlType.SELECT ){

            if (type == SqlType.CREATE_TABLE_AS_SELECT) {
                CreateTable cts = (CreateTable) statement;
                select = cts.getSelect();

            } else if (type == SqlType.INSERT_AS_SELECT) {
                Insert inst = (Insert) statement;
                select = inst.getSelect();
            }
//        else if( type ==  SqlType.UPDATE){
//            Update update = (Update) statement;
//
//
//        }
//
            else if (type == SqlType.SELECT) {
                select = (Select) statement;
            }

            plainSelect = (PlainSelect) select.getSelectBody();

            //parse select items
            for (SelectItem item : plainSelect.getSelectItems()) {
                JSONObject column = new JSONObject();

                if (item.toString().contains("*")) {
                    //System.out.println("select all items: " + item.toString());
                    column.put("colname", "*");

                } else {

                    Expression expression = ((SelectExpressionItem) item).getExpression();
                    //System.out.println("Expression: "+expression);

                    if (((SelectExpressionItem) item).getAlias() != null) {
                        String alias = ((SelectExpressionItem) item).getAlias().getName();
                        column.put("alias", alias);
                       // System.out.println("alias: " + alias);
                    }

                    //get column 정보
                    if (expression instanceof Column) {
                        Column col = (Column) expression;
                        String colname = col.getColumnName();
                        column.put("colname", colname);

                        //System.out.println("colname: " + colname);

                    } else if (expression instanceof Function) {
                        Function function = (Function) expression;
                        //System.out.println("Funtion : " + function.getAttribute() + "," + function.getName() + "" + function.getParameters());

                    }
                }

                columns.add(column.toJSONString());
            }

            return columns;

        }else{
            return null;
        }

        //return columns;
    }


    //TODO : 컬럼 리니지 만들 방법 정리
    public void getSubSelectLists(Statement statement,SqlType type){

        if(type == SqlType.CREATE_TABLE_AS_SELECT || type == SqlType.INSERT_AS_SELECT || type ==SqlType.SELECT) {

            ColumnsInfo subColLists = new ColumnsInfo();
            List <SubSelect> subLists = subColLists.getAllSubSelectList(statement);
            ArrayList<JSONObject> subSelectInfoList = new ArrayList<JSONObject>();

            //System.out.println(subLists.size());
            //List<PlainSelect>

            for (int i  = subLists.size()-1; i >=0 ; i--) {

                JSONObject subInfo = new JSONObject();
                ArrayList<String> tables = new ArrayList<String>();
                PlainSelect selecBody = (PlainSelect)subLists.get(i).getSelectBody();

                subInfo.put("sql",subLists.get(i));

                if(selecBody.getSelectItems() != null)
                    subInfo.put("collists", selecBody.getSelectItems().toString());

                if(subLists.get(i).getAlias()!= null)
                    subInfo.put("alias", subLists.get(i).getAlias().toString());




                System.out.println(subLists.get(i)+ " , " + subLists.get(i).getSelectBody());

            }
        }

    }


    public void getAllSubSelectList(Statement statement,SqlType type){

        if(type == SqlType.CREATE_TABLE_AS_SELECT || type == SqlType.INSERT_AS_SELECT || type ==SqlType.SELECT) {

            ColumnsInfo selecColtLists = new ColumnsInfo();
            List <PlainSelect> selectLists = selecColtLists.getAllSelectList(statement);

//            System.out.println(selectLists.size());

            for (PlainSelect info : selectLists) {

//                System.out.println(info.toString());
                if(info.getFromItem() != null ) {
//                    System.out.println(info.getFromItem());
//                    System.out.println(info.getFromItem().getAlias());
                }
//                if(info.getJoins() != null )
////                    System.out.println(info.getJoins().toString());
//                if(info.getSelectItems()!= null )
////                    System.out.println(info.getSelectItems().toString());


            }
        }

    }



    //get SQL Type
    public SqlType getSqlType(String query,Statement stmt){
        SqlType type= SqlType.NONE;

        //System.out.println("class Name : " + stmt.getClass().getSimpleName());
        //TODO: 복잡한 구문의 파싱이 되지 않음
        try {
            //stmt 파싱이 안되면 구문상에 문제가 있는 것으로 간주하고 lineage를 그릴 필요 없음
            if(stmt.getClass().getSimpleName().equals("CreateTable")){
                //check create stmt
                CreateTable cts = (CreateTable) stmt;
                if(query.toUpperCase().contains("SELECT")){
                    //check select stmt
                    Select select = cts.getSelect();
                    type =  SqlType.CREATE_TABLE_AS_SELECT;
                }else{
                    type =  SqlType.CREATE_TABLE;
                }

            }else if(stmt.getClass().getSimpleName().equals("CreateView")){
                CreateView ctv = (CreateView) stmt;
                if(query.toUpperCase().contains("SELECT")){
                    //check select stmt
                    Select select = ctv.getSelect();
                    type =  SqlType.CREATE_VIEW_AS_SELECT;
                }else{
                    type =  SqlType.CREATE_VIEW;
                }

            }else if(stmt.getClass().getSimpleName().equals("Insert")){
                Insert inst = (Insert) stmt;
                if(query.toUpperCase().contains("SELECT")){
                    //check select stmt
                    Select select = inst.getSelect();
                    type =  SqlType.INSERT_AS_SELECT;
                }else{
                    type =  SqlType.INSERT_VALUE;
                }

            }else if(stmt.getClass().getSimpleName().equals("Update")){
                Update upt = (Update) stmt;
                type =  SqlType.UPDATE;

            }else if(stmt.getClass().getSimpleName().equals("Select")){
                Select select = (Select) stmt;
                type =  SqlType.SELECT;

            }else{
                type =  SqlType.NONE;
            }

        }catch(Exception e){
            type = SqlType.NONE;
            System.out.println("sql type parser error : " + e);

        }

        //System.out.println("sql type : " +  type.toString() + " , sql : " + query);

        return type;

    }


}

