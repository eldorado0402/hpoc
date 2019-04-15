package com.metatron.sqlstatics;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import com.metatron.sqlstatics.QueryParser.SqlType;


public class Main {

    public static void main(String[] args) {
        //read config
        try {
            //SQLConfiguration sqlConfiguration = new SQLConfiguration();
            //System.out.println(sqlConfiguration.get("engineType"));
            MakeJsonLog sample = new MakeJsonLog();
            //make sample
            //sample.makeSample();

            //get query parsing result -> to result file
            QueryParser queryParser = new QueryParser();
            queryParser.getQueryStatics();

        }catch(Exception e){
            System.out.println(e);
        }

    }


    private static void printResult(ArrayList<JSONObject> results){

        for(JSONObject result : results ){

            System.out.println(result.get("selectedColumns"));

        }

    }
}
