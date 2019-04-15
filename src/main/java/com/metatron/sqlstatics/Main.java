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
            //read sample
            ArrayList<JSONObject> logs = new ArrayList<JSONObject>();

            ArrayList<JSONObject> results =  new ArrayList<JSONObject>();

            logs = sample.readJsonFile();

            for(JSONObject log : logs ){
                //System.out.println(log.get("sqlId"));//test code
                QueryParser parser = new QueryParser();
                String query = log.get("sql").toString();

                Statement statement = CCJSqlParserUtil.parse(query);
                SqlType type = parser.getSqlType(query,statement);

                //get source tables
                JSONObject reslut = new JSONObject();
                //set sqlID
                reslut.put("sqlId",log.get("sqlId").toString());

                //set type
                reslut.put("sqlType",type);

                //set target table
                reslut.put("target",parser.getTargetTable(statement,type));

                //set source table
                reslut.put("source",parser.getSourcrTables(statement,type));

                //add result
                results.add(reslut);

            }


            //print!!!
            //printResult(results);
            System.out.println(results.toString());

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
