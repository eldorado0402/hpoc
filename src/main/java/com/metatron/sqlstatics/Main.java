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

            MakeJsonLog sample = new MakeJsonLog();
            //make sample
            //sample.makeSample();

            //get query parsing result -> to result file
            QueryParser queryParser = new QueryParser();
            queryParser.getQueryStatics();

            //TODO: write to hive table (DruidLineageRecordOrcWriter)

//            if(args == null || args.length < 2) {
//                System.out.println("java DruidLineageRecordOrcWriter <lineage-log path> <orc outfile path> [<overwrite>]");
//                System.exit(1);
//            }

            //String logPathDir = args[0];
            //String orcFilePath = args[1];
            //boolean overwrite = false;

            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            String logPathDir = sqlConfiguration.get("log_path");
            String orcFilePath = sqlConfiguration.get("orcFilePath");
            boolean overwrite = true;

//            if(args.length == 3 && "overwrite".equals(args[2]))
//                overwrite = true;

            DruidLineageRecordOrcWriter druidLineageRecordOrcWriter = new DruidLineageRecordOrcWriter();
            druidLineageRecordOrcWriter.processOrc(logPathDir, orcFilePath, overwrite);


        }catch(Exception e){
            System.out.println(e);
        }

    }


//    private static void printResult(ArrayList<JSONObject> results){
//
//        for(JSONObject result : results ){
//
//            System.out.println(result.get("selectedColumns"));
//
//        }
//
//    }
}
