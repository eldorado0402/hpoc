package com.metatron.sqlstatics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.metatron.popularity.*;

import java.util.ArrayList;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
/*
        //make sample
        MakeJsonLogSample sample = new MakeJsonLogSample();

//        sample.makeSample();
        sample.readApplicationLogFile();
*/

        //read config
/*
        try {

            //get query parsing result -> to result file
            QueryParser queryParser = new QueryParser();
            //queryParser.getQueryStatics(); //from Local
            queryParser.getQueryStaticsFromApplicationLogFile(); //from HDFS

            //TODO: write to hive table (DruidLineageRecordOrcWriter)

            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            String logPathDir = sqlConfiguration.get("log_path");
            String orcFilePath = sqlConfiguration.get("orcFilePath");
            boolean overwrite = Boolean.parseBoolean(sqlConfiguration.get("overwrite"));

            DruidLineageRecordOrcWriter druidLineageRecordOrcWriter = new DruidLineageRecordOrcWriter();
            druidLineageRecordOrcWriter.processOrc(logPathDir, orcFilePath, overwrite);


        } catch (Exception e) {
            //System.out.println(e);
            logger.error(e.getMessage());
        }

*/

/*
        try {

            QueryPopularity queryPopularity = new QueryPopularity();
            queryPopularity.getQueryStaticsFromApplicationLogFile();

            //TODO: write to hive table (DruidLineageRecordOrcWriter)
            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            String logPathDir = sqlConfiguration.get("popularity_log_path");
            String orcFilePath = sqlConfiguration.get("popularity_orcFilePath");
            boolean overwrite = Boolean.parseBoolean(sqlConfiguration.get("overwrite"));

            DruidPopularityRecordOrcWriter druidPopularityRecordOrcWriter = new DruidPopularityRecordOrcWriter();
            druidPopularityRecordOrcWriter.processOrc(logPathDir, orcFilePath, overwrite);


        } catch (Exception e) {
            //System.out.println(e);
            logger.error(e.getMessage());
        }
*/

        GetLineage lineage = new GetLineage();
        lineage.makeLineageInfos("test");

//        MakeJsonLogSample sample = new MakeJsonLogSample();
//        ArrayList<String> a = sample.getORACLEQueryList();
//        lineage.makeLineageInfos(a.get(17));

//        int index=0;
//        for( String sql : sample.getORACLEQueryList() ) {
//            System.out.println("index : " +index++);
//            lineage.makeLineageInfos(sql);
//        }
    }


}
