package com.metatron.sqlstatics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.metatron.popularity.*;

import java.util.ArrayList;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        String analyzeType = args[0];
        String logfileType = args[1];
        String coreSitePath = args[2];
        String hdfsSitePath = args[3];
        String applogPath = args[4];

        if(args.length < 4){
            logger.error("The input args are few in number. Please input args needed");
            return;
        }

        if(analyzeType.equals("STATICS") ) {
            //read config

            try {

                //get query parsing result -> to result file
                QueryParser queryParser = new QueryParser();

                if (logfileType.equals("APPLOG")) {
                    queryParser.getQueryStaticsFromApplicationLogFile(coreSitePath, hdfsSitePath, applogPath);
                } else if (logfileType.equals("HDFS")) {
                    queryParser.getQueryStaticsFromHdfsFile(coreSitePath, hdfsSitePath);
                } else if (logfileType.equals("LOCAL")) {
                    queryParser.getQueryStatics(); //from Local
                }

                //TODO: write to hive table (DruidLineageRecordOrcWriter)
                SQLConfiguration sqlConfiguration = new SQLConfiguration();

                String logPathDir = sqlConfiguration.get("log_path");
                String orcFilePath = sqlConfiguration.get("orcFilePath");
                boolean overwrite = Boolean.parseBoolean(sqlConfiguration.get("overwrite"));

                DruidLineageRecordOrcWriter druidLineageRecordOrcWriter = new DruidLineageRecordOrcWriter();
                druidLineageRecordOrcWriter.processOrc(logPathDir, orcFilePath, overwrite,coreSitePath,hdfsSitePath);

                System.out.println("complete!!");


            } catch (Exception e) {
                //System.out.println(e);
                logger.error(e.getMessage());
            }

        }else if(analyzeType.equals("POPULARITY")) {

            try {

                QueryPopularity queryPopularity = new QueryPopularity();

                if (logfileType.equals("APPLOG")) {
                    queryPopularity.getQueryStaticsFromApplicationLogFile(coreSitePath, hdfsSitePath, applogPath);
                } else if (logfileType.equals("HDFS")) {
                    queryPopularity.getQueryStaticsFromHdfsFile(coreSitePath, hdfsSitePath);
                }

                //TODO: write to hive table (DruidLineageRecordOrcWriter)
                SQLConfiguration sqlConfiguration = new SQLConfiguration();

                String logPathDir = sqlConfiguration.get("popularity_log_path");
                String orcFilePath = sqlConfiguration.get("popularity_orcFilePath");
                boolean overwrite = Boolean.parseBoolean(sqlConfiguration.get("overwrite"));

                DruidPopularityRecordOrcWriter druidPopularityRecordOrcWriter = new DruidPopularityRecordOrcWriter();
                druidPopularityRecordOrcWriter.processOrc(logPathDir, orcFilePath, overwrite, coreSitePath,hdfsSitePath);


            } catch (Exception e) {
                //System.out.println(e);
                logger.error(e.getMessage());
            }
        }


    }


}
