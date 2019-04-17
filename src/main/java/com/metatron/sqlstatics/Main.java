package com.metatron.sqlstatics;

public class Main {

    public static void main(String[] args) {
        //read config
        try {

            MakeJsonLogSample sample = new MakeJsonLogSample();
            //make sample
//            sample.makeSample();
//            sample.makeSampleFromCsvFile();

            //get query parsing result -> to result file
            QueryParser queryParser = new QueryParser();
            queryParser.getQueryStatics();

            //TODO: write to hive table (DruidLineageRecordOrcWriter)

            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            String logPathDir = sqlConfiguration.get("log_path");
            String orcFilePath = sqlConfiguration.get("orcFilePath");
            boolean overwrite = Boolean.parseBoolean(sqlConfiguration.get("overwrite"));

            DruidLineageRecordOrcWriter druidLineageRecordOrcWriter = new DruidLineageRecordOrcWriter();
            druidLineageRecordOrcWriter.processOrc(logPathDir, orcFilePath, overwrite);


        } catch (Exception e) {
            System.out.println(e);
        }

    }

}
