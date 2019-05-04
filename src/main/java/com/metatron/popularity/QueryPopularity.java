package com.metatron.popularity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metatron.sqlstatics.ParseDataRecord;
import com.metatron.sqlstatics.QueryParser;
import com.metatron.sqlstatics.SQLConfiguration;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class QueryPopularity {

    private static final Logger logger = LoggerFactory.getLogger(QueryPopularity.class);

    public void getQueryStaticsFromHdfsFile() {

        //read log file
        BufferedReader br = null;
        FSDataOutputStream fsDataOutputStream = null;
        PrintWriter writer = null;
        JSONParser jsonParser = new JSONParser();

        //read config
        try {
            //TODO : 추후에는 변경 필요
            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            Configuration conf = new Configuration();

            String coreSitePath = sqlConfiguration.get("hadoop_core_site");
            String hdfsSitePathe = sqlConfiguration.get("hadoop_hdfs_site");

            conf.addResource(new Path(coreSitePath));
            conf.addResource(new Path(hdfsSitePathe));


            Path inputFile = new Path(sqlConfiguration.get("popularity_input_filename"));
            Path outputFile = new Path(sqlConfiguration.get("popularity_output_filename"));

            FileSystem fs = FileSystem.get(conf);

            //input file
            br = new BufferedReader(new InputStreamReader(fs.open(inputFile)));
            String line;

            //output file
            if (fs.exists(outputFile)) {
                fs.delete(outputFile, true);
                logger.info("delete previous parser file path in : " + outputFile);
            }

            fsDataOutputStream = fs.create(outputFile);
            writer = new PrintWriter(fsDataOutputStream);

            while ((line = br.readLine()) != null) {
                //inputdata
                JSONObject jsonData = (JSONObject) jsonParser.parse(line);

                QueryParser parser = new QueryParser();
                List<String> sourceTables = null;
                Statement statement;
                QueryParser.SqlType type;
                String targetTable;

                String query = jsonData.get("sql").toString();

                // sql 이 없으면 다음 라인 처리
                if (query == null || query.trim().equals(""))
                    continue;

                //TODO : lineage list 를 가져오기
                GetLineage lineage = new GetLineage();
                ArrayList<LineageInfo> lineageLists=lineage.makeLineageInfos(query);


                if (lineageLists != null && lineageLists.size() >= 1) {

                    for (LineageInfo info : lineageLists) {
                        //output data
                        PopularityDataRecord popularityData = new PopularityDataRecord();

                        //read from log file
                        popularityData.setSql(jsonData.get("sql").toString());

                        if (jsonData.get("engineType") != null)
                            popularityData.setEngineType(jsonData.get("engineType").toString());

                        if (jsonData.get("createTime") != null)
                            popularityData.setCreatedTime((Long) jsonData.get("createTime"));

                        if (jsonData.get("sqlId") != null)
                            popularityData.setSqlId(jsonData.get("sqlId").toString());

                        //set schema
                        popularityData.setSchema(info.getSchema());
                        //set source Table
                        popularityData.setTable(info.getTable());
                        //set source Table alisas
                        popularityData.setTableAlias(info.getTableAlias());

                        popularityData.setColumn(info.getColumn());
                        //set source Table alisas
                        popularityData.setColumnAlias(info.getColumnAlias());

                        //write
                        writeResultToHdfsFile(writer, popularityData);

                    }

                } else { //파싱되는 정보가 없으면 쿼리 기본 정보만 찍음

                    //output data
                    PopularityDataRecord popularityData = new PopularityDataRecord();

                    //read from log file
                    popularityData.setSql(jsonData.get("sql").toString());

                    if (jsonData.get("engineType") != null)
                        popularityData.setEngineType(jsonData.get("engineType").toString());

                    if (jsonData.get("createTime") != null)
                        popularityData.setCreatedTime((Long) jsonData.get("createTime"));

                    if (jsonData.get("sqlId") != null)
                        popularityData.setSqlId(jsonData.get("sqlId").toString());

                    //write
                    writeResultToHdfsFile(writer, popularityData);

                }


            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            //logger.info(e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                if (writer != null) {
                    writer.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                if (fsDataOutputStream != null) {
                    fsDataOutputStream.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void writeResultToHdfsFile(PrintWriter writer, PopularityDataRecord popularityData) {
        StringBuilder sb = new StringBuilder();
        ObjectMapper mapper = new ObjectMapper();

        try {
            sb.append(mapper.writer().writeValueAsString(popularityData)).append("\n");

            if (sb.length() > 1) {
                writer.write(sb.toString().substring(0, sb.length() - 1));
                writer.write("\n");
                writer.flush();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
