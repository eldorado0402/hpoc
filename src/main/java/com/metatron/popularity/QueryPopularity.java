package com.metatron.popularity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metatron.sqlstatics.ParseDataRecord;
import com.metatron.sqlstatics.QueryParser;
import com.metatron.sqlstatics.SQLConfiguration;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.commons.io.IOUtils;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                QueryParser.SqlType type=null;
                String targetTable;

                String query = jsonData.get("sql").toString();

                // sql 이 없으면 다음 라인 처리
                if (query == null || query.trim().equals(""))
                    continue;

                //get query type
                try {
                    statement = CCJSqlParserUtil.parse(query);
                    type = parser.getSqlType(query, statement);
                }catch(Exception e){
                    System.out.println(query);
                    System.out.println(e);
                }

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

                        if (type != null)
                            popularityData.setSqlType(type.toString());

                        //set schema
                        popularityData.setSchema(info.getSchema());
                        //set source Table
                        popularityData.setTable(info.getTable());
                        //set source Table alisas
                        popularityData.setTableAlias(info.getTableAlias());

                        popularityData.setColumn(info.getColumn());
                        //set source Table alisas
                        popularityData.setColumnAlias(info.getColumnAlias());

                        //set depth
                        popularityData.setDepth(info.getDepth());

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


    public void getQueryStaticsFromApplicationLogFile() {

        //read log file
        BufferedReader br = null;
        FSDataOutputStream fsDataOutputStream = null;
        PrintWriter writer = null;
        JSONParser jsonParser = new JSONParser();

        //read config
        try {
            //TODO : 추후에는 변경 필요
            SQLConfiguration sqlConfiguration = new SQLConfiguration();
            String engineType= sqlConfiguration.get("engineType");

            Configuration conf = new Configuration();

            String coreSitePath = sqlConfiguration.get("hadoop_core_site");
            String hdfsSitePathe = sqlConfiguration.get("hadoop_hdfs_site");

            conf.addResource(new Path(coreSitePath));
            conf.addResource(new Path(hdfsSitePathe));

            Path outputFile = new Path(sqlConfiguration.get("popularity_output_filename"));

            FileSystem fs = FileSystem.get(conf);

            //output file
            if (fs.exists(outputFile)) {
                fs.delete(outputFile, true);
                logger.info("delete previous parser file path in : " + outputFile);
            }

            fsDataOutputStream = fs.create(outputFile);
            writer = new PrintWriter(fsDataOutputStream);

            ArrayList<String> logs = readApplicationLogFile();

            for( String query : logs )
            {
                QueryParser parser = new QueryParser();
                List<String> sourceTables = null;
                Statement statement;
                QueryParser.SqlType type=null;
                String targetTable;

                // sql 이 없으면 다음 라인 처리
                if (query == null || query.trim().equals(""))
                    continue;

                //get query type
                try {
                    statement = CCJSqlParserUtil.parse(query);
                    type = parser.getSqlType(query, statement);
                }catch(Exception e){
                    System.out.println(query);
                    System.out.println(e);
                }

                //TODO : lineage list 를 가져오기
                GetLineage lineage = new GetLineage();
                ArrayList<LineageInfo> lineageLists=lineage.makeLineageInfos(query);


                if (lineageLists != null && lineageLists.size() >= 1) {

                    for (LineageInfo info : lineageLists) {
                        //output data
                        PopularityDataRecord popularityData = new PopularityDataRecord();

                        //read from log file
                        popularityData.setSql(query);

                        popularityData.setCreatedTime(System.currentTimeMillis());
                        popularityData.setSqlId(UUID.randomUUID().toString());
                        popularityData.setEngineType(engineType);

                        if (type != null)
                            popularityData.setSqlType(type.toString());

                        //set schema
                        popularityData.setSchema(info.getSchema());
                        //set source Table
                        popularityData.setTable(info.getTable());
                        //set source Table alisas
                        popularityData.setTableAlias(info.getTableAlias());

                        popularityData.setColumn(info.getColumn());
                        //set source Table alisas
                        popularityData.setColumnAlias(info.getColumnAlias());

                        //set depth
                        popularityData.setDepth(info.getDepth());

                        //write
                        writeResultToHdfsFile(writer, popularityData);

                    }

                } else { //파싱되는 정보가 없으면 쿼리 기본 정보만 찍음

                    //output data
                    PopularityDataRecord popularityData = new PopularityDataRecord();

                    //read from log file
                    popularityData.setSql(query);

                    popularityData.setCreatedTime(System.currentTimeMillis());
                    popularityData.setSqlId(UUID.randomUUID().toString());
                    popularityData.setEngineType(engineType);

                    if (type != null)
                        popularityData.setSqlType(type.toString());

                    //write
                    writeResultToHdfsFile(writer, popularityData);

                }


            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
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


    private ArrayList<String> readApplicationLogFile() {

        FileWriter writer = null;
        InputStream inStream = null;
        File file = null;
        ArrayList<String> sqls = new ArrayList<String>();;

        try {

            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            file = new File(sqlConfiguration.get("application_log_filename"));
            inStream = new FileInputStream(file);
            String logs = IOUtils.toString(inStream, StandardCharsets.UTF_8.name());

            Pattern MY_PATTERN = Pattern.compile("\\[sfx\\](.*?)\\d{4}-\\d{2}-\\d{2}",Pattern.DOTALL);//[sfx] query ~ next line 로그
            Matcher matcher = MY_PATTERN.matcher(logs);

            while (matcher.find()) {
                sqls.add(matcher.group(1));
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(inStream != null){
                    inStream.close();
                }

                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                logger.info(e.getMessage());
            }

        }
        return sqls;
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
