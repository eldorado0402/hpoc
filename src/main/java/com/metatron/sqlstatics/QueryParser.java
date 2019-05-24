package com.metatron.sqlstatics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metatron.util.HtmlQueryCollector;
import com.metatron.util.SQLConfiguration;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
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

public class QueryParser {

    private static final Logger logger = LoggerFactory.getLogger(QueryParser.class);

    //LOCAL
    public void getQueryStatics() {

        //read log file
        BufferedReader br = null;
        FileWriter writer = null;
        JSONParser jsonParser = new JSONParser();

        //read config
        try {

            //TODO : 추후에는 변경 필요
            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            File inputFile = new File(sqlConfiguration.get("input_filename"));
            File outputFile = new File(sqlConfiguration.get("output_filename"));

            br = new BufferedReader(new FileReader(inputFile));
            String line;

            writer = new FileWriter(outputFile, true);

            while ((line = br.readLine()) != null) {
                //inputdata
                JSONObject jsonData = (JSONObject) jsonParser.parse(line);

                QueryParser parser = new QueryParser();
                List <String> sourceTables = null;
                Statement statement;
                SqlType type;
                String targetTable;
                String engineType = null;
                String cluster = null;
                long createdTime = -1;
                String sqlId = null;


                String query = jsonData.get("sql").toString();

                // sql 이 없으면 다음 라인 처리
                if (query == null || query.trim().equals(""))
                    continue;

                if (jsonData.get("engineType") != null)
                    engineType = jsonData.get("engineType").toString();

                if (jsonData.get("createTime") != null)
                    createdTime = (Long) jsonData.get("createTime");

                if (jsonData.get("cluster") != null)
                    cluster = jsonData.get("cluster").toString();

                if (jsonData.get("sqlId") != null)
                    sqlId = jsonData.get("sqlId").toString();

                //sql 이 있으면 처리 시작
                //parsing이 안되면 다음 라인 처
                try {
                    statement = CCJSqlParserUtil.parse(query);
                } catch (Exception e) {
                    logger.info(e.getMessage());
                    continue;
                }

                type = parser.getSqlType(query, statement);
                targetTable = parser.getTargetTable(statement, type);

                //get source table list
                if (parser.getSourcrTables(statement, type) != null) {
                    sourceTables = parser.getSourcrTables(statement, type);
                }


                if (sourceTables != null && sourceTables.size() >= 1) {

                    for (String source : sourceTables) {

                        writeParseDataRecord(writer,engineType,createdTime,sqlId, query,
                                cluster, source,targetTable, type.toString());

                    }

                } else {

                    writeParseDataRecord(writer,engineType,createdTime,sqlId, query,
                             cluster, null,targetTable, type.toString());

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

            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (writer != null) {
                    writer.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    //HDFS
    public void getQueryStaticsFromHdfsFile(String coreSitePath,String hdfsSitePath) {

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

//            String coreSitePath = sqlConfiguration.get("hadoop_core_site");
//            String hdfsSitePathe = sqlConfiguration.get("hadoop_hdfs_site");

            conf.addResource(new Path(coreSitePath));
            conf.addResource(new Path(hdfsSitePath));


            Path inputFile = new Path(sqlConfiguration.get("hdfs_input_filename"));
            Path outputFile = new Path(sqlConfiguration.get("hdfs_output_filename"));

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
                List <String> sourceTables = null;
                Statement statement;
                SqlType type;
                String targetTable;
                String engineType = null;
                String cluster = null;
                long createdTime = -1;
                String sqlId = null;

                String query = jsonData.get("sql").toString();

                // sql 이 없으면 다음 라인 처리
                if (query == null || query.trim().equals(""))
                    continue;


                if (jsonData.get("engineType") != null)
                    engineType = jsonData.get("engineType").toString();

                if (jsonData.get("createTime") != null)
                    createdTime = (Long) jsonData.get("createTime");

                if (jsonData.get("cluster") != null)
                    cluster = jsonData.get("cluster").toString();

                if (jsonData.get("sqlId") != null)
                    sqlId = jsonData.get("sqlId").toString();

                //sql 이 있으면 처리 시작
                //parsing이 안되면 다음 라인 처
                try {
                    statement = CCJSqlParserUtil.parse(query);
                } catch (Exception e) {
                    logger.error(e + ", query :" + query);
                    continue;
                }

                type = parser.getSqlType(query, statement);
                targetTable = parser.getTargetTable(statement, type);

                //get source table list
                if (parser.getSourcrTables(statement, type) != null) {
                    sourceTables = parser.getSourcrTables(statement, type);
                }


                if (sourceTables != null && sourceTables.size() >= 1) {

                    for (String source : sourceTables) {

                        writeParseDataRecord(writer,engineType,createdTime,sqlId, query,
                                cluster, source,targetTable, type.toString());

                    }

                } else {

                    writeParseDataRecord(writer,engineType,createdTime,sqlId, query,
                            cluster, null,targetTable, type.toString());

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



    //APPLOG local
    public void getQueryStaticsFromApplicationLogFile(String coreSitePath,String hdfsSitePath, String applogPath) {

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

//            String coreSitePath = sqlConfiguration.get("hadoop_core_site");
//            String hdfsSitePathe = sqlConfiguration.get("hadoop_hdfs_site");

            conf.addResource(new Path(coreSitePath));
            conf.addResource(new Path(hdfsSitePath));

            Path outputFile = new Path(sqlConfiguration.get("hdfs_output_filename"));

            FileSystem fs = FileSystem.get(conf);

            //output file
            if (fs.exists(outputFile)) {
                fs.delete(outputFile, true);
                logger.info("delete previous parser file path in : " + outputFile);
            }

            fsDataOutputStream = fs.create(outputFile);
            writer = new PrintWriter(fsDataOutputStream);

            ArrayList<String> logs = readApplicationLogFile(applogPath);

            for( String query : logs )
            {
                QueryParser parser = new QueryParser();
                List <String> sourceTables = null;
                Statement statement;
                SqlType type = QueryParser.SqlType.NONE;;
                String targetTable = null;
                long createdTime;
                String sqlId;

                // sql 이 없으면 다음 라인 처리
                if (query == null || query.trim().equals(""))
                    continue;

                // set uuid
                sqlId = UUID.randomUUID().toString();
                //set
                createdTime = System.currentTimeMillis();

                //sql 이 있으면 처리 시작
                //parsing이 안되면 다음 라인 처
                try {
                    statement = CCJSqlParserUtil.parse(query);
                } catch (Exception e) {
                    logger.error(e + ", query :" + query);
                    continue;
                }

                type = parser.getSqlType(query, statement);
                targetTable = parser.getTargetTable(statement, type);

                //get source table list
                if (parser.getSourcrTables(statement, type) != null) {
                    sourceTables = parser.getSourcrTables(statement, type);
                }


                if (sourceTables != null && sourceTables.size() >= 1) {

                    for (String source : sourceTables) {
                        writeParseDataRecord(writer,engineType,createdTime,sqlId, query,
                                "localhost", source,targetTable, type.toString());

                    }

                } else {

                    writeParseDataRecord(writer,engineType,createdTime,sqlId, query,
                            "localhost", null,targetTable, type.toString());

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


    //APPLOG local
    public void getQueryStaticsFromWordPressBlog(String coreSitePath,String hdfsSitePath, String blogUrl) {

        //read log file
        BufferedReader br = null;
        FSDataOutputStream fsDataOutputStream = null;
        PrintWriter writer = null;
        HtmlQueryCollector htmlQueryCollector = new HtmlQueryCollector();

        //read config
        try {
            //TODO : 추후에는 변경 필요
            SQLConfiguration sqlConfiguration = new SQLConfiguration();
            String engineType= sqlConfiguration.get("engineType");

            Configuration conf = new Configuration();

            conf.addResource(new Path(coreSitePath));
            conf.addResource(new Path(hdfsSitePath));

            Path outputFile = new Path(sqlConfiguration.get("hdfs_output_filename"));

            FileSystem fs = FileSystem.get(conf);

            //output file
            if (fs.exists(outputFile)) {
                fs.delete(outputFile, true);
                logger.info("delete previous parser file path in : " + outputFile);
            }

            fsDataOutputStream = fs.create(outputFile);
            writer = new PrintWriter(fsDataOutputStream);

            ArrayList<String> logs = htmlQueryCollector.collectSqls(blogUrl);

            for( String query : logs )
            {
                QueryParser parser = new QueryParser();
                List <String> sourceTables = null;
                Statement statement;
                SqlType type = QueryParser.SqlType.NONE;;
                String targetTable = null;
                long createdTime;
                String sqlId;

                // sql 이 없으면 다음 라인 처리
                if (query == null || query.trim().equals(""))
                    continue;

                // set uuid
                sqlId = UUID.randomUUID().toString();
                //set
                createdTime = System.currentTimeMillis();

                //sql 이 있으면 처리 시작
                //parsing이 안되면 다음 라인 처
                try {
                    statement = CCJSqlParserUtil.parse(query);
                } catch (Exception e) {
                    logger.error(e + ", query :" + query);
                    continue;
                }

                type = parser.getSqlType(query, statement);
                targetTable = parser.getTargetTable(statement, type);

                //get source table list
                if (parser.getSourcrTables(statement, type) != null) {
                    sourceTables = parser.getSourcrTables(statement, type);
                }


                if (sourceTables != null && sourceTables.size() >= 1) {

                    for (String source : sourceTables) {
                        writeParseDataRecord(writer,engineType,createdTime,sqlId, query,
                                "localhost", source,targetTable, type.toString());

                    }

                } else {

                    writeParseDataRecord(writer,engineType,createdTime,sqlId, query,
                            "localhost", null,targetTable, type.toString());

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

    //Get source table
    public List <String> getSourcrTables(Statement statement, SqlType type) {

        List <String> list = null;

        //todo : create view 일때 확인해 보기
        if ((type == SqlType.CREATE_TABLE)
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
    public String getTargetTable(Statement statement, SqlType type) {
        String target = null;

        if (type == SqlType.CREATE_TABLE) {
            CreateTable createTable = (CreateTable) statement;
            //Table name
            target = createTable.getTable().getName();

        } else if (type == SqlType.CREATE_VIEW) {  //TODO : view 는 어떻게 처리 해야 하나...
            CreateView createView = (CreateView) statement;
            //Table name
            target = createView.getView().getName();

        } else if (type == SqlType.INSERT) {
            Insert insert = (Insert) statement;
            //Table name
            target = insert.getTable().getName();

        } else if (type == SqlType.MERGE) { //mergeUpdate , mergeInsert 2개 있음

            Merge merge = (Merge) statement;
            //Table name
            target = merge.getTable().getName();

        }else if (type == SqlType.UPSERT) {
            Upsert upsert = (Upsert) statement;
            //Table name
            target = upsert.getTable().getName();
        } else if (type == SqlType.DELETE) {
            Delete delete = (Delete) statement;
            //Table name
            target = delete.getTable().getName();

        } else if (type == SqlType.DROP) {
            Drop drop = (Drop) statement;
            //Table name
            if (drop.getName() != null) //drop table 일 경우
                target = drop.getName().toString();

        }
        //TODO : update 처리 필요 !!

        return target;
    }

    //get SQL Type
    //TODO : instance of 로 수
    public SqlType getSqlType(String query, Statement stmt) {

        SqlType sqlType = null;

        if (stmt instanceof CreateTable) {
            sqlType = SqlType.CREATE_TABLE;

        } else if (stmt instanceof CreateView) {
            sqlType = SqlType.CREATE_VIEW;

        } else if (stmt instanceof Select) {
            sqlType = SqlType.SELECT;

        } else if (stmt instanceof Insert) {
            sqlType = SqlType.INSERT;

        } else if (stmt instanceof Merge) {
            sqlType = SqlType.MERGE;

        } else if (stmt instanceof Update) {
            sqlType = SqlType.UPDATE;

        } else if (stmt instanceof Upsert) {
            sqlType = SqlType.UPSERT;

        } else if (stmt instanceof Delete) {
            sqlType = SqlType.DELETE;

        } else if (stmt instanceof Drop) {
            sqlType = SqlType.DROP;

        } else {
            sqlType = SqlType.NONE;

        }


        return sqlType;

    }

    private void writeParseDataRecord(FileWriter writer,String engineType,long createdTime,String sqlId, String sql,
                                      String cluster, String sourceTable, String targetTable, String sqlType){

        //output data
        ParseDataRecord parseData = new ParseDataRecord();

        //read from log file
        parseData.setSql(sql);
        parseData.setEngineType(engineType);

        parseData.setCreatedTime(createdTime);
        parseData.setCluster(cluster);
        parseData.setSqlId(sqlId);

        //set sql Type
        if(sqlType!=null)
            parseData.setSqlType(sqlType);

        //set target Table
        parseData.setTargetTable(targetTable);
        //set source Table
        parseData.setSourceTable(sourceTable);

        //write
        writeResultToFile(writer, parseData);

    }

    private void writeParseDataRecord(PrintWriter writer,String engineType,long createdTime,String sqlId, String sql,
                                      String cluster, String sourceTable, String targetTable, String sqlType){

        //output data
        ParseDataRecord parseData = new ParseDataRecord();

        //read from log file
        parseData.setSql(sql);
        parseData.setEngineType(engineType);

        parseData.setCreatedTime(createdTime);
        parseData.setCluster(cluster);
        parseData.setSqlId(sqlId);

        //set sql Type
        if(sqlType!=null)
            parseData.setSqlType(sqlType);

        //set target Table
        parseData.setTargetTable(targetTable);
        //set source Table
        parseData.setSourceTable(sourceTable);

        //write
        writeResultToHdfsFile(writer, parseData);

    }

    private void writeResultToFile(FileWriter writer, ParseDataRecord parseData) {
        StringBuilder sb = new StringBuilder();
        ObjectMapper mapper = new ObjectMapper();

        try {
            sb.append(mapper.writer().writeValueAsString(parseData)).append("\n");

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

    private void writeResultToHdfsFile(PrintWriter writer, ParseDataRecord parseData) {
        StringBuilder sb = new StringBuilder();
        ObjectMapper mapper = new ObjectMapper();

        try {
            sb.append(mapper.writer().writeValueAsString(parseData)).append("\n");

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


    private ArrayList<String> readApplicationLogFile(String applogPath) {

        FileWriter writer = null;
        InputStream inStream = null;
        ArrayList<String> sqls = new ArrayList<String>();;
        File folder = null;
        File[] listOfFiles = null;

        try {

            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            folder = new File(applogPath);
            listOfFiles = folder.listFiles();

            for (File file : listOfFiles) {
                if (file.isFile()) {

                    //file = new File(sqlConfiguration.get(file));
                    inStream = new FileInputStream(file);
                    String logs = IOUtils.toString(inStream, StandardCharsets.UTF_8.name());

                    Pattern MY_PATTERN = Pattern.compile("\\[sfx\\](.*?)\\d{4}-\\d{2}-\\d{2}", Pattern.DOTALL);//[sfx] query ~ next line 로그
                    Matcher matcher = MY_PATTERN.matcher(logs);

                    while (matcher.find()) {
                        sqls.add(matcher.group(1));
                    }
                }
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

    public enum JoinType{
        OUTER,
        RIGHT,
        LEFT,
        NATURAL,
        FULL,
        INNER,
        SIMPLE,
        CROSS,
        SEMI,
        NONE
    }




}

