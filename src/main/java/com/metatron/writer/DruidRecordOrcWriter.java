package com.metatron.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metatron.popularity.PopularityDataRecord;
import com.metatron.sqlstatics.ParseDataRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.logging.log4j.core.util.Closer;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.Arrays;


public class DruidRecordOrcWriter {

    private static final Logger logger = LoggerFactory.getLogger(DruidRecordOrcWriter.class);

    private static final ArrayList<String> PopularityColNames = new ArrayList<String>(
      Arrays.asList("sqlid", "enginetype", "sql" ,"sqltype","eventtime" ,"shcema" ,"sourcetablename" ,"tableAlias"
          , "columnname", "columnAlias", "depth"));
    private static final ArrayList<String> PopularityColTypes = new ArrayList<String>(
      Arrays.asList("string", "string", "string", "string", "bigint", "string", "string", "string", "string", "string", "int"));

    private static final ArrayList<String> ParserColNames = new ArrayList<String>(
        Arrays.asList("sqlid", "enginetype", "cluster" ,"sqltype","sourcetablename" ,"targettablename" ,"sql" ,"eventtime"));
    private static final ArrayList<String> ParserColTypes = new ArrayList<String>(
        Arrays.asList("string", "string", "string", "string", "string", "string", "string", "bigint"));


    public static boolean isGzipped(DataInputStream is) {
        try {
            byte[] signature = new byte[2];
            int nread = is.read(signature); //read the gzip signature
            return nread == 2 && signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {

            //todo : file close

            Closer.closeSilently(is);
        }
    }


  public void processOrc(String logPathDir, String type,String orcFilePath, boolean overwrite,String coreSitePath,String hdfsSitePath) throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    Path logPath = new Path(logPathDir);
    Configuration hadoopConf = new Configuration();
    //TODO: 실제 실행시 삭제하고, 하둡의 conf path를 지정해 주면 됨.

    try {
      hadoopConf.addResource(new Path(coreSitePath));
      hadoopConf.addResource(new Path(hdfsSitePath));

    } catch (Exception e) {
      logger.info(e.getMessage());

    }

    FileSystem fs = FileSystem.get(hadoopConf);

    if (!fs.exists(logPath)) {
      logger.info("Popularity log path does not exist");
      System.exit(1);
    }

    //LOG PATH 가 디렉터리 일 때만 구현해 놓음
    if (fs.isDirectory(logPath)) {
      RemoteIterator <LocatedFileStatus> iterator = fs.listFiles(logPath, false);
      List <Object> records = new ArrayList <Object>();

      while (iterator.hasNext()) {
        FileStatus status = iterator.next();
        if (status.isFile()) {
          String line;
          BufferedReader br;

          boolean isGzipped = isGzipped(fs.open(status.getPath()));
          if (isGzipped) {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(status.getPath()))));
          } else {
            br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
          }

          try {
            int linenum = 0;
            while ((line = br.readLine()) != null) {
              try {
                linenum++;
                if(type.equals("POPULARITY")) {
                  PopularityDataRecord record = mapper.readValue(line.getBytes(), PopularityDataRecord.class);
                  records.add(record);
                }else {
                  ParseDataRecord record = mapper.readValue(line.getBytes(), ParseDataRecord.class);
                  records.add(record);
                }
              } catch (Exception e) {
                logger.info(e.toString());
                logger.info("line number : [" + linenum + "]");
                logger.info(line);
              }
            }
          } finally {
            if (br != null) {
              br.close();
            }
          }
        }
      }

      if (overwrite && fs.exists(new Path(orcFilePath)))
        fs.delete(new Path(orcFilePath), true);

       wirteOrc(records, type,orcFilePath,coreSitePath,hdfsSitePath);

    }
  }

  private void wirteOrc(List<Object> records,String type, String orcFilePath, String coreSitePath, String hdfsSitePath) throws Exception {
    Configuration conf = new Configuration();

    //TODO: 실제 실행시 삭제하고, 하둡의 conf path를 지정해 주면 됨.
    try {
      conf.addResource(new Path(coreSitePath));
      conf.addResource(new Path(hdfsSitePath));

    } catch (Exception e) {
      logger.info(e.getMessage());

    }

    ArrayList<String> colName=null;
    ArrayList<String> colType=null;

    if(type.equalsIgnoreCase("POPULARITY")){
      colName = PopularityColNames;
      colType = PopularityColTypes;
    }else if (type.equalsIgnoreCase("STATICS")){
      colName = ParserColNames;
      colType = ParserColTypes;
    }


    //make TypeDescription
    StringBuilder sb = new StringBuilder();
    sb.append("struct<");

    for(int i=0 ; i< colName.size(); i++){
      sb.append(colName.get(i)+":"+colType.get(i));
      //last column
      if(i == colName.size()-1){
        sb.append(">");
      }else{
        sb.append(",");
      }

    }

    TypeDescription schema = TypeDescription.fromString(sb.toString());

    Writer writer = OrcFile.createWriter(new Path(orcFilePath),
                                         OrcFile.writerOptions(conf)
                                                .setSchema(schema));

    VectorizedRowBatch batch = schema.createRowBatch();

    int row;

    for (Object record : records) {
      row = batch.size++;

      ArrayList<Object> colVal = new ArrayList<Object>();
      if(record instanceof PopularityDataRecord){
        colVal.add(((PopularityDataRecord)record).getSqlId());
        colVal.add(((PopularityDataRecord)record).getEngineType());
        colVal.add(((PopularityDataRecord)record).getSql());
        colVal.add(((PopularityDataRecord)record).getSqlType());
        colVal.add(((PopularityDataRecord)record).getCreatedTime());
        colVal.add(((PopularityDataRecord)record).getSchema());
        colVal.add(((PopularityDataRecord)record).getTable());
        colVal.add(((PopularityDataRecord)record).getTableAlias());
        colVal.add(((PopularityDataRecord)record).getColumn());
        colVal.add(((PopularityDataRecord)record).getColumnAlias());
        colVal.add(((PopularityDataRecord)record).getDepth());
      }else if(record instanceof ParseDataRecord){
        colVal.add(((ParseDataRecord)record).getSqlId());
        colVal.add(((ParseDataRecord)record).getEngineType());
        colVal.add(((ParseDataRecord)record).getCluster());
        colVal.add(((ParseDataRecord)record).getSqlType());
        colVal.add(((ParseDataRecord)record).getSourceTable());
        colVal.add(((ParseDataRecord)record).getTargetTable());
        colVal.add(((ParseDataRecord)record).getsql());
        colVal.add(((ParseDataRecord)record).getCreatedTime());
      }


      //set value
      for(int i=0 ; i< colName.size(); i++){
        if(colType.get(i).equalsIgnoreCase("string")) {
          ((BytesColumnVector) batch.cols[i]).setVal(row,  toBytes((String)colVal.get(i)));
        }else if(colType.get(i).equalsIgnoreCase("bigint")){
          ((LongColumnVector)  batch.cols[i]).vector[row] = (Long)colVal.get(i);
        }else if (colType.get(i).equalsIgnoreCase("int") ){
          ((LongColumnVector) batch.cols[i]).vector[row] = Long.valueOf((Integer)colVal.get(i));
        }

      }

      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }


    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }

    writer.close();
  }



    public void readOrc(String coreSitePath,String hdfsSitePath) throws Exception {
        Configuration conf = new Configuration();

        //TODO: 실제 실행시 삭제하고, 하둡의 conf path를 지정해 주면 됨.
        try {
            conf.addResource(new Path(coreSitePath));
            conf.addResource(new Path(hdfsSitePath));

        } catch (Exception e) {
            logger.info(e.getMessage());

        }

        Reader reader = OrcFile.createReader(new Path("/user/hive/warehouse/lineage/sample.orc"),
                OrcFile.readerOptions(conf));
        RecordReader rows = reader.rows();

        VectorizedRowBatch batch1 = reader.getSchema().createRowBatch();
        while (rows.nextBatch(batch1)) {
            logger.info(batch1.toString());
        }
    }

    private byte[] toBytes(String inputData) {
        return (inputData == null) ? "".getBytes() : inputData.getBytes();
    }

    private byte[] toBytes(String inputData, boolean toLowerCase) {
        if (toLowerCase) {
            return toBytes(inputData == null ? null : inputData.toLowerCase());
        }
        return toBytes(inputData);
    }
}
