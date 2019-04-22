package com.metatron.sqlstatics;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DruidLineageRecordOrcWriter {

    Logger logger = LoggerFactory.getLogger(DruidLineageRecordOrcWriter.class);

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

    public void processOrc(String logPathDir, String orcFilePath, boolean overwrite) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        Path logPath = new Path(logPathDir);
        Configuration hadoopConf = new Configuration();
        //TODO: 실제 실행시 삭제하고, 하둡의 conf path를 지정해 주면 됨.

        try {
            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            String coreSitePath = sqlConfiguration.get("hadoop_core_site");
            String hdfsSitePathe = sqlConfiguration.get("hadoop_hdfs_site");

            hadoopConf.addResource(new Path(coreSitePath));
            hadoopConf.addResource(new Path(hdfsSitePathe));

        }catch(Exception e){
            logger.info(e.getMessage());

        }


        FileSystem fs = FileSystem.get(hadoopConf);

        if (!fs.exists(logPath)) {
           logger.info("Lineage log path does not exist");
            System.exit(1);
        }

        //LOG PATH 가 디렉터리 일 때만 구현해 놓음
        if (fs.isDirectory(logPath)) {
            RemoteIterator <LocatedFileStatus> iterator = fs.listFiles(logPath, false);
            List <ParseDataRecord> records = new ArrayList <ParseDataRecord>();

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
                                ParseDataRecord record = mapper.readValue(line.getBytes(), ParseDataRecord.class);
                                records.add(record);
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

            //DruidLineageRecordOrcWriter writer = new DruidLineageRecordOrcWriter();

            if (overwrite && fs.exists(new Path(orcFilePath)))
                fs.delete(new Path(orcFilePath), true);

            //writer.writeOrc(records, orcFilePath);
            writeOrc(records, orcFilePath);
        }
    }

    public void writeOrc(List <ParseDataRecord> records, String orcFilePath) throws Exception {
        //ORC 스키마 정의, Hive 내 bdpown.lineage 테이블 스키마 참조
        Configuration conf = new Configuration();

        //TODO: 실제 실행시 삭제하고, 하둡의 conf path를 지정해 주면 됨.
        try {
            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            String coreSitePath = sqlConfiguration.get("hadoop_core_site");
            String hdfsSitePathe = sqlConfiguration.get("hadoop_hdfs_site");

            conf.addResource(new Path(coreSitePath));
            conf.addResource(new Path(hdfsSitePathe));

        }catch(Exception e){
            logger.info(e.getMessage());

        }


        TypeDescription schema = TypeDescription.fromString("struct<" +
                "sqlid:string," +
                "enginetype:string," +
                "cluster:string," +
                "sqltype:string," +
                "sourcetablename:string," +
                "targettablename:string," +
                "sql:string," +
                "eventtime:bigint" +
                ">");

        Writer writer = OrcFile.createWriter(new Path(orcFilePath),
                OrcFile.writerOptions(conf)
                        .setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch();

        BytesColumnVector sqlId = (BytesColumnVector) batch.cols[0];
        BytesColumnVector engineType = (BytesColumnVector) batch.cols[1];
        BytesColumnVector cluster = (BytesColumnVector) batch.cols[2];
        BytesColumnVector sqlType = (BytesColumnVector) batch.cols[3];
        BytesColumnVector sourceTableName = (BytesColumnVector) batch.cols[4];
        BytesColumnVector targetTableName = (BytesColumnVector) batch.cols[5];
        BytesColumnVector sql = (BytesColumnVector) batch.cols[6];
        LongColumnVector eventTime = (LongColumnVector) batch.cols[7];

        int row;

        for (ParseDataRecord record : records) {
            row = batch.size++;

            sqlId.setVal(row, toBytes(record.getSqlId()));
            engineType.setVal(row, toBytes(record.getEngineType()));
            cluster.setVal(row, toBytes(record.getCluster()));
            sqlType.setVal(row, toBytes(record.getSqlType()));
            sourceTableName.setVal(row, toBytes(record.getSourceTable(), true));
            targetTableName.setVal(row, toBytes(record.getTargetTable(), true));
            sql.setVal(row, toBytes(record.getsql()));
            eventTime.vector[row] = record.getCreatedTime();

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

    public void readOrc() throws Exception {
        Configuration conf = new Configuration();

        //TODO: 실제 실행시 삭제하고, 하둡의 conf path를 지정해 주면 됨.
        try {
            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            String coreSitePath = sqlConfiguration.get("hadoop_core_site");
            String hdfsSitePathe = sqlConfiguration.get("hadoop_hdfs_site");

            conf.addResource(new Path(coreSitePath));
            conf.addResource(new Path(hdfsSitePathe));

        }catch(Exception e){
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
