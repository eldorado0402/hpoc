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

public class DruidLineageRecordOrcWriter {
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
//        hadoopConf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/core-site.xml"));
//        hadoopConf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(hadoopConf);

        if (!fs.exists(logPath)) {
            System.out.println("Lineage log path does not exist");
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
                                System.out.println(e.toString());
                                System.out.println("line number : [" + linenum + "]");
                                System.out.println(line);
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
//        conf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/core-site.xml"));
//        conf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/hdfs-site.xml"));


        TypeDescription schema = TypeDescription.fromString("struct<eventtime:bigint,cluster:string,engineType:string," +
                "sourcetablename:string," +
                "targettablename:string," +
                "sql:string,sqlid:string,sqltype:string" +
                ">");

        Writer writer = OrcFile.createWriter(new Path(orcFilePath),
                OrcFile.writerOptions(conf)
                        .setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch();

        LongColumnVector eventTime = (LongColumnVector) batch.cols[0];
        BytesColumnVector cluster = (BytesColumnVector) batch.cols[1];
        BytesColumnVector engineType = (BytesColumnVector) batch.cols[2];
        ;
        BytesColumnVector sourceTableName = (BytesColumnVector) batch.cols[3];
        BytesColumnVector targetTableName = (BytesColumnVector) batch.cols[4];
        BytesColumnVector sql = (BytesColumnVector) batch.cols[5];
        BytesColumnVector sqlId = (BytesColumnVector) batch.cols[6];
        BytesColumnVector sqlType = (BytesColumnVector) batch.cols[7];

        int row;

        for (ParseDataRecord record : records) {
            row = batch.size++;

            eventTime.vector[row] = record.getCreatedTime();
            cluster.setVal(row, toBytes(record.getCluster()));
            sourceTableName.setVal(row, toBytes(record.getSourceTable(), true));
            targetTableName.setVal(row, toBytes(record.getTargetTable(), true));
            sql.setVal(row, toBytes(record.getsql()));
            sqlId.setVal(row, toBytes(record.getSqlId()));
            sqlType.setVal(row, toBytes(record.getSqlType()));
            engineType.setVal(row, toBytes(record.getEngineType()));

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
//        conf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/core-site.xml"));
//        conf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/hdfs-site.xml"));

        Reader reader = OrcFile.createReader(new Path("/user/hive/warehouse/lineage/sample.orc"),
                OrcFile.readerOptions(conf));
        RecordReader rows = reader.rows();

        VectorizedRowBatch batch1 = reader.getSchema().createRowBatch();
        while (rows.nextBatch(batch1)) {
            System.out.println(batch1);
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
