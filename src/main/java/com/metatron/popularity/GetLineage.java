package com.metatron.popularity;

import com.metatron.sqlstatics.QueryParser;
import com.metatron.sqlstatics.SQLConfiguration;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import net.sf.jsqlparser.statement.insert.*;


public class GetLineage {

    final static String defalutSchema = "polaris_dev";
    private static final Logger logger = LoggerFactory.getLogger(GetLineage.class);
    private ArrayList <LineageInfo> lineageLists = new ArrayList <LineageInfo>();

    public ArrayList <LineageInfo> makeLineageInfos(String sql) {
        //for test
//        sql = getSQL();

        Statement statement;
        QueryParser parser = new QueryParser();
        MetadataInfo metadata = new MetadataInfo();

        System.out.println("query : " + sql);

        //TODO : where 절도 파싱해야 하나?

        try {
            statement = CCJSqlParserUtil.parse(sql);
            if (parser.getSqlType(sql, statement) == QueryParser.SqlType.SELECT) {
                ColumnsFinder info = new ColumnsFinder();
                ArrayList <PlainSelect> selectLists = info.getSelectList(statement);

                int listsize = selectLists.size();

                int depth = 0;

                while (listsize > 0) {
                    PlainSelect selectList = selectLists.get(listsize - 1);
                    //get select list
                    List <SelectItem> list = selectList.getSelectItems();

                    //sub query depth

                    //get source table
                    ColumnsFinder tablesInfoFinder = new ColumnsFinder();
                    //TODO: 스키마까지 함께 붙어 있는 형태로 테이블 이름이 넘어 옮
                    //TODO: source 테이블들을 가지고 올 필요가 있나?...
                    //TODO :테이블 정보를 db 와 table name 정보로 따로 조
                    ArrayList <MetadataInfo> sources = tablesInfoFinder.getTableInfoList(CCJSqlParserUtil.parse(selectList.toString()));

                    for (SelectItem selectItem : list) {

                        //check column's table name
                        if (selectItem instanceof AllTableColumns || selectItem instanceof AllColumns) {
                            //TODO:이전 리지니 정보가 있으면 다 가지고 오던가... 아니면 메타의 테이블 정보를 다 긁어 보던가 둘중에 한가지...
                            //이전 리니지 정보는 이미 저장되어 있으므로 다시 가지고 올 필요는 없어 보임...

                            if (selectItem instanceof AllTableColumns) {
                                String table;
                                String schema;
                                table = ((AllTableColumns) selectItem).getTable().getName();
                                schema = ((AllTableColumns) selectItem).getTable().getSchemaName();
                                //schema 가 없으면 source 테이블의 동일 테이블의 schema 정보를 읽어 옮
                                if (schema == null) {
                                    // schema = getKey(sourceTableInfos, table).get(0);
                                    schema = metadata.searchSchemaName(sources, table);
                                }

                                //테이블 이름이 alias 명인지 체크
                                if (metadata.checkTable(sources, table) == null) {
                                    MetadataInfo tableInfo = metadata.checkTableAlias(sources, table);
                                    if (tableInfo != null) {
                                        schema = tableInfo.getSchema();
                                        //schema = tableInfo.getSchema() != null ? tableInfo.getSchema() : defalutSchema;;
                                        table = tableInfo.getTable();
                                    }
                                }

                                //테이블과 db명으로 컬럼 정보를 가지고 옮
                                ArrayList <String> cols = getMetadataByMDM(null, schema, table, SearchType.Column);
                                //TODO: 테이블 이름인지 ... 테이블 alias 인지... 아니면 이전 서브 쿼리의 alias 인지 확인해야 함
                                if(cols.size() > 0) {
                                    setLineageLists(cols, table, depth, schema);
                                }
                                else{ // mdm 에 컬럼 정보가 없으면 "*"을 컬럼으로 그냥 넣어 줌
                                    cols.add(((AllTableColumns) selectItem).toString().split("\\.")[1]);
                                    setLineageLists(cols, table, depth, schema);
                                }


                            } else if (selectItem instanceof AllColumns) {
                                if (lineageLists.size() == 0) {
                                    //select 문이 없는 최하위 select 문 이므로 소스 테이블의 모든 컬럼을 추가
                                    //mdm에서 조회해 오도록 변경
                                    for (MetadataInfo source : sources) {
                                        String table = source.getTable();
                                        String schema = source.getSchema();
                                        ArrayList <String> cols = getMetadataByMDM(null, schema, table, SearchType.Column);

                                        if(cols.size() > 0) {
                                            setLineageLists(cols, table, depth, schema);
                                        }
                                        else{ // mdm 에 컬럼 정보가 없으면 "*"을 컬럼으로 그냥 넣어 줌
                                            cols.add(((AllColumns) selectItem).toString());
                                            setLineageLists(cols, table, depth, schema);
                                        }

                                    }

                                } else { //* 을 선택하고 최하위 select 문이 아닌 경우


                                    if (selectList.getFromItem() != null) {
                                        if (selectList.getFromItem().getClass().getSimpleName().equals("Table")) {
                                            //from 테이블의 meta를 추가함
                                            String schema = ((Table) selectList.getFromItem()).getSchemaName();
                                            String table = ((Table) selectList.getFromItem()).getName();

                                            ArrayList <String> cols = getMetadataByMDM(null, schema, table, SearchType.Column);
                                            setLineageLists(cols, table, depth, schema);

                                        }
                                    }


                                    if (selectList.getJoins() != null) {
                                        System.out.println(selectList.getJoins().toString());
                                        for (Join join : selectList.getJoins()) {
                                            if (join.getRightItem() != null) {
                                                //join.getRightItem() 가 table 형태
                                                if (join.getRightItem().getClass().getSimpleName().equals("Table")) {
                                                    String schema = ((Table) selectList.getFromItem()).getSchemaName();
                                                    String table = ((Table) join.getRightItem()).getName();

                                                    ArrayList <String> cols = getMetadataByMDM(null, schema, table, SearchType.Column);
                                                    setLineageLists(cols, table, depth, schema);

                                                }

                                            }

                                        }

                                    }

                                    //이전 depth 의 lineage 의 select 구문 모두
                                    ArrayList <LineageInfo> newinfos = new ArrayList <LineageInfo>();

                                    for (LineageInfo lineageinfo : lineageLists) {
                                        if (lineageinfo.depth == (depth - 1)) {
                                            LineageInfo newinfo = new LineageInfo();
                                            newinfo.setColumn(lineageinfo.getColumn());
                                            newinfo.setTable(lineageinfo.getTable());
                                            newinfo.setTableAlias(lineageinfo.getTableAlias());
                                            newinfo.setColumnAlias(lineageinfo.getColumnAlias());
                                            newinfo.setDepth(depth);
                                            newinfo.setSchema(lineageinfo.getSchema());

                                            newinfos.add(newinfo);

                                        }

                                    }

                                    lineageLists.addAll(newinfos);


                                }

                            }

                        } else if (selectItem instanceof SelectExpressionItem) {

                            String col_name = null;
                            String table_name = null;
                            String schema_name = null;
                            LineageInfo lineageinfo = new LineageInfo();

                            //expression 경우 alias 를 여기서 떼어야 할거 같은데...
                            if (((SelectExpressionItem) selectItem).getExpression().getClass().getSimpleName().equals("Column")) {
                                col_name = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getColumnName();

                                if (((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable() != null) {
                                    table_name = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getName();
                                    schema_name = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getSchemaName();

                                    //테이블 이름이 alias 명인지 체크
                                    if (metadata.checkTable(sources, table_name) == null) {
                                        MetadataInfo tableInfo = metadata.checkTableAlias(sources, table_name);
                                        if (tableInfo != null) {
                                            schema_name = tableInfo.getSchema();
                                            //schema = tableInfo.getSchema() != null ? tableInfo.getSchema() : defalutSchema;;
                                            table_name = tableInfo.getTable();
                                        }
                                    }

                                }

                                //TODO : 컬럼 정보가 MDM에 있는지 없는지 체크
                                if (getMetadataByMDM(col_name, schema_name, table_name, SearchType.Column).size() > 0) {

                                    //schema 정보가 문제임
                                    //TODO : checkTable 새로 만들어야 함 getMetadataByMDM 결과가 리스트로 옮
                                    MetadataInfo table = metadata.checkTable(sources, schema_name, getMetadataByMDM(col_name, schema_name, table_name, SearchType.Table));
                                    if (table != null) {
                                        lineageinfo.setTable(table.getTable());
                                        lineageinfo.setTableAlias(table.getTableAlias());
                                        if (schema_name != null) {
                                            lineageinfo.setSchema(schema_name);
                                        } else if (table.getSchema() != null) {
                                            lineageinfo.setSchema(table.getSchema());
                                        } else {
                                            lineageinfo.setSchema(defalutSchema);
                                        }

                                    }

                                    lineageinfo.setColumn(col_name);

                                    if (((SelectExpressionItem) selectItem).getAlias() != null) {
                                        lineageinfo.setColumnAlias(((SelectExpressionItem) selectItem).getAlias().getName());
                                    }

                                    lineageinfo.setDepth(depth);


                                } else { //TODO:metadata에 정보가 없는 경우..... 이전 쿼리에서 정보가 있는 경우라고 볼수 있음
                                    //TODO: lineagelist 에 정보가 있는지 체크
                                    for (LineageInfo colinfo : lineageLists) {
                                        if ((colinfo.getColumn() != null && colinfo.getColumn().equals(col_name)) ||
                                                (colinfo.getColumnAlias() != null && colinfo.getColumnAlias().equals(col_name))) {
                                            //TODO: colinfo.getColumn() or col_name 중에 어느것이 더 나은지 결정해야 함
                                            //lineageinfo.setColumn(colinfo.getColumn());
                                            lineageinfo.setColumn(col_name);
                                            lineageinfo.setTable(colinfo.getTable());
                                            lineageinfo.setTableAlias(colinfo.getTableAlias());
                                            lineageinfo.setColumnAlias(colinfo.getColumnAlias());
                                            lineageinfo.setSchema(colinfo.getSchema());
                                            lineageinfo.setDepth(depth);

                                        }

                                    }

                                    //이전 쿼리에도 정보가 없는 경우 그냥 테이블과 컬럼 정보 set
                                    if( lineageinfo.getColumn() == null && lineageinfo.getTable() == null){
                                        lineageinfo.setColumn(col_name);
                                        if(table_name !=null) {
                                            lineageinfo.setTable(table_name);
                                        }
                                        if(((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable() != null &&
                                                ((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getAlias()!=null) {
                                            lineageinfo.setTableAlias(((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getAlias().getName());
                                        }


                                        if(((SelectExpressionItem) selectItem).getAlias() != null) {
                                            lineageinfo.setColumnAlias(((SelectExpressionItem) selectItem).getAlias().getName());
                                        }
                                        lineageinfo.setSchema(schema_name);
                                        lineageinfo.setDepth(depth);
                                    }
                                }


                            } else if (((SelectExpressionItem) selectItem).getExpression().getClass().getSimpleName().equals("Function")) {
                                //TODO: expression 자체를 넣어야 하는가?... 컬럼만 사용해야 하는가... 컬럼을 뗴어 내는 것은 추후 고려
                                col_name = ((SelectExpressionItem) selectItem).getExpression().toString();
                                lineageinfo.setColumn(col_name);

                                if (((SelectExpressionItem) selectItem).getAlias() != null) {
                                    lineageinfo.setColumnAlias(((SelectExpressionItem) selectItem).getAlias().getName());
                                }

                                lineageinfo.setDepth(depth);
                                lineageinfo.setTable(null);
                                lineageinfo.setTableAlias(null);
                                lineageinfo.setSchema(null);

                            } else { //selectbody...and so on
                                col_name = ((SelectExpressionItem) selectItem).getExpression().toString();
                                lineageinfo.setColumn(col_name);

                                if (((SelectExpressionItem) selectItem).getAlias() != null) {
                                    lineageinfo.setColumnAlias(((SelectExpressionItem) selectItem).getAlias().getName());
                                }

                                lineageinfo.setDepth(depth);
                                lineageinfo.setTable(null);
                                lineageinfo.setTableAlias(null);
                                lineageinfo.setSchema(null);

                            }

                            lineageLists.add(lineageinfo);
                        }

                    }


                    depth++;
                    listsize--;
                }


                printLineagelist(lineageLists);
                return lineageLists;


            }


        } catch (Exception e) {
            logger.info(e.getMessage());
        }


        return lineageLists;
    }


    private void setLineageLists(ArrayList <String> cols, String table, int depth, String schema) {

        String defaultSchema = null;
        try {
            SQLConfiguration sqlConfiguration = new SQLConfiguration();
            defaultSchema = sqlConfiguration.get("defalut_schema");


        } catch (Exception e) {
            e.printStackTrace();
        }


        for (String col : cols) {
            LineageInfo lineageinfo = new LineageInfo();

            lineageinfo.setTable(table);
            lineageinfo.setColumn(col);
            lineageinfo.setDepth(depth);
            if (schema != null) {
                lineageinfo.setSchema(schema);
            } else {

                lineageinfo.setSchema(defaultSchema);
            }
            this.lineageLists.add(lineageinfo);
        }

    }

    private String getSQL() {
        //String sql = "select * from test3, test2, ( select a , b , e from test1, test2) k";
        //String sql = "select test3.* from polaris.test3, polrais.test2";
        //String sql = "select a,b,c from test3, test2, ( select a , b , e from test1, test2) k";
        //String sql = "select t1.a ,t2.d from test1 t1,test2 t2";
        //TODO: join 구문 테이블 리스트 확인해 볼 필요 있음
        //String sql = "select * from test1 join test2 on test1.a = test2.d";
        //TODO : 구문 파싱 체크 해야 함
        //String sql = "select COUNT from (SELECT g, (SELECT COUNT(b) as cnt FROM test1 o WHERE o.a=k.g) COUNT FROM test3 k)";
        //String sql = "SELECT g, (SELECT COUNT(*) as cnt FROM test1 o WHERE o.a=k.g) COUNT FROM test3 k";
        //String sql = "select cnt from (select count(*) as cnt from test1)"
        // String sql = "SELECT a, c FROM test1 WHERE b IN (SELECT a FROM test1 WHERE b = 'MA2100')";

        //metatron mdm
        // test1 : users, test2: workspace , test3: roles
//        String sql = "select * from users, roles, ( select user_name , ws_pub_type , user_full_name from workspace, users) k"; //pass
//        String sql = "select * from roles, users"; //pass
//        String sql = "select user_name,user_full_name,role_name from roles, workspace, ( select user_name , user_full_name , ws_pub_type from users, workspace) k"; //pass
//        String sql = "select t1.user_name ,t2.ws_pub_type from users t1, workspace t2"; //pass
//        String sql = "select * from users join workspace on users.id = workspace.id"; //pass
//        String sql = "SELECT role_name, (SELECT COUNT(*) as cnt FROM users o WHERE o.id=k.id) COUNT FROM roles k"; //pass
//        String sql = "select cnt from (select count(*) as cnt from users)"; //pass
//        String sql = "SELECT user_status, user_name FROM users WHERE user_name IN (SELECT user_full_name FROM users WHERE user_full_name = 'MA2100')"; //pass

//               String sql ="SELECT * FROM polaris.context";
//        String sql ="SELECT * FROM polaris_dev.dashboard";
//        String sql ="SELECT * FROM polaris.audit";
//        String sql ="SELECT abc FROM information_schema.CLIENT_STATISTICS"; //fail(없는 테이블)
//        String sql ="select * from datasources"; //fail (없는 테이블 )
//        String sql ="SELECT * FROM polaris_dev.mdm_metadata_popularity";
//        String sql ="SELECT dc_database FROM polaris_dev.dataconnection where id = '0c8633f8-03ed-4790-836f-bc5e93f95f60'";
//        String sql = "SELECT B.TBL_ID, (A.PARAM_VALUE * 1) as numRows\n" +
//                "FROM hive.PARTITION_PARAMS AS A, hive.PARTITIONS AS B\n" +
//                "WHERE A.PARAM_KEY='numRows'\n" +
//                "  AND A.PART_ID=B.PART_ID\n" +
//                "order by numRows desc";
//        String sql= "select a.book_name, a.id, a.type, b.descendant as child, b.edpth from book a\n" +
//                "join book_tree b\n" +
//                "on b.book_ancestor = a.id";

        String sql="SELECT description as d FROM information_schema.CHARACTER_SETS";

//        String sql ="SELECT A.PART_ID,\n" +
//                "   B.part_name,\n" +
//                "   A.numRows,\n" +
//                "   A.totalSize,\n" +
//                "   B.create_time,\n" +
//                "   FROM_UNIXTIME(B.create_time)\n" +
//                "FROM \n" +
//                " (SELECT part_id,\n" +
//                "   (SELECT param_value FROM PARTITION_PARAMS WHERE param_key = 'numRows' AND part_id = t1.part_id) AS numRows, \n" +
//                "   (SELECT param_value FROM PARTITION_PARAMS WHERE param_key = 'totalSize' AND part_id = t1.part_id) AS totalSize\n" +
//                "   FROM PARTITION_PARAMS AS t1\n" +
//                "   WHERE t1.PART_ID IN \n" +
//                "    (SELECT PART_ID\n" +
//                "    FROM PARTITIONS\n" +
//                "    WHERE TBL_ID =\n" +
//                "     (SELECT A.TBL_ID\n" +
//                "     FROM TBLS AS A, DBS AS B\n" +
//                "     WHERE A.DB_ID = B.DB_ID\n" +
//                "       AND B.NAME = 'default'\n" +
//                "       AND A.TBL_NAME = 'sample_ingestion_partitions' ) )\n" +
//                "     GROUP BY part_id ) AS A, PARTITIONS AS B\n" +
//                "WHERE A.PART_ID=B.PART_ID\n" +
//                "ORDER BY b.PART_NAME desc";

        return sql;
    }

    private void printLineagelist(ArrayList <LineageInfo> lineageLists) {

        for (LineageInfo lineage : lineageLists)
            System.out.println("LineageInfo : " + lineage.toString());


    }

    //db에서 불러 오기
    private ArrayList <String> getMetadataByMDM(String colName, String schemaName, String tableName, SearchType type)
            throws ClassNotFoundException, SQLException {
        logger.info("Read Meata Info From MDM");

        Connection conn = null;
        java.sql.Statement stmt = null; //jsqlparser type과 duplicate
        ResultSet rs = null;

        ArrayList <String> results = new ArrayList <String>();

        try {
            SQLConfiguration sqlConfiguration = new SQLConfiguration();
            String url = sqlConfiguration.get("metatron.metastore.url");
            String userName = sqlConfiguration.get("metatron.metastore.username");
            String password = sqlConfiguration.get("metatron.metastore.password");
            String jdbcDriver = sqlConfiguration.get("metatron.metastore.driver");

            //load jdbc class
            Class.forName(jdbcDriver);
            conn = DriverManager.getConnection(url, userName, password);

            //make query string
            StringBuilder sb = new StringBuilder();
            ;
            sb.append(sqlConfiguration.get("metadata_search_query"));

            sb.append(" where ");

            //스키마 정보가 없을때는 디폴트 스키마를 set 해서 사용
            if (schemaName != null) {
                sb.append("b.meta_schema='" + schemaName + "'");
            } else {
                sb.append("b.meta_schema='" + sqlConfiguration.get("defalut_schema") + "'");

            }

            //column 정보가 있을 때
            if (colName != null) {
                sb.append(" and ");
                sb.append("a.column_name='" + colName + "'");
            }

            //table은  null 이 오면 안됨.
            if (tableName != null) {
                sb.append(" and ");
                sb.append("b.meta_name='" + tableName + "'");
            }

            String query = sb.toString();

            //table 찾을
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);

            while (rs.next()) {
                if (type == SearchType.Column)
                    results.add(rs.getString("column_name"));
                else if (type == SearchType.Table)
                    results.add(rs.getString("meta_name"));
                //System.out.println(rs.getString("column_name") + ", "+rs.getString("meta_name"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                }
                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                }
                stmt = null;
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ex) {
                }
                conn = null;
            }
        }

        return results;

    }


    public enum SearchType {
        //
        Table,
        Column,
        Schema
    }


}
