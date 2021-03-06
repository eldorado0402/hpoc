package com.metatron.popularity;

import com.metatron.sqlstatics.QueryParser;
import com.metatron.util.SQLConfiguration;
import com.metatron.util.ColumnsCollector;
import com.metatron.util.LineageInfosCollector;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class GetLineage {

    final static String defalutSchema = "polaris_dev";
    private static final Logger logger = LoggerFactory.getLogger(GetLineage.class);
    private ArrayList <LineageInfo> lineageLists = new ArrayList <LineageInfo>();
    private static Connection conn = null;

    public ArrayList <LineageInfo> makeLineageInfos(String sql) {
        //for test
        //sql = getSQL();

        Statement statement;
        QueryParser parser = new QueryParser();
        MetadataInfo metadata = new MetadataInfo();

        //System.out.println("query : " + sql);

        try {
            statement = CCJSqlParserUtil.parse(sql);
            conn = getMDMConnection();

            if (parser.getSqlType(sql, statement) == QueryParser.SqlType.SELECT) {

                LineageInfosCollector info = new LineageInfosCollector();
                ArrayList <PlainSelect> selectLists = info.getSelectList(statement);
                //r각자 accept 구문이 있어서 그런데 accept 구분을 한개로 통일하던가 아니면 아래처럼 새로 객체를 생성해야 함
                LineageInfosCollector subinfo = new LineageInfosCollector();
                ArrayList <SubSelect> subSelectLists = subinfo.getSubSelectLists(statement);
                //System.out.println(subSelectLists);

                int listsize = selectLists.size();

                int depth = 0;

                while (listsize > 0) {
                    PlainSelect selectList = selectLists.get(listsize - 1);
                    //get select list
                    List <SelectItem> list = selectList.getSelectItems();

                    //System.out.println(selectList);
                    //sub query depth
                    String selectAlias = getSelectAliasName(subSelectLists, selectList.toString());

                    //get source table
                    LineageInfosCollector tablesInfoFinder = new LineageInfosCollector();
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
                                Table table = ((AllTableColumns) selectItem).getTable();

                                addAllColumnsLineageListByTableType(sources, table, selectAlias, depth, selectItem);
                            } else if (selectItem instanceof AllColumns) {
                                if (lineageLists.size() == 0) {
                                    //select 문이 없는 최하위 select 문 이므로 소스 테이블의 모든 컬럼을 추가
                                    //mdm에서 조회해 오도록 변경
                                    for (MetadataInfo source : sources) {
                                        String table = source.getTable();
                                        String schema = source.getSchema();

                                        searchAllColumnsAndAddLineageList(schema, table, selectAlias, depth, selectItem);
                                    }
                                } else { //* 을 선택하고 최하위 select 문이 아닌 경우

                                    if (selectList.getFromItem() != null) {
                                        if (selectList.getFromItem() instanceof Table) {
                                            //from 테이블의 meta를 추가함
                                            Table table = (Table) selectList.getFromItem();
                                            addAllColumnsLineageListByTableType(sources, table, selectAlias, depth, selectItem);
                                        }
                                    }

                                    if (selectList.getJoins() != null) {
                                        //System.out.println(selectList.getJoins().toString());
                                        for (Join join : selectList.getJoins()) {
                                            if (join.getRightItem() != null) {
                                                //join.getRightItem() 가 table 형태
                                                if (join.getRightItem() instanceof Table) {
                                                    Table table = (Table) join.getRightItem();
                                                    addAllColumnsLineageListByTableType(sources, table, selectAlias, depth, selectItem);
                                                }
                                            }
                                        }

                                    }
                                    //이전 depth 의 lineage 의 select 구문 모두
                                    addPreviousLineageList(depth, selectItem, selectAlias);
                                }

                            }

                        } else if (selectItem instanceof SelectExpressionItem) {

                            String col_name = null;
                            String tableName = null;
                            String tableAliasName = null;
                            String schemaName = null;
                            String colAliasName = null;
                            TableNameType type = null;
                            //LineageInfo lineageinfo = new LineageInfo();

                            //expression 경우 alias 를 여기서 떼어야 할거 같은데...
                            if (((SelectExpressionItem) selectItem).getExpression() instanceof Column) {
                                col_name = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getColumnName();

                                if (((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable() != null) {
                                    tableName = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getName();
                                    schemaName = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getSchemaName();
                                    //테이블 이름이 alias 명인지 체크
                                    type = getTableNameType(sources, tableName);

                                }
                                //TODO : 컬럼 정보가 MDM에 있는지 없는지 체크
                                if ((type == TableNameType.Table || type == TableNameType.TableAlias)
                                        && getMetadataByMDM(col_name, schemaName, tableName, SearchType.Column,conn).size() > 0) {

                                    MetadataInfo table = null;
                                    //schema 정보가 문제임
                                    //TODO : checkTable 새로 만들어야 함 getMetadataByMDM 결과가 리스트로 옮
                                    //TODO: 여기 로직이 좀 이상함.....TableAlias 일때
                                    if (type == TableNameType.Table) {
                                        table = metadata.checkTable(sources, schemaName, getMetadataByMDM(col_name, schemaName, tableName, SearchType.Table, conn));
                                    } else if (type == TableNameType.TableAlias) {
                                        MetadataInfo tableAlias = metadata.checkTableAlias(sources, tableName);
                                        table = metadata.checkTable(sources, tableAlias.getSchema(), getMetadataByMDM(col_name, tableAlias.getSchema(), tableAlias.getTable(), SearchType.Table, conn));
                                    }
                                    //set table & schema
                                    if (table != null) {
                                        tableName = table.getTable();
                                        tableAliasName = table.getTableAlias();
                                        schemaName = schemaName != null ? schemaName : table.getSchema();
                                    }

                                    this.lineageLists.add(setLineageInfo(schemaName, tableName, tableAliasName, col_name, depth, selectAlias, selectItem));

                                } else { //TODO:metadata에 정보가 없는 경우..... 이전 쿼리에서 정보가 있는 경우라고 볼수 있음
                                    //TODO: lineagelist 에 정보가 있는지 체크
                                    boolean previousExistFlag = false;

                                    ArrayList<LineageInfo> newinfos = new ArrayList<LineageInfo>();

                                    for (LineageInfo colinfo : lineageLists) {
                                        if (type == TableNameType.SelectAlias && tableName.equalsIgnoreCase(colinfo.getSelectAlias()) &&
                                                ((colinfo.getColumn() != null && colinfo.getColumn().equalsIgnoreCase(col_name)) ||
                                                (colinfo.getColumnAlias() != null && colinfo.getColumnAlias().equalsIgnoreCase(col_name)))) {

                                            newinfos.add(setLineageInfo(colinfo.getSchema(), colinfo.getTable(), null, col_name, depth, selectAlias, selectItem));
                                            previousExistFlag = true;

                                        } else if (type == TableNameType.Table && tableName.equalsIgnoreCase(colinfo.getTable()) &&
                                                ((colinfo.getColumn() != null && colinfo.getColumn().equalsIgnoreCase(col_name)) ||
                                                (colinfo.getColumnAlias() != null && colinfo.getColumnAlias().equalsIgnoreCase(col_name)))) {
                                            //TODO: colinfo.getColumn() or col_name 중에 어느것이 더 나은지 결정해야 함
                                            //lineageinfo.setColumn(colinfo.getColumn());
                                            newinfos.add(setLineageInfo(colinfo.getSchema(), colinfo.getTable(), colinfo.getTableAlias(), col_name, depth, selectAlias, selectItem));
                                            previousExistFlag = true;
                                        }

                                    }

                                    //이전 쿼리에도 정보가 없는 경우 그냥 테이블과 컬럼 정보 set
                                    if (previousExistFlag == false) {

                                        //TODO: 테이블 type에 따라서 테이블 이름이 다름
                                        if (tableName != null) {
                                            MetadataInfo table = null;
                                            type = getTableNameType(sources, tableName);
                                            //TODO: 테이블이 alias 일수도 있음...
                                            if (type == TableNameType.Table) {
                                                schemaName = metadata.checkTable(sources, schemaName, tableName).getSchema();
                                                tableName = metadata.checkTable(sources, schemaName, tableName).getTable();
                                            } else if (type == TableNameType.TableAlias) {
                                                schemaName = metadata.checkTableAlias(sources, tableName).getSchema();
                                                tableName = metadata.checkTableAlias(sources, tableName).getTable();
                                            } else if(type == TableNameType.SelectAlias){
                                                for (LineageInfo lineageinfo : lineageLists) {
                                                    if (lineageinfo.getSelectAlias() != null && lineageinfo.getSelectAlias().equalsIgnoreCase(tableName)) {
                                                        tableName = lineageinfo.getTable();
                                                        schemaName = lineageinfo.getSchema();
                                                    }
                                                };
                                            }

                                        } else {
                                            HashMap <String, String> tableinfo = getFromTableInfo(selectList, sources);
                                            schemaName = tableinfo.get("schemaName");
                                            tableName = tableinfo.get("tableName");
                                        }

                                        if (((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable() != null &&
                                                ((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getAlias() != null) {
                                            tableAliasName = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getAlias().getName();
                                        }
                                        newinfos.add(setLineageInfo(schemaName, tableName, tableAliasName, col_name, depth, selectAlias, selectItem));
                                    }

                                    lineageLists.addAll(newinfos);
                                }

                            } else if (((SelectExpressionItem) selectItem).getExpression() instanceof SubSelect) {
                                //subselect 문은 select 문에 포함되어 있으므로 ....
                                continue;
                            } else {
                                //else if (((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                                //TODO: expression 자체를 넣어야 하는가?... 컬럼만 사용해야 하는가... 컬럼을 뗴어 내는 것은 추후 고려

                                //TODO : Function column 리스트, column에 테이블 정보가 있는지 없는지 체크
                                ColumnsCollector columnCollect = new ColumnsCollector();
                                List <Column> columns = columnCollect.getColumns(((SelectExpressionItem) selectItem).getExpression());

                                ArrayList<LineageInfo> newinfos = new ArrayList<LineageInfo>();
//                                System.out.println("Fuction columns" + columns);
                                //TODO : 컬럼의 이름을 찾음, 여러개를 다 lineage list에 추가해 주어야 함
                                if (columns.size() > 0) {
                                    for (Column column : columns) {
                                        boolean colFindFlag = false;

                                        if(column.getTable()!= null)
                                            type = getTableNameType(sources, column.getTable().getName());
                                        else
                                            type = TableNameType.Empty;

                                        for (LineageInfo colinfo : lineageLists) {
                                            //System.out.println(column.getColumnName() + ", " + colinfo.getColumn() + ", " + colinfo.getColumnAlias());
                                            if (type == TableNameType.Table &&
                                                    ((colinfo.getColumn() != null && colinfo.getColumn().equalsIgnoreCase(column.getColumnName())) ||
                                                    (colinfo.getColumnAlias() != null && colinfo.getColumnAlias().equalsIgnoreCase(column.getColumnName())))) {
                                                //TODO: colinfo.getColumn() or col_name 중에 어느것이 더 나은지 결정해야 함
                                                newinfos.add(setLineageInfo(colinfo.getSchema(), colinfo.getTable(), colinfo.getTableAlias(), colinfo.getColumn()
                                                        ,  depth, selectAlias, selectItem));
                                                colFindFlag = true;

                                                continue;
                                            }
                                        }

                                        if (colFindFlag == false) {
                                            if (column.getTable() != null) {
                                                MetadataInfo table = null;
                                                //type = getTableNameType(sources, column.getTable().getName());
                                                //TODO: 테이블이 alias 일수도 있음...
                                                if (type == TableNameType.Table) {
                                                    tableName = column.getTable().getName();
                                                    schemaName = column.getTable().getSchemaName();
                                                } else if (type == TableNameType.TableAlias) {
                                                    tableName = metadata.checkTableAlias(sources, column.getTable().getName()).getTable();
                                                    schemaName = metadata.checkTableAlias(sources, column.getTable().getName()).getSchema();
                                                } else if(type == TableNameType.SelectAlias){
                                                    for (LineageInfo lineageinfo : lineageLists) {
                                                        if (lineageinfo.getSelectAlias() != null && lineageinfo.getSelectAlias().equalsIgnoreCase(column.getTable().getName())) {
                                                            tableName = lineageinfo.getTable();
                                                            schemaName = lineageinfo.getSchema();
                                                        }
                                                    };
                                                }

                                                tableAliasName = column.getTable().getAlias() != null ?
                                                        column.getTable().getAlias().getName() : null;
                                            } else {
                                                HashMap <String, String> tableinfo = getFromTableInfo(selectList, sources);
                                                schemaName = tableinfo.get("schemaName");
                                                tableName = tableinfo.get("tableName");
                                            }

                                            newinfos.add(setLineageInfo(schemaName, tableName, tableAliasName, column.getColumnName(), depth, selectAlias, selectItem));
                                        }

                                    }
                                    lineageLists.addAll(newinfos);
                                    continue; // 요기 처리는 끝남 .lineageinfo 는 null값이라서 더해 줄게 없음


                                } else { //Fuction 을 그냥 씀.
                                    col_name = ((SelectExpressionItem) selectItem).getExpression().toString();

                                    HashMap <String, String> tableinfo = getFromTableInfo(selectList, sources);
                                    schemaName = tableinfo.get("schemaName");
                                    tableName = tableinfo.get("tableName");

                                    lineageLists.add(setLineageInfo(schemaName, tableName, null, col_name, depth, selectAlias, selectItem));
                                }
                            }

                            //lineageLists.add(lineageinfo);
                        }

                    }

                    depth++;
                    listsize--;
                }

                //printLineagelist(lineageLists);
                return lineageLists;

            }


        } catch (Exception e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        } finally{
            if( conn != null){
                try {
                    conn.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

            conn = null;
        }


        return lineageLists;
    }


    private void setLineageLists(ArrayList <String> cols, String table, int depth, String schema, String expression, String selectAlias) {

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
            lineageinfo.setExpression(expression);
            lineageinfo.setSelectAlias(selectAlias);

            this.lineageLists.add(lineageinfo);
        }

    }

    //하나만 추가할 때
    private LineageInfo setLineageInfo(String schema, String table, String tableAlias, String column, int depth, String selectAlias, SelectItem selectItem) {
        LineageInfo lineageinfo = new LineageInfo();

        lineageinfo.setSchema(schema);
        lineageinfo.setTable(table);
        lineageinfo.setTableAlias(tableAlias);
        lineageinfo.setColumn(column);

        if (((SelectExpressionItem) selectItem).getAlias() != null)
            lineageinfo.setColumnAlias(((SelectExpressionItem) selectItem).getAlias().getName());

        lineageinfo.setDepth(depth);
        lineageinfo.setSelectAlias(selectAlias);
        lineageinfo.setExpression(selectItem.toString());

        return lineageinfo;

        //this.lineageLists.add(lineageinfo);

    }


    private HashMap <String, String> getFromTableInfo(PlainSelect selectList, ArrayList <MetadataInfo> sources) {
        HashMap <String, String> tableInfo = new HashMap <String, String>();
        MetadataInfo metadata = new MetadataInfo();
        String tableName = null;
        String schemaName = null;

        if (selectList.getFromItem() != null) {
            if (selectList.getFromItem() instanceof Table) {
                TableNameType fromType = getTableNameType(sources, ((Table) selectList.getFromItem()).getName());

                if (fromType == TableNameType.Table) {
                    tableName = metadata.checkTable(sources, ((Table) selectList.getFromItem()).getName()).getTable();
                    schemaName = metadata.checkTable(sources, ((Table) selectList.getFromItem()).getName()).getSchema();
                } else if (fromType == TableNameType.TableAlias) {
                    tableName = metadata.checkTableAlias(sources, ((Table) selectList.getFromItem()).getName()).getTable();
                    schemaName = metadata.checkTableAlias(sources, ((Table) selectList.getFromItem()).getName()).getSchema();
                }
            } else {
                tableName = sources.get(0).getTable();
                schemaName = sources.get(0).getSchema();
            }

        }

        tableInfo.put("schemaName", schemaName);
        tableInfo.put("tableName", tableName);

        return tableInfo;
    }


    private void printLineagelist(ArrayList <LineageInfo> lineageLists) {

        for (LineageInfo lineage : lineageLists)
            System.out.println("LineageInfo : " + lineage.toString());


    }

    //db에서 불러 오기
    private ArrayList <String> getMetadataByMDM(String colName, String schemaName, String tableName, SearchType type, Connection conn)
            throws ClassNotFoundException, SQLException {
        //TODO : connection 정보 수정
        logger.info("Read Meata Info From MDM");

        java.sql.Statement stmt = null; //jsqlparser type과 duplicate
        ResultSet rs = null;

        ArrayList <String> results = new ArrayList <String>();

        try {
            SQLConfiguration sqlConfiguration = new SQLConfiguration();

            //make query string
            StringBuilder sb = new StringBuilder();

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
                rs.close();
            }

            if (stmt != null) {
                stmt.close();
            }
        }

        return results;

    }

    private void addPreviousLineageList(int depth, SelectItem selectItem, String selectAlias) {
        ArrayList <LineageInfo> newinfos = new ArrayList <LineageInfo>();

        for (LineageInfo lineageinfo : lineageLists) {
            if (lineageinfo.depth == (depth - 1)) {
                LineageInfo newinfo = new LineageInfo();
                newinfo.setColumn(lineageinfo.getColumn());
                newinfo.setTable(lineageinfo.getTable());
                //newinfo.setTableAlias(lineageinfo.getTableAlias());
                newinfo.setDepth(depth);
                newinfo.setSchema(lineageinfo.getSchema());
                newinfo.setExpression(selectItem.toString());
                newinfo.setSelectAlias(selectAlias);

                newinfos.add(newinfo);
            }
        }
        this.lineageLists.addAll(newinfos);
    }

    private void addAllColumnsLineageListByTableType(ArrayList <MetadataInfo> sources, Table table, String selectAlias, int depth, SelectItem selectItem) {
        MetadataInfo metadata = new MetadataInfo();
        String tableName = table.getName();
        String schemaName = null;
        TableNameType type = getTableNameType(sources, tableName);

        if (type == TableNameType.SelectAlias) {
            ArrayList <LineageInfo> searchLists = searchSelectList(selectItem, tableName, selectAlias, depth);
            if (searchLists.size() > 0) {
                lineageLists.addAll(searchSelectList(selectItem, tableName, selectAlias, depth));
            }
        } else {
            if (type == TableNameType.Table) {
                schemaName = table.getSchemaName() != null
                        ? table.getSchemaName() : metadata.searchSchemaName(sources, tableName);

            } else if (type == TableNameType.TableAlias) {
                MetadataInfo tableInfo = metadata.checkTableAlias(sources, tableName);
                tableName = tableInfo.getTable();
                schemaName = tableInfo.getSchema();
            }

            searchAllColumnsAndAddLineageList(schemaName, tableName, selectAlias, depth, selectItem);
        }
    }

    private void searchAllColumnsAndAddLineageList(String schema, String table, String selectAlias, int depth, SelectItem selectItem) {
        //테이블과 db명으로 컬럼 정보를 가지고 옮
        try {
            ArrayList <String> cols = getMetadataByMDM(null, schema, table, SearchType.Column,getMDMConnection());

            //TODO: 테이블 이름인지 ... 테이블 alias 인지... 아니면 이전 서브 쿼리의 alias 인지 확인해야 함
            if (cols.size() > 0) {
                setLineageLists(cols, table, depth, schema, selectItem.toString(), selectAlias);
            } else { // mdm 에 컬럼 정보가 없으면 "*"을 컬럼으로 그냥 넣어 줌
                cols.add("*");
                setLineageLists(cols, table, depth, schema, selectItem.toString(), selectAlias);
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("searchAllColumnsAndAddLineageList : " + e.getMessage());
        }


    }


    private String getSelectAliasName(ArrayList <SubSelect> subSelectLists, String selectList) {
        String selectAlias = null;
        for (SubSelect subSelect : subSelectLists) {
            if (selectList.equals(subSelect.getSelectBody().toString())) {
                if (subSelect.getAlias() != null) {
                    selectAlias = subSelect.getAlias().getName();
                    return selectAlias;
                }
            }
        }
        return null;
    }

    //테이블 이름이 테이블 이름인지, 테이블 Alias 이름인지, SelectAlias인지 체크
    private TableNameType getTableNameType(ArrayList <MetadataInfo> sources, String tableName) {
        TableNameType type = TableNameType.Empty;
        MetadataInfo metadata = new MetadataInfo();

        if (metadata.checkTable(sources, tableName) != null) {
            type = TableNameType.Table;
        } else if (metadata.checkTableAlias(sources, tableName) != null) {
            type = TableNameType.TableAlias;
        } else {
            for (LineageInfo lineageinfo : lineageLists) {
                if (lineageinfo.getSelectAlias() != null && lineageinfo.getSelectAlias().equalsIgnoreCase(tableName)) {
                    type = TableNameType.SelectAlias;
                }
            }
        }
        return type;
    }

    //전체 리니지 리스트에서 alias 로 체크해서 select 리스트를 찾아서 리니지 리스트에 추가 함
    private ArrayList <LineageInfo> searchSelectList(SelectItem selectItem, String tableName, String selectAlias, int depth) {
        ArrayList <LineageInfo> newinfos = new ArrayList <LineageInfo>();
        for (LineageInfo lineageinfo : lineageLists) {
            if (lineageinfo.getSelectAlias() != null && lineageinfo.getSelectAlias().equalsIgnoreCase(tableName)) {
                LineageInfo newinfo = new LineageInfo();

                newinfo.setColumn(lineageinfo.getColumn());
                newinfo.setTable(lineageinfo.getTable());
                newinfo.setDepth(depth);
                newinfo.setSchema(lineageinfo.getSchema());
                newinfo.setExpression(selectItem.toString());
                newinfo.setSelectAlias(selectAlias);

                newinfos.add(newinfo);

            }
        }
        return newinfos;
    }

    private Connection getMDMConnection(){

        try {

            if(conn == null) {
                SQLConfiguration sqlConfiguration = new SQLConfiguration();
                String url = sqlConfiguration.get("metatron.metastore.url");
                String userName = sqlConfiguration.get("metatron.metastore.username");
                String password = sqlConfiguration.get("metatron.metastore.password");
                String jdbcDriver = sqlConfiguration.get("metatron.metastore.driver");

                //load jdbc class
                Class.forName(jdbcDriver);
                conn = DriverManager.getConnection(url, userName, password);
            }

        }catch(Exception e){
            e.printStackTrace();
        }

        return conn;
    }

    public enum SearchType {
        //
        Table,
        Column,
        Schema
    }

    private enum TableNameType {
        //
        Table,
        TableAlias,
        SelectAlias,
        Empty
    }


}
