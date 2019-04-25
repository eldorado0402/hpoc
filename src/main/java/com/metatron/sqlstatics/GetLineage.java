package com.metatron.sqlstatics;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class GetLineage {

    private static final Logger logger = LoggerFactory.getLogger(GetLineage.class);
    // private ArrayList <LineageInfo> lineageInfos = new ArrayList <LineageInfo>();
    private ArrayList <LineageInfo> lineageLists = new ArrayList <LineageInfo>();
    private HashMap <String, String> metadatas = new HashMap <String, String>(); // col_name, table_name
    private String sql;

    public static ArrayList <String> getKey(HashMap <String, String> m, Object value) {
        ArrayList <String> cols = new ArrayList <String>();
        for (Object o : m.keySet()) {
            if (m.get(o).equals(value)) {
                cols.add(o.toString());
                //return o;
            }
        }
        return cols;
    }

//    public void setOriginMetadatas(ArrayList<String> columns){
//
//
//    }

    public void setOriginMetadatas(HashMap <String, String> metadatas) {
        this.metadatas = metadatas;

    }

    public void setOriginMetadatas(String table) {
        this.metadatas.put(table, null);
    }

    public void makeLineageInfos(String sql) {
        //for test
        sampleMetadatas();
        sql = getSQL();


        Statement statement;
        QueryParser parser = new QueryParser();

        try {
            statement = CCJSqlParserUtil.parse(sql);
            if (parser.getSqlType(sql, statement) == QueryParser.SqlType.SELECT) {
                QueryInfo info = new QueryInfo();
                ArrayList <PlainSelect> selectLists = info.getSelectList(statement);

                int listsize = selectLists.size();

                //ArrayList <LineageInfo> lineageLists = new ArrayList <LineageInfo>();
                int depth = 0;

                while (listsize > 0) {
                    PlainSelect selectList = selectLists.get(listsize - 1);
                    //get select list
                    List <SelectItem> list = selectList.getSelectItems();

                    //sub query depth

                    //get source table
                    QueryInfo tablesInfoFinder = new QueryInfo();
                    HashMap <String, String> sources = tablesInfoFinder.getTableInfoList(CCJSqlParserUtil.parse(selectList.toString()));
                    //from table, join table
                    //TODO: from 절에 있는 테이블의 alias를 가지고 와야 함

                    // LineageInfo lineageinfo = new LineageInfo();

                    for (SelectItem selectItem : list) {

                        //check column's table name
                        if (selectItem instanceof AllTableColumns || selectItem instanceof AllColumns) {
                            //TODO:이전 리지니 정보가 있으면 다 가지고 오던가... 아니면 메타의 테이블 정보를 다 긁어 보던가 둘중에 한가지...
                            //이전 리니지 정보는 이미 저장되어 있으므로 다시 가지고 올 필요는 없어 보임...

                            if (selectItem instanceof AllTableColumns) {
                                String table = ((AllTableColumns) selectItem).getTable().getName();
                                //TODO: 테이블 이름인지 ... 테이블 alias 인지... 아니면 이전 서브 쿼리의 alias 인지 확인해야 함
                                ArrayList <String> cols = getKey(metadatas, table);
                                setLineageLists(cols, table, depth);

                            } else if (selectItem instanceof AllColumns) {
                                if (lineageLists.size() == 0) {
                                    //select 문이 없는 최하위 select 문 이므로 소스 테이블의 모든 컬럼을 추가
                                    for (Object tableKey : sources.keySet()) {

                                        String table = tableKey.toString();
                                        ArrayList <String> cols = getKey(metadatas, table);
                                        setLineageLists(cols, table, depth);

                                    }

                                } else {
//                                    System.out.println(selectList.getFromItem());

                                    if (selectList.getFromItem() != null) {
                                        //TODO: from 절에 있는 테이블 정보만 빼올 수 있는 방법?!... from 에 테이블이 있는 경우와 없는 경우가 있음
                                        try { //selectList.getFromItem() 이 table 이 아닐때를 위해서.. ( 다른 방법은 없나 확인 필요)
                                            if (((Table) selectList.getFromItem()) != null) {
                                                //from 테이블의 meta를 추가함
                                                String table = ((Table) selectList.getFromItem()).getName();
                                                ArrayList <String> cols = getKey(metadatas, table);
                                                setLineageLists(cols, table, depth);

                                            }
                                        } catch (Exception e) {
                                            System.out.println("error : " + e + ",fromItem : " + selectList.getFromItem().toString());
                                            continue;
                                        }
                                    }


                                    if (selectList.getJoins() != null) {
                                        System.out.println(selectList.getJoins().toString());
                                        for (Join join : selectList.getJoins()) {
                                            if (join.getRightItem() != null) {
                                                //TODO : join.getRightItem() 가 table 형태가 아닐때 exception 발생
                                                try {
                                                    if (((Table) join.getRightItem()) != null) {
                                                        String table = ((Table) join.getRightItem()).getName();
                                                        ArrayList <String> cols = getKey(metadatas, table);
                                                        setLineageLists(cols, table, depth);
                                                    }
                                                } catch (Exception e) {
                                                    System.out.println("error : " + e + ", join.getRightItem : " + join.getRightItem().toString());
                                                    continue;
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

                                            newinfos.add(newinfo);

                                        }

                                    }

                                    lineageLists.addAll(newinfos);


                                }

                            }

                        } else if (selectItem instanceof SelectExpressionItem) {
//                            if (selectItem instanceof Expression){
//                                (Expression)selectItem.
//                            }

                            String col_name = null;
                            String table_name = null;
                            LineageInfo lineageinfo = new LineageInfo();

                            //TODO : ((SelectExpressionItem) selectItem).getExpression().getClass().getSimpleName() -> select 아이템의 종류를 나타냄
                            //expression 경우 alias 를 여기서 떼어야 할거 같은데...
                            if (((SelectExpressionItem) selectItem).getExpression().getClass().getSimpleName().equals("Column")) {
                                col_name = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getColumnName();

                                if (((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable() != null)
                                    table_name = ((Column) ((SelectExpressionItem) selectItem).getExpression()).getTable().getName();


                                if (metadatas.containsKey(col_name)) { //메타 정보에서 추출하기

                                    //table 정보
                                    if (sources.containsKey(metadatas.get(col_name))) {
                                        lineageinfo.setTable(metadatas.get(col_name));
                                        lineageinfo.setTableAlias(sources.get(metadatas.get(col_name)));
                                    }

                                    //
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
                                            lineageinfo.setDepth(depth);

                                        }

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

                            } else { //selectbody...and so on
                                col_name = ((SelectExpressionItem) selectItem).getExpression().toString();
                                lineageinfo.setColumn(col_name);

                                if (((SelectExpressionItem) selectItem).getAlias() != null) {
                                    lineageinfo.setColumnAlias(((SelectExpressionItem) selectItem).getAlias().getName());
                                }

                                lineageinfo.setDepth(depth);
                                lineageinfo.setTable(null);
                                lineageinfo.setTableAlias(null);

                            }

                            lineageLists.add(lineageinfo);
                        }

                        //System.out.println("lineageinfo : " + lineageinfo.toString());

                    }


                    depth++;
                    listsize--;
                }


                printLineagelist(lineageLists);

                //System.out.println("lineagelist : " + lineageLists.toString());


            }


        } catch (Exception e) {
            logger.info(e.getMessage());
        }


    }


    private void setLineageLists(ArrayList <String> cols, String table, int depth) {

        for (String col : cols) {
            LineageInfo lineageinfo = new LineageInfo();

            lineageinfo.setTable(table);
            lineageinfo.setColumn(col);
            lineageinfo.setDepth(depth);
            this.lineageLists.add(lineageinfo);
        }

    }

    //for test
    private void sampleMetadatas() {
        //table : test1 : a,b,c
        //table : test2 : d,e,f
        //table : test3 : g,h
        this.metadatas.put("a", "test1");
        this.metadatas.put("b", "test1");
        this.metadatas.put("c", "test1");

        this.metadatas.put("d", "test2");
        this.metadatas.put("e", "test2");
        this.metadatas.put("f", "test2");

        this.metadatas.put("g", "test3");
        this.metadatas.put("h", "test3");

    }

    private String getSQL() {
        //String sql = "select * from test3, test2, ( select a , b , e from test1, test2) k";
        //String sql = "select * from test3, test2";
        //String sql = "select a,b,c from test3, test2, ( select a , b , e from test1, test2) k";
        //String sql = "select t1.a ,t2.d from test1 t1,test2 t2";
        //TODO: join 구문 테이블 리스트 확인해 볼 필요 있음
        //String sql = "select * from test1 join test2 on test1.a = test2.d";
        //TODO : 구문 파싱 체크 해야 함
        //String sql = "select COUNT from (SELECT g, (SELECT COUNT(b) as cnt FROM test1 o WHERE o.a=k.g) COUNT FROM test3 k)";
        String sql = "SELECT g, (SELECT COUNT(*) as cnt FROM test1 o WHERE o.a=k.g) COUNT FROM test3 k";
        //String sql = "select cnt from (select count(*) as cnt from test1)";
        return sql;
    }

    private void printLineagelist(ArrayList <LineageInfo> lineageLists) {

        for (LineageInfo lineage : lineageLists)
            System.out.println("LineageInfo : " + lineage.toString());


    }


}
