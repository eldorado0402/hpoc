package com.metatron.util;

import com.metatron.sqlstatics.JoinTablesInfo;
import com.metatron.sqlstatics.QueryParser;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Block;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;


public class JoinTablesCollector implements SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor, SelectItemVisitor, StatementVisitor{

    private static final String NOT_SUPPORTED_YET = "Not supported yet.";
    //private HashMap<String, String> joinTables; // table name, table alias
    //private String targetTable=null;
    private boolean allowColumnProcessing = false;
    //private List<String> otherItemNames;
    private JoinTablesInfo joinTablesInfo;

    public JoinTablesCollector() {
    }

    public JoinTablesInfo getJoinTablesInfo(Statement statement) {
        this.init(false);
        statement.accept(this);
        return this.joinTablesInfo;
    }

    public void visit(Select select) {
        if (select.getWithItemsList() != null) {
            Iterator var2 = select.getWithItemsList().iterator();

            while(var2.hasNext()) {
                WithItem withItem = (WithItem)var2.next();
                withItem.accept(this);
            }
        }

        select.getSelectBody().accept(this);
    }


    public void visit(WithItem withItem) {
        //this.otherItemNames.add(withItem.getName().toLowerCase());
        withItem.getSelectBody().accept(this);
    }

    public void visit(PlainSelect plainSelect) {
        Iterator var2;
        if (plainSelect.getSelectItems() != null) {
            var2 = plainSelect.getSelectItems().iterator();

            while(var2.hasNext()) {
                SelectItem item = (SelectItem)var2.next();
                item.accept(this);
            }
        }

        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
            if(plainSelect.getFromItem() instanceof Table){
                //targetTable = this.extractTableName((Table)plainSelect.getFromItem());
                joinTablesInfo.setTargetTable(this.extractTableName((Table)plainSelect.getFromItem()));
            }
        }

        if (plainSelect.getJoins() != null) {
            var2 = plainSelect.getJoins().iterator();

            while(var2.hasNext()) {
                Join join = (Join)var2.next();
                join.getRightItem().accept(this);

                //add code

                QueryParser.JoinType tableJoinType = checkJoinType(join);
                if(tableJoinType != QueryParser.JoinType.NONE){
                    if(join.getRightItem() instanceof Table){
                        Table tableName = (Table)join.getRightItem();
                        String tableWholeName = this.extractTableName(tableName );
                        HashMap sourceTableInfo = new HashMap();
                        sourceTableInfo.put(tableWholeName, tableJoinType);
                        joinTablesInfo.addSourceTableInfo(sourceTableInfo);
                    }
                }
            }
        }

        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }

        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(this);
        }

        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(this);
        }

    }

    protected String extractTableName(Table table) {
        return table.getFullyQualifiedName();
    }

    public void visit(Table tableName) {

    }

    public void visit(SubSelect subSelect) {
        if (subSelect.getWithItemsList() != null) {
            Iterator var2 = subSelect.getWithItemsList().iterator();

            while(var2.hasNext()) {
                WithItem withItem = (WithItem)var2.next();
                withItem.accept(this);
            }
        }

        subSelect.getSelectBody().accept(this);
    }

    public void visit(Addition addition) {
        this.visitBinaryExpression(addition);
    }

    public void visit(AndExpression andExpression) {
        this.visitBinaryExpression(andExpression);
    }

    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    public void visit(Column tableColumn) {
        if (this.allowColumnProcessing && tableColumn.getTable() != null && tableColumn.getTable().getName() != null) {
            this.visit(tableColumn.getTable());
        }

    }

    public void visit(Division division) {
        this.visitBinaryExpression(division);
    }

    public void visit(DoubleValue doubleValue) {
    }

    public void visit(EqualsTo equalsTo) {
        this.visitBinaryExpression(equalsTo);
    }

    public void visit(Function function) {
        ExpressionList exprList = function.getParameters();
        if (exprList != null) {
            this.visit(exprList);
        }

    }

    public void visit(GreaterThan greaterThan) {
        this.visitBinaryExpression(greaterThan);
    }

    public void visit(GreaterThanEquals greaterThanEquals) {
        this.visitBinaryExpression(greaterThanEquals);
    }

    public void visit(InExpression inExpression) {
        if (inExpression.getLeftExpression() != null) {
            inExpression.getLeftExpression().accept(this);
        } else if (inExpression.getLeftItemsList() != null) {
            inExpression.getLeftItemsList().accept(this);
        }

        inExpression.getRightItemsList().accept(this);
    }

    public void visit(SignedExpression signedExpression) {
        signedExpression.getExpression().accept(this);
    }

    public void visit(IsNullExpression isNullExpression) {
    }

    public void visit(JdbcParameter jdbcParameter) {
    }

    public void visit(LikeExpression likeExpression) {
        this.visitBinaryExpression(likeExpression);
    }

    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    public void visit(LongValue longValue) {
    }

    public void visit(MinorThan minorThan) {
        this.visitBinaryExpression(minorThan);
    }

    public void visit(MinorThanEquals minorThanEquals) {
        this.visitBinaryExpression(minorThanEquals);
    }

    public void visit(Multiplication multiplication) {
        this.visitBinaryExpression(multiplication);
    }

    public void visit(NotEqualsTo notEqualsTo) {
        this.visitBinaryExpression(notEqualsTo);
    }

    public void visit(NullValue nullValue) {
    }

    public void visit(OrExpression orExpression) {
        this.visitBinaryExpression(orExpression);
    }

    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    public void visit(StringValue stringValue) {
    }

    public void visit(Subtraction subtraction) {
        this.visitBinaryExpression(subtraction);
    }

    public void visit(NotExpression notExpr) {
        notExpr.getExpression().accept(this);
    }

    public void visit(BitwiseRightShift expr) {
        this.visitBinaryExpression(expr);
    }

    public void visit(BitwiseLeftShift expr) {
        this.visitBinaryExpression(expr);
    }

    public void visitBinaryExpression(BinaryExpression binaryExpression) {
        binaryExpression.getLeftExpression().accept(this);
        binaryExpression.getRightExpression().accept(this);
    }

    public void visit(ExpressionList expressionList) {
        Iterator var2 = expressionList.getExpressions().iterator();

        while(var2.hasNext()) {
            Expression expression = (Expression)var2.next();
            expression.accept(this);
        }

    }

    public void visit(NamedExpressionList namedExpressionList) {
        Iterator var2 = namedExpressionList.getExpressions().iterator();

        while(var2.hasNext()) {
            Expression expression = (Expression)var2.next();
            expression.accept(this);
        }

    }

    public void visit(DateValue dateValue) {
    }

    public void visit(TimestampValue timestampValue) {
    }

    public void visit(TimeValue timeValue) {
    }

    public void visit(CaseExpression caseExpression) {
        if (caseExpression.getSwitchExpression() != null) {
            caseExpression.getSwitchExpression().accept(this);
        }

        if (caseExpression.getWhenClauses() != null) {
            Iterator var2 = caseExpression.getWhenClauses().iterator();

            while(var2.hasNext()) {
                WhenClause when = (WhenClause)var2.next();
                when.accept(this);
            }
        }

        if (caseExpression.getElseExpression() != null) {
            caseExpression.getElseExpression().accept(this);
        }

    }

    public void visit(WhenClause whenClause) {
        if (whenClause.getWhenExpression() != null) {
            whenClause.getWhenExpression().accept(this);
        }

        if (whenClause.getThenExpression() != null) {
            whenClause.getThenExpression().accept(this);
        }

    }

    public void visit(AllComparisonExpression allComparisonExpression) {
        allComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    public void visit(AnyComparisonExpression anyComparisonExpression) {
        anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    public void visit(SubJoin subjoin) {
        subjoin.getLeft().accept(this);
        Iterator var2 = subjoin.getJoinList().iterator();

        while(var2.hasNext()) {
            Join join = (Join)var2.next();
            join.getRightItem().accept(this);
        }

    }

    public void visit(Concat concat) {
        this.visitBinaryExpression(concat);
    }

    public void visit(Matches matches) {
        this.visitBinaryExpression(matches);
    }

    public void visit(BitwiseAnd bitwiseAnd) {
        this.visitBinaryExpression(bitwiseAnd);
    }

    public void visit(BitwiseOr bitwiseOr) {
        this.visitBinaryExpression(bitwiseOr);
    }

    public void visit(BitwiseXor bitwiseXor) {
        this.visitBinaryExpression(bitwiseXor);
    }

    public void visit(CastExpression cast) {
        cast.getLeftExpression().accept(this);
    }

    public void visit(Modulo modulo) {
        this.visitBinaryExpression(modulo);
    }

    public void visit(AnalyticExpression analytic) {
    }

    public void visit(SetOperationList list) {
        Iterator var2 = list.getSelects().iterator();

        while(var2.hasNext()) {
            SelectBody plainSelect = (SelectBody)var2.next();
            plainSelect.accept(this);
        }

    }

    public void visit(ExtractExpression eexpr) {
    }

    public void visit(LateralSubSelect lateralSubSelect) {
        lateralSubSelect.getSubSelect().getSelectBody().accept(this);
    }

    public void visit(MultiExpressionList multiExprList) {
        Iterator var2 = multiExprList.getExprList().iterator();

        while(var2.hasNext()) {
            ExpressionList exprList = (ExpressionList)var2.next();
            exprList.accept(this);
        }

    }

    public void visit(ValuesList valuesList) {
    }

    protected void init(boolean allowColumnProcessing) {
//        this.otherItemNames = new ArrayList();
//        this.joinTables = new HashMap <String, String>();
        this.joinTablesInfo = new JoinTablesInfo();
        this.allowColumnProcessing = allowColumnProcessing;
    }

    public void visit(IntervalExpression iexpr) {
    }

    public void visit(JdbcNamedParameter jdbcNamedParameter) {
    }

    public void visit(OracleHierarchicalExpression oexpr) {
        if (oexpr.getStartExpression() != null) {
            oexpr.getStartExpression().accept(this);
        }

        if (oexpr.getConnectExpression() != null) {
            oexpr.getConnectExpression().accept(this);
        }

    }

    public void visit(RegExpMatchOperator rexpr) {
        this.visitBinaryExpression(rexpr);
    }

    public void visit(RegExpMySQLOperator rexpr) {
        this.visitBinaryExpression(rexpr);
    }

    public void visit(JsonExpression jsonExpr) {
    }

    public void visit(JsonOperator jsonExpr) {
    }

    public void visit(AllColumns allColumns) {
    }

    public void visit(AllTableColumns allTableColumns) {
    }

    public void visit(SelectExpressionItem item) {
        item.getExpression().accept(this);
    }

    public void visit(UserVariable var) {
    }

    public void visit(NumericBind bind) {
    }

    public void visit(KeepExpression aexpr) {
    }

    public void visit(MySQLGroupConcat groupConcat) {
    }

    public void visit(ValueListExpression valueList) {
        valueList.getExpressionList().accept(this);
    }

    public void visit(Delete delete) {
        this.visit(delete.getTable());
        if (delete.getJoins() != null) {
            Iterator var2 = delete.getJoins().iterator();

            while(var2.hasNext()) {
                Join join = (Join)var2.next();
                join.getRightItem().accept(this);
            }
        }

        if (delete.getWhere() != null) {
            delete.getWhere().accept(this);
        }

    }

    public void visit(Update update) {
        Iterator var2 = update.getTables().iterator();

        while(var2.hasNext()) {
            Table table = (Table)var2.next();
            this.visit(table);
        }

        if (update.getExpressions() != null) {
            var2 = update.getExpressions().iterator();

            while(var2.hasNext()) {
                Expression expression = (Expression)var2.next();
                expression.accept(this);
            }
        }

        if (update.getFromItem() != null) {
            update.getFromItem().accept(this);
        }

        if (update.getJoins() != null) {
            var2 = update.getJoins().iterator();

            while(var2.hasNext()) {
                Join join = (Join)var2.next();
                join.getRightItem().accept(this);
            }
        }

        if (update.getWhere() != null) {
            update.getWhere().accept(this);
        }

    }

    public void visit(Insert insert) {
        this.visit(insert.getTable());
        if (insert.getItemsList() != null) {
            insert.getItemsList().accept(this);
        }

        if (insert.getSelect() != null) {
            this.visit(insert.getSelect());
        }

    }

    public void visit(Replace replace) {
        this.visit(replace.getTable());
        if (replace.getExpressions() != null) {
            Iterator var2 = replace.getExpressions().iterator();

            while(var2.hasNext()) {
                Expression expression = (Expression)var2.next();
                expression.accept(this);
            }
        }

        if (replace.getItemsList() != null) {
            replace.getItemsList().accept(this);
        }

    }

    public void visit(Drop drop) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Truncate truncate) {
        this.visit(truncate.getTable());
    }

    public void visit(CreateIndex createIndex) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(CreateTable create) {
        this.visit(create.getTable());
        if (create.getSelect() != null) {
            create.getSelect().accept(this);
        }

    }

    public void visit(CreateView createView) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Alter alter) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Statements stmts) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Execute execute) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(SetStatement set) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(ShowStatement set) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(RowConstructor rowConstructor) {
        Iterator var2 = rowConstructor.getExprList().getExpressions().iterator();

        while(var2.hasNext()) {
            Expression expr = (Expression)var2.next();
            expr.accept(this);
        }

    }

    public void visit(HexValue hexValue) {
    }

    public void visit(Merge merge) {

    }

    public void visit(OracleHint hint) {
    }

    public void visit(TableFunction valuesList) {
    }

    public void visit(AlterView alterView) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(TimeKeyExpression timeKeyExpression) {
    }

    public void visit(DateTimeLiteralExpression literal) {
    }

    public void visit(Commit commit) {
    }

    public void visit(Upsert upsert) {
        this.visit(upsert.getTable());
        if (upsert.getItemsList() != null) {
            upsert.getItemsList().accept(this);
        }

        if (upsert.getSelect() != null) {
            this.visit(upsert.getSelect());
        }

    }

    public void visit(UseStatement use) {
    }

    public void visit(ParenthesisFromItem parenthesis) {
        parenthesis.getFromItem().accept(this);
    }

    public void visit(Block block) {
        if (block.getStatements() != null) {
            this.visit(block.getStatements());
        }

    }

    public void visit(Comment comment) {
        if (comment.getTable() != null) {
            this.visit(comment.getTable());
        }

        if (comment.getColumn() != null) {
            Table table = comment.getColumn().getTable();
            if (table != null) {
                this.visit(table);
            }
        }

    }

    public void visit(ValuesStatement values) {
        Iterator var2 = values.getExpressions().iterator();

        while(var2.hasNext()) {
            Expression expr = (Expression)var2.next();
            expr.accept(this);
        }

    }

    public void visit(DescribeStatement describe) {
        describe.getTable().accept(this);
    }

    public void visit(ExplainStatement explain) {
        explain.getStatement().accept(this);
    }

    public void visit(NextValExpression nextVal) {
    }

    public void visit(CollateExpression col) {
        col.getLeftExpression().accept(this);
    }


    private QueryParser.JoinType checkJoinType(Join join){
        QueryParser.JoinType type = QueryParser.JoinType.NONE;

        if(join.isInner())
            type = QueryParser.JoinType.INNER;
        else if(join.isCross())
            type = QueryParser.JoinType.CROSS;
        else if(join.isLeft())
            type = QueryParser.JoinType.LEFT;
        else if(join.isFull())
            type = QueryParser.JoinType.FULL;

        else if(join.isNatural())
            type = QueryParser.JoinType.NATURAL;

        else if(join.isOuter())
            type = QueryParser.JoinType.OUTER;

        else if(join.isRight())
            type = QueryParser.JoinType.RIGHT;

//        else if(join.isSimple())
//            type = QueryParser.JoinType.SIMPLE;

        else if(join.isSemi())
            type = QueryParser.JoinType.SEMI;

        return type;

    }
}
