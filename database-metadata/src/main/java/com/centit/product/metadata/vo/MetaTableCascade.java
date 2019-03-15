package com.centit.product.metadata.vo;

import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelDetail;
import com.centit.product.metadata.po.MetaTable;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class MetaTableCascade {

    private String databaseType;
    private String databaseCode;
    //表ID
    private String tableId;
    //表名
    private String table;
    private String tableAlias;
    private String title;
    private List<Column> tableFields;
    // 关联的表
    private List<Table> relationTable;

    @Data
    class Table{
        String tableId;
        String table;
        String title;
        String tableAlias;
        List<JoinColumn> joinColumns;
    }

    @Data
    class JoinColumn{
        String leftColumn;
        String rightColumn;
    }

    @Data
    class Column {
        String title;
        String tableAlias;
        String column;
        String columnType;
    }

    public void setTableInfo(MetaTable metaTable){
        this.databaseCode = metaTable.getDatabaseCode();
        this.table = metaTable.getTableName();
        this.tableId = metaTable.getTableId();
        this.title = metaTable.getTableLabelName();
    }

    public void addRelationTable(MetaTable metaTable, List<MetaRelDetail> relDetails, String tableAlias){
        if(this.relationTable == null){
            this.relationTable = new ArrayList<>();
        }
        Table table = new Table();
        table.setTable(metaTable.getTableName());
        table.setTitle(metaTable.getTableLabelName());
        table.setTableId(metaTable.getTableId());
        table.setTableAlias(tableAlias);

        if(table.getJoinColumns() == null){
            table.setJoinColumns(new ArrayList<>());
        }
        for(MetaRelDetail relDetail : relDetails) {
            JoinColumn joinColumn = new JoinColumn();
            joinColumn.setLeftColumn(relDetail.getParentColumnName());
            joinColumn.setRightColumn(relDetail.getChildColumnName());
            table.getJoinColumns().add(joinColumn);
        }
        this.relationTable.add(table);
    }

    public void setTableFields(List<MetaColumn> columns) {
        if (this.tableFields == null) {
            this.tableFields = new ArrayList<>();
        }
        for (MetaColumn metaColumn : columns) {
            Column column = new Column();
            column.setColumn(metaColumn.getColumnName());
            column.setTitle(metaColumn.getFieldLabelName());
            column.setColumnType(metaColumn.getColumnType());
            tableFields.add(column);
        }
    }

}
