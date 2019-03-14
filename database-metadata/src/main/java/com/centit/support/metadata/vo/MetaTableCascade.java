package com.centit.support.metadata.vo;

import com.centit.support.metadata.po.MetaColumn;
import com.centit.support.metadata.po.MetaRelDetail;
import com.centit.support.metadata.po.MetaTable;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class MetaTableCascade {
    private String databaseType;

    private List<Table> tableList;

    private List<Table> tableFields;

    @Data
    class Table{
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
    class Column extends Table{
        String column;
    }

    public void addTable(MetaTable metaTable){
        if(this.tableList == null){
            this.tableList = new ArrayList<>();
        }
        Table table = new Table();
        table.setTable(metaTable.getTableName());
        table.setTitle(metaTable.getTableLabelName());
        this.tableList.add(table);
    }

    public void addTable(MetaTable metaTable, List<MetaRelDetail> relDetails){
        if(this.tableList == null){
            this.tableList = new ArrayList<>();
        }
        Table table = new Table();
        table.setTable(metaTable.getTableName());
        table.setTitle(metaTable.getTableLabelName());
        if(table.getJoinColumns() == null){
            table.setJoinColumns(new ArrayList<>());
        }
        for(MetaRelDetail relDetail : relDetails) {
            JoinColumn joinColumn = new JoinColumn();
            joinColumn.setLeftColumn(relDetail.getParentColumnName());
            joinColumn.setRightColumn(relDetail.getChildColumnName());
            table.getJoinColumns().add(joinColumn);
        }
        this.tableList.add(table);

        if (this.tableFields == null) {
            this.tableFields = new ArrayList<>();
        }
        Table table1 = new Table();
        table1.setTitle(metaTable.getTableLabelName());
        table1.setTable(metaTable.getTableName());
        tableFields.add(table1);
    }

    public void setTableFields(List<MetaColumn> columns) {
        if (this.tableFields == null) {
            this.tableFields = new ArrayList<>();
        }
        for (MetaColumn metaColumn : columns) {
            Column column = new Column();
            column.setColumn(metaColumn.getColumnName());
            column.setTitle(metaColumn.getFieldLabelName());
            tableFields.add(column);
        }
    }

}
