package com.centit.product.dbdesign.pdmutils;

import com.centit.product.dbdesign.po.PendingMetaColumn;
import com.centit.product.dbdesign.po.PendingMetaTable;
import com.centit.support.database.metadata.PdmReader;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public abstract class PdmTableInfoUtils {

    public static List<Pair<String, String>> listTablesInPdm(String pdmFilePath) {
        PdmReader pdmReader = new PdmReader();
        if(!pdmReader.loadPdmFile(pdmFilePath))
            return null;

        return pdmReader.getAllTableCode();
    }

    public static PendingMetaTable importTableFromPdm(String pdmFilePath, String tableCode, String databaseCode) {
        PdmReader pdmReader = new PdmReader();
        if(!pdmReader.loadPdmFile(pdmFilePath))
            return null;
        SimpleTableInfo pdmTable = pdmReader.getTableMetadata(tableCode);
        PendingMetaTable metaTable = new PendingMetaTable();

        metaTable.setDatabaseCode(databaseCode);
        metaTable.setTableName(pdmTable.getTableName());
        metaTable.setTableLabelName(pdmTable.getTableLabelName());
        metaTable.setTableState("N");
        metaTable.setTableComment(pdmTable.getTableComment());

        for(SimpleTableField field : pdmTable.getColumns()){
            PendingMetaColumn mdColumn = new PendingMetaColumn();
            mdColumn.setColumnName(field.getColumnName());
            mdColumn.setFieldType(field.getColumnType());
            mdColumn.setColumnComment(field.getColumnComment());
            mdColumn.setMaxLength(field.getMaxLength());
            mdColumn.setScale(field.getScale());
            mdColumn.setMandatory(field.isMandatory());
            mdColumn.setPrimaryKey(field.isPrimaryKey());

            metaTable.addMdColumn(mdColumn);
        }

        return metaTable;
    }

    public static List<SimpleTableInfo> importTableFromPdm(String pdmFilePath) {
        PdmReader pdmReader = new PdmReader();
        if(!pdmReader.loadPdmFile(pdmFilePath))
            return null;
        List<SimpleTableInfo> pdmTables = new ArrayList<>();
        List<Pair<String,String>> tabNames = pdmReader.getAllTableCode();
        for (Pair<String, String> tabName : tabNames) {
            SimpleTableInfo pdmTable = pdmReader.getTableMetadata(tabName.getKey());
            pdmTables.add(pdmTable);
        }
        return pdmTables;
    }

    public static List<SimpleTableInfo> importTableFromPdm(String pdmFilePath, List<String> tables) {
        PdmReader pdmReader = new PdmReader();
        if(!pdmReader.loadPdmFile(pdmFilePath))
            return null;
        List<SimpleTableInfo> pdmTables = new ArrayList<>();
        for (String table : tables) {
            SimpleTableInfo pdmTable = pdmReader.getTableMetadata(table);
            pdmTables.add(pdmTable);
        }
        return pdmTables;
    }

}
