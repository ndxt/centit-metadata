package com.centit.product.metadata.service;

import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaTable;

public interface SyncDBPretreatment {
    int pretreatmentTable(MetaTable table);

    int pretreatmentColumn(MetaColumn column);

}
