package com.centit.product.metadata;

import com.centit.product.dbdesign.service.TranslateColumn;
import com.centit.product.dbdesign.service.impl.TranslateColumnImpl;

public class Sample {
    public static void main(String[] args) {
        TranslateColumn translateColumn = new TranslateColumnImpl();
        System.out.println(translateColumn.transLabelToColumn("实际税率（%）"));
        System.out.println(translateColumn.transLabelToProperty("实际税率（%）"));
    }
}
