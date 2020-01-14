package com.centit.product.metadata.service.impl;

import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.transaction.ConnectThreadHolder;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.database.utils.DatabaseAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class SqlDictionaryMapSupplier implements Supplier<Map<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(SqlDictionaryMapSupplier.class);

    private DatabaseInfo databaseInfo;
    private String sqlSen;

    public SqlDictionaryMapSupplier(DatabaseInfo databaseInfo, String sqlSen){
        this.databaseInfo = databaseInfo;
        this.sqlSen = sqlSen;
    }

    @Override
    public Map<String, String> get() {
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(databaseInfo));
            List<Object[]> datas = DatabaseAccess.findObjectsBySql(conn, sqlSen);
            if(datas!=null){
                Map<String, String> dictionary = new HashMap<>(datas.size()*5/4+1);
                for(Object[] objs : datas){
                    if(objs != null && objs.length>1){
                        dictionary.put(StringBaseOpt.castObjectToString(objs[0]),
                            StringBaseOpt.castObjectToString(objs[1]));
                    }
                }
                return dictionary;
            }
        } catch (SQLException | IOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }
}
