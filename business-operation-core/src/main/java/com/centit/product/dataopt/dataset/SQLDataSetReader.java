package com.centit.product.dataopt.dataset;

import com.alibaba.fastjson.JSONArray;
import com.centit.product.dataopt.core.DataSetReader;
import com.centit.product.dataopt.core.SimpleDataSet;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库数据集 读取和写入类
 * 需要设置的参数有：
 *      数据库连接信息 DatabaseInfo
 *      对应的表信息 SimpleTableInfo
 */
public class SQLDataSetReader implements DataSetReader {

    private static final Logger logger = LoggerFactory.getLogger(SQLDataSetReader.class);
    private DataSourceDescription dataSource;
    /**
     * 参数驱动sql
     */
    private String sqlSen;

    private Connection connection;
    /**
     * 读取 dataSet 数据集
     * @param params 模块的自定义参数
     * @return dataSet 数据集
     */
    @Override
    @SuppressWarnings("unchecked")
    public SimpleDataSet load(final Map<String, Object> params) {
        Connection conn = connection;
        boolean createConnect = false;
        try {
            if(conn == null){
                conn = DbcpConnectPools.getDbcpConnect(dataSource);
                createConnect = true;
            }
            QueryAndNamedParams qap = QueryUtils.translateQuery( sqlSen, params);
            Map<String, Object> paramsMap = new HashMap<>(params.size() + 6);
            paramsMap.putAll(params);
            paramsMap.putAll(qap.getParams());

            JSONArray jsonArray = DatabaseAccess.findObjectsByNamedSqlAsJSON(
                conn, qap.getQuery(), paramsMap);

            SimpleDataSet dataSet = new SimpleDataSet();
            dataSet.setData((List)jsonArray);
            return dataSet;
        } catch (SQLException | IOException e) {
            logger.error(e.getLocalizedMessage());
            throw new ObjectException(e.getLocalizedMessage());
        } finally {
            if(createConnect){
                DbcpConnectPools.closeConnect(conn);
            }
        }
    }

    public void setDataSource(DataSourceDescription dataSource) {
        this.dataSource = dataSource;
    }

    public void setSqlSen(String sqlSen) {
        this.sqlSen = sqlSen;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
