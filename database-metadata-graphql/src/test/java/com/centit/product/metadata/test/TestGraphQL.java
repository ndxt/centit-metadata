package com.centit.product.metadata.test;

import com.alibaba.fastjson2.JSON;
import com.centit.framework.jdbc.config.JdbcConfig;
import com.centit.product.metadata.graphql.GraphQLExecutor;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.database.utils.DataSourceDescription;
import graphql.ExecutionResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sql.DataSource;

@PropertySource(value = "classpath:application.properties")
@ComponentScan(basePackages = {"com.centit"},
    excludeFilters = @ComponentScan.Filter(value = org.springframework.stereotype.Controller.class))
@Import(value = {JdbcConfig.class})
@ContextConfiguration(classes = TestGraphQL.class)
@RunWith(SpringRunner.class)
public class TestGraphQL {
    //protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaDataService metaDataService;

    @Autowired
    private DataSource dataSource;

    @Test
    public void test(){
        DataSourceDescription dataSourceDesc = new DataSourceDescription(
            "jdbc:mysql://192.168.137.95/bizdata?useUniCode=true&characterEncoding=utf-8",
            "bizdata",
            "bizdata"
        );
        //metaDataService.
        //System.out.println(dataSource.getUrl());
        dataSourceDesc.setDatabaseCode("0000000124");
        GraphQLExecutor executor = new GraphQLExecutor(metaDataService, dataSourceDesc);

        ExecutionResult result =executor.execute("mutation  receipt { contractReceipts(receiptId:\"10\",receiptAmount:300)\n" +
            "{\n" +
            "  receiptAmount\n" +
            "  contractId\n" +
            "  receiptId\n" +

            "}"+
            "}");
        System.out.println(JSON.toJSONString(result.getErrors()));
        System.out.println(JSON.toJSONString(result.getData()));
        result =executor.execute("query  receipt { contractReceipts(receiptId:\"10\")\n" +
            "{\n" +
            "  receiptAmount\n" +
            "  contractId\n" +
            "  receiptId\n" +

            "}"+
            "}");
        System.out.println(JSON.toJSONString(result.getErrors()));
        System.out.println(JSON.toJSONString(result.getData()));
    }

}
