package com.centit.product.metadata.test;

import com.centit.framework.jdbc.config.JdbcConfig;
import com.centit.product.metadata.graphql.GraphQLExecutor;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.database.utils.DataSourceDescription;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootContextLoader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;

@ComponentScan(basePackages = {"com.centit"},
    excludeFilters = @ComponentScan.Filter(value = org.springframework.stereotype.Controller.class))
@PropertySource(value = "classpath:system.properties")
@Import(value = {JdbcConfig.class})
@SpringBootTest
@ContextConfiguration(loader = SpringBootContextLoader.class, classes = TestGraphQL.class)
public class TestGraphQL {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaDataService metaDataService;

    @Autowired
    private BasicDataSource dataSource;

    @Test
    public void test(){
        DataSourceDescription dataSourceDesc = new DataSourceDescription(
            "jdbc:mysql://192.168.128.32:3306/metadata?useUniCode=true&characterEncoding=utf-8",
            "framework",
            "framework"
        );
        System.out.println(dataSource.getUrl());
        dataSourceDesc.setDatabaseCode("stat");
        GraphQLExecutor executor = new GraphQLExecutor(metaDataService, dataSourceDesc);
        executor.execute("");
    }

}
