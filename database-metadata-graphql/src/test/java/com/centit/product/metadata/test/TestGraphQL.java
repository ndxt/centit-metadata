package com.centit.product.metadata.test;

import com.centit.framework.jdbc.config.JdbcConfig;
import com.centit.product.metadata.graphql.GraphQLExecutor;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.database.utils.DataSourceDescription;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

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
    private BasicDataSource dataSource;

    @Test
    public void test(){
        DataSourceDescription dataSourceDesc = new DataSourceDescription(
            "jdbc:mysql://192.168.128.32:3306/metadata?useUniCode=true&characterEncoding=utf-8",
            "framework",
            "framework"
        );
        //metaDataService.
        System.out.println(dataSource.getUrl());
        dataSourceDesc.setDatabaseCode("stat");
        GraphQLExecutor executor = new GraphQLExecutor(metaDataService, dataSourceDesc);
        executor.execute("");
    }

}
