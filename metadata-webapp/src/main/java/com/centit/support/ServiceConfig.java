package com.centit.support;

import com.centit.framework.components.impl.NotificationCenterImpl;
import com.centit.framework.config.SpringSecurityDaoConfig;
import com.centit.framework.jdbc.config.JdbcConfig;
import com.centit.framework.model.adapter.NotificationCenter;
import com.centit.framework.security.StandardPasswordEncoderImpl;
import org.springframework.context.annotation.*;

/**
 * Created by codefan on 17-7-18.
 */
@Configuration
@ComponentScan(basePackages = {"com.centit"},
    excludeFilters = @ComponentScan.Filter(value = org.springframework.stereotype.Controller.class))
@PropertySource(value = "classpath:system.properties")
@Import(value = {JdbcConfig.class, SpringSecurityDaoConfig.class})
public class ServiceConfig {

    @Bean
    public NotificationCenter notificationCenter() {
        NotificationCenterImpl notificationCenter = new NotificationCenterImpl();
        notificationCenter.initDummyMsgSenders();
        //notificationCenter.registerMessageSender("innerMsg",innerMessageManager);
        return notificationCenter;
    }

    @Bean("passwordEncoder")
    public StandardPasswordEncoderImpl passwordEncoder() {
        return new StandardPasswordEncoderImpl();
    }

  /*  @Bean("integrationEnvironment")
    public IntegrationEnvironment integrationEnvironment(){
        return new JsonIntegrationEnvironment();
    }*/

}
