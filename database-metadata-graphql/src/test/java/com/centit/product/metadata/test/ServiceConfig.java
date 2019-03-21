package com.centit.product.metadata.test;

import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.framework.ip.service.impl.JsonIntegrationEnvironment;
import com.centit.framework.security.model.StandardPasswordEncoderImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by codefan on 17-7-18.
 */
@Configuration
public class ServiceConfig {

    @Bean
    public IntegrationEnvironment integrationEnvironment(){
        return new JsonIntegrationEnvironment();
    }

    @Bean("passwordEncoder")
    public StandardPasswordEncoderImpl passwordEncoder() {
        return  new StandardPasswordEncoderImpl();
    }

}
