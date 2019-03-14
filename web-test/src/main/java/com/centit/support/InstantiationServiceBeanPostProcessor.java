package com.centit.support;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Created by codefan on 17-7-6.
 */
public class InstantiationServiceBeanPostProcessor
    implements ApplicationListener<ContextRefreshedEvent>{

//    @Autowired
//    protected NotificationCenter notificationCenter;
//
//    @Autowired
//    private OperationLogWriter operationLogWriter;
//
//    @Autowired(required = false)
//    private EmailMessageSenderImpl emailMessageManager;
//
//    @Autowired(required = false)
//    protected PlatformEnvironment platformEnvironment;
//
//    @Value("${http.exception.notAsHttpError:false}")
//    protected boolean exceptionNotAsHttpError;
//
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event){
//        CodeRepositoryCache.setPlatformEnvironment(platformEnvironment);
//        CodeRepositoryCache.setAllCacheFreshPeriod(CodeRepositoryCache.CACHE_KEEP_FRESH);
//        WebOptUtils.setExceptionNotAsHttpError(exceptionNotAsHttpError);
//        if(emailMessageManager!=null) {
//            notificationCenter.registerMessageSender("email", emailMessageManager);
//            notificationCenter.appointDefaultSendType("email");
//        }
//        if(operationLogWriter!=null) {
//            OperationLogCenter.registerOperationLogWriter(operationLogWriter);
//        }
    }

}
