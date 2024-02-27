package com.centit.product.metadata.utils;

import com.centit.framework.common.WebOptUtils;
import com.centit.framework.filter.RequestThreadLocal;
import com.centit.framework.model.security.CentitUserDetails;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;

public abstract class SessionDataUtils {
    public static HashMap<String, Object> createSessionDataMap(CentitUserDetails userDetails, HttpServletRequest request) {
        if(userDetails==null)
            return null;
        HashMap<String, Object> hashMap = new HashMap<>(16);
        hashMap.put("topUnit", userDetails.getTopUnitCode());
        //当前用户信息
        hashMap.put("currentUser", userDetails.getUserInfo());
        hashMap.put("currentUnit", userDetails.getCurrentStation());
        hashMap.put("currentStation", userDetails.getCurrentUnitCode());
        hashMap.put("currentUnitCode", userDetails.getCurrentUnitCode());
        hashMap.put("topUnitCode", userDetails.getTopUnitCode());
        //hashMap.put("userSetting", userDetails.getUserSettings());
        hashMap.put("userUnits", userDetails.getUserUnits());
        hashMap.put("userRoles", userDetails.getUserRoles());
        /*CurrentUserContext context = new CurrentUserContext(userDetails.getUserInfo(), userDetails.getTopUnitCode(),
            userDetails.getCurrentUnitCode());*/
        if(request == null){
            request = RequestThreadLocal.getLocalThreadWrapperRequest();
        }
        if(request!=null) {
            String remoteHost = request.getRemoteHost();
            String loginIp = WebOptUtils.getRequestAddr(request);
            if (!loginIp.startsWith(remoteHost)) {
                loginIp = remoteHost + ":" + loginIp;
            }
            hashMap.put("loginIp", loginIp);
        }
        return hashMap;
    }
}
