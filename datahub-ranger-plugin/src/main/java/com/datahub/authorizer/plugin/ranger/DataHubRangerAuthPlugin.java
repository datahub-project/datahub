package com.datahub.authorizer.plugin.ranger;

import org.apache.ranger.plugin.client.BaseClient;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Datahub Apache Ranger Plugin.
 * It assists in creating policies on Apache Ranger Admin Portal.
 *
 */
public class DataHubRangerAuthPlugin extends RangerBaseService {
    private static Logger log = Logger.getLogger(DataHubRangerAuthPlugin.class.getName());

    /**
     * This is dummy function. As this plugin doesn't have any configuration
     * @return A Map with success message
     * @throws Exception
     */
    @Override
    public Map<String, Object> validateConfig() throws Exception {
        throw new UnsupportedOperationException("validateConfig is not supported.");
    }

    /**
     * This is dummy function. As this plugin doesn't support the resource lookup
     * @param resourceLookupContext
     * @return Empty list of string
     * @throws Exception
     */
    @Override
    public List<String> lookupResource(ResourceLookupContext resourceLookupContext) throws Exception {
        throw new UnsupportedOperationException("lookupResource is not supported.");
    }

    private Map<String, Object>  returnSuccessMap() {
        String message = "Connection test successful";
        Map<String, Object> retMap = new HashMap<>();
        BaseClient.generateResponseDataMap(true, message, message, null, null, retMap);
        return retMap;
    }

    private Map<String, Object> returnFailMap() {
        String message = "Connection test fail";
        Map<String, Object> retMap = new HashMap<>();
        BaseClient.generateResponseDataMap(false, message, message, null, null, retMap);
        return retMap;
    }

}
