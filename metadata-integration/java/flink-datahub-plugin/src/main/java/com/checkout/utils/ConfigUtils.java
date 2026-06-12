package com.checkout.utils;

import com.linkedin.data.template.StringMap;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;

public class ConfigUtils {
    public static String getClusterId(Configuration configuration) {
        String kubernetesNamespace = configuration.getValue(ConfigOptions.key("kubernetes.namespace").stringType().defaultValue(""));
        String kubernetesClusterId = configuration.getValue(ConfigOptions.key("kubernetes.cluster-id").stringType().defaultValue(""));

        if (!kubernetesNamespace.isBlank() && !kubernetesClusterId.isBlank()) {
            return kubernetesNamespace + "." + kubernetesClusterId;
        } else if (!kubernetesClusterId.isBlank()) {
            return kubernetesClusterId;
        }

        String clusterId = configuration.getString(HighAvailabilityOptions.HA_CLUSTER_ID, "");
        if (!clusterId.isBlank()) {
            return clusterId;
        }
        throw new IllegalStateException("Cluster ID not found in configuration. This probably isn't running in Session Mode.");
    }

    public static StringMap getConfigMap(Configuration configuration) {
        StringMap configMap = new StringMap();
        configMap.putAll(configuration.toMap());
        return configMap;
    }


}
