package com.linkedin.metadata.params;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class ExtraIngestParams {

    private Map<String, Long> createdOnMap;

    public Long getCreatedOn(String urn, String aspectName) {
        if (urn == null || aspectName == null || createdOnMap == null) {
            return null;
        }
        return createdOnMap.containsKey(urn + "+" + aspectName) ? createdOnMap.get(urn + "+" + aspectName) : null;
    }

}