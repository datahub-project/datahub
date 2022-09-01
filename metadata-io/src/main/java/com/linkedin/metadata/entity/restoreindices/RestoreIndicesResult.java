package com.linkedin.metadata.entity.restoreindices;


public class RestoreIndicesResult {
    public int ignored = 0;
    public int rowsMigrated = 0;
    public long timeSqlQueryMs = 0;
    public long timeGetRowMs = 0;
    public long timeUrnMs = 0;
    public long timeEntityRegistryCheckMs = 0;
    public long aspectCheckMs = 0;
    public long createRecordMs = 0;
    public long sendMessageMs = 0;

    @Override
    public String toString() {
        return "RestoreIndicesResult{" +
                "ignored=" + ignored +
                ", rowsMigrated=" + rowsMigrated +
                ", timeSqlQueryMs=" + timeSqlQueryMs +
                ", timeGetRowMs=" + timeGetRowMs +
                ", timeUrnMs=" + timeUrnMs +
                ", timeEntityRegistryCheckMs=" + timeEntityRegistryCheckMs +
                ", aspectCheckMs=" + aspectCheckMs +
                ", createRecordMs=" + createRecordMs +
                ", sendMessageMs=" + sendMessageMs +
                '}';
    }
}
