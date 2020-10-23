package com.linkedin.common.urn;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UrnUtils {

    private static final CorpuserUrn UNKNOWN_ACTOR_URN = new CorpuserUrn("unknown");

    private UrnUtils() {
    }

    /**
     * Convert platform + dataset + origin into DatasetUrn
     * @param platformName String, e.g. hdfs, oracle
     * @param datasetName String, e.g. /jobs/xxx, ABOOK.ADDRESS
     * @param origin PROD, CORP, EI, DEV
     * @return DatasetUrn
     */
    @Nonnull
    public static DatasetUrn toDatasetUrn(@Nonnull String platformName, @Nonnull String datasetName,
                                          @Nonnull String origin) {
        return new DatasetUrn(new DataPlatformUrn(platformName), datasetName, toFabricType(origin));
    }

    /**
     * Convert fabric String to FabricType
     * @param fabric PROD, CORP, EI, DEV, LIT, PRIME
     * @return FabricType
     */
    @Nonnull
    public static FabricType toFabricType(@Nonnull String fabric) {
        switch (fabric.toUpperCase()) {
            case "PROD":
                return FabricType.PROD;
            case "CORP":
                return FabricType.CORP;
            case "EI":
                return FabricType.EI;
            case "DEV":
                return FabricType.DEV;
            default:
                throw new IllegalArgumentException("Unsupported Fabric Type: " + fabric);
        }
    }

    /**
     * Get audit stamp without time. If actor is null, set as Unknown Application URN.
     * @param actor Urn
     * @return AuditStamp
     */
    @Nonnull
    public static AuditStamp getAuditStamp(@Nullable Urn actor) {
        return new AuditStamp().setActor(getActorOrDefault(actor));
    }

    /**
     * Return actor URN, if input actor is null, return Unknown Application URN.
     */
    @Nonnull
    public static Urn getActorOrDefault(@Nullable Urn actor) {
        return actor != null ? actor : UNKNOWN_ACTOR_URN;
    }
}
