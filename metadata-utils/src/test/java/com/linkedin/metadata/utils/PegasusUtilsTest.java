package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Map;
import org.testng.annotations.Test;

public class PegasusUtilsTest {

  @Test
  public void constructMCLDropsAttributedActorHeader() {
    MetadataChangeProposal base =
        new MetadataChangeProposal()
            .setHeaders(
                new StringMap(
                    Map.of(
                        "x-datahub-attributed-actor",
                        "urn:li:corpuser:ceo",
                        Constants.SYNC_INDEX_UPDATE_HEADER_NAME,
                        "true")));

    MetadataChangeLog mcl =
        PegasusUtils.constructMCL(
            base,
            "dataset",
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)"),
            ChangeType.UPSERT,
            "status",
            AuditStampUtils.createDefaultAuditStamp(),
            null,
            null,
            null,
            null);

    assertEquals(mcl.getHeaders(), Map.of(Constants.SYNC_INDEX_UPDATE_HEADER_NAME, "true"));
  }
}
