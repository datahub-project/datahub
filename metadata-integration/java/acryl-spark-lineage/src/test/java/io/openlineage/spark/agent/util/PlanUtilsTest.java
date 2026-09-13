/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PlanUtilsTest {
  @Test
  void parentRunFacetForwardsParentAndRootFacets() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    OpenLineage.RunFacet parentRunMarker = openLineage.newRunFacet();
    OpenLineage.JobFacet parentJobMarker = openLineage.newJobFacet();
    OpenLineage.RunFacet rootRunMarker = openLineage.newRunFacet();
    OpenLineage.JobFacet rootJobMarker = openLineage.newJobFacet();

    OpenLineage.ParentRunFacetRunFacets parentRunFacets =
        openLineage
            .newParentRunFacetRunFacetsBuilder()
            .put("parentRunMarker", parentRunMarker)
            .build();
    OpenLineage.ParentRunFacetJobFacets parentJobFacets =
        openLineage
            .newParentRunFacetJobFacetsBuilder()
            .put("parentJobMarker", parentJobMarker)
            .build();
    OpenLineage.RootRunFacets rootRunFacets =
        openLineage.newRootRunFacetsBuilder().put("rootRunMarker", rootRunMarker).build();
    OpenLineage.RootJobFacets rootJobFacets =
        openLineage.newRootJobFacetsBuilder().put("rootJobMarker", rootJobMarker).build();
    UUID parentRunId = UUID.randomUUID();
    UUID rootRunId = UUID.randomUUID();

    OpenLineage.ParentRunFacet facet =
        PlanUtils.parentRunFacet(
            parentRunId,
            "parent_job",
            "parent_namespace",
            parentRunFacets,
            parentJobFacets,
            rootRunId,
            "root_job",
            "root_namespace",
            rootRunFacets,
            rootJobFacets);

    assertEquals(parentRunId, facet.getRun().getRunId());
    assertSame(
        parentRunMarker,
        facet.getRun().getFacets().getAdditionalProperties().get("parentRunMarker"));
    assertSame(
        parentJobMarker,
        facet.getJob().getFacets().getAdditionalProperties().get("parentJobMarker"));
    assertEquals(rootRunId, facet.getRoot().getRun().getRunId());
    assertSame(
        rootRunMarker,
        facet.getRoot().getRun().getFacets().getAdditionalProperties().get("rootRunMarker"));
    assertSame(
        rootJobMarker,
        facet.getRoot().getJob().getFacets().getAdditionalProperties().get("rootJobMarker"));
  }
}
