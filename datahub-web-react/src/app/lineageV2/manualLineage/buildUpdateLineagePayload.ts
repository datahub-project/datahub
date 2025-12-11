/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Entity, LineageDirection, LineageEdge } from '@types';

export function buildUpdateLineagePayload(
    lineageDirection: LineageDirection,
    entitiesToAdd: Entity[],
    entitiesToRemove: Entity[],
    entityUrn: string,
) {
    let edgesToAdd: LineageEdge[] = [];
    let edgesToRemove: LineageEdge[] = [];

    if (lineageDirection === LineageDirection.Upstream) {
        edgesToAdd = entitiesToAdd.map((entity) => ({ upstreamUrn: entity.urn, downstreamUrn: entityUrn }));
        edgesToRemove = entitiesToRemove.map((entity) => ({ upstreamUrn: entity.urn, downstreamUrn: entityUrn }));
    }
    if (lineageDirection === LineageDirection.Downstream) {
        edgesToAdd = entitiesToAdd.map((entity) => ({ upstreamUrn: entityUrn, downstreamUrn: entity.urn }));
        edgesToRemove = entitiesToRemove.map((entity) => ({ upstreamUrn: entityUrn, downstreamUrn: entity.urn }));
    }

    return { edgesToAdd, edgesToRemove };
}
