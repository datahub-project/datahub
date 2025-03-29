import { Entity, LineageDirection, LineageEdge } from '../../../types.generated';

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
