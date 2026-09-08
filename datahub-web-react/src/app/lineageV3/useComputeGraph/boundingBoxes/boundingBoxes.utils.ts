const MEMBER_NODE_ID_SEPARATOR = '␟';

/** An entity in multiple bounding boxes gets one node per box, so node ids must include both urns. */
export function createMemberNodeId(boundingBoxUrn: string, urn: string): string {
    return `${boundingBoxUrn}${MEMBER_NODE_ID_SEPARATOR}${urn}`;
}

/** The urn of the bounding box a member node renders in, or undefined for a non-member node id.
 * Inverse of `createMemberNodeId`. */
export function getMemberBoundingBoxUrn(nodeId: string): string | undefined {
    const separatorIndex = nodeId.indexOf(MEMBER_NODE_ID_SEPARATOR);
    return separatorIndex === -1 ? undefined : nodeId.slice(0, separatorIndex);
}
