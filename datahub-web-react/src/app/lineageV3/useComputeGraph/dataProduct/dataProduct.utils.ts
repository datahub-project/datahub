const MEMBER_NODE_ID_SEPARATOR = '␟';

/** An entity in multiple data products gets one node per data product, so node ids must include both urns. */
export function createMemberNodeId(dataProductUrn: string, urn: string): string {
    return `${dataProductUrn}${MEMBER_NODE_ID_SEPARATOR}${urn}`;
}

/** The urn of the data product whose bounding box a member node renders in, or undefined for a
 * non-member node id. Inverse of `createMemberNodeId`. */
export function getMemberDataProductUrn(nodeId: string): string | undefined {
    const separatorIndex = nodeId.indexOf(MEMBER_NODE_ID_SEPARATOR);
    return separatorIndex === -1 ? undefined : nodeId.slice(0, separatorIndex);
}
