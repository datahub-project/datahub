const MEMBER_NODE_ID_SEPARATOR = '␟';

/** An entity in multiple containers gets one node per container, so node ids must include both urns. */
export function createMemberNodeId(containerUrn: string, urn: string): string {
    return `${containerUrn}${MEMBER_NODE_ID_SEPARATOR}${urn}`;
}

/** The urn of the container whose bounding box a member node renders in, or undefined for a
 * non-member node id. Inverse of `createMemberNodeId`. */
export function getMemberContainerUrn(nodeId: string): string | undefined {
    const separatorIndex = nodeId.indexOf(MEMBER_NODE_ID_SEPARATOR);
    return separatorIndex === -1 ? undefined : nodeId.slice(0, separatorIndex);
}
