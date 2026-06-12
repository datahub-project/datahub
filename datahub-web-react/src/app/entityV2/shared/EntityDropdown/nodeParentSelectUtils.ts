import { GlossaryNode } from '@types';

// Drop entity itself + any node whose ancestor chain contains it. Prevents users from picking a
// parent that would create a cycle when moving a node under another node.
export function filterResultsForMove(entity: GlossaryNode, entityUrn: string) {
    return (
        entity.urn !== entityUrn &&
        entity.__typename === 'GlossaryNode' &&
        !entity.parentNodes?.nodes?.some((node) => node.urn === entityUrn)
    );
}
