import { HierarchyPointNode } from '@vx/hierarchy/lib/types';
import { NodeData, Direction } from '../types';

export default function adjustVXTreeLayout({
    tree,
    direction,
}: {
    tree: HierarchyPointNode<NodeData>;
    direction: Direction;
}) {
    const nodeByUrn: { [x: string]: HierarchyPointNode<NodeData> } = {};
    // adjust node's positions
    tree.descendants().forEach((descendent) => {
        // second, duplicate nodes will be created if dependencies are in a dag rather than a tree.
        // this is expected, however the <Tree /> component will try to lay them out independently.
        // We need to force them to be laid out in the same place so each copy's edges begin at the same source.
        if (descendent.data.urn && !nodeByUrn[descendent.data.urn]) {
            nodeByUrn[descendent.data.urn] = descendent;
        } else if (descendent.data.urn) {
            const existing = nodeByUrn[descendent.data.urn];
            if (descendent.height < existing.height) {
                // eslint-disable-next-line  no-param-reassign
                descendent.x = existing.x;
                // eslint-disable-next-line  no-param-reassign
                descendent.y = existing.y;
            } else {
                // eslint-disable-next-line  no-param-reassign
                existing.x = descendent.x;
                // eslint-disable-next-line  no-param-reassign
                existing.y = descendent.y;
                nodeByUrn[descendent.data.urn] = descendent;
            }
        }

        // first, we need to flip the position of nodes who are going upstream
        // eslint-disable-next-line  no-param-reassign
        if (direction === Direction.Upstream) {
            // eslint-disable-next-line  no-param-reassign
            descendent.y = -Math.abs(descendent.y);
        }
    });

    const nodesToReturn = tree.descendants().map((descendentToCopy) => ({
        x: descendentToCopy.x,
        y: descendentToCopy.y,
        data: { ...descendentToCopy.data },
    }));

    const edgesToReturn = tree.links().map((linkToCopy) => ({
        target: {
            x: linkToCopy.target.x,
            y: linkToCopy.target.y,
            data: { ...linkToCopy.target.data },
        },
        source: {
            x: linkToCopy.source.x,
            y: linkToCopy.source.y,
            data: { ...linkToCopy.source.data },
        },
    }));

    return { nodesToRender: nodesToReturn, edgesToRender: edgesToReturn };
}
