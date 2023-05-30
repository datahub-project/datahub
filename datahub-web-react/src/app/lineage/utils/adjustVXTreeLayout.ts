import { HierarchyPointNode } from '@vx/hierarchy/lib/types';
import { NodeData, Direction } from '../types';
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import { width as nodeWidth } from '../constants';

export default function adjustVXTreeLayout({
    tree,
    direction,
}: {
    tree: HierarchyPointNode<NodeData>;
    direction: Direction;
}) {
    const nodesByUrn: { [x: string]: HierarchyPointNode<NodeData>[] } = {};
    // adjust node's positions
    tree.descendants().forEach((descendent) => {
        // first, we need to flip the position of nodes who are going upstream
        // eslint-disable-next-line  no-param-reassign
        if (direction === Direction.Upstream) {
            // eslint-disable-next-line  no-param-reassign
            descendent.y = -Math.abs(descendent.y);
        }
        // second, duplicate nodes will be created if dependencies are in a dag rather than a tree.
        // this is expected, however the <Tree /> component will try to lay them out independently.
        // We need to force them to be laid out in the same place so each copy's edges begin at the same source.
        if (descendent.data.urn && !nodesByUrn[descendent.data.urn]) {
            nodesByUrn[descendent.data.urn] = [descendent];
        } else if (descendent.data.urn) {
            const existing = nodesByUrn[descendent.data.urn];
            if (
                descendent.height < Math.max(...existing.map((nodeByUrn) => nodeByUrn.height)) &&
                descendent.depth <= Math.max(...existing.map((nodeByUrn) => nodeByUrn.depth))
            ) {
                // eslint-disable-next-line  no-param-reassign
                descendent.x = existing[0].x;
                // eslint-disable-next-line  no-param-reassign
                descendent.y = existing[0].y;
            } else {
                existing.forEach((nodeByUrn) => {
                    // eslint-disable-next-line  no-param-reassign
                    nodeByUrn.x = descendent.x;
                    // eslint-disable-next-line  no-param-reassign
                    nodeByUrn.y = descendent.y;
                });
                nodesByUrn[descendent.data.urn] = [...nodesByUrn[descendent.data.urn], descendent];
            }
        }
    });

    const nodesToReturn = tree.descendants().map((descendentToCopy) => ({
        x: descendentToCopy.x,
        y: descendentToCopy.y,
        data: { ...descendentToCopy.data },
    }));

    const edgesToReturn = tree.links().map((linkToCopy) => {
        if (linkToCopy.target.y === linkToCopy.source.y) {
            const sourceHigher = linkToCopy.source.x > linkToCopy.target.x;

            return {
                target: {
                    x: linkToCopy.target.x + (sourceHigher ? 40 : 0),
                    y: linkToCopy.target.y,
                    data: { ...linkToCopy.target.data },
                },
                source: {
                    x: linkToCopy.source.x - (sourceHigher ? 0 : 40),
                    y: linkToCopy.source.y,
                    data: { ...linkToCopy.source.data },
                },
            };
        }
        return {
            target: {
                x: linkToCopy.target.x,
                y: linkToCopy.target.y + (direction === Direction.Upstream ? nodeWidth / 2 - 10 : -(nodeWidth / 2)),
                data: { ...linkToCopy.target.data },
            },
            source: {
                x: linkToCopy.source.x,
                y: linkToCopy.source.y + (direction === Direction.Upstream ? -(nodeWidth / 2) : nodeWidth / 2 - 10),
                data: { ...linkToCopy.source.data },
            },
        };
    });

    return { nodesToRender: nodesToReturn, edgesToRender: edgesToReturn, nodesByUrn };
}
