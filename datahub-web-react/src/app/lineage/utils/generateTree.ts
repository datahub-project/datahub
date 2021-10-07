import { HierarchyNode } from '@vx/hierarchy/lib/types';
import { NodeData } from '../types';

export default function generateTree(data: HierarchyNode<NodeData>, renderedNodes: NodeData[]) {
    console.log({ data });
    const result = {
        node: data.data,
        children: data.children.map,
    };
    return result;
}
