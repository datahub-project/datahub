import React from 'react';
import styled from 'styled-components';

import TreeNodeRenderer from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeNodeRenderer';
import { useTreeViewContext } from '@app/homeV3/modules/hierarchyViewModule/treeView/context';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

export default function TreeNodesRenderer() {
    const { nodes } = useTreeViewContext();

    return (
        <Wrapper>
            {nodes.map((node) => (
                <TreeNodeRenderer node={node} depth={0} key={node.value} />
            ))}
        </Wrapper>
    );
}
