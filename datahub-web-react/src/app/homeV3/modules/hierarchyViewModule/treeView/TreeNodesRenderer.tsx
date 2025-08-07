import React from 'react';
import styled from 'styled-components';

import TreeNodeRenderer from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeNodeRenderer';
import useTreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/useTreeViewContext';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const InlineBlockWrapper = styled.div<{ $hasExpanded: boolean }>`
    display: inline-block;
    min-width: calc(100% - 20px);
    ${(props) => !props.$hasExpanded && 'width: calc(100% - 20px);'}
`;

export default function TreeNodesRenderer() {
    const { nodes, hasAnyExpanded } = useTreeViewContext();

    return (
        <InlineBlockWrapper $hasExpanded={hasAnyExpanded}>
            <Wrapper data-testid="hierarchy-module-nodes">
                {nodes.map((node) => (
                    <TreeNodeRenderer node={node} depth={0} key={node.value} />
                ))}
            </Wrapper>
        </InlineBlockWrapper>
    );
}
