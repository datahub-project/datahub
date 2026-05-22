import React, { useCallback, useContext, useEffect, useState } from 'react';
import { useOnSelectionChange, useStore } from 'reactflow';
import styled from 'styled-components/macro';

import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { LineageDisplayContext, LineageEntity, LineageNodesContext } from '@app/lineageV3/common';
import CompactContext from '@app/shared/CompactContext';
import TabFullsizedContext from '@app/shared/TabFullsizedContext';
import EntitySidebarContext, { FineGrainedOperation } from '@app/sharedV2/EntitySidebarContext';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';

const SidebarWrapper = styled.div<{ $distanceFromTop: number }>`
    position: absolute;
    right: 0;
    top: 0;
    display: flex;
    flex-direction: column;
    z-index: 1;
    height: 100vh;

    && {
        &::-webkit-scrollbar {
            display: none;
        }
    }
`;

export default function LineageSidebar() {
    const { rootUrn } = useContext(LineageNodesContext);
    const entityRegistry = useEntityRegistry();
    const [selectedEntity, setSelectedEntity] = useSelectedNode();
    const resetSelectedElements = useStore((actions) => actions.resetSelectedElements);
    const queryDetails = useQueryDetails(selectedEntity);
    const width = useSidebarWidth();
    // When the lineage graph is mounted inside a profile tab (Domain / DataProduct / Dataset
    // lineage tab), the host page already renders its own EntityProfileSidebar on the right.
    // Rendering the click-popup on top stacks two parallel right-rail tab strips side by side,
    // which is confusing — suppress the popup in that case. We still want it on the full-screen
    // standalone lineage page (?is_lineage_mode=true, where `setTabFullsize` is undefined) and
    // when the user manually expands the lineage tab to fullsize (the host sidebar is hidden).
    const { isTabFullsize, setTabFullsize } = useContext(TabFullsizedContext);
    const isInsideProfileTab = !!setTabFullsize && !isTabFullsize;

    const setSidebarClosed = useCallback(
        (closed) => {
            if (closed) {
                resetSelectedElements();
                setSelectedEntity(null);
            }
        },
        [resetSelectedElements, setSelectedEntity],
    );

    useEffect(() => {
        setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [rootUrn]);

    // This manages closing, rather than isClosed
    if (!selectedEntity || isInsideProfileTab) {
        return null;
    }

    return (
        <EntitySidebarContext.Provider
            value={{
                width,
                isClosed: false,
                setSidebarClosed,
                forLineage: true,
                separateSiblings: !selectedEntity.entity?.lineageSiblingIcon,
                fineGrainedOperations: queryDetails,
            }}
        >
            <SidebarWrapper $distanceFromTop={0}>
                <CompactContext.Provider key={selectedEntity.urn} value>
                    {entityRegistry.renderProfile(selectedEntity.type, selectedEntity.urn)}
                </CompactContext.Provider>
            </SidebarWrapper>
        </EntitySidebarContext.Provider>
    );
}

function useSelectedNode(): [LineageEntity | null, (v: LineageEntity | null) => void] {
    // Entity Profile sidebar, not lineage sidebar
    const { setSidebarClosed } = useContext(EntitySidebarContext);
    const [selectedNode, setSelectedNode] = useState<LineageEntity | null>(null);

    useOnSelectionChange({
        onChange: ({ nodes }) => {
            if (nodes.length) setSidebarClosed(true);
            setSelectedNode(nodes.length ? nodes[nodes.length - 1].data : null);
        },
    });

    return [selectedNode, setSelectedNode];
}

function useQueryDetails(selectedNode: LineageEntity | null): FineGrainedOperation[] | undefined {
    const { nodes } = useContext(LineageNodesContext);
    const { cllHighlightedNodes, fineGrainedOperations } = useContext(LineageDisplayContext);

    if (selectedNode) {
        return Array.from(cllHighlightedNodes.get(selectedNode.urn) || []).map((ref) => {
            const data = fineGrainedOperations.get(ref);
            return {
                inputColumns: getColumnNames(nodes, data?.inputColumns),
                outputColumns: getColumnNames(nodes, data?.outputColumns),
                transformOperation: data?.transformOperation,
            };
        });
    }
    return [];
}

// TODO: Clean this up
function getColumnNames(
    nodes: Map<string, LineageEntity>,
    columns?: Array<[string, string]>,
): Array<[string, string]> | undefined {
    return columns?.map(([urn, column]) => [nodes.get(urn)?.entity?.name || urn, translateFieldPath(column)]);
}
