import React, { useCallback, useContext, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { useStore } from 'reactflow';
import styled from 'styled-components/macro';

import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { LineageDisplayContext, LineageEntity, LineageNodesContext } from '@app/lineageV3/common';
import CompactContext from '@app/shared/CompactContext';
import EntitySidebarContext, { FineGrainedOperation } from '@app/sharedV2/EntitySidebarContext';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

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
    const { selectedNode, setSelectedNode } = useContext(LineageDisplayContext);
    const resetSelectedElements = useStore((actions) => actions.resetSelectedElements);
    const queryDetails = useQueryDetails(selectedNode);
    const width = useSidebarWidth();

    const setSidebarClosed = useCallback(
        (closed) => {
            if (closed) {
                resetSelectedElements();
                setSelectedNode(null);
            }
        },
        [resetSelectedElements, setSelectedNode],
    );

    useEffect(() => {
        setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [rootUrn]);

    // This manages closing, rather than isClosed
    if (!selectedNode) {
        return null;
    }

    // Don't show sidebar for restricted entities
    if (selectedNode.type === EntityType.Restricted) {
        return null;
    }

    return (
        <EntitySidebarContext.Provider
            value={{
                width,
                isClosed: false,
                setSidebarClosed,
                forLineage: true,
                separateSiblings: !selectedNode.entity?.lineageSiblingIcon,
                fineGrainedOperations: queryDetails,
            }}
        >
            {createPortal(
                <SidebarWrapper $distanceFromTop={0} data-testid="lineage-sidebar">
                    <CompactContext.Provider key={selectedNode.urn} value>
                        {entityRegistry.renderProfile(selectedNode.type, selectedNode.urn)}
                    </CompactContext.Provider>
                </SidebarWrapper>,
                document.body,
            )}
        </EntitySidebarContext.Provider>
    );
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
