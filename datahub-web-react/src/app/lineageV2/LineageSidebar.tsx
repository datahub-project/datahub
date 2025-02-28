import React, { useCallback, useContext, useState } from 'react';
import { useOnSelectionChange, useStore } from 'reactflow';
import styled from 'styled-components/macro';
import translateFieldPath from '../entityV2/dataset/profile/schema/utils/translateFieldPath';
import EntitySidebarContext, { FineGrainedOperation } from '../sharedV2/EntitySidebarContext';
import CompactContext from '../shared/CompactContext';
import useSidebarWidth from '../sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '../useEntityRegistry';
import { LineageDisplayContext, LineageEntity, LineageNodesContext } from './common';

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
    const entityRegistry = useEntityRegistry();
    const [selectedEntity, setSelectedEntity] = useSelectedNode();
    const resetSelectedElements = useStore((actions) => actions.resetSelectedElements);
    const queryDetails = useQueryDetails(selectedEntity);
    const width = useSidebarWidth();

    const setSidebarClosed = useCallback(
        (closed) => {
            if (closed) {
                resetSelectedElements();
                setSelectedEntity(null);
            }
        },
        [resetSelectedElements, setSelectedEntity],
    );

    // This manages closing, rather than isClosed
    if (!selectedEntity) {
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
