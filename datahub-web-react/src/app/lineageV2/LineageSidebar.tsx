import React, { useCallback, useContext, useEffect, useState } from 'react';
import { useOnSelectionChange, useStore } from 'reactflow';
import styled from 'styled-components/macro';

import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import { LineageDisplayContext, LineageEntity, LineageNodesContext } from '@app/lineageV2/common';
import CompactContext from '@app/shared/CompactContext';
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

interface Props {
    urn: string;
}

export default function LineageSidebar({ urn }: Props) {
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

    useEffect(() => {
        setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [urn]);

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
