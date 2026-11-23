import React, { useContext } from 'react';
import { NodeProps, useStore } from 'reactflow';
import styled from 'styled-components';

import { ContractLineageControl } from '@app/lineageV3/LineageEntityNode/ContractLineageControl';
import { ExpandLineageButton } from '@app/lineageV3/LineageEntityNode/ExpandLineageButton';
import { FetchStatus, LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

export const DATA_FLOW_LINEAGE_CONTROL_NODE_NAME = 'data-flow-lineage-control';

export interface DataFlowLineageControlData {
    urn: string;
    type: EntityType;
    direction: LineageDirection;
}

// React Flow hides nodes it measures as zero-sized
const NodeRoot = styled.div`
    width: 1px;
    height: 1px;
`;

// React Flow disables pointer events on nodes that are neither selectable nor draggable
const Anchor = styled.div`
    position: absolute;
    width: 0;
    height: 0;
    pointer-events: all;
`;

/**
 * Expand, contract and paginate controls for a DataFlow's own lineage, on one side of its bounding box.
 * A child node of the box rather than part of it: the box sits below edges, and these controls must not.
 */
export default function DataFlowLineageControlNode({ data }: NodeProps<DataFlowLineageControlData>) {
    const { urn, type, direction } = data;
    const { nodes, adjacencyList } = useContext(LineageNodesContext);
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const boxWidth = useStore((state) => state.nodeInternals.get(urn)?.width ?? 0);

    const node = nodes.get(urn);
    const count = adjacencyList[direction].get(urn)?.size;
    if (!node || !count || node.fetchStatus[direction] !== FetchStatus.COMPLETE) return <NodeRoot />;

    return (
        <NodeRoot>
            <Anchor style={{ left: direction === LineageDirection.Upstream ? 0 : boxWidth }}>
                {node.isExpanded[direction] ? (
                    <ContractLineageControl urn={urn} direction={direction} />
                ) : (
                    <ExpandLineageButton
                        urn={urn}
                        type={type}
                        direction={direction}
                        display
                        fetchStatus={node.fetchStatus}
                        ignoreSchemaFieldStatus={ignoreSchemaFieldStatus}
                        count={count}
                    />
                )}
            </Anchor>
        </NodeRoot>
    );
}
