import React, { useContext } from 'react';
import styled from 'styled-components';

import { ContractLineageControl } from '@app/lineageV3/LineageEntityNode/ContractLineageControl';
import { ExpandLineageButton } from '@app/lineageV3/LineageEntityNode/ExpandLineageButton';
import { FetchStatus, LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

const SideElementsWrapper = styled.div`
    position: absolute;
    top: 50%;
    left: 0;
    width: 100%;
`;

interface Props {
    urn: string;
    type: EntityType;
}

/** Expand, contract and paginate controls for a DataFlow's own lineage, on the sides of its bounding box. */
export default function DataFlowLineageControls({ urn, type }: Props) {
    const { nodes, adjacencyList } = useContext(LineageNodesContext);
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const node = nodes.get(urn);
    if (!node) return null;

    return (
        <SideElementsWrapper>
            {Object.values(LineageDirection).map((direction) => {
                const count = adjacencyList[direction].get(urn)?.size;
                if (!count || node.fetchStatus[direction] !== FetchStatus.COMPLETE) return null;
                return node.isExpanded[direction] ? (
                    <ContractLineageControl key={direction} urn={urn} direction={direction} />
                ) : (
                    <ExpandLineageButton
                        key={direction}
                        urn={urn}
                        type={type}
                        direction={direction}
                        display
                        fetchStatus={node.fetchStatus}
                        ignoreSchemaFieldStatus={ignoreSchemaFieldStatus}
                        count={count}
                    />
                );
            })}
        </SideElementsWrapper>
    );
}
