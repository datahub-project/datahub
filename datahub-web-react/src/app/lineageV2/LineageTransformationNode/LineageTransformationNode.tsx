import { ConsoleSqlOutlined, HomeOutlined } from '@ant-design/icons';
import React, { useContext, useEffect } from 'react';
import { Handle, NodeProps, Position } from 'reactflow';
import styled from 'styled-components';
import { EntityType, LineageDirection } from '../../../types.generated';
import {
    COLUMN_QUERY_ID_PREFIX,
    FetchStatus,
    LineageDisplayContext,
    LineageEntity,
    LineageNodesContext,
} from '../common';
import useSearchAcrossLineage from '../useSearchAcrossLineage';
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';
import { useGetQueryQuery } from '../../../graphql/query.generated';

export const LINEAGE_TRANSFORMATION_NODE_NAME = 'lineage-transformation';
export const TRANSFORMATION_NODE_SIZE = 30;

const NO_FETCH_STATUSES = [FetchStatus.COMPLETE, FetchStatus.UNNEEDED, FetchStatus.LOADING];

// TODO: Share with LineageEntityNode
const HomeNodeBubble = styled.div`
    align-items: center;
    background-color: ${LINEAGE_COLORS.PURPLE_3};
    border-radius: 10px;
    color: white;
    display: flex;
    font-size: 10px;
    font-weight: 600;
    height: 22px;
    justify-content: center;
    left: -50%;
    padding: 4px 8px;
    position: absolute;
    top: -26px;
`;

const NodeWrapper = styled.div<{ selected: boolean; opacity: number }>`
    background-color: white;
    border: 1px solid;
    border-color: ${({ selected }) => (selected ? `${LINEAGE_COLORS.PURPLE_3}90` : '#EEE')};
    border-radius: 50%;
    opacity: ${({ opacity }) => opacity};

    align-items: center;
    display: flex;
    justify-content: center;
    height: ${TRANSFORMATION_NODE_SIZE}px;
    width: ${TRANSFORMATION_NODE_SIZE}px;
    cursor: pointer;
`;

const CustomHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    top: 50%;
    left: 50%;
`;

const CustomIcon = styled.img`
    height: 18px;
    width: 18px;
`;

export default function LineageTransformationNode(props: NodeProps<LineageEntity>) {
    const { id, data, selected } = props;
    const { urn, type, direction, fetchStatus } = data;
    const isQuery = type === EntityType.Query;

    const context = useContext(LineageNodesContext);
    const { setHoveredNode } = useContext(LineageDisplayContext);

    const entity = context.nodes.get(urn)?.entity;

    // Note: Direction default is to pass typing. Should not ever be queries.
    const { fetchLineage } = useSearchAcrossLineage(urn, context, direction || LineageDirection.Downstream, true);
    const backupLogoUrl = useFetchQuery(urn);
    const icon = entity?.icon || backupLogoUrl;

    useEffect(() => {
        if (direction && fetchStatus[direction] && !NO_FETCH_STATUSES.includes(fetchStatus[direction])) {
            fetchLineage();
        }
    }, [fetchLineage, direction, fetchStatus]);

    const { selectedColumn } = useContext(LineageDisplayContext);
    const opacity = selectedColumn && isQuery && !id.startsWith(COLUMN_QUERY_ID_PREFIX) ? 0.3 : 1;

    // TODO: Combine home node code with LineageEntityNode
    return (
        <NodeWrapper
            opacity={opacity}
            selected={selected}
            onMouseEnter={() => setHoveredNode(urn)}
            onMouseLeave={() => setHoveredNode(null)}
        >
            {urn === context.rootUrn && (
                <HomeNodeBubble>
                    <HomeOutlined style={{ marginRight: 4 }} />
                    Home
                </HomeNodeBubble>
            )}
            {icon && <CustomIcon src={icon} />}
            {!icon && isQuery && <ConsoleSqlOutlined />}
            <CustomHandle type="target" position={Position.Left} isConnectable={false} />
            <CustomHandle type="source" position={Position.Right} isConnectable={false} />
        </NodeWrapper>
    );
}

function useFetchQuery(urn: string) {
    const { nodes } = useContext(LineageNodesContext);
    const { data } = useGetQueryQuery({
        skip: nodes.has(urn),
        variables: { urn },
    });

    return data?.entity?.__typename === 'QueryEntity' && data.entity.platform?.properties?.logoUrl;
}
