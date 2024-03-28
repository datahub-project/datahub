import { ConsoleSqlOutlined, HomeOutlined, LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import React, { useContext } from 'react';
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
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';
import { useGetQueryQuery } from '../../../graphql/query.generated';
import { LoadingWrapper } from '../LineageEntityNode/NodeContents';

export const LINEAGE_TRANSFORMATION_NODE_NAME = 'lineage-transformation';
export const TRANSFORMATION_NODE_SIZE = 30;

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

const CustomHandle = styled(Handle)<{ position: Position; $onEdge: boolean }>`
    background: initial;
    border: initial;
    top: 50%;
    left: 50%;

    ${({ position, $onEdge }) => {
        if ($onEdge && position === Position.Left) return 'left: 0;';
        if ($onEdge && position === Position.Right) return 'right: 0;';
        return '';
    }};
`;

const CustomIcon = styled.img`
    height: 18px;
    width: 18px;
`;

export default function LineageTransformationNode(props: NodeProps<LineageEntity>) {
    const { id, data, selected } = props;
    const { urn, type, fetchStatus } = data;
    const isQuery = type === EntityType.Query;

    const { nodes, rootUrn } = useContext(LineageNodesContext);
    const { setHoveredNode } = useContext(LineageDisplayContext);

    const entity = nodes.get(urn)?.entity;

    const backupLogoUrl = useFetchQuery(urn);
    const icon = entity?.icon || backupLogoUrl;

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
            {urn === rootUrn && (
                <HomeNodeBubble>
                    <HomeOutlined style={{ marginRight: 4 }} />
                    Home
                </HomeNodeBubble>
            )}
            {icon && <CustomIcon src={icon} />}
            {!icon && isQuery && <ConsoleSqlOutlined />}
            {fetchStatus[LineageDirection.Upstream] === FetchStatus.LOADING && (
                <LoadingWrapper className="nodrag" style={{ left: -30 }}>
                    <Spin delay={urn === rootUrn ? undefined : 500} indicator={<LoadingOutlined />} />
                </LoadingWrapper>
            )}
            {fetchStatus[LineageDirection.Downstream] === FetchStatus.LOADING && (
                <LoadingWrapper className="nodrag" style={{ right: -30 }}>
                    <Spin delay={urn === rootUrn ? undefined : 500} indicator={<LoadingOutlined />} />
                </LoadingWrapper>
            )}
            <CustomHandle type="target" position={Position.Left} isConnectable={false} $onEdge={!isQuery} />
            <CustomHandle type="source" position={Position.Right} isConnectable={false} $onEdge={!isQuery} />
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
