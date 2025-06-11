import { LoadingOutlined } from '@ant-design/icons';
import { Icon, Popover } from '@components';
import { Skeleton, Spin } from 'antd';
import React, { useContext } from 'react';
import { Handle, NodeProps, Position } from 'reactflow';
import styled from 'styled-components';

import HomePill from '@app/lineageV3/LineageEntityNode/HomePill';
import { LoadingWrapper, StyledVersioningBadge } from '@app/lineageV3/LineageEntityNode/NodeContents';
import LineageVisualizationContext from '@app/lineageV3/LineageVisualizationContext';
import NodeWrapper from '@app/lineageV3/NodeWrapper';
import {
    FetchStatus,
    LineageDisplayContext,
    LineageEntity,
    LineageNodesContext,
    isGhostEntity,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import LineageCard from '@app/lineageV3/components/LineageCard';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetQueryQuery } from '@graphql/query.generated';
import { EntityType, LineageDirection } from '@types';

export const LINEAGE_TRANSFORMATION_NODE_NAME = 'lineage-transformation';
export const TRANSFORMATION_NODE_SIZE = 40;

const HomeIndicatorWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;

    position: absolute;
    top: -18px;
`;

const TransformationalNodeWrapper = styled(NodeWrapper)<{
    opacity: number;
}>`
    border-radius: 8px;
    opacity: ${({ opacity }) => opacity};

    justify-content: center;
    height: ${TRANSFORMATION_NODE_SIZE}px;
    width: ${TRANSFORMATION_NODE_SIZE}px;
`;

const IconWrapper = styled.div<{ isGhost: boolean }>`
    display: flex;
    opacity: ${({ isGhost }) => (isGhost ? 0.5 : 1)};
    font-size: 18px;
`;

const CustomHandle = styled(Handle)<{ position: Position; $onEdge: boolean }>`
    background: initial;
    border: initial;
    top: 50%;

    ${({ position, $onEdge }) => {
        if ($onEdge && position === Position.Left) return 'left: 0;';
        if ($onEdge && position === Position.Right) return 'right: 0;';
        return '';
    }};
`;

const CustomIcon = styled.img`
    height: 1em;
    width: 1em;
`;

const PopoverWrapper = styled(NodeWrapper)`
    cursor: auto;
    transform: none;
`;

export default function LineageTransformationNode(props: NodeProps<LineageEntity>) {
    const { data, selected, dragging } = props;
    const { urn, type, entity, fetchStatus } = data;
    const entityRegistry = useEntityRegistryV2();
    const isQuery = type === EntityType.Query;
    const isDataProcessInstance = type === EntityType.DataProcessInstance;

    const { rootUrn } = useContext(LineageNodesContext);
    const { cllHighlightedNodes, setHoveredNode } = useContext(LineageDisplayContext);
    const { searchedEntity } = useContext(LineageVisualizationContext);

    // TODO: Support ghost queries and schema fields, once they are supported in the backend
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const isGhost = isGhostEntity(entity, ignoreSchemaFieldStatus);
    const isSearchedEntity = searchedEntity === urn;

    const backupLogoUrl = useFetchQuery(urn); // TODO: Remove when query nodes not instantiated on column select
    const icon = entity?.icon || backupLogoUrl;

    const { selectedColumn } = useContext(LineageDisplayContext);
    const opacity = selectedColumn && !cllHighlightedNodes.has(urn) ? 0.3 : 1;

    // TODO: Combine home node code with LineageEntityNode
    const contents = (
        <TransformationalNodeWrapper
            urn={urn}
            opacity={opacity}
            selected={selected}
            dragging={dragging}
            onMouseEnter={() => setHoveredNode(urn)}
            onMouseLeave={() => setHoveredNode(null)}
            isGhost={isGhost}
            isSearchedEntity={isSearchedEntity}
        >
            {urn === rootUrn && (
                <HomeIndicatorWrapper>
                    <HomePill showText={false} />
                </HomeIndicatorWrapper>
            )}
            <IconWrapper isGhost={isGhost}>
                {icon && <CustomIcon src={icon} alt={entity?.platform?.name} />}
                {!icon && isDataProcessInstance && entityRegistry.getIcon(EntityType.DataProcessInstance, 18)}
                {!icon && isQuery && <Icon icon="Tilde" source="phosphor" color="gray" size="inherit" />}
                {!icon && !isQuery && !isDataProcessInstance && (
                    <Skeleton.Avatar active shape="circle" size={TRANSFORMATION_NODE_SIZE} />
                )}
            </IconWrapper>
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
        </TransformationalNodeWrapper>
    );

    if (isQuery) {
        return contents;
    }

    const popoverContent = (
        <PopoverWrapper
            urn={urn}
            selected={false}
            dragging={false}
            isGhost={isGhost}
            isSearchedEntity={isSearchedEntity}
        >
            <LineageCard
                type={type}
                loading={!entity}
                onMouseEnter={() => setHoveredNode(urn)}
                onMouseLeave={() => setHoveredNode(null)}
                name={entity ? entity?.name || urn : ''}
                nameExtra={
                    entity?.versionProperties && (
                        <StyledVersioningBadge
                            showPopover={false}
                            versionProperties={entity.versionProperties}
                            size="inherit"
                        />
                    )
                }
                properties={entity?.genericEntityProperties}
                platformIcons={icon ? [icon] : []}
                childrenOpen={false}
            />
        </PopoverWrapper>
    );
    return (
        <Popover
            content={popoverContent}
            overlayInnerStyle={{ boxShadow: 'none', background: 'none', transform: 'translateY(20px)' }}
            overlayClassName="sectioned-tooltip"
        >
            {contents}
        </Popover>
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
