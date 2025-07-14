import { LoadingOutlined } from '@ant-design/icons';
import { Icon, Popover } from '@components';
import { Skeleton, Spin } from 'antd';
import React, { useContext } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
import { Handle, NodeProps, Position } from 'reactflow';
import styled from 'styled-components';

import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import HomePill from '@app/lineageV3/LineageEntityNode/HomePill';
import ManageLineageMenu from '@app/lineageV3/LineageEntityNode/ManageLineageMenu';
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
import useRefetchLineage from '@app/lineageV3/queries/useRefetchLineage';
import { getLineageUrl } from '@app/lineageV3/utils/lineageUtils';
import HealthIcon from '@app/previewV2/HealthIcon';
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
    top: -17px;
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

const IconWrapper = styled.div`
    display: flex;
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

    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistryV2();
    const refetch = useRefetchLineage(urn, type);

    const isQuery = type === EntityType.Query;
    const isDataProcessInstance = type === EntityType.DataProcessInstance;

    const { rootUrn } = useContext(LineageNodesContext);
    const { cllHighlightedNodes, setHoveredNode, displayedMenuNode, setDisplayedMenuNode } =
        useContext(LineageDisplayContext);
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
            onDoubleClick={isGhost ? undefined : () => history.push(getLineageUrl(urn, type, location, entityRegistry))}
            isGhost={isGhost}
            isSearchedEntity={isSearchedEntity}
        >
            {urn === rootUrn && (
                <HomeIndicatorWrapper>
                    <HomePill showText={false} />
                </HomeIndicatorWrapper>
            )}
            <IconWrapper>
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

    const menuActions = [
        entity?.deprecation?.deprecated && (
            <DeprecationIcon urn={urn} deprecation={entity?.deprecation} showText={false} showUndeprecate={false} />
        ),
        entity?.health && (
            <HealthIcon urn={urn} health={entity.health} baseUrl={entityRegistry.getEntityUrl(type, urn)} />
        ),
        <ManageLineageMenu
            node={data}
            refetch={refetch}
            isRootUrn={urn === rootUrn}
            isGhost={isGhost}
            isOpen={displayedMenuNode === urn}
            setDisplayedMenuNode={setDisplayedMenuNode}
        />,
    ];

    const popoverContent = (
        <PopoverWrapper
            urn={urn}
            selected={false}
            dragging={false}
            isGhost={isGhost}
            isSearchedEntity={isSearchedEntity}
        >
            <LineageCard
                urn={urn}
                type={type}
                loading={!entity}
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
                menuActions={menuActions}
            />
        </PopoverWrapper>
    );
    return (
        <Popover
            content={popoverContent}
            overlayInnerStyle={{ boxShadow: 'none', background: 'none', padding: 0 }}
            overlayStyle={{ padding: 0 }}
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
