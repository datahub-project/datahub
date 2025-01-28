import { LoadingOutlined } from '@ant-design/icons';
import { ANTD_GRAY, LINEAGE_COLORS, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { FetchStatus } from '@app/lineageV2/common';
import ContainerPath from '@app/lineageV2/LineageEntityNode/ContainerPath';
import { ContractLineageButton } from '@app/lineageV2/LineageEntityNode/ContractLineageButton';
import { ExpandLineageButton } from '@app/lineageV2/LineageEntityNode/ExpandLineageButton';
import { LoadingWrapper } from '@app/lineageV2/LineageEntityNode/NodeContents';
import NodeSkeleton from '@app/lineageV2/LineageEntityNode/NodeSkeleton';
import { FetchedEntityV2 } from '@app/lineageV2/types';
import { COLORS } from '@app/sharedV2/colors';
import getTypeIcon from '@app/sharedV2/icons/getTypeIcon';
import OverflowTitle from '@app/sharedV2/text/OverflowTitle';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { EntityType, LineageDirection } from '@types';
import { Skeleton, Spin } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import LinkOut from '@images/link-out.svg?react';
import { downgradeV2FieldPath, useGetLineageUrl } from '../lineageUtils';

export const SCHEMA_FIELD_NODE_HEIGHT = 80;
export const SCHEMA_FIELD_NODE_WIDTH = 240;
const NODE_COLOR = COLORS.blue_7;

const NodeWrapper = styled.div<{
    selected: boolean;
    dragging: boolean;
    color: string;
    isGhost: boolean;
    isSearchedEntity: boolean;
}>`
    align-items: center;
    background-color: white;
    border: 1px solid
        ${({ color, selected, isGhost }) => {
            if (selected) return color;
            if (isGhost) return `${LINEAGE_COLORS.NODE_BORDER}50`;
            return LINEAGE_COLORS.NODE_BORDER;
        }};
    box-shadow: ${({ isSearchedEntity }) =>
        isSearchedEntity ? `0 0 4px 4px ${REDESIGN_COLORS.TITLE_PURPLE}95` : 'none'};
    outline: ${({ color, selected }) => (selected ? `1px solid ${color}` : 'none')};
    border-left: none;
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    overflow-y: hidden;
    width: ${SCHEMA_FIELD_NODE_WIDTH}px;
    cursor: ${({ isGhost, dragging }) => {
        if (isGhost) return 'not-allowed';
        if (dragging) return 'grabbing';
        return 'grab';
    }};
`;

const CARD_HEIGHT = SCHEMA_FIELD_NODE_HEIGHT - 2; // Inside border

// Maintains height of node as CardWrapper has position: absolute
// Also allows the expand lineage buttons to not be children of CardWrapper
const FakeCard = styled.div`
    min-height: ${CARD_HEIGHT}px;
    max-height: ${CARD_HEIGHT}px;
    width: 100%;
`;

const CardWrapper = styled.div<{ isGhost: boolean }>`
    display: flex;
    flex-direction: row;
    align-items: center;
    height: ${CARD_HEIGHT}px;
    position: absolute;
    padding: 8px 11px;
    width: 100%;
    ${({ isGhost }) => isGhost && 'opacity: 0.5;'}
`;

const EntityTypeShadow = styled.div<{ color: string }>`
    background: ${({ color }) => color};
    border-radius: 6px;
    position: absolute;

    height: 100%;
    width: 22px;

    left: -3px;
    top: 0;
    z-index: -1;
`;

const CustomHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    top: 50%;
    ${({ position }) => (position === Position.Left ? 'left: -3px;' : 'right: 0;')}
`;

const IconsWrapper = styled.div`
    align-items: center;
    color: ${ANTD_GRAY[10]};
    display: flex;
    flex-direction: column;
    font-size: 24px;
    gap: 4px;
`;

const PlatformIcon = styled.img`
    height: 1em;
    width: 1em;
`;

const VerticalDivider = styled.hr<{ margin: number }>`
    align-self: stretch;
    height: auto;
    margin: 0 ${({ margin }) => margin}px;
    border: 0.5px solid;
    opacity: 0.1;
    vertical-align: text-top;
`;

const MainTextWrapper = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    flex-grow: 1;
    height: 100%;
    min-width: 0;
`;

const TitleWrapper = styled.div`
    overflow: hidden;
    flex: 1 0 fit-content;
    min-height: 12px;
`;

const ParentLine = styled.span`
    display: flex;
    align-items: center;
    height: min-content;
    gap: 4px;

    color: ${REDESIGN_COLORS.SUBTITLE};
    font-weight: 600;
`;

const InvalidSchemaFieldLine = styled.span`
    display: flex;
    align-items: center;
    height: min-content;
    gap: 4px;

    font-size: 1.1em;
    font-weight: 700;
    color: inherit;
`;

const SchemaFieldLine = styled(Link)`
    display: flex;
    align-items: center;
    height: min-content;
    width: max-content;
    gap: 4px;

    font-size: 1.1em;
    font-weight: 700;
    color: inherit;

    :hover {
        color: inherit;
        text-decoration: underline;
    }
`;

const SkeletonImage = styled(Skeleton.Avatar)`
    line-height: 0;
`;

const StyledNodeSkeleton = styled(NodeSkeleton)`
    font-size: 10px;
`;

const ParentContainerPath = styled(ContainerPath)`
    font-size: 10px;
`;

const ColumnLinkWrapper = styled(Link)`
    display: flex;
    color: inherit;

    :hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

interface Props {
    urn: string;
    type: EntityType;
    rootUrn: string;
    selected: boolean;
    dragging: boolean;
    isSearchedEntity: boolean;
    isGhost: boolean;
    hasUpstreamChildren: boolean;
    hasDownstreamChildren: boolean;
    isExpanded?: Record<LineageDirection, boolean>;
    fetchStatus: Record<LineageDirection, FetchStatus>;
    entity?: FetchedEntityV2;
    platformName?: string;
    platformIcon?: string;
    searchQuery: string;
    setHoveredNode: (urn: string | null) => void;
    ignoreSchemaFieldStatus: boolean;
}

export default function SchemaFieldNodeContents({
    urn,
    type,
    rootUrn,
    selected,
    dragging,
    isSearchedEntity,
    isGhost,
    hasUpstreamChildren,
    hasDownstreamChildren,
    isExpanded,
    fetchStatus,
    entity,
    platformName,
    platformIcon,
    searchQuery,
    setHoveredNode,
    ignoreSchemaFieldStatus,
}: Props) {
    const entityRegistry = useEntityRegistryV2();

    const isExpandedDownstream = isExpanded?.[LineageDirection.Downstream];
    const isExpandedUpstream = isExpanded?.[LineageDirection.Upstream];
    const isDownstreamHidden =
        fetchStatus[LineageDirection.Downstream] === FetchStatus.COMPLETE && !isExpandedDownstream;
    const isUpstreamHidden = fetchStatus[LineageDirection.Upstream] === FetchStatus.COMPLETE && !isExpandedUpstream;

    const parent = entity?.parent;
    const parentLineageUrl = useGetLineageUrl(parent?.urn, parent?.type);
    const lineageUrl = useGetLineageUrl(urn, EntityType.SchemaField);

    const highlightColor = isSearchedEntity ? REDESIGN_COLORS.YELLOW_500 : REDESIGN_COLORS.YELLOW_200;
    const contents = (
        <NodeWrapper
            selected={selected}
            dragging={dragging}
            isGhost={isGhost}
            isSearchedEntity={isSearchedEntity}
            color={NODE_COLOR}
        >
            <EntityTypeShadow color={NODE_COLOR} />
            <FakeCard />
            <FakeCard style={{ position: 'absolute' }}>
                {hasUpstreamChildren &&
                    ([FetchStatus.UNFETCHED, FetchStatus.LOADING].includes(fetchStatus[LineageDirection.Upstream]) ||
                        isUpstreamHidden) && (
                        <ExpandLineageButton
                            urn={urn}
                            type={type}
                            direction={LineageDirection.Upstream}
                            display={
                                fetchStatus[LineageDirection.Upstream] === FetchStatus.UNFETCHED || !isExpandedUpstream
                            }
                            fetchStatus={fetchStatus}
                            ignoreSchemaFieldStatus={ignoreSchemaFieldStatus}
                        />
                    )}
                {hasDownstreamChildren &&
                    ([FetchStatus.UNFETCHED, FetchStatus.LOADING].includes(fetchStatus[LineageDirection.Downstream]) ||
                        isDownstreamHidden) && (
                        <ExpandLineageButton
                            urn={urn}
                            type={type}
                            direction={LineageDirection.Downstream}
                            display={
                                fetchStatus[LineageDirection.Downstream] === FetchStatus.UNFETCHED ||
                                !isExpandedDownstream
                            }
                            fetchStatus={fetchStatus}
                            ignoreSchemaFieldStatus={ignoreSchemaFieldStatus}
                        />
                    )}
                {fetchStatus[LineageDirection.Upstream] === FetchStatus.COMPLETE &&
                    isExpandedUpstream &&
                    hasUpstreamChildren && <ContractLineageButton urn={urn} direction={LineageDirection.Upstream} />}
                {fetchStatus[LineageDirection.Downstream] === FetchStatus.COMPLETE &&
                    isExpandedDownstream &&
                    hasDownstreamChildren && (
                        <ContractLineageButton urn={urn} direction={LineageDirection.Downstream} />
                    )}
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
            </FakeCard>
            <CardWrapper
                isGhost={isGhost}
                onMouseEnter={() => setHoveredNode(urn)}
                onMouseLeave={() => setHoveredNode(null)}
            >
                <CustomHandle type="target" position={Position.Left} isConnectable={false} />
                <CustomHandle type="source" position={Position.Right} isConnectable={false} />
                <IconsWrapper>
                    {platformIcon ? (
                        <PlatformIcon src={platformIcon} alt={platformName || 'platform'} title={platformName} />
                    ) : (
                        <SkeletonImage size="small" shape="square" style={{ borderRadius: '20%' }} />
                    )}
                    {parent?.type ? (
                        getTypeIcon(entityRegistry, parent.type, parent.subTypes?.[0], true)
                    ) : (
                        <SkeletonImage size="small" shape="square" style={{ borderRadius: '20%' }} />
                    )}
                </IconsWrapper>
                <VerticalDivider margin={8} />
                <MainTextWrapper>
                    <ParentContainerPath parents={parent?.parentContainers?.containers} />
                    {!!parent && (
                        <TitleWrapper>
                            <ParentLine>
                                <OverflowTitle
                                    title={parent?.name ?? undefined}
                                    highlightText={searchQuery}
                                    highlightColor={highlightColor}
                                />
                                {!!parent.urn && !!parent.type && (
                                    <ColumnLinkWrapper
                                        to={parentLineageUrl}
                                        onClick={(e) => e.stopPropagation()}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                    >
                                        <Tooltip title="Explore parent lineage" mouseEnterDelay={0.5}>
                                            <LinkOut />
                                        </Tooltip>
                                    </ColumnLinkWrapper>
                                )}
                            </ParentLine>
                        </TitleWrapper>
                    )}
                    {!!entity && (
                        <TitleWrapper>
                            {isGhost || urn === rootUrn ? (
                                <InvalidSchemaFieldLine>
                                    <OverflowTitle
                                        title={downgradeV2FieldPath(entity.name)}
                                        highlightText={searchQuery}
                                        highlightColor={highlightColor}
                                    />
                                </InvalidSchemaFieldLine>
                            ) : (
                                <Tooltip title="Change home node" mouseEnterDelay={0.3}>
                                    <SchemaFieldLine to={lineageUrl}>
                                        <OverflowTitle
                                            title={downgradeV2FieldPath(entity.name)}
                                            highlightText={searchQuery}
                                            highlightColor={highlightColor}
                                        />
                                    </SchemaFieldLine>
                                </Tooltip>
                            )}
                        </TitleWrapper>
                    )}
                </MainTextWrapper>
                {!entity && <StyledNodeSkeleton numRows={2} />}
            </CardWrapper>
        </NodeWrapper>
    );

    if (isGhost) {
        const message =
            entity?.status?.removed || entity?.parent?.status?.removed
                ? 'has been deleted'
                : 'does not exist in DataHub';
        return (
            <Tooltip title={`This entity ${message}`} mouseEnterDelay={0.3}>
                {contents}
            </Tooltip>
        );
    }
    return contents;
}
