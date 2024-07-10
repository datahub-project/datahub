import { LoadingOutlined } from '@ant-design/icons';
import { ANTD_GRAY, LINEAGE_COLORS, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { FetchStatus } from '@app/lineageV2/common';
import ContainerPath from '@app/lineageV2/LineageEntityNode/ContainerPath';
import { ContractLineageButton } from '@app/lineageV2/LineageEntityNode/ContractLineageButton';
import { ExpandLineageButton } from '@app/lineageV2/LineageEntityNode/ExpandLineageButton';
import { LoadingWrapper } from '@app/lineageV2/LineageEntityNode/NodeContents';
import NodeSkeleton from '@app/lineageV2/LineageEntityNode/NodeSkeleton';
import { FetchedEntityV2 } from '@app/lineageV2/types';
import HealthIcon from '@app/previewV2/HealthIcon';
import { COLORS } from '@app/sharedV2/colors';
import getTypeIcon from '@app/sharedV2/icons/getTypeIcon';
import OverflowTitle from '@app/sharedV2/text/OverflowTitle';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { EntityType, LineageDirection } from '@types';
import { Skeleton, Spin, Tooltip } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import LinkOut from '@images/link-out.svg?react';

export const SCHEMA_FIELD_NODE_HEIGHT = 40;
export const SCHEMA_FIELD_NODE_WIDTH = 160;
const NODE_COLOR = COLORS.blue_7;

const LinkOutIcon = styled(LinkOut)``;

const NodeWrapper = styled.div<{
    selected: boolean;
    color: string;
}>`
    align-items: center;
    background-color: white;
    border: 1px solid ${({ color, selected }) => (selected ? color : LINEAGE_COLORS.NODE_BORDER)};
    border-bottom: 1px solid ${LINEAGE_COLORS.NODE_BORDER};
    // outline: ${({ color, selected }) => (selected ? `1px solid ${color}` : 'none')};
    border-left: none;
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    overflow-y: hidden;
    width: ${SCHEMA_FIELD_NODE_WIDTH}px;
    cursor: pointer;

    ${LinkOutIcon} {
        display: none;
    }

    :hover {
        ${LinkOutIcon} {
            display: inline;
        }
    }
`;

const CARD_HEIGHT = SCHEMA_FIELD_NODE_HEIGHT - 2; // Inside border

// Maintains height of node as CardWrapper has position: absolute
// Also allows the expand lineage buttons to not be children of CardWrapper
const FakeCard = styled.div`
    min-height: ${CARD_HEIGHT}px;
    max-height: ${CARD_HEIGHT}px;
    width: 100%;
`;

const CardWrapper = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    height: ${CARD_HEIGHT}px;
    position: absolute;
    padding: 6px 9px;
    width: 100%;
`;

const ParentWrapper = styled.div`
    display: flex;
    flex-direction: row;
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
    margin-right: 8px;
`;

const PlatformIcon = styled.img`
    height: 1em;
    width: 1em;
`;

const MainTextWrapper = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    flex-grow: 1;
    height: 100%;
    min-width: 0;
`;

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    overflow: hidden;
`;

const Title = styled(OverflowTitle)`
    font-weight: 600;
    line-height: 1.25em;
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
    margin-left: auto;

    color: inherit;
    :hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

const SchemaFieldDrawer = styled.div<{
    selected: boolean;
    color: string;
}>`
    display: flex;
    align-items: center;

    position: absolute;
    left: 2%;
    bottom: -19px;
    width: 96%;
    z-index: -1;
    min-height: 23px;

    background-color: ${REDESIGN_COLORS.WHITE};
    border: 1px solid ${({ color, selected }) => (selected ? color : LINEAGE_COLORS.NODE_BORDER)};
    outline: ${({ color, selected }) => (selected ? `1px solid ${color}` : 'none')};
    border-radius: 4px;

    padding: 4px 6px 2px;
`;

interface Props {
    urn: string;
    type: EntityType;
    rootUrn: string;
    selected: boolean;
    hasUpstreamChildren: boolean;
    hasDownstreamChildren: boolean;
    isExpanded?: Record<LineageDirection, boolean>;
    fetchStatus: Record<LineageDirection, FetchStatus>;
    entity?: FetchedEntityV2;
    platformName?: string;
    platformIcon?: string;
    setHoveredNode: (urn: string | null) => void;
}

export default function SchemaFieldNodeContents({
    urn,
    type,
    rootUrn,
    selected,
    hasUpstreamChildren,
    hasDownstreamChildren,
    isExpanded,
    fetchStatus,
    entity,
    platformName,
    platformIcon,
    setHoveredNode,
}: Props) {
    const entityRegistry = useEntityRegistryV2();

    const isExpandedDownstream = isExpanded?.[LineageDirection.Downstream];
    const isExpandedUpstream = isExpanded?.[LineageDirection.Upstream];
    const isDownstreamHidden =
        fetchStatus[LineageDirection.Downstream] === FetchStatus.COMPLETE && !isExpandedDownstream;
    const isUpstreamHidden = fetchStatus[LineageDirection.Upstream] === FetchStatus.COMPLETE && !isExpandedUpstream;

    const parent = entity?.parents?.[0];

    return (
        <NodeWrapper selected={selected} color={NODE_COLOR}>
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
            <CardWrapper onMouseEnter={() => setHoveredNode(urn)} onMouseLeave={() => setHoveredNode(null)}>
                <ParentWrapper>
                    <CustomHandle type="target" position={Position.Left} isConnectable={false} />
                    <CustomHandle type="source" position={Position.Right} isConnectable={false} />
                    <IconsWrapper>
                        {platformIcon ? (
                            <PlatformIcon src={platformIcon} alt={platformName || 'platform'} title={platformName} />
                        ) : (
                            <SkeletonImage size="small" shape="square" style={{ borderRadius: '20%' }} />
                        )}
                    </IconsWrapper>
                    {entity && (
                        <MainTextWrapper>
                            <ParentContainerPath parents={entity?.parents?.[0].parentContainers?.containers} />
                            {parent?.type && (
                                <Header>
                                    <TitleWrapper>
                                        {getTypeIcon(entityRegistry, parent.type, parent.subTypes?.[0], true)}
                                        <Title title={parent?.name ?? undefined} />
                                        {parent?.health && (
                                            <HealthIcon
                                                health={parent.health}
                                                baseUrl={entityRegistry.getEntityUrl(type, urn)}
                                            />
                                        )}
                                        {parent.urn && (
                                            <ColumnLinkWrapper
                                                to={`${entityRegistry.getEntityUrl(parent.type, parent.urn)}/Lineage`}
                                                onClick={(e) => e.stopPropagation()}
                                                target="_blank"
                                                rel="noopener noreferrer"
                                            >
                                                <Tooltip title="Explore parent lineage" mouseEnterDelay={0.5}>
                                                    <LinkOutIcon />
                                                </Tooltip>
                                            </ColumnLinkWrapper>
                                        )}
                                    </TitleWrapper>
                                </Header>
                            )}
                        </MainTextWrapper>
                    )}
                    {!entity && <StyledNodeSkeleton numRows={2} />}
                </ParentWrapper>
                {entity && (
                    <SchemaFieldDrawer selected={selected} color={NODE_COLOR}>
                        <Title title={entity.name} />
                    </SchemaFieldDrawer>
                )}
            </CardWrapper>
        </NodeWrapper>
    );
}
