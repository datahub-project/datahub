import { LoadingOutlined } from '@ant-design/icons';
import ContainerPath from '@app/lineageV2/LineageEntityNode/ContainerPath';
import GhostEntityMenu from '@app/lineageV2/LineageEntityNode/GhostEntityMenu';
import SchemaFieldNodeContents from '@app/lineageV2/LineageEntityNode/SchemaFieldNodeContents';
import MatchTextSizeWrapper from '@app/sharedV2/text/MatchTextSizeWrapper';
import { DeprecationIcon } from '@src/app/entityV2/shared/components/styled/DeprecationIcon';
import { KeyboardArrowDown, KeyboardArrowUp } from '@mui/icons-material';
import StructuredPropertyBadge from '@src/app/entityV2/shared/containers/profile/header/StructuredPropertyBadge';
import { Skeleton, Spin } from 'antd';
import { Tooltip } from '@components';
import React, { Dispatch, SetStateAction, useCallback } from 'react';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import { EntityType, LineageDirection } from '../../../types.generated';
import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import { ANTD_GRAY, LINEAGE_COLORS, REDESIGN_COLORS } from '../../entityV2/shared/constants';
import HealthIcon from '../../previewV2/HealthIcon';
import getTypeIcon from '../../sharedV2/icons/getTypeIcon';
import OverflowTitle from '../../sharedV2/text/OverflowTitle';
import { useEntityRegistry } from '../../useEntityRegistry';
import { FetchStatus, getNodeColor, isGhostEntity, LineageEntity, onClickPreventSelect } from '../common';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import { FetchedEntityV2 } from '../types';
import Columns from './Columns';
import { ContractLineageButton } from './ContractLineageButton';
import { ExpandLineageButton } from './ExpandLineageButton';
import ManageLineageMenu from './ManageLineageMenu';
import NodeSkeleton from './NodeSkeleton';
import useAvoidIntersections from './useAvoidIntersections';
import { DisplayedColumns, LINEAGE_NODE_HEIGHT, LINEAGE_NODE_WIDTH } from './useDisplayedColumns';

const NodeWrapper = styled.div<{
    selected: boolean;
    dragging: boolean;
    expandHeight?: number;
    color: string;
    $transitionDuration: number;
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
    max-height: ${({ expandHeight }) => expandHeight}px;
    overflow-y: hidden;
    transition: max-height ${({ $transitionDuration }) => $transitionDuration}ms ease-in-out;
    width: ${LINEAGE_NODE_WIDTH}px;
    cursor: ${({ isGhost, dragging }) => {
        if (isGhost) return 'not-allowed';
        if (dragging) return 'grabbing';
        return 'pointer';
    }};
`;

const CARD_HEIGHT = LINEAGE_NODE_HEIGHT - 2; // Inside border

// Maintains height of node as CardWrapper has position: absolute
// Also allows the expand lineage buttons to not be children of CardWrapper
const FakeCard = styled.div`
    min-height: ${CARD_HEIGHT}px;
    max-height: ${CARD_HEIGHT}px;
    width: 100%;
`;

const CardWrapper = styled.div<{ isGhost: boolean }>`
    align-items: center;
    display: flex;
    flex-direction: row;
    height: ${CARD_HEIGHT}px;
    position: absolute;
    padding: 8px 11px;
    width: 100%;
    ${({ isGhost }) => isGhost && 'opacity: 0.5;'}
`;

const EntityTypeShadow = styled.div<{ color: string; isGhost: boolean }>`
    background: ${({ color }) => color};
    opacity: ${({ isGhost }) => (isGhost ? 0.5 : 1)};
    border-radius: 6px;
    position: absolute;

    height: 100%;
    width: 22px;

    left: -3px;
    top: 0;
    z-index: -1;
`;

export const LoadingWrapper = styled.div`
    color: ${LINEAGE_COLORS.PURPLE_3};
    font-size: 32px;
    line-height: 0;
    pointer-events: none;
    position: absolute;
    top: 10px;
    transform: translateY(-50%);
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

const PlatformIconWithSibling = styled.img`
    height: 0.9em;
    width: 0.9em;
    //clip-path: polygon(0 0, 100% 0, 100% 0%, 0% 100%, 0 100%);

    margin-bottom: 0.8em;
`;

const SiblingPlatformIcon = styled.img`
    height: 0.9em;
    width: 0.9em;
    //clip-path: polygon(100% 0, 100% 0, 100% 100%, 0 100%, 0 100%);

    position: absolute;
    top: 1.1em;
`;

const HorizontalDivider = styled.hr<{ margin: number }>`
    border: 0.5px solid;
    margin: ${({ margin }) => margin}px 0;
    opacity: 0.1;
    width: 100%;
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
    flex-grow: 1;
    justify-content: center;
    gap: 4px;
    height: 100%;
    min-width: 0;
`;

// Expands to fit vertical space
const TitleWrapper = styled.div`
    overflow: hidden;
    flex: 1 0 fit-content;
    min-height: 12px;
`;

// Positions and aligns title text with health icon
// Can't be combined with TitleWrapper, or else centered health icon will not align with text when wrapper expands
const TitleLine = styled.span`
    font-size: 14px;
    font-weight: 600;

    display: flex;
    align-items: center;
    height: min-content;
    gap: 4px;
`;

const ExpandColumnsWrapper = styled(MatchTextSizeWrapper)`
    align-items: center;
    border: 0.5px solid ${LINEAGE_COLORS.BLUE_1}50;
    border-radius: 10px;
    color: ${LINEAGE_COLORS.BLUE_1};
    display: flex;
    justify-content: center;
    width: 100%;

    flex: 1 1 16px;
    min-height: 12px;
    max-height: 16px;

    :hover {
        background-color: ${LINEAGE_COLORS.BLUE_1}20;
        cursor: pointer;
    }
`;

const SkeletonImage = styled(Skeleton.Avatar)`
    line-height: 0;
`;

const PropertyBadgeWrapper = styled.div`
    position: absolute;
    right: 12px;
    top: -16px;
`;

interface Props {
    urn: string;
    type: EntityType;
    selected: boolean;
    dragging: boolean;
    isSearchedEntity: boolean;
    entity?: FetchedEntityV2;
    transitionDuration: number;
    rootUrn: string;
    searchQuery: string;
    setHoveredNode: (urn: string | null) => void;
    showColumns: boolean;
    setShowColumns: Dispatch<SetStateAction<boolean>>;
    onlyWithLineage: boolean;
    setOnlyWithLineage: Dispatch<SetStateAction<boolean>>;
    filterText: string;
    setFilterText: Dispatch<SetStateAction<string>>;
    pageIndex: number;
    setPageIndex: Dispatch<SetStateAction<number>>;
    refetch: Record<LineageDirection, () => void>;
    ignoreSchemaFieldStatus: boolean;
}

const MemoizedNodeContents = React.memo(NodeContents);
export default MemoizedNodeContents;

function NodeContents(props: Props & LineageEntity & DisplayedColumns) {
    const {
        urn,
        type,
        selected,
        dragging,
        isSearchedEntity,
        entity,
        fetchStatus,
        isExpanded,
        transitionDuration,
        rootUrn,
        searchQuery,
        setHoveredNode,
        showColumns,
        setShowColumns,
        onlyWithLineage,
        setOnlyWithLineage,
        filterText,
        setFilterText,
        pageIndex,
        setPageIndex,
        paginatedColumns,
        extraHighlightedColumns,
        numColumnsTotal,
        numFilteredColumns,
        numColumnsWithLineage,
        refetch,
        ignoreSchemaFieldStatus,
    } = props;

    const entityRegistry = useEntityRegistry();

    const isGhost = isGhostEntity(entity, ignoreSchemaFieldStatus);

    const numDisplayedColumns = extraHighlightedColumns.length + (showColumns ? paginatedColumns.length : 0);
    const expandHeight =
        LINEAGE_NODE_HEIGHT +
        (numDisplayedColumns || onlyWithLineage ? 17 : 0) + // Expansion base
        (showColumns && numColumnsTotal ? 30 : 0) + // Search bar
        20.5 * numDisplayedColumns + // Columns
        (showColumns && paginatedColumns.length && extraHighlightedColumns.length ? 9 : 0) + // Column divider
        (showColumns && numFilteredColumns > NUM_COLUMNS_PER_PAGE ? 38 : 0); // Pagination

    useAvoidIntersections(urn, expandHeight);

    const platformName = entityRegistry.getDisplayName(EntityType.DataPlatform, entity?.platform);
    const [nodeColor] = getNodeColor(type);
    const highlightColor = isSearchedEntity ? REDESIGN_COLORS.YELLOW_500 : REDESIGN_COLORS.YELLOW_200;
    const hasUpstreamChildren = !!entity?.numUpstreamChildren;
    const hasDownstreamChildren = !!entity?.numDownstreamChildren;
    const isExpandedDownstream = isExpanded[LineageDirection.Downstream];
    const isExpandedUpstream = isExpanded[LineageDirection.Upstream];
    const isDownstreamHidden =
        fetchStatus[LineageDirection.Downstream] === FetchStatus.COMPLETE && !isExpandedDownstream;
    const isUpstreamHidden = fetchStatus[LineageDirection.Upstream] === FetchStatus.COMPLETE && !isExpandedUpstream;

    const showHideColumns = useCallback(
        (e: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
            onClickPreventSelect(e);
            analytics.event({
                type: EventType.ShowHideLineageColumnsEvent,
                action: showColumns ? 'hide' : 'show',
                entityUrn: urn,
                entityType: type,
                entityPlatformUrn: entity?.platform?.urn,
            });
            setShowColumns((prevShowColumns) => !prevShowColumns);
        },
        [showColumns, setShowColumns, urn, type, entity?.platform?.urn],
    );

    // TODO: Refactor into separate node, that doesn't have columns, with shared code?
    if (type === EntityType.SchemaField) {
        return (
            <SchemaFieldNodeContents
                urn={urn}
                type={type}
                rootUrn={rootUrn}
                selected={selected}
                dragging={dragging}
                isGhost={isGhost}
                isSearchedEntity={isSearchedEntity}
                hasUpstreamChildren={hasUpstreamChildren}
                hasDownstreamChildren={hasDownstreamChildren}
                isExpanded={isExpanded}
                fetchStatus={fetchStatus}
                entity={entity}
                platformName={platformName}
                platformIcon={entity?.icon}
                searchQuery={searchQuery}
                setHoveredNode={setHoveredNode}
                ignoreSchemaFieldStatus={ignoreSchemaFieldStatus}
            />
        );
    }

    const contents = (
        <NodeWrapper
            selected={selected}
            dragging={dragging}
            expandHeight={expandHeight}
            color={nodeColor}
            $transitionDuration={transitionDuration}
            data-testid={`lineage-node-${urn}`}
            isGhost={isGhost}
            isSearchedEntity={isSearchedEntity}
        >
            <EntityTypeShadow color={nodeColor} isGhost={isGhost} />
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
                    {entity?.icon && entity.lineageSiblingIcon && (
                        <>
                            <PlatformIconWithSibling src={entity.lineageSiblingIcon} />
                            <SiblingPlatformIcon src={entity.icon} />
                        </>
                    )}
                    {entity?.icon && !entity.lineageSiblingIcon && (
                        <PlatformIcon src={entity.icon} alt={platformName || 'platform'} title={platformName} />
                    )}
                    {!entity && <SkeletonImage size="small" shape="square" style={{ borderRadius: '20%' }} />}
                    {entity ? (
                        getTypeIcon(entityRegistry, entity.type, entity.subtype, true)
                    ) : (
                        <SkeletonImage size="small" shape="square" style={{ borderRadius: '20%' }} />
                    )}
                </IconsWrapper>
                <VerticalDivider margin={8} />
                {entity && (
                    <MainTextWrapper>
                        <ContainerPath parents={entity?.containers} />
                        <TitleWrapper>
                            <TitleLine>
                                <OverflowTitle
                                    title={entity?.name}
                                    highlightText={searchQuery}
                                    highlightColor={highlightColor}
                                />
                                {entity?.deprecation?.deprecated && (
                                    <DeprecationIcon
                                        urn={urn}
                                        deprecation={entity?.deprecation}
                                        showText={false}
                                        showUndeprecate={false}
                                    />
                                )}
                                {entity?.health && (
                                    <HealthIcon
                                        urn={urn}
                                        health={entity.health}
                                        baseUrl={entityRegistry.getEntityUrl(type, urn)}
                                    />
                                )}
                            </TitleLine>
                        </TitleWrapper>
                        {!!numColumnsTotal && !isGhost && (
                            <ExpandColumnsWrapper onClick={showHideColumns} defaultHeight={10}>
                                {numColumnsTotal} columns
                                {showColumns && <KeyboardArrowUp fontSize="inherit" style={{ marginLeft: 3 }} />}
                                {!showColumns && <KeyboardArrowDown fontSize="inherit" style={{ marginLeft: 3 }} />}
                            </ExpandColumnsWrapper>
                        )}
                        {isGhost ? <GhostEntityMenu urn={urn} /> : <ManageLineageMenu node={props} refetch={refetch} />}
                        {entity && (
                            <PropertyBadgeWrapper>
                                <StructuredPropertyBadge structuredProperties={entity.structuredProperties} />
                            </PropertyBadgeWrapper>
                        )}
                    </MainTextWrapper>
                )}
                {!entity && <NodeSkeleton />}
            </CardWrapper>
            {!!entity && !!numColumnsTotal && (
                <>
                    <HorizontalDivider margin={0} />
                    <Columns
                        entity={entity}
                        isGhost={isGhost}
                        showAllColumns={showColumns}
                        paginatedColumns={paginatedColumns}
                        highlightedColumns={extraHighlightedColumns}
                        numFiltered={numFilteredColumns}
                        pageIndex={pageIndex}
                        setPageIndex={setPageIndex}
                        filterText={filterText}
                        setFilterText={setFilterText}
                        numColumnsWithLineage={numColumnsWithLineage}
                        onlyWithLineage={onlyWithLineage}
                        setOnlyWithLineage={setOnlyWithLineage}
                    />
                </>
            )}
        </NodeWrapper>
    );

    if (isGhost) {
        const message = entity?.status?.removed ? 'has been deleted' : 'does not exist in DataHub';
        return (
            <Tooltip title={`This entity ${message}`} mouseEnterDelay={0.3}>
                {contents}
            </Tooltip>
        );
    }
    return contents;
}
