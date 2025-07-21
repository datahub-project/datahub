import { LoadingOutlined } from '@ant-design/icons';
import { Pill, Tooltip, colors } from '@components';
import { Spin } from 'antd';
import React, { Dispatch, SetStateAction, useCallback } from 'react';
import { Link, useHistory, useLocation } from 'react-router-dom';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { LastRunIcon } from '@app/entityV2/dataJob/tabs/RunsTab';
import { LINEAGE_COLORS } from '@app/entityV2/shared/constants';
import StructuredPropertyBadge from '@app/entityV2/shared/containers/profile/header/StructuredPropertyBadge';
import VersioningBadge from '@app/entityV2/shared/versioning/VersioningBadge';
import Columns from '@app/lineageV3/LineageEntityNode/Columns';
import { ContractLineageButton } from '@app/lineageV3/LineageEntityNode/ContractLineageButton';
import { ExpandLineageButton } from '@app/lineageV3/LineageEntityNode/ExpandLineageButton';
import HomePill from '@app/lineageV3/LineageEntityNode/HomePill';
import ManageLineageMenu from '@app/lineageV3/LineageEntityNode/ManageLineageMenu';
import useAvoidIntersections from '@app/lineageV3/LineageEntityNode/useAvoidIntersections';
import { DisplayedColumns } from '@app/lineageV3/LineageEntityNode/useDisplayedColumns';
import NodeWrapper from '@app/lineageV3/NodeWrapper';
import {
    FetchStatus,
    LINEAGE_NODE_HEIGHT,
    LineageEntity,
    VERTICAL_HANDLE,
    isGhostEntity,
    onClickPreventSelect,
} from '@app/lineageV3/common';
import LineageCard from '@app/lineageV3/components/LineageCard';
import { NUM_COLUMNS_PER_PAGE } from '@app/lineageV3/constants';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { getLineageUrl } from '@app/lineageV3/utils/lineageUtils';
import HealthIcon from '@app/previewV2/HealthIcon';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import OverflowTitle from '@app/sharedV2/text/OverflowTitle';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { DeprecationIcon } from '@src/app/entityV2/shared/components/styled/DeprecationIcon';

import { DataProcessRunStatus, EntityType, LineageDirection } from '@types';

import LinkOut from '@images/link-out.svg?react';

export const LoadingWrapper = styled.div`
    color: ${LINEAGE_COLORS.PURPLE_3};
    font-size: 32px;
    line-height: 0;
    pointer-events: none;
    position: absolute;
    top: 10px;
    transform: translateY(-50%);
`;

const HorizontalHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    ${({ position }) => (position === Position.Left ? 'left: 0;' : 'right: 0;')}
`;

const VerticalHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    left: 50%;
    ${({ position }) => (position === Position.Top ? 'top: 0;' : 'bottom: 0;')}
`;

const PropertyBadgeWrapper = styled.div`
    position: absolute;
    right: 12px;
    top: -20px;
`;

const HomeIndicatorWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;

    position: absolute;
    top: -20px;
    left: 12px;
    z-index: -1;
`;

const ColumnsWrapper = styled.div<{
    height: number;
    transitionDuration: number;
}>`
    max-height: ${({ height }) => height}px;
    transition: max-height ${({ transitionDuration }) => transitionDuration}ms ease-in-out;
    overflow-y: hidden;
    width: 100%;
`;

const ChildrenTextWrapper = styled.span`
    display: flex;
    gap: 8px;
`;

export const StyledVersioningBadge = styled(VersioningBadge)`
    padding: 0 4px;
    line-height: 1;
    max-width: 100px;
`;

const LinkOutWrapper = styled(Link)`
    display: flex;
    color: inherit;

    :hover {
        color: ${(props) => props.theme.styles['primary-color']};
    }
`;

const ExtraDetailsWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;

    color: ${colors.gray[1700]};
    font-size: 12px;
    font-weight: 400;
    line-height: normal;
    margin-top: 2px;
`;

const DataJobLastRunWrapper = styled(ExtraDetailsWrapper)`
    align-items: end;
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
    rootType: EntityType;
    parentDataJob?: string;
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
    isMenuDisplayed: boolean;
    setDisplayedMenuNode: Dispatch<SetStateAction<string | null>>;
    selectedColumn: string | null;
    setSelectedColumn: Dispatch<SetStateAction<string | null>>;
    hoveredColumn: string | null;
    setHoveredColumn: Dispatch<SetStateAction<string | null>>;
    refetch: Record<LineageDirection, () => void>;
    ignoreSchemaFieldStatus: boolean;
    numUpstreams?: number;
    numDownstreams?: number;
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
        rootType,
        parentDataJob,
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
        isMenuDisplayed,
        setDisplayedMenuNode,
        selectedColumn,
        setSelectedColumn,
        hoveredColumn,
        setHoveredColumn,
        paginatedColumns,
        extraHighlightedColumns,
        numColumnsTotal,
        numFilteredColumns,
        numColumnsWithLineage,
        refetch,
        ignoreSchemaFieldStatus,
        numUpstreams,
        numDownstreams,
    } = props;

    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    const isGhost = isGhostEntity(entity, ignoreSchemaFieldStatus);
    const isVertical = !!parentDataJob;

    const numDisplayedColumns = extraHighlightedColumns.length + (showColumns ? paginatedColumns.length : 0);
    const columnsHeight =
        (numDisplayedColumns || showColumns ? 12 : 0) + // Expansion base
        (showColumns && numColumnsTotal ? 40 : 0) + // Search bar
        29 * numDisplayedColumns + // Columns
        4 * Math.max(numDisplayedColumns - 1, 0) + // Column gap
        (showColumns && paginatedColumns.length && extraHighlightedColumns.length ? 17 : 0) + // Column divider
        (showColumns && numFilteredColumns > NUM_COLUMNS_PER_PAGE ? 40 : 0); // Pagination

    useAvoidIntersections(urn, columnsHeight + LINEAGE_NODE_HEIGHT, rootType, isVertical);

    const highlightColor = isSearchedEntity ? colors.yellow[400] : colors.yellow[200];

    const hasUpstreamChildren = !!(numUpstreams ?? !!entity?.numUpstreamChildren);
    const hasDownstreamChildren = !!(numDownstreams ?? !!entity?.numDownstreamChildren);
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

    const platformIcons = [entity?.icon, entity?.lineageSiblingIcon].filter((icon): icon is string => !!icon);

    const menuActions = [
        entity?.deprecation?.deprecated && (
            <DeprecationIcon urn={urn} deprecation={entity?.deprecation} showText={false} showUndeprecate={false} />
        ),
        entity?.health && (
            <HealthIcon urn={urn} health={entity.health} baseUrl={entityRegistry.getEntityUrl(type, urn)} />
        ),
        <ManageLineageMenu
            node={props}
            refetch={refetch}
            isRootUrn={urn === rootUrn}
            isGhost={isGhost}
            isOpen={isMenuDisplayed}
            setDisplayedMenuNode={setDisplayedMenuNode}
        />,
    ];

    let properties = entity?.genericEntityProperties;
    let extraDetails: React.ReactNode = null;
    if (type === EntityType.SchemaField) {
        if (entity?.parent) {
            const { parent } = entity;
            properties = parent;
            extraDetails = (
                <ExtraDetailsWrapper>
                    <OverflowTitle title={parent.name || parent.urn} />
                    {!!parent.urn && !!parent.type && (
                        <LinkOutWrapper
                            to={getLineageUrl(parent.urn, parent.type, location, entityRegistry)}
                            onClick={(e) => e.stopPropagation()}
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            <Tooltip title="Explore parent lineage" mouseEnterDelay={0.5}>
                                <LinkOut />
                            </Tooltip>
                        </LinkOutWrapper>
                    )}
                </ExtraDetailsWrapper>
            );
        } else {
            extraDetails = <></>; // Tells LineageCard to display a skeleton
        }
    } else if (type === EntityType.DataJob) {
        const lastRun = entity?.genericEntityProperties?.lastRun;
        const lastRunEvent = entity?.genericEntityProperties?.lastRunEvent;

        const time = lastRun?.properties?.created.time || lastRun?.created?.time;
        if (time) {
            extraDetails = (
                <DataJobLastRunWrapper>
                    {lastRunEvent?.status === DataProcessRunStatus.Started ? (
                        <>Running</>
                    ) : (
                        <>Last run {toRelativeTimeString(time)}</>
                    )}
                    {!!lastRunEvent?.status && (
                        <LastRunIcon
                            status={lastRunEvent.status}
                            resultType={lastRunEvent.result?.resultType ?? undefined}
                            showTooltip
                        />
                    )}
                </DataJobLastRunWrapper>
            );
        } else {
            extraDetails = <></>; // Tells LineageCard to display a skeleton
        }
    }

    const contents = (
        <>
            {urn === rootUrn && (
                <HomeIndicatorWrapper>
                    <HomePill showText />
                </HomeIndicatorWrapper>
            )}
            <NodeWrapper
                urn={urn}
                selected={selected}
                dragging={dragging}
                isGhost={isGhost}
                isSearchedEntity={isSearchedEntity}
            >
                <LineageCard
                    urn={urn}
                    type={type}
                    loading={!entity}
                    onMouseEnter={() => setHoveredNode(urn)}
                    onMouseLeave={() => setHoveredNode(null)}
                    onDoubleClick={
                        isGhost ? undefined : () => history.push(getLineageUrl(urn, type, location, entityRegistry))
                    }
                    name={entity?.name || urn || ''}
                    nameHighlight={{ text: searchQuery, color: highlightColor }}
                    nameExtra={
                        <>
                            {entity?.versionProperties && (
                                <StyledVersioningBadge
                                    showPopover={false}
                                    versionProperties={entity.versionProperties}
                                    size="inherit"
                                />
                            )}
                        </>
                    }
                    extraDetails={extraDetails}
                    properties={properties}
                    platformIcons={platformIcons}
                    childrenOpen={showColumns}
                    childrenText={
                        !!numColumnsTotal && (
                            <ChildrenTextWrapper>
                                Columns
                                <Pill label={`${numColumnsTotal}`} size="xs" />
                            </ChildrenTextWrapper>
                        )
                    }
                    toggleChildren={showHideColumns}
                    menuActions={menuActions}
                    sideElements={
                        <>
                            <HorizontalHandle type="target" position={Position.Left} isConnectable={false} />
                            <HorizontalHandle type="source" position={Position.Right} isConnectable={false} />
                            {hasUpstreamChildren &&
                                ([FetchStatus.UNFETCHED, FetchStatus.LOADING].includes(
                                    fetchStatus[LineageDirection.Upstream],
                                ) ||
                                    isUpstreamHidden) && (
                                    <ExpandLineageButton
                                        urn={urn}
                                        type={type}
                                        direction={LineageDirection.Upstream}
                                        display={
                                            fetchStatus[LineageDirection.Upstream] === FetchStatus.UNFETCHED ||
                                            !isExpandedUpstream
                                        }
                                        fetchStatus={fetchStatus}
                                        ignoreSchemaFieldStatus={ignoreSchemaFieldStatus}
                                        count={numUpstreams}
                                        parentDataJob={parentDataJob}
                                    />
                                )}
                            {hasDownstreamChildren &&
                                ([FetchStatus.UNFETCHED, FetchStatus.LOADING].includes(
                                    fetchStatus[LineageDirection.Downstream],
                                ) ||
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
                                        count={numDownstreams}
                                        parentDataJob={parentDataJob}
                                    />
                                )}
                            {fetchStatus[LineageDirection.Upstream] === FetchStatus.COMPLETE &&
                                isExpandedUpstream &&
                                hasUpstreamChildren && (
                                    <ContractLineageButton urn={urn} direction={LineageDirection.Upstream} />
                                )}
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
                        </>
                    }
                    verticalElements={
                        isVertical && (
                            <>
                                <VerticalHandle
                                    id={VERTICAL_HANDLE}
                                    type="target"
                                    position={Position.Top}
                                    isConnectable={false}
                                />
                                <VerticalHandle
                                    id={VERTICAL_HANDLE}
                                    type="source"
                                    position={Position.Bottom}
                                    isConnectable={false}
                                />
                            </>
                        )
                    }
                />
                <ColumnsWrapper transitionDuration={transitionDuration} height={columnsHeight}>
                    {!!entity && (
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
                            selectedColumn={selectedColumn}
                            setSelectedColumn={setSelectedColumn}
                            hoveredColumn={hoveredColumn}
                            setHoveredColumn={setHoveredColumn}
                        />
                    )}
                    {entity && (
                        <PropertyBadgeWrapper>
                            <StructuredPropertyBadge structuredProperties={entity.structuredProperties} />
                        </PropertyBadgeWrapper>
                    )}
                </ColumnsWrapper>
            </NodeWrapper>
        </>
    );

    if (isGhost) {
        const message = entity?.status?.removed ? 'has been deleted' : 'does not exist in DataHub';
        // Put below context path popover
        return (
            <Tooltip title={`This entity ${message}`} mouseEnterDelay={0.3} overlayStyle={{ zIndex: 10 }}>
                {contents}
            </Tooltip>
        );
    }
    return contents;
}
