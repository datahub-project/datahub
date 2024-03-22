import React, { Dispatch, SetStateAction, useCallback, useEffect, useState } from 'react';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import { KeyboardArrowDown, KeyboardArrowUp } from '@mui/icons-material';
import { Skeleton, Spin } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import { EntityType, LineageDirection } from '../../../types.generated';
import { ContainerIconBase } from '../../entityV2/shared/containers/profile/header/PlatformContent/ContainerIcon';
import { useEntityRegistry } from '../../useEntityRegistry';
import { FetchStatus, getNodeColor, LineageEntity, onMouseDownCapturePreventSelect } from '../common';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import Columns from './Columns';
import { ExpandLineageButton } from './ExpandLineageButton';
import NodeSkeleton from './NodeSkeleton';
import useAvoidIntersections from './useAvoidIntersections';
import { DisplayedColumns, LINEAGE_NODE_HEIGHT, LINEAGE_NODE_WIDTH } from './useDisplayedColumns';
import { ANTD_GRAY, LINEAGE_COLORS } from '../../entityV2/shared/constants';
import getTypeIcon from '../../sharedV2/icons/getTypeIcon';
import { EntityHealth } from '../../entityV2/shared/containers/profile/header/EntityHealth';
import { FetchedEntityV2 } from '../types';

const NodeWrapper = styled.div<{
    selected: boolean;
    expandHeight?: number;
    color: string;
    $transitionDuration: number;
}>`
    align-items: center;
    background-color: white;
    border: 1px solid ${({ color, selected }) => (selected ? color : ANTD_GRAY[4.5])};
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    max-height: ${({ expandHeight }) => expandHeight}px;
    overflow-y: hidden;
    transition: max-height ${({ $transitionDuration }) => $transitionDuration}ms ease-in-out;
    width: ${LINEAGE_NODE_WIDTH}px;
    cursor: pointer;
`;

const CARD_HEIGHT = LINEAGE_NODE_HEIGHT - 2; // Inside border

// Maintains height of node as CardWrapper has position: absolute
// Also allows the expand lineage buttons to not be children of CardWrapper
const FakeCard = styled.div`
    min-height: ${CARD_HEIGHT}px;
    max-height: ${CARD_HEIGHT}px;
    width: 100%;
`;

const CardWrapper = styled.div`
    align-items: center;
    display: flex;
    flex-direction: row;
    height: ${CARD_HEIGHT}px;
    position: absolute;
    padding: 8px 11px;
    width: 100%;
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

const LoadingWrapper = styled.div`
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
    gap: 4px;
    height: 100%;
    min-width: 0;
`;

const TitleWrapper = styled.div`
    display: flex;
    justify-content: space-between;
`;

const Title = styled(OverflowTitle)`
    font-weight: 600;
    font-size: 12px;
    line-height: 125%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const StyledEntityHealth = styled(EntityHealth)`
    margin-left: 4px;
`;

const ExpandColumnsWrapper = styled.div`
    align-items: center;
    border: 0.5px solid ${LINEAGE_COLORS.BLUE_1}50;
    border-radius: 10px;
    color: ${LINEAGE_COLORS.BLUE_1};
    display: flex;
    flex-grow: 1;
    font-size: 10px;
    justify-content: center;
    width: 100%;

    :hover {
        background-color: ${LINEAGE_COLORS.BLUE_1}20;
        cursor: pointer;
    }
`;

const SkeletonImage = styled(Skeleton.Avatar)`
    line-height: 0;
`;

interface Props {
    urn: string;
    type: EntityType;
    selected: boolean;
    entity?: FetchedEntityV2;
    transitionDuration: number;
    rootUrn: string;
    setHoveredNode: (urn: string | null) => void;
    expanded: boolean;
    setExpanded: Dispatch<SetStateAction<boolean>>;
    onlyWithLineage: boolean;
    setOnlyWithLineage: Dispatch<SetStateAction<boolean>>;
    filterText: string;
    setFilterText: Dispatch<SetStateAction<string>>;
    pageIndex: number;
    setPageIndex: Dispatch<SetStateAction<number>>;
}

export default React.memo(NodeContents);

function NodeContents(props: Props & LineageEntity & DisplayedColumns) {
    const {
        urn,
        type,
        selected,
        entity,
        fetchStatus,
        transitionDuration,
        rootUrn,
        setHoveredNode,
        expanded,
        setExpanded,
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
    } = props;

    const entityRegistry = useEntityRegistry();

    useEffect(() => {
        setPageIndex(0);
    }, [filterText, setPageIndex]);

    const numDisplayedColumns = extraHighlightedColumns.length + (expanded ? paginatedColumns.length : 0);
    const expandHeight =
        LINEAGE_NODE_HEIGHT +
        (numDisplayedColumns ? 17 : 0) + // Expansion base
        (expanded && numColumnsTotal ? 30 : 0) + // Search bar
        20 * numDisplayedColumns + // Columns
        (expanded && paginatedColumns.length && extraHighlightedColumns.length ? 9 : 0) + // Column divider
        (expanded && numFilteredColumns > NUM_COLUMNS_PER_PAGE ? 38 : 0); // Pagination

    useAvoidIntersections(urn, expandHeight);

    const platformName = entityRegistry.getDisplayName(EntityType.DataPlatform, entity?.platform);
    const [nodeColor] = getNodeColor(type);
    return (
        <NodeWrapper
            selected={selected}
            expandHeight={expandHeight}
            color={nodeColor}
            $transitionDuration={transitionDuration}
        >
            <EntityTypeShadow color={nodeColor} />
            <FakeCard />
            <FakeCard style={{ position: 'absolute' }}>
                {!!entity?.upstreamChildren?.length &&
                    [FetchStatus.UNFETCHED, FetchStatus.LOADING].includes(fetchStatus[LineageDirection.Upstream]) && (
                        <ExpandLineageButton urn={urn} direction={LineageDirection.Upstream} />
                    )}
                {!!entity?.downstreamChildren?.length &&
                    [FetchStatus.UNFETCHED, FetchStatus.LOADING].includes(fetchStatus[LineageDirection.Downstream]) && (
                        <ExpandLineageButton urn={urn} direction={LineageDirection.Downstream} />
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
                <CustomHandle type="target" position={Position.Left} isConnectable={false} />
                <CustomHandle type="source" position={Position.Right} isConnectable={false} />
                <IconsWrapper>
                    {entity?.icon ? (
                        <PlatformIcon src={entity.icon} alt={platformName || 'platform'} title={platformName} />
                    ) : (
                        <SkeletonImage size="small" shape="square" style={{ borderRadius: '20%' }} />
                    )}
                    {entity ? (
                        getTypeIcon(entityRegistry, entity.type, entity.subtype, true)
                    ) : (
                        <SkeletonImage size="small" shape="square" style={{ borderRadius: '20%' }} />
                    )}
                </IconsWrapper>
                <VerticalDivider margin={8} />
                {entity && (
                    <MainTextWrapper>
                        <TitleWrapper>
                            <Title title={entity?.name} />
                            {entity?.health && (
                                <StyledEntityHealth
                                    health={entity.health}
                                    baseUrl={entityRegistry.getEntityUrl(type, urn)}
                                    fontSize={10}
                                />
                            )}
                        </TitleWrapper>
                        <ContainerPath parentContainers={entity?.parentContainers} />
                        {!!numColumnsTotal && (
                            <>
                                <ExpandColumnsWrapper
                                    onMouseDownCapture={onMouseDownCapturePreventSelect}
                                    onClick={() => setExpanded((v) => !v)}
                                >
                                    {numColumnsTotal} columns
                                    {expanded && <KeyboardArrowUp fontSize="inherit" style={{ marginLeft: 3 }} />}
                                    {!expanded && <KeyboardArrowDown fontSize="inherit" style={{ marginLeft: 3 }} />}
                                </ExpandColumnsWrapper>
                            </>
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
                        showAllColumns={expanded}
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
}

const ContainerPathWrapper = styled.div`
    display: flex;
    height: 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    width: 100%;
`;

// TODO: Put ellipsis on last item correctly
const ContainerEntry = styled.div<{ numItems?: number; isLast: boolean }>`
    align-items: center;
    color: ${ANTD_GRAY[9]};
    display: flex;
    flex-direction: row;
    font-size: 12px;
    max-width: ${({ numItems, isLast }) => (numItems && !isLast ? 100 / numItems : 100)}%;
`;

const ContainerText = styled.span`
    font-size: 8px;
    margin-left: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

function ContainerPath({ parentContainers }: Pick<FetchedEntityV2, 'parentContainers'>) {
    const entityRegistry = useEntityRegistry();
    const containers = parentContainers?.slice(0, 1);

    if (!containers?.length) {
        return null;
    }

    return (
        <ContainerPathWrapper>
            {containers?.map((container, i) => (
                <ContainerEntry key={container.urn} isLast={i === containers.length - 1} numItems={containers.length}>
                    {i > 0 && <VerticalDivider margin={4} />}
                    <ContainerIconBase container={container} />
                    <ContainerText>{entityRegistry.getDisplayName(EntityType.Container, container)}</ContainerText>
                </ContainerEntry>
            ))}
        </ContainerPathWrapper>
    );
}

export function OverflowTitle({ title, className }: { title?: string; className?: string }) {
    const [showTitle, setShowTitle] = useState(false);

    const ref = useCallback(
        (node) => {
            if (node && title) {
                const resizeObserver = new ResizeObserver(() => {
                    setShowTitle(node.scrollWidth > node.clientWidth);
                });
                resizeObserver.observe(node);
            }
        },
        [title],
    );

    return (
        <span ref={ref} className={className} title={showTitle ? title : undefined}>
            {title}
        </span>
    );
}
