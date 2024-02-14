import React, { useCallback, useContext, useEffect, useState } from 'react';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import { KeyboardArrowDown, KeyboardArrowUp } from '@mui/icons-material';
import { Spin } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import { EntityType, LineageDirection } from '../../../types.generated';
import { ContainerIconBase } from '../../entityV2/shared/containers/profile/header/PlatformContent/ContainerIcon';
import { useEntityRegistry } from '../../useEntityRegistry';
import {
    FetchStatus,
    getNodeColor,
    LineageDisplayContext,
    LineageEntity,
    onMouseDownCapturePreventSelect,
} from '../common';
import { NUM_COLUMNS_PER_PAGE } from '../constants';
import Columns from './Columns';
import { ExpandLineageButton } from './ExpandLineageButton';
import useAvoidIntersections from './useAvoidIntersections';
import useDisplayedColumns, {
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    TRANSITION_DURATION_MS,
} from './useDisplayedColumns';
import { ANTD_GRAY, LINEAGE_COLORS } from '../../entityV2/shared/constants';
import getTypeIcon from '../../sharedV2/icons/getTypeIcon';
import { EntityHealth } from '../../entityV2/shared/containers/profile/header/EntityHealth';
import { FetchedEntityV2 } from '../types';

const NodeWrapper = styled.div<{ selected: boolean; expandHeight?: number; color: string }>`
    align-items: center;
    background-color: white;
    border: 1px solid ${({ color, selected }) => (selected ? color : ANTD_GRAY[4.5])};
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    max-height: ${({ expandHeight }) => expandHeight}px;
    overflow-y: hidden;
    transition: max-height ${TRANSITION_DURATION_MS}ms ease-in-out;
    width: ${LINEAGE_NODE_WIDTH}px;
    cursor: pointer;
`;

const CARD_HEIGHT = LINEAGE_NODE_HEIGHT - 2; // Inside border

// Maintains height of node as CardWrapper has position: absolute
// Also allows the expand lineage buttons to not be children of CardWrapper
const FakeCard = styled.div`
    min-height: ${CARD_HEIGHT}px;
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
    font-size: 20px;
    gap: 4px;
`;

const PlatformIcon = styled.img`
    height: 1.2em;
    width: 1.2em;
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

interface Props {
    urn: string;
    type: EntityType;
    selected: boolean;
    entity?: FetchedEntityV2;
}

export default function NodeContents(props: Props & LineageEntity) {
    const { urn, type, selected, entity, fetchStatus } = props;

    const { setHoveredNode } = useContext(LineageDisplayContext);

    const [expanded, setExpanded] = useState(false);
    const [onlyWithLineage, setOnlyWithLineage] = useState(false);
    const showAllColumns = expanded;

    const entityRegistry = useEntityRegistry();

    // In here, not Columns.tsx, to calculate height correctly...
    const [pageIndex, setPageIndex] = useState(0);
    const [filterText, setFilterText] = useState('');

    useEffect(() => {
        setPageIndex(0);
    }, [filterText]);

    const { paginatedColumns, extraHighlightedColumns, numFilteredColumns, numColumnsWithLineage, numColumnsTotal } =
        useDisplayedColumns({
            urn,
            entity,
            showAllColumns,
            filterText,
            pageIndex,
            onlyWithLineage,
        });

    const numDisplayedColumns = extraHighlightedColumns.length + (showAllColumns ? paginatedColumns.length : 0);
    const expandHeight =
        LINEAGE_NODE_HEIGHT +
        (numDisplayedColumns ? 17 : 0) + // Expansion base
        (showAllColumns && numColumnsTotal ? 30 : 0) + // Search bar
        20 * numDisplayedColumns + // Columns
        (showAllColumns && paginatedColumns.length && extraHighlightedColumns.length ? 9 : 0) + // Column divider
        (showAllColumns && numFilteredColumns > NUM_COLUMNS_PER_PAGE ? 38 : 0); // Pagination

    useAvoidIntersections(urn, expandHeight);

    const platformName = entityRegistry.getDisplayName(EntityType.DataPlatform, entity?.platform);
    const [nodeColor] = getNodeColor(type);
    return (
        <NodeWrapper selected={selected} expandHeight={expandHeight} color={nodeColor}>
            <EntityTypeShadow color={nodeColor} />
            <FakeCard>
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
                        <Spin delay={500} indicator={<LoadingOutlined />} />
                    </LoadingWrapper>
                )}
                {fetchStatus[LineageDirection.Downstream] === FetchStatus.LOADING && (
                    <LoadingWrapper className="nodrag" style={{ right: -30 }}>
                        <Spin delay={500} indicator={<LoadingOutlined />} />
                    </LoadingWrapper>
                )}
            </FakeCard>
            <CardWrapper onMouseEnter={() => setHoveredNode(urn)} onMouseLeave={() => setHoveredNode(null)}>
                <CustomHandle type="target" position={Position.Left} isConnectable={false} />
                <>
                    <IconsWrapper>
                        {!!entity?.icon && (
                            <PlatformIcon src={entity.icon} alt={platformName || 'platform'} title={platformName} />
                        )}
                        {!!entity && getTypeIcon(entityRegistry, entity.type, entity.subtype, true)}
                    </IconsWrapper>
                    <VerticalDivider margin={8} />
                </>
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
                <CustomHandle type="source" position={Position.Right} isConnectable={false} />
            </CardWrapper>
            {!!entity && !!numColumnsTotal && (
                <>
                    <HorizontalDivider margin={0} />
                    <Columns
                        entity={entity}
                        showAllColumns={showAllColumns}
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
