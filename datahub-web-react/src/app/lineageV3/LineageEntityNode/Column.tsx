import { LoadingOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Spin, Typography } from 'antd';
import React, { Dispatch, SetStateAction, useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { generateSchemaFieldUrn } from '@app/entityV2/shared/tabs/Lineage/utils';
import useFetchColumnCounts from '@app/lineageV3/LineageEntityNode/Column.hooks';
import { ColumnLineageControl } from '@app/lineageV3/LineageEntityNode/ColumnLineageControl';
import { LineageDisplayColumn, columnHasLineage } from '@app/lineageV3/LineageEntityNode/useDisplayedColumns';
import { createColumnRef, onClickPreventSelect } from '@app/lineageV3/common';
import { useGetLineageUrl } from '@app/lineageV3/utils/lineageUtils';
import { CompactFieldIconWithTooltip } from '@app/sharedV2/icons/CompactFieldIcon';
import { useAppConfig } from '@app/useAppConfig';

import { EntityType, LineageDirection } from '@types';

import LinkOut from '@images/link-out.svg?react';

const HOVER_REQUEST_DELAY = 300;

const LinkOutIcon = styled(LinkOut)``;

// Anchors the column's lineage controls, which render outside the node on either side
const ColumnPositioner = styled.div`
    position: relative;
    width: 100%;
`;

const ColumnWrapper = styled.div<{
    selected: boolean;
    highlighted: boolean;
    fromSelect?: boolean;
    disabled: boolean;
}>`
    border-radius: 6px;

    ${({ selected, highlighted, fromSelect, theme }) => {
        if (selected) {
            return `border: ${theme.colors.borderSelected} 1px solid; background-color: ${theme.colors.bgSelected};`;
        }
        if (highlighted) {
            if (fromSelect) {
                return `border: 1px solid ${theme.colors.border}; background-color: ${theme.colors.bgSelected};`;
            }
            return `border: 1px solid ${theme.colors.border}; background-color: ${theme.colors.bgHover};`;
        }
        return `border: 1px solid ${theme.colors.border};`;
    }}
    color: ${({ disabled, theme }) => (disabled ? theme.colors.textDisabled : theme.colors.text)};
    display: flex;
    align-items: center;
    font-size: 12px;
    gap: 8px;
    padding: 6px 8px;
    position: relative;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    width: 100%;

    ${({ disabled }) =>
        disabled &&
        `
        ${LinkOutIcon} {
            display: none;
        }
    
        :hover {
            ${LinkOutIcon} {
                display: inline;
            }
        }
    `}
`;

const CustomHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    ${({ position }) => (position === Position.Left ? 'left: -11px;' : 'right: -10px;')}
    top: 50%;
`;

const TypeWrapper = styled.div`
    color: ${(props) => props.theme.colors.textDisabled};
    width: 11px;
`;

const ColumnLinkWrapper = styled(Link)`
    display: flex;
    margin-left: auto;

    color: ${(props) => props.theme.colors.textDisabled};

    :hover {
        color: ${(props) => props.theme.colors.textBrand};
    }
`;

const ColumnText = styled(Typography.Text)`
    color: inherit;
`;

const StyledLoadingIndicator = styled(LoadingOutlined)`
    display: flex;
    font-size: inherit;
`;

type Props = LineageDisplayColumn & {
    parentUrn: string;
    entityType: EntityType;
    allNeighborsFetched: boolean;
    mayHideUpstreamLineage: boolean;
    mayHideDownstreamLineage: boolean;
    selectedColumn: string | null;
    setSelectedColumn: Dispatch<SetStateAction<string | null>>;
    hoveredColumn: string | null;
    setHoveredColumn: Dispatch<SetStateAction<string | null>>;
};

export default function Column({
    parentUrn,
    entityType,
    fieldPath,
    highlighted,
    connectedToHomeNode,
    type,
    nativeDataType,
    lineageAsset,
    shownRelated,
    allNeighborsFetched,
    mayHideUpstreamLineage,
    mayHideDownstreamLineage,
    selectedColumn,
    setSelectedColumn,
    hoveredColumn,
    setHoveredColumn,
}: Props) {
    const { t } = useTranslation('lineage');
    const { config } = useAppConfig();
    const id = useMemo(() => createColumnRef(parentUrn, fieldPath), [parentUrn, fieldPath]);
    const selected = selectedColumn === id;
    // Lineage filter nodes cover hidden column lineage themselves, with an edge to the filter node
    const showLineageControls = !!shownRelated && !config.featureFlags.showLineageFilterNodes;
    const showUpstreamControl = showLineageControls && mayHideUpstreamLineage;
    const showDownstreamControl = showLineageControls && mayHideDownstreamLineage;

    let columnName = fieldPath;
    try {
        columnName = decodeURI(columnName);
    } catch (e) {
        console.error(`Failed to decode URI for fieldPath: ${fieldPath}`);
    }

    const schemaFieldUrn = generateSchemaFieldUrn(fieldPath, parentUrn) || '';
    const lineageUrl = useGetLineageUrl(schemaFieldUrn, EntityType.SchemaField);

    const [showDisabledTooltipOnHover, setShowDisabledTooltipOnHover] = useState(false);
    const [showDisabledTooltipOnSelect, setShowDisabledTooltipOnSelect] = useState(false);
    const turnOnDisabledTooltipOnHover = useCallback(() => setShowDisabledTooltipOnHover(true), []);

    const { initiateRequest, cancelRequest, loading } = useFetchColumnCounts(
        parentUrn,
        schemaFieldUrn,
        lineageAsset,
        turnOnDisabledTooltipOnHover,
    );
    // Recomputed here rather than taken from props: counts are written onto `lineageAsset` when the
    // query resolves, which re-renders this component but leaves its props stale
    const hasLineage = columnHasLineage(lineageAsset, connectedToHomeNode);
    const hasFetchedCounts = lineageAsset.numUpstream !== undefined || lineageAsset.numDownstream !== undefined;
    const isFullyFetched = lineageAsset.lineageCountsFetched || allNeighborsFetched;
    const showAsDisabled = !hasLineage && isFullyFetched;

    useEffect(() => {
        // Deselect if we queried lineage counts and found out it has none
        if (id === selectedColumn && isFullyFetched && !hasLineage) {
            setSelectedColumn(null);
            setShowDisabledTooltipOnSelect(true);
            setTimeout(() => setShowDisabledTooltipOnSelect(false), 3000);
        }
    }, [selectedColumn, id, hasLineage, isFullyFetched, setSelectedColumn]);

    // Counts back the controls of every column in the traversal, not just the one under the cursor.
    // `isFullyFetched` is not enough to skip the request: it is set without fetching counts when a
    // node's neighbors are all loaded, which is also true of a node whose lineage is contracted.
    useEffect(() => {
        if ((!showUpstreamControl && !showDownstreamControl) || hasFetchedCounts) {
            cancelRequest(); // No longer interested, e.g. the cursor moved on to another column
        } else {
            initiateRequest(HOVER_REQUEST_DELAY);
        }
    }, [showUpstreamControl, showDownstreamControl, hasFetchedCounts, initiateRequest, cancelRequest]);

    const handleMouseEnter = useCallback(() => {
        if (!selectedColumn && !showAsDisabled) {
            setHoveredColumn(id);
            if (!allNeighborsFetched) {
                initiateRequest(HOVER_REQUEST_DELAY);
            }
        }
    }, [allNeighborsFetched, showAsDisabled, id, selectedColumn, initiateRequest, setHoveredColumn]);

    const handleMouseLeave = useCallback(() => {
        if (!selectedColumn) {
            setShowDisabledTooltipOnHover(false);
            cancelRequest();
        }
    }, [selectedColumn, cancelRequest]);

    // TODO: Add hover text if overflowed
    const contents = (
        <ColumnPositioner>
            <ColumnWrapper
                highlighted={highlighted && !showAsDisabled}
                fromSelect={!!selectedColumn}
                selected={selected}
                disabled={showAsDisabled}
                onClick={(e) => {
                    if (!showAsDisabled) {
                        onClickPreventSelect(e);
                        if (selectedColumn !== id && !allNeighborsFetched) {
                            initiateRequest();
                        }
                        // Toggle if already selected
                        setSelectedColumn((v) => (v === id ? null : id));
                        analytics.event({
                            type: EventType.DrillDownLineageEvent,
                            action: selectedColumn === id ? 'deselect' : 'select',
                            parentUrn,
                            parentEntityType: entityType,
                            entityUrn: schemaFieldUrn,
                            entityType: EntityType.SchemaField,
                            dataType: type,
                        });
                    }
                }}
                onMouseEnter={handleMouseEnter}
                onMouseLeave={handleMouseLeave}
                data-testid={`column-${columnName}`}
            >
                <CustomHandle id={id} type="target" position={Position.Left} isConnectable={false} />
                {type && (
                    <TypeWrapper>
                        <CompactFieldIconWithTooltip type={type} nativeDataType={nativeDataType} />
                    </TypeWrapper>
                )}
                <ColumnText ellipsis={{ tooltip: { showArrow: false } }}>{columnName}</ColumnText>
                {loading && !hasLineage && <Spin indicator={<StyledLoadingIndicator />} />}
                {config.featureFlags.schemaFieldCLLEnabled && (
                    <ColumnLinkWrapper
                        to={lineageUrl}
                        onClick={(e) => e.stopPropagation()}
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        <Tooltip title={t('column.exploreCompleteLineage.tooltip')} mouseEnterDelay={0.3}>
                            <LinkOutIcon />
                        </Tooltip>
                    </ColumnLinkWrapper>
                )}
                <CustomHandle id={id} type="source" position={Position.Right} isConnectable={false} />
            </ColumnWrapper>
            {shownRelated && showUpstreamControl && (
                <ColumnLineageControl
                    direction={LineageDirection.Upstream}
                    lineageAsset={lineageAsset}
                    shownRelated={shownRelated}
                />
            )}
            {shownRelated && showDownstreamControl && (
                <ColumnLineageControl
                    direction={LineageDirection.Downstream}
                    lineageAsset={lineageAsset}
                    shownRelated={shownRelated}
                />
            )}
        </ColumnPositioner>
    );

    return (
        <Tooltip
            title={t('column.noLineage.tooltip')}
            // Only claim a column has no lineage if nothing says otherwise: the counts query can
            // come back empty for a column that has column lineage drawn on the graph
            open={(showDisabledTooltipOnHover && hoveredColumn === id && !hasLineage) || showDisabledTooltipOnSelect}
            placement="right"
            showArrow={false}
        >
            {contents}
        </Tooltip>
    );
}
