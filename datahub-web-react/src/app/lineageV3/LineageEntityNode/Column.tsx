import { LoadingOutlined } from '@ant-design/icons';
import { Tooltip, colors } from '@components';
import { Spin, Typography } from 'antd';
import React, { Dispatch, SetStateAction, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { generateSchemaFieldUrn } from '@app/entityV2/shared/tabs/Lineage/utils';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { LineageDisplayColumn } from '@app/lineageV3/LineageEntityNode/useDisplayedColumns';
import {
    HOVER_COLOR,
    LineageNodesContext,
    SELECT_COLOR,
    createColumnRef,
    onClickPreventSelect,
    useIgnoreSchemaFieldStatus,
} from '@app/lineageV3/common';
import { ColumnAsset } from '@app/lineageV3/types';
import { useGetLineageUrl } from '@app/lineageV3/utils/lineageUtils';
import { CompactFieldIconWithTooltip } from '@app/sharedV2/icons/CompactFieldIcon';
import { useAppConfig } from '@app/useAppConfig';

import { useGetLineageCountsLazyQuery } from '@graphql/lineage.generated';
import { EntityType } from '@types';

import LinkOut from '@images/link-out.svg?react';

const HOVER_REQUEST_DELAY = 300;

const LinkOutIcon = styled(LinkOut)``;

const ColumnWrapper = styled.div<{
    selected: boolean;
    highlighted: boolean;
    fromSelect?: boolean;
    disabled: boolean;
}>`
    border-radius: 6px;

    ${({ selected, highlighted, fromSelect }) => {
        if (selected) {
            return `border: ${SELECT_COLOR} 1px solid; background-color: ${SELECT_COLOR}20;`;
        }
        if (highlighted) {
            if (fromSelect) {
                return `border: 1px solid ${colors.gray[100]}; background-color: ${SELECT_COLOR}20;`;
            }
            return `border: 1px solid ${colors.gray[100]}; background-color: ${HOVER_COLOR}20;`;
        }
        return `border: 1px solid ${colors.gray[100]};`;
    }}
    color: ${({ disabled }) => (disabled ? colors.gray[1800] : colors.gray[600])};
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
    color: ${colors.gray[1800]};
    width: 11px;
`;

const ColumnLinkWrapper = styled(Link)`
    display: flex;
    margin-left: auto;

    color: ${colors.gray[1800]};

    :hover {
        color: ${(props) => props.theme.styles['primary-color']};
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
    hasLineage,
    type,
    nativeDataType,
    lineageAsset,
    allNeighborsFetched,
    selectedColumn,
    setSelectedColumn,
    hoveredColumn,
    setHoveredColumn,
}: Props) {
    const { config } = useAppConfig();
    const id = useMemo(() => createColumnRef(parentUrn, fieldPath), [parentUrn, fieldPath]);
    const selected = selectedColumn === id;

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
        schemaFieldUrn,
        lineageAsset,
        turnOnDisabledTooltipOnHover,
    );
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
                    <Tooltip title="Explore complete column lineage" mouseEnterDelay={0.3}>
                        <LinkOutIcon />
                    </Tooltip>
                </ColumnLinkWrapper>
            )}
            <CustomHandle id={id} type="source" position={Position.Right} isConnectable={false} />
        </ColumnWrapper>
    );

    return (
        <Tooltip
            title="Column has no lineage"
            open={(showDisabledTooltipOnHover && hoveredColumn === id) || showDisabledTooltipOnSelect}
            placement="right"
            showArrow={false}
        >
            {contents}
        </Tooltip>
    );
}

function useFetchColumnCounts(schemaFieldUrn: string, lineageAsset: ColumnAsset, onDisabled: () => void) {
    const { showGhostEntities, setColumnEdgeVersion } = useContext(LineageNodesContext);
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();

    const assetToWrite = lineageAsset;
    const [fetchCounts, { loading }] = useGetLineageCountsLazyQuery({
        variables: {
            urn: schemaFieldUrn,
            startTimeMillis,
            endTimeMillis,
            separateSiblings: true,
            includeGhostEntities: showGhostEntities || ignoreSchemaFieldStatus,
        },
        onCompleted: (data) => {
            assetToWrite.lineageCountsFetched = true;
            if (data.entity && 'upstream' in data?.entity && data.entity.upstream?.total !== undefined) {
                assetToWrite.numUpstream = (data.entity.upstream.total || 0) - (data.entity.upstream.filtered || 0);
            }
            if (data.entity && 'downstream' in data?.entity && data.entity.downstream?.total !== undefined) {
                assetToWrite.numDownstream =
                    (data.entity.downstream.total || 0) - (data.entity.downstream.filtered || 0);
            }
            if (!assetToWrite.numUpstream && !assetToWrite.numDownstream) {
                onDisabled();
            }
            setColumnEdgeVersion((v) => v + 1);
        },
    });

    const timeoutRef = useRef<NodeJS.Timeout | null>(null);
    const initiateRequest = useCallback(
        (delay = 0) => {
            if (!lineageAsset.lineageCountsFetched && !loading) {
                timeoutRef.current = setTimeout(() => fetchCounts(), delay);
            }
        },
        [lineageAsset.lineageCountsFetched, fetchCounts, loading],
    );
    const cancelRequest = useCallback(() => {
        if (timeoutRef.current) {
            clearTimeout(timeoutRef.current);
        }
    }, []);
    return { initiateRequest, cancelRequest, loading };
}
