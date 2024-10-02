import { LoadingOutlined } from '@ant-design/icons';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { useGetLineageUrl } from '@app/lineageV2/lineageUtils';
import { useAppConfig } from '@app/useAppConfig';
import { useGetLineageCountsLazyQuery } from '@graphql/lineage.generated';
import LinkOut from '@images/link-out.svg?react';
import { Spin, Tooltip, Typography } from 'antd';
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import { EntityType } from '../../../types.generated';
import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { generateSchemaFieldUrn } from '../../entityV2/shared/tabs/Lineage/utils';
import { CompactFieldIconWithTooltip } from '../../sharedV2/icons/CompactFieldIcon';
import {
    createColumnRef,
    HOVER_COLOR,
    LineageDisplayContext,
    LineageNodesContext,
    onClickPreventSelect,
    SELECT_COLOR,
    useIgnoreSchemaFieldStatus,
} from '../common';
import { LineageDisplayColumn } from './useDisplayedColumns';

const CLICK_REQUEST_DELAY = 0;
const HOVER_REQUEST_DELAY = 300;
const LOADING_INDICATOR_DELAY = 300;

const LinkOutIcon = styled(LinkOut)``;

const ColumnWrapper = styled.div<{
    selected: boolean;
    highlighted: boolean;
    fromSelect?: boolean;
    hasLineage: boolean;
}>`
    border: 1px solid transparent;

    ${({ selected, highlighted, fromSelect }) => {
        if (selected) {
            return `border: ${SELECT_COLOR} 1px solid; background-color: ${SELECT_COLOR}20;`;
        }
        if (highlighted) {
            if (fromSelect) {
                return `background-color: ${SELECT_COLOR}20;`;
            }
            return `background-color: ${HOVER_COLOR}20;`;
        }
        return 'background-color: white;';
    }}
    border-radius: 4px;
    color: ${({ hasLineage }) => (hasLineage ? ANTD_GRAY[11] : ANTD_GRAY[7])};
    display: flex;
    font-size: 10px;
    gap: 4px;
    max-height: 20.5px;
    padding: 3px;
    position: relative;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    width: 100%;

    ${LinkOutIcon} {
        display: none;
    }

    :hover {
        ${LinkOutIcon} {
            display: inline;
        }
    }
`;

const CustomHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    ${({ position }) => (position === Position.Left ? 'left: -15px;' : 'right: -12px;')}
    top: 50%;
`;

const TypeWrapper = styled.div`
    color: ${ANTD_GRAY[7]};
    width: 11px;
`;

const ColumnLinkWrapper = styled(Link)`
    display: flex;
    margin-left: auto;

    color: inherit;

    :hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

const ColumnText = styled(Typography.Text)`
    color: inherit;
`;

const StyledLoadingIndicator = styled(LoadingOutlined)`
    font-size: inherit;
`;

type Props = LineageDisplayColumn & { urn: string; entityType: EntityType };

export default function Column({ urn, entityType, fieldPath, highlighted, hasLineage, type, nativeDataType }: Props) {
    const { config } = useAppConfig();
    const { selectedColumn, setSelectedColumn, setHoveredColumn } = useContext(LineageDisplayContext);
    const id = useMemo(() => createColumnRef(urn, fieldPath), [urn, fieldPath]);
    const selected = selectedColumn === id;

    let columnName = fieldPath;
    try {
        columnName = decodeURI(columnName);
    } catch (e) {
        console.error(`Failed to decode URI for fieldPath: ${fieldPath}`);
    }

    const schemaFieldUrn = generateSchemaFieldUrn(fieldPath, urn) || '';
    const lineageUrl = useGetLineageUrl(schemaFieldUrn, EntityType.SchemaField);

    const { initiateRequest, cancelRequest, loading } = useFetchColumnCounts(urn, schemaFieldUrn, fieldPath);
    const handleMouseEnter = useCallback(() => {
        if (hasLineage) {
            setHoveredColumn(id);
            initiateRequest(HOVER_REQUEST_DELAY);
        }
    }, [hasLineage, id, initiateRequest, setHoveredColumn]);
    const handleMouseLeave = useCallback(() => {
        setHoveredColumn(null);
        cancelRequest();
    }, [cancelRequest, setHoveredColumn]);

    useEffect(() => {
        if (selected) {
            initiateRequest(CLICK_REQUEST_DELAY);
        }
        return () => cancelRequest();
    }, [selected, initiateRequest, cancelRequest]);

    // TODO: Add hover text if overflowed
    return (
        <ColumnWrapper
            highlighted={highlighted}
            fromSelect={!!selectedColumn}
            selected={selected}
            hasLineage={hasLineage}
            onClick={(e) => {
                if (hasLineage) {
                    onClickPreventSelect(e);
                    // Toggle if already selected
                    setSelectedColumn((v) => (v === id ? null : id));
                    analytics.event({
                        type: EventType.DrillDownLineageEvent,
                        action: selectedColumn === id ? 'deselect' : 'select',
                        parentUrn: urn,
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
            {selected && loading && <Spin delay={LOADING_INDICATOR_DELAY} indicator={<StyledLoadingIndicator />} />}
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
}

function useFetchColumnCounts(entityUrn: string, schemaFieldUrn: string, fieldPath: string) {
    const { nodes, showGhostEntities, setColumnEdgeVersion } = useContext(LineageNodesContext);
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const [fetched, setFetched] = useState(false);

    const [fetchCounts, { loading }] = useGetLineageCountsLazyQuery({
        variables: {
            urn: schemaFieldUrn,
            startTimeMillis,
            endTimeMillis,
            separateSiblings: true,
            includeGhostEntities: showGhostEntities || ignoreSchemaFieldStatus,
        },
        onCompleted: (data) => {
            setFetched(true);
            const node = nodes.get(entityUrn);
            const lineageAsset = node?.entity?.lineageAssets?.get(fieldPath);
            if (!lineageAsset) return;

            if (data.entity && 'upstream' in data?.entity && data.entity.upstream?.total) {
                lineageAsset.numUpstream = data.entity.upstream.total - (data.entity.upstream.filtered || 0);
            }
            if (data.entity && 'downstream' in data?.entity && data.entity.downstream?.total) {
                lineageAsset.numDownstream = data.entity.downstream.total - (data.entity.downstream.filtered || 0);
            }
            setColumnEdgeVersion((v) => v + 1);
        },
    });

    const timeoutRef = useRef<NodeJS.Timeout | null>(null);
    const initiateRequest = useCallback(
        (delay: number) => {
            if (!fetched && !loading) {
                timeoutRef.current = setTimeout(() => fetchCounts(), delay);
            }
        },
        [fetchCounts, fetched, loading],
    );
    const cancelRequest = useCallback(() => {
        if (timeoutRef.current) {
            clearTimeout(timeoutRef.current);
        }
    }, []);
    return { initiateRequest, cancelRequest, loading };
}
