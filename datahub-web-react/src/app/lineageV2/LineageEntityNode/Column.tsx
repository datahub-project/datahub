import { useGetLineageUrl } from '@app/lineageV2/lineageUtils';
import { useAppConfig } from '@app/useAppConfig';
import LinkOut from '@images/link-out.svg?react';
import { Tooltip, Typography } from 'antd';
import React, { useContext, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import { EntityType } from '../../../types.generated';
import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { generateSchemaFieldUrn } from '../../entityV2/shared/tabs/Lineage/utils';
import { CompactFieldIconWithTooltip } from '../../sharedV2/icons/CompactFieldIcon';
import { createColumnRef, HOVER_COLOR, LineageDisplayContext, onClickPreventSelect, SELECT_COLOR } from '../common';
import { LineageDisplayColumn } from './useDisplayedColumns';

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
    margin-right: 4px;
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

type Props = LineageDisplayColumn & { urn: string; entityType: EntityType };

export default function Column({ urn, entityType, fieldPath, highlighted, hasLineage, type, nativeDataType }: Props) {
    const { config } = useAppConfig();
    const { selectedColumn, setSelectedColumn, setHoveredColumn } = useContext(LineageDisplayContext);
    const id = useMemo(() => createColumnRef(urn, fieldPath), [urn, fieldPath]);

    let columnName = fieldPath;
    try {
        columnName = decodeURI(columnName);
    } catch (e) {
        console.error(`Failed to decode URI for fieldPath: ${fieldPath}`);
    }

    const schemaFieldUrn = generateSchemaFieldUrn(fieldPath, urn) || '';
    const lineageUrl = useGetLineageUrl(schemaFieldUrn, EntityType.SchemaField);

    // TODO: Add hover text if overflowed
    return (
        <ColumnWrapper
            highlighted={highlighted}
            fromSelect={!!selectedColumn}
            selected={id === selectedColumn}
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
            onMouseEnter={() => hasLineage && setHoveredColumn(id)}
            onMouseLeave={() => setHoveredColumn(null)}
        >
            <CustomHandle id={id} type="target" position={Position.Left} isConnectable={false} />
            {type && (
                <TypeWrapper>
                    <CompactFieldIconWithTooltip type={type} nativeDataType={nativeDataType} />
                </TypeWrapper>
            )}
            <ColumnText ellipsis={{ tooltip: { showArrow: false } }}>{columnName}</ColumnText>
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
