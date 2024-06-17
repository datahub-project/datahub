import React, { useContext, useMemo } from 'react';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import { EntityType } from '../../../types.generated';
import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import { ANTD_GRAY, LINEAGE_COLORS } from '../../entityV2/shared/constants';
import { generateSchemaFieldUrn } from '../../entityV2/shared/tabs/Lineage/utils';
import { CompactFieldIconWithTooltip } from '../../sharedV2/icons/CompactFieldIcon';
import OverflowTitle from '../../sharedV2/text/OverflowTitle';
import { createColumnRef, LineageDisplayContext, onClickPreventSelect } from '../common';
import { LineageDisplayColumn } from './useDisplayedColumns';

const ColumnWrapper = styled.div<{
    selected: boolean;
    highlighted: boolean;
    fromSelect?: boolean;
    hasLineage: boolean;
}>`
    border: 1px solid transparent;

    ${({ selected, highlighted, fromSelect }) => {
        if (selected) {
            return `border: ${LINEAGE_COLORS.PURPLE_3} 1px solid; background-color: ${LINEAGE_COLORS.PURPLE_3}20;`;
        }
        if (highlighted) {
            if (fromSelect) {
                return `background-color: ${LINEAGE_COLORS.PURPLE_3}20;`;
            }
            return `background-color: ${LINEAGE_COLORS.BLUE_2}20;`;
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

type Props = LineageDisplayColumn & { urn: string; entityType: EntityType };

export default function Column({ urn, entityType, fieldPath, highlighted, type, nativeDataType }: Props) {
    const { selectedColumn, setSelectedColumn, setHoveredColumn, fineGrainedLineage } =
        useContext(LineageDisplayContext);
    const id = useMemo(() => createColumnRef(urn, fieldPath), [urn, fieldPath]);
    const hasLineage = fineGrainedLineage.downstream.has(id) || fineGrainedLineage.upstream.has(id);

    let columnName = fieldPath;
    try {
        columnName = decodeURI(columnName);
    } catch (e) {
        console.error(`Failed to decode URI for fieldPath: ${fieldPath}`);
    }

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
                        entityUrn: generateSchemaFieldUrn(fieldPath, urn),
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
            <OverflowTitle title={columnName} />
            <CustomHandle id={id} type="source" position={Position.Right} isConnectable={false} />
        </ColumnWrapper>
    );
}
