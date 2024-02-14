import React, { useContext, useMemo } from 'react';
import { Handle, Position } from 'reactflow';
import styled from 'styled-components';
import { SchemaFieldDataType } from '../../../types.generated';
import { createColumnRef, LineageDisplayContext, onMouseDownCapturePreventSelect } from '../common';
import { CompactFieldIconWithTooltip } from '../../sharedV2/icons/CompactFieldIcon';
import { ANTD_GRAY, LINEAGE_COLORS } from '../../entityV2/shared/constants';

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
    color: ${({ hasLineage }) => (hasLineage ? '#262626' : ANTD_GRAY[7])};
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

interface Props {
    // TODO: Deduplicate with LineageDisplayColumn
    urn: string;
    fieldPath: string;
    highlighted: boolean;
    fromSelect?: boolean;
    type?: SchemaFieldDataType;
    nativeDataType?: string | null;
}

export default function Column({ urn, fieldPath, highlighted, fromSelect, type, nativeDataType }: Props) {
    const { selectedColumn, setSelectedColumn, setHoveredColumn, fineGrainedLineage } =
        useContext(LineageDisplayContext);
    const id = useMemo(() => createColumnRef(urn, fieldPath), [urn, fieldPath]);
    const hasLineage = fineGrainedLineage.forward.has(id) || fineGrainedLineage.backward.has(id);

    // TODO: Add hover text if overflowed
    return (
        <ColumnWrapper
            highlighted={highlighted}
            fromSelect={fromSelect}
            selected={id === selectedColumn}
            hasLineage={hasLineage}
            onMouseDownCapture={(e) => {
                if (hasLineage) {
                    onMouseDownCapturePreventSelect(e);
                }
            }}
            onClick={() => {
                if (hasLineage) {
                    // Toggle if already selected
                    setSelectedColumn((v) => (v === id ? null : id));
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
            {fieldPath}
            <CustomHandle id={id} type="source" position={Position.Right} isConnectable={false} />
        </ColumnWrapper>
    );
}
