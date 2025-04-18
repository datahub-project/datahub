import React, { useContext, useState } from 'react';
import styled from 'styled-components/macro';
import { Group } from '@visx/group';
import { SchemaField } from '../../types.generated';
import { downgradeV2FieldPath } from '../entity/dataset/profile/schema/utils/utils';
import { NodeData } from './types';
import { LineageExplorerContext } from './utils/LineageExplorerContext';
import { ANTD_GRAY } from '../entity/shared/constants';
import { centerY, COLUMN_HEIGHT, EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT, iconX, width } from './constants';
import { truncate } from '../entity/shared/utils';
import { highlightColumnLineage } from './utils/highlightColumnLineage';

const MAX_NUM_FIELD_CHARACTERS = 25;
const HOVER_TEXT_SHIFT = 10;

const UnselectableText = styled.text`
    user-select: none;
`;

interface Props {
    field: SchemaField;
    index: number;
    node: { x: number; y: number; data: Omit<NodeData, 'children'> };
    titleHeight: number;
    onHover: (EntitySelectParams) => void;
}

export default function ColumnNode({ field, index, node, titleHeight, onHover }: Props) {
    const { highlightedEdges, setHighlightedEdges, selectedField, setSelectedField, fineGrainedMap } =
        useContext(LineageExplorerContext);
    const [showHoverText, setShowHoverText] = useState(false);

    const nodeUrn = node.data.urn;
    const isFieldSelected = selectedField?.urn === nodeUrn && selectedField?.path === field.fieldPath;
    const isFieldHighlighted = highlightedEdges.find(
        (edge) =>
            (edge.sourceUrn === nodeUrn && edge.sourceField === field.fieldPath) ||
            (edge.targetUrn === nodeUrn && edge.targetField === field.fieldPath),
    );

    const hasEdge =
        nodeUrn &&
        ((fineGrainedMap.forward[nodeUrn] && fineGrainedMap.forward[nodeUrn][field.fieldPath]) ||
            (fineGrainedMap.reverse[nodeUrn] && fineGrainedMap.reverse[nodeUrn][field.fieldPath]));
    const fieldPath = downgradeV2FieldPath(field.fieldPath);
    const isTruncated = fieldPath && fieldPath.length > MAX_NUM_FIELD_CHARACTERS;
    const titleAndToggleHeight = titleHeight + EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT;

    return (
        <Group
            onMouseOver={(e) => {
                if (hasEdge && (!selectedField || isFieldSelected)) {
                    highlightColumnLineage(field.fieldPath, fineGrainedMap, nodeUrn || '', setHighlightedEdges);
                    onHover(undefined);
                    e.stopPropagation();
                }
            }}
            onMouseOut={() => {
                if (!selectedField) {
                    setHighlightedEdges([]);
                }
            }}
            onClick={(e) => {
                if (hasEdge) {
                    if (!isFieldSelected) {
                        setSelectedField({
                            urn: nodeUrn as string,
                            path: field.fieldPath,
                        });
                        highlightColumnLineage(field.fieldPath, fineGrainedMap, nodeUrn || '', setHighlightedEdges);
                    } else {
                        setSelectedField(null);
                    }
                    e.stopPropagation();
                } else {
                    setSelectedField(null);
                }
            }}
        >
            <rect
                x={iconX - 21}
                y={centerY + 60 + titleAndToggleHeight + index * COLUMN_HEIGHT}
                width={width - 2}
                height="29"
                fill={isFieldSelected ? '#e7f3ff' : 'white'}
                stroke={isFieldHighlighted && hasEdge ? '#1890FF' : 'transparent'}
                strokeWidth="1px"
                ry="4"
                rx="4"
            />
            {showHoverText && (
                <>
                    <rect
                        x={iconX - 21 + HOVER_TEXT_SHIFT}
                        y={centerY + 30 + titleAndToggleHeight + index * COLUMN_HEIGHT}
                        width={width + (fieldPath?.substring(MAX_NUM_FIELD_CHARACTERS).length || 0) * 7}
                        height="29"
                        fill="white"
                        stroke={ANTD_GRAY[8]}
                        strokeWidth="1px"
                        filter="drop-shadow( 0 0 5px rgba(0, 0, 0, .12))"
                        ry="4"
                        rx="4"
                    />
                    <UnselectableText
                        dy=".33em"
                        x={iconX + HOVER_TEXT_SHIFT}
                        y={centerY + 45 + titleAndToggleHeight + index * COLUMN_HEIGHT}
                        fontSize={12}
                        fontFamily="'Roboto Mono',monospace"
                        fill={hasEdge ? 'black' : ANTD_GRAY[7]}
                    >
                        {fieldPath}
                    </UnselectableText>
                </>
            )}
            <UnselectableText
                dy=".33em"
                x={iconX}
                y={centerY + 75 + titleAndToggleHeight + index * COLUMN_HEIGHT}
                fontSize={12}
                fontFamily="'Roboto Mono',monospace"
                fill={hasEdge ? 'black' : ANTD_GRAY[7]}
                onMouseEnter={() => {
                    if (isTruncated) setShowHoverText(true);
                }}
                onMouseLeave={() => setShowHoverText(false)}
            >
                {truncate(MAX_NUM_FIELD_CHARACTERS, fieldPath)}
            </UnselectableText>
        </Group>
    );
}
