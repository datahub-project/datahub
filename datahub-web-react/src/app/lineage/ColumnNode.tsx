import React, { useContext, useState } from 'react';
import styled from 'styled-components/macro';
import { Group } from '@vx/group';
import { SchemaField } from '../../types.generated';
import { downgradeV2FieldPath } from '../entity/dataset/profile/schema/utils/utils';
import { NodeData, VizEdge } from './types';
import { LineageExplorerContext } from './utils/LineageExplorerContext';
import { ANTD_GRAY } from '../entity/shared/constants';
import { centerY, iconX, width } from './constants';
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
    edgesToRender: VizEdge[];
    titleHeight: number;
    onHover: (EntitySelectParams) => void;
}

export default function ColumnNode({ field, index, node, edgesToRender, titleHeight, onHover }: Props) {
    const { highlightedEdges, setHighlightedEdges, selectedField, setSelectedField, fineGrainedMap } =
        useContext(LineageExplorerContext);
    const [showHoverText, setShowHoverText] = useState(false);

    const isFieldSelected = selectedField?.urn === node?.data?.urn && selectedField?.path === field.fieldPath;
    const isFieldHighlighted = highlightedEdges.find(
        (edge) =>
            (edge.sourceUrn === node.data.urn && edge.sourceField === field.fieldPath) ||
            (edge.targetUrn === node.data.urn && edge.targetField === field.fieldPath),
    );

    const fieldEdge = edgesToRender.find(
        (edge) =>
            (edge.source.data.urn === node.data.urn && edge.sourceField === field.fieldPath) ||
            (edge.target.data.urn === node.data.urn && edge.targetField === field.fieldPath),
    );
    const fieldPath = downgradeV2FieldPath(field.fieldPath);
    const isTruncated = fieldPath && fieldPath.length > MAX_NUM_FIELD_CHARACTERS;

    return (
        <Group
            onMouseOver={(e) => {
                if (fieldEdge && (!selectedField || isFieldSelected)) {
                    highlightColumnLineage(field.fieldPath, fineGrainedMap, node.data.urn || '', setHighlightedEdges);
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
                if (fieldEdge) {
                    if (!isFieldSelected) {
                        setSelectedField({
                            urn: node.data.urn as string,
                            path: field.fieldPath,
                        });
                        highlightColumnLineage(
                            field.fieldPath,
                            fineGrainedMap,
                            node.data.urn || '',
                            setHighlightedEdges,
                        );
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
                y={centerY + 60 + titleHeight + index * 30}
                width={width - 2}
                height="29"
                fill={isFieldSelected ? '#e7f3ff' : 'white'}
                stroke={isFieldHighlighted && fieldEdge ? '#1890FF' : 'transparent'}
                strokeWidth="1px"
                ry="4"
                rx="4"
            />
            {showHoverText && (
                <>
                    <rect
                        x={iconX - 21 + HOVER_TEXT_SHIFT}
                        y={centerY + 30 + titleHeight + index * 30}
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
                        y={centerY + 45 + titleHeight + index * 30}
                        fontSize={12}
                        fontFamily="'Roboto Mono',monospace"
                        fill={fieldEdge ? 'black' : ANTD_GRAY[7]}
                    >
                        {fieldPath}
                    </UnselectableText>
                </>
            )}
            <UnselectableText
                dy=".33em"
                x={iconX}
                y={centerY + 75 + titleHeight + index * 30}
                fontSize={12}
                fontFamily="'Roboto Mono',monospace"
                fill={fieldEdge ? 'black' : ANTD_GRAY[7]}
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
