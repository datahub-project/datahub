import { Badge, Tooltip } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import Highlight from 'react-highlighter';
import styled, { useTheme } from 'styled-components';

import StructuredPropertyTooltip from '@app/entityV2/shared/tabs/Properties/StructuredPropertyTooltip';
import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';

const MIN_CONTENT = 'min-content' as const;

const INDENT_PER_DEPTH = 16;
const CARET_SLOT_WIDTH = 16;

const NameContainer = styled.div`
    display: flex;
    align-items: center;
    min-width: 0;
`;

const CaretSlot = styled.span<{ clickable?: boolean }>`
    flex-shrink: 0;
    width: ${CARET_SLOT_WIDTH}px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    cursor: ${(props) => (props.clickable ? 'pointer' : 'default')};
    color: ${(props) => props.theme.colors.textSecondary};
`;

// Truncation/layout only — font size, weight, family and color are inherited from the alchemy
// Table cell (the name column is the table's first cell, which the Table styles on its own).
const NameText = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
`;

const NameLabelWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
`;

interface Props {
    propertyRow: PropertyRow;
    filterText?: string;
    isExpanded?: boolean;
    onToggleExpand?: () => void;
}

export default function NameColumn({ propertyRow, filterText, isExpanded, onToggleExpand }: Props) {
    const theme = useTheme();
    const { structuredProperty } = propertyRow;
    const hasChildren = !!propertyRow.children;
    const indent = (propertyRow.depth || 0) * INDENT_PER_DEPTH;

    return (
        <NameContainer style={{ paddingLeft: indent }}>
            <CaretSlot
                clickable={hasChildren}
                onClick={(e) => {
                    if (!hasChildren) return;
                    e.stopPropagation();
                    onToggleExpand?.();
                }}
                data-testid={hasChildren ? `property-caret-${propertyRow.qualifiedName}` : undefined}
            >
                {hasChildren &&
                    (isExpanded ? <CaretDown size={12} weight="bold" /> : <CaretRight size={12} weight="bold" />)}
            </CaretSlot>
            {hasChildren ? (
                <NameLabelWrapper>
                    <NameText>
                        <Highlight search={filterText}>{propertyRow.displayName}</Highlight>
                    </NameText>
                    {propertyRow.childrenCount ? <Badge count={propertyRow.childrenCount} size="sm" /> : null}
                </NameLabelWrapper>
            ) : (
                <NameLabelWrapper>
                    <Tooltip
                        color={theme.colors.bgTooltip}
                        placement="topRight"
                        overlayStyle={{ minWidth: MIN_CONTENT }}
                        title={structuredProperty ? <StructuredPropertyTooltip propertyRow={propertyRow} /> : ''}
                    >
                        <NameText>
                            <Highlight search={filterText}>{propertyRow.displayName}</Highlight>
                        </NameText>
                    </Tooltip>
                </NameLabelWrapper>
            )}
        </NameContainer>
    );
}
